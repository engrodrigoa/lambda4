"""Read the lakehouse from DuckDB with the SAME table names Spark uses (rfb_bronze.estabelecimentos, ctrl.download_ref).

    from pipelines.commons.lake import duckdb_lake
    con = duckdb_lake()                      # every Delta table registered in the Hive metastore, as a view
    con.sql("SELECT count(*) FROM rfb_bronze.socios WHERE _ref_month = 202609").show()

How: the Hive metastore (Postgres `hive_metastore`) lists each table and its real location. Delta tables are stored
there in Spark's own format, so the column SDS.LOCATION holds a placeholder and the real path is the serde parameter
`path`. Each table becomes a DuckDB view over `delta_scan('s3://...')` (the Delta log is honoured: no stale files).
Credentials come from $SPARK_CONF_DIR/spark-defaults.conf (mode 600), the single source the pipelines already use.

DuckDB is for reading and exploring (fast, single process, no JVM). Writing stays in Spark (one writer per table).
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

from pipelines.commons.settings import get_settings

METASTORE_SQL = """
SELECT d."NAME" AS db, t."TBL_NAME" AS tbl,
       (SELECT p."PARAM_VALUE" FROM "SERDE_PARAMS" p
         WHERE p."SERDE_ID" = s."SERDE_ID" AND p."PARAM_KEY" = 'path') AS path,
       (SELECT tp."PARAM_VALUE" FROM "TABLE_PARAMS" tp
         WHERE tp."TBL_ID" = t."TBL_ID" AND tp."PARAM_KEY" = 'spark.sql.sources.provider') AS provider
FROM "TBLS" t JOIN "DBS" d USING ("DB_ID") JOIN "SDS" s USING ("SD_ID")
ORDER BY 1, 2
"""


@dataclass(frozen=True)
class LakeTable:
    database: str
    table: str
    location: str        # s3://bucket/prefix (s3a:// normalised for DuckDB)

    @property
    def name(self) -> str:
        return f"{self.database}.{self.table}"


def spark_defaults(path: Path | None = None) -> dict[str, str]:
    path = path or Path(get_settings().spark_conf_dir) / "spark-defaults.conf"
    conf: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        key, _, value = line.partition(" ")
        conf[key.strip()] = value.strip()
    return conf


def delta_tables(conf: dict[str, str] | None = None) -> list[LakeTable]:
    """Delta tables registered in the Hive metastore."""
    import psycopg

    conf = conf or spark_defaults()
    url = urlparse(conf["spark.hadoop.javax.jdo.option.ConnectionURL"].removeprefix("jdbc:"))
    with psycopg.connect(host=url.hostname, port=url.port or 5432, dbname=url.path.lstrip("/"),
                         user=conf["spark.hadoop.javax.jdo.option.ConnectionUserName"],
                         password=conf["spark.hadoop.javax.jdo.option.ConnectionPassword"], connect_timeout=5) as pg:
        rows = pg.execute(METASTORE_SQL).fetchall()
    return [LakeTable(db, tbl, path.replace("s3a://", "s3://", 1))
            for db, tbl, path, provider in rows if provider == "delta" and path]


def duckdb_lake(databases: list[str] | None = None, *, threads: int | None = None, memory_limit: str = "8GB"):
    """DuckDB connection with S3 (SeaweedFS) access and one view per Delta table (optionally only some databases)."""
    import duckdb

    conf = spark_defaults()
    endpoint = urlparse(conf["spark.hadoop.fs.s3a.endpoint"])
    con = duckdb.connect()
    con.execute(f"SET memory_limit = '{memory_limit}'")
    if threads:
        con.execute(f"SET threads = {int(threads)}")
    con.execute("INSTALL delta; LOAD delta;")
    con.execute("CREATE OR REPLACE SECRET lake (TYPE S3, KEY_ID ?, SECRET ?, ENDPOINT ?, USE_SSL ?, "
                "URL_STYLE 'path', REGION ?)",
                [conf["spark.hadoop.fs.s3a.access.key"], conf["spark.hadoop.fs.s3a.secret.key"],
                 endpoint.netloc, endpoint.scheme == "https",
                 conf.get("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")])
    for t in delta_tables(conf):
        if databases and t.database not in databases:
            continue
        con.execute(f'CREATE SCHEMA IF NOT EXISTS "{t.database}"')
        con.execute(f'CREATE OR REPLACE VIEW "{t.database}"."{t.table}" AS '
                    f"SELECT * FROM delta_scan('{t.location}')")
    return con


def list_views(con) -> list[str]:
    return [f"{s}.{n}" for s, n in con.execute(
        "SELECT schema_name, view_name FROM duckdb_views() WHERE NOT internal ORDER BY 1, 2").fetchall()]
