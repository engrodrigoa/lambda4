# ADR 0002 — Pin Spark to 4.1.0 (the version Delta Lake is built against)

**Status:** accepted (2026-10-20)

## Context

The lab ran Spark 4.1.3 with Delta Lake 4.1.0. SQL-level Delta (CREATE, INSERT, MERGE, DESCRIBE HISTORY) worked, but
the first real bronze load hit two binary incompatibilities inside the JVM:

- `delta.tables.DeltaTable` (Python and Scala API) could not load: `NoClassDefFoundError:
  org/apache/spark/sql/catalyst/plans/logical/IgnoreCachedData` — the class does not exist in Spark 4.1.3.
- `DELETE` on a table with data files failed: `NoSuchMethodError: ParquetToSparkSchemaConverter.<init>(7 booleans)`
  — the constructor signature changed in a 4.1.x patch release.

Delta Lake links against Spark *internal* (non-public) classes, so it is only guaranteed for the exact Spark version it
was compiled with. The Maven poms show that **every** `delta-spark_4.1_2.13` release (4.1.0, 4.2.0, 4.3.x, 4.4.x)
declares `spark-sql 4.1.0`. Upgrading Delta does not help; aligning Spark does.

## Decision

- Install Spark **4.1.0** (`spark-4.1.0-bin-hadoop3.tgz` from archive.apache.org, SHA-512 verified) in
  `/opt/spark-4.1.0`, repoint `/opt/spark` to it, and pin `pyspark==4.1.0` in `~/.envs/env-data`.
- Keep `/opt/spark-4.1.3` untouched for rollback (`ln -sfn /opt/spark-4.1.3 /opt/spark` + `pyspark==4.1.3`).
- Extra jars carried over unchanged (same Hadoop 3.4.2): delta-spark, delta-storage, hadoop-aws, AWS SDK bundle,
  iceberg-spark-runtime + aws bundle, postgresql JDBC. Configuration stays in `/etc/spark` (independent of install).

## Consequences

- Verified after the switch: DeltaTable API (history, update), DELETE with data files, and the RFB bronze loads.
- Rule for the future: **never upgrade Spark or Delta alone**. Before any change, read the target Delta artifact's pom
  (`spark-sql` version) and move both together — the same reason Databricks ships them pinned in each runtime.
- Unchanged by this ADR: the Delta SQL extension still breaks `spark.sql(..., args=...)` parameter binding (also on
  4.1.0); pipelines use the DataFrame API for filtered reads.
