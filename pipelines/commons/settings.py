"""Single source of configuration for every pipeline.

Precedence: process environment > env file (LAMBDA_ENV_FILE, default ~/.config/lambda/lambda.env) > defaults below.
The env file is optional and lives outside git (it may hold secrets). Every value is a plain env var, so Airflow can
override any of them per task.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path

DEFAULT_ENV_FILE = Path.home() / ".config" / "lambda" / "lambda.env"


def _load_env_file(path: Path) -> dict[str, str]:
    """Minimal KEY=VALUE parser (comments, blank lines, optional quotes). No python-dotenv dependency."""
    values: dict[str, str] = {}
    if not path.is_file():
        return values
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        values[key.strip().removeprefix("export ").strip()] = value.strip().strip('"').strip("'")
    return values


def _flag(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Settings:
    env_file: Path
    # storage
    landing_uri: str
    scratch_dir: Path
    log_dir: Path
    # S3 (SeaweedFS) buckets; lake_root is the URI prefix of every table (tests point it to a local folder)
    lake_root: str
    s3_endpoint: str
    bucket_bronze: str
    bucket_silver: str
    bucket_gold: str
    bucket_ctrl: str
    bucket_audit: str
    # audit (Postgres, peer auth over the local socket: no password)
    audit_enabled: bool
    audit_dsn: str
    audit_log_min_level: str
    audit_spool_dir: Path
    # metrics
    metrics_enabled: bool
    pushgateway_url: str
    # traces (OpenTelemetry SDK -> OTLP/HTTP -> Alloy -> Tempo)
    otel_enabled: bool
    otel_endpoint: str
    # spark
    spark_home: str
    spark_conf_dir: str
    pyspark_python: str
    ctrl_spark_master: str
    extra: dict[str, str] = field(default_factory=dict, repr=False)

    def lake_uri(self, bucket: str, *parts: str) -> str:
        """s3a://<bucket>/<parts...> in production; file:///tmp/.../<bucket>/... in tests."""
        root = self.lake_root if self.lake_root.endswith("/") else self.lake_root + "/"
        return root + "/".join([bucket.strip("/"), *[p.strip("/") for p in parts if p]])

    def get(self, key: str, default: str | None = None) -> str | None:
        """Any other variable (pipeline specific, e.g. RFB_SHARE_URL) with the same precedence rules."""
        return os.environ.get(key, self.extra.get(key, default))


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    env_file = Path(os.environ.get("LAMBDA_ENV_FILE", str(DEFAULT_ENV_FILE)))
    file_values = _load_env_file(env_file)

    def get(key: str, default: str) -> str:
        return os.environ.get(key, file_values.get(key, default))

    scratch = Path(get("LAMBDA_SCRATCH_DIR", "/srv/scratch"))
    return Settings(
        env_file=env_file,
        landing_uri=get("LANDING_URI", "file:///mnt/hdd1tb/lambda-dump/landing"),
        scratch_dir=scratch,
        log_dir=Path(get("LAMBDA_LOG_DIR", "/var/log/lambda")),
        lake_root=get("LAMBDA_LAKE_ROOT", "s3a://"),
        s3_endpoint=get("LAMBDA_S3_ENDPOINT", "http://127.0.0.1:8333"),
        bucket_bronze=get("LAMBDA_BUCKET_BRONZE", "bronze"),
        bucket_silver=get("LAMBDA_BUCKET_SILVER", "silver"),
        bucket_gold=get("LAMBDA_BUCKET_GOLD", "gold"),
        bucket_ctrl=get("LAMBDA_BUCKET_CTRL", "ctrl"),
        bucket_audit=get("LAMBDA_BUCKET_AUDIT", "audit"),
        audit_enabled=_flag(get("LAMBDA_AUDIT_ENABLED", "1")),
        audit_dsn=get("LAMBDA_AUDIT_DSN", "dbname=lambda host=/var/run/postgresql connect_timeout=3"),
        audit_log_min_level=get("AUDIT_LOG_MIN_LEVEL", "INFO").upper(),
        audit_spool_dir=Path(get("LAMBDA_AUDIT_SPOOL_DIR", str(scratch / "audit-spool"))),
        metrics_enabled=_flag(get("LAMBDA_METRICS_ENABLED", "1")),
        pushgateway_url=get("LAMBDA_PUSHGATEWAY_URL", "127.0.0.1:9091"),
        otel_enabled=_flag(get("LAMBDA_OTEL_ENABLED", "1")),
        otel_endpoint=get("LAMBDA_OTEL_ENDPOINT", "http://127.0.0.1:4318"),
        spark_home=get("SPARK_HOME", "/opt/spark"),
        spark_conf_dir=get("SPARK_CONF_DIR", "/etc/spark"),
        pyspark_python=get("PYSPARK_PYTHON", str(Path.home() / ".envs" / "env-data" / "bin" / "python")),
        # small control reads/writes run on a local JVM so they never wait for (or take) the cluster's cores
        ctrl_spark_master=get("LAMBDA_CTRL_SPARK_MASTER", "local[2]"),
        extra=file_values,
    )
