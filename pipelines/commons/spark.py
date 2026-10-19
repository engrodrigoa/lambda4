"""SparkSession factory: one place for the environment, app naming and session defaults.

Cluster-level config (master, Delta, Hive metastore, s3a, secrets) lives in $SPARK_CONF_DIR/spark-defaults.conf
(/etc/spark, mode 600), so code never carries credentials. Here we only set what is per-session.
"""
from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from pipelines.commons.settings import get_settings
from pipelines.commons.task import current_task


def _prepare_env() -> None:
    s = get_settings()
    os.environ.setdefault("SPARK_HOME", s.spark_home)
    os.environ.setdefault("SPARK_CONF_DIR", s.spark_conf_dir)
    os.environ.setdefault("PYSPARK_PYTHON", s.pyspark_python)


def build_spark(app_name: str, master: str | None = None, conf: dict[str, Any] | None = None):
    _prepare_env()
    from pyspark.sql import SparkSession  # imported late: SPARK_HOME must be set first

    task = current_task()
    full_name = f"lambda-{app_name}" + (f"-{task.ctx.run_id}" if task else "")
    builder = SparkSession.builder.appName(full_name[:200])
    if master:
        builder = builder.master(master)
    defaults = {"spark.sql.session.timeZone": "UTC"}
    for k, v in {**defaults, **(conf or {})}.items():
        builder = builder.config(k, v)
    spark = builder.enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel(os.environ.get("LAMBDA_SPARK_LOG_LEVEL", "WARN"))
    if task is not None:
        task.spark_app_id = spark.sparkContext.applicationId
        task.metric(spark_master=spark.sparkContext.master)
    return spark


@contextmanager
def spark_session(app_name: str, master: str | None = None, conf: dict[str, Any] | None = None) -> Iterator[Any]:
    spark = build_spark(app_name, master=master, conf=conf)
    try:
        yield spark
    finally:
        spark.stop()
