"""Local Spark + Delta for tests: in-memory catalog, tables in a temp folder. Never touches /etc/spark or the lake."""
import os
import sys

import pytest

from pipelines.commons import settings


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    os.environ["SPARK_CONF_DIR"] = str(tmp_path_factory.mktemp("sparkconf"))   # empty: no cluster, no metastore
    os.environ.setdefault("SPARK_HOME", "/opt/spark")
    os.environ["PYSPARK_PYTHON"] = sys.executable
    from pyspark.sql import SparkSession

    session = (SparkSession.builder.master("local[2]").appName("lambda4-tests")
               .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
               .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
               .config("spark.sql.catalogImplementation", "in-memory")
               .config("spark.sql.warehouse.dir", str(tmp_path_factory.mktemp("warehouse")))
               .config("spark.sql.shuffle.partitions", "2")
               .config("spark.default.parallelism", "2")
               .config("spark.ui.enabled", "false")
               .config("spark.sql.session.timeZone", "UTC")
               .getOrCreate())
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def lake(tmp_path, monkeypatch, spark):
    """A fresh lake root and fresh databases for each test."""
    monkeypatch.setenv("LAMBDA_LAKE_ROOT", (tmp_path / "lake").as_uri() + "/")
    settings.get_settings.cache_clear()
    for db in ("rfb_silver", "rfb_gold", "t"):
        spark.sql(f"DROP DATABASE IF EXISTS {db} CASCADE")
    spark.sql(f"CREATE DATABASE rfb_silver LOCATION '{(tmp_path / 'lake' / 'silver').as_uri()}'")
    spark.sql(f"CREATE DATABASE rfb_gold LOCATION '{(tmp_path / 'lake' / 'gold').as_uri()}'")
    spark.sql(f"CREATE DATABASE t LOCATION '{(tmp_path / 'lake' / 't').as_uri()}'")
    yield tmp_path / "lake"
    settings.get_settings.cache_clear()
