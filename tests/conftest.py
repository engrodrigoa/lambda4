"""Unit tests never touch Postgres, the Pushgateway, Spark or the network."""
import pytest

from pipelines.commons import audit, logger, settings


@pytest.fixture(autouse=True)
def isolated_env(tmp_path, monkeypatch):
    monkeypatch.setenv("LAMBDA_ENV_FILE", str(tmp_path / "missing.env"))
    monkeypatch.setenv("LAMBDA_AUDIT_ENABLED", "0")
    monkeypatch.setenv("LAMBDA_METRICS_ENABLED", "0")
    monkeypatch.setenv("LAMBDA_LOG_DIR", str(tmp_path / "logs"))
    monkeypatch.setenv("LAMBDA_AUDIT_SPOOL_DIR", str(tmp_path / "spool"))
    monkeypatch.setenv("LAMBDA_RUN_ID", "manual__test")
    for var in ("AIRFLOW_CTX_DAG_RUN_ID", "AIRFLOW_CTX_DAG_ID", "AIRFLOW_CTX_TRY_NUMBER", "RFB_REF_MONTH"):
        monkeypatch.delenv(var, raising=False)
    settings.get_settings.cache_clear()
    logger.reset_logging()
    audit.reset_sink(None)
    yield tmp_path
    logger.reset_logging()
    audit.reset_sink(None)
    settings.get_settings.cache_clear()
