"""Unit tests never touch Postgres, the Pushgateway, Spark or the network."""
import pytest

from pipelines.commons import audit, logger, settings, tracing


@pytest.fixture(autouse=True)
def isolated_env(tmp_path, monkeypatch):
    monkeypatch.setenv("LAMBDA_ENV_FILE", str(tmp_path / "missing.env"))
    monkeypatch.setenv("LAMBDA_AUDIT_ENABLED", "0")
    monkeypatch.setenv("LAMBDA_METRICS_ENABLED", "0")
    monkeypatch.setenv("LAMBDA_OTEL_ENABLED", "0")
    monkeypatch.setenv("LAMBDA_LOG_DIR", str(tmp_path / "logs"))
    monkeypatch.setenv("LAMBDA_AUDIT_SPOOL_DIR", str(tmp_path / "spool"))
    monkeypatch.setenv("LAMBDA_RUN_ID", "manual__test")
    for var in ("AIRFLOW_CTX_DAG_RUN_ID", "AIRFLOW_CTX_DAG_ID", "AIRFLOW_CTX_TRY_NUMBER", "AIRFLOW_CTX_TASK_ID",
                "RFB_REF_MONTH", "TRACEPARENT"):
        monkeypatch.delenv(var, raising=False)
    settings.get_settings.cache_clear()
    logger.reset_logging()
    audit.reset_sink(None)
    tracing.shutdown()
    yield tmp_path
    tracing.shutdown()
    logger.reset_logging()
    audit.reset_sink(None)
    settings.get_settings.cache_clear()
