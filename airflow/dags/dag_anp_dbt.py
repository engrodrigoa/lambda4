"""
### anp_dbt — silver/gold da ANP (dbt + Cosmos)

Disparado pelo asset `anp_bronze.precos_raw` (task bronze do anp_pipeline). Sem arquivo novo → sem evento → não roda.
Cada modelo vira uma task de run + test (TestBehavior.AFTER_EACH): teste com severity error bloqueia o downstream.
Conexão: socket local + peer auth (mesmo padrão do pipelines.commons.pg), sem senha.
"""
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from airflow.sdk import Asset
from cosmos import DbtDag, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import InvocationMode, TestBehavior


DBT_DIR = Path.home() / "lambda4" / "dbt_lambda"
BRONZE = Asset(name="anp_bronze.precos_raw", uri="lambda://anp/bronze/precos_raw")

anp_dbt = DbtDag(
    dag_id="anp_dbt",
    project_config=ProjectConfig(DBT_DIR, install_dbt_deps=False),   # deps já instalados em dbt_lambda/dbt_packages
    profile_config=ProfileConfig(
        profile_name="dbt_lambda",
        target_name="dev",
        profiles_yml_filepath=DBT_DIR / "profiles.yml",
    ),
    execution_config=ExecutionConfig(
        dbt_executable_path=str(DBT_DIR / ".venv" / "bin" / "dbt"),
        invocation_mode=InvocationMode.SUBPROCESS,
    ),
    render_config=RenderConfig(
        test_behavior=TestBehavior.AFTER_EACH,
        emit_datasets=False,          # socket local não gera URI de asset válida no Airflow 3
    ),
    schedule=[BRONZE],
)