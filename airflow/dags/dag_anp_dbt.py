"""
### anp_dbt — silver/gold da ANP (dbt + Cosmos)

Disparado pelo asset `anp_bronze.precos_raw` (task bronze do anp_pipeline). Sem arquivo novo → sem evento → não roda.
Cada modelo vira uma task de run + test (TestBehavior.AFTER_EACH): teste com severity error bloqueia o downstream.
Conexão: socket local + peer auth (mesmo padrão do pipelines.commons.pg), sem senha.

Airflow DAG file: keep the words "airflow" and "dag" in it. The dag-processor's safe mode skips any file without
both, silently (this file imports only cosmos, so the docstring is what keeps it visible).
"""
import sys
from pathlib import Path

from cosmos import DbtDag, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import InvocationMode, SourceRenderingBehavior, TestBehavior

REPO = Path(__file__).resolve().parents[2]          # the DAG folder is a symlink into the repository
sys.path.insert(0, str(REPO))
from pipelines.commons.assets import ANP_BRONZE  # noqa: E402

DBT_DIR = REPO / "dbt_lambda"

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
        source_rendering_behavior=SourceRenderingBehavior.WITH_TESTS_OR_FRESHNESS,
        emit_datasets=False,
    ),
    schedule=[ANP_BRONZE],
)
