"""
### anp_pipeline — ANP fuel prices (Série Histórica de Preços de Combustíveis e de GLP): portal → landing → bronze

**Schedule:** daily 08:00 (America/Sao_Paulo), created **paused**. A run with nothing new ends in seconds: discover
exits 99 and everything downstream is skipped. Params let a manual run stay small (`kinds`, `limit`).

| Step | Script (`pipelines.anp.*`) | Writes |
|---|---|---|
| preflight | `anp00_preflight` | `audit.health_check` (Postgres + ANP tables, landing disk, portal, observability) |
| discover | `anp01_discover` | `ctrl.source_file` / `source_file_event` (catalog, changes, what is pending) |
| download | `anp02_download` | raw files on the landing disk (immutable, `<label>__<sha12>.<ext>`), ctrl |
| bronze | `anp03_bronze` | `anp_bronze.precos_raw` + `precos_quarantine`, ctrl in the same transaction |
| finalize | `commons.finalize_run` | closes `audit.pipeline_run`, run metrics; fails if the run failed |

Silver and gold are dbt models (owner's project): schedule them on the asset `anp_bronze.precos_raw`.
Every task logs JSON events (Loki, `audit.log_event`), records itself in `audit.task_run`, pushes metrics to the
Pushgateway, evaluates data-quality checks (`audit.dq_event`) and is a span of the run's trace (Tempo).
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from airflow.sdk import DAG, Asset, Param

REPO = Path(__file__).resolve().parents[2]          # the DAG folder is a symlink into the repository
sys.path.insert(0, str(REPO))
from pipelines.commons.airflow_callbacks import close_running_attempt  # noqa: E402
from pipelines.commons.airflow_ops import TracedBashOperator  # noqa: E402

PY = Path.home() / ".envs" / "env-data" / "bin" / "python"
ENV = {"PYTHONUNBUFFERED": "1"}                     # stream task output to the Airflow log while it runs
SUBSET = "--kinds {{ params.kinds }} --limit {{ params.limit }}"
BRONZE = Asset(name="anp_bronze.precos_raw", uri="lambda://anp/bronze/precos_raw")


def step(task_id: str, module: str, args: str = "", *, timeout_min: int = 30, retries: int = 1,
         **kw) -> TracedBashOperator:
    return TracedBashOperator(
        task_id=task_id,
        bash_command=f"{PY} -m {module} {args}".strip(),
        cwd=str(REPO), env=ENV, append_env=True,
        skip_on_exit_code=99,
        retries=retries, execution_timeout=timedelta(minutes=timeout_min),
        **kw,
    )


with DAG(
    dag_id="anp_pipeline",
    description="ANP fuel prices: portal scraping -> landing (raw files) -> bronze (Postgres), observability first",
    doc_md=__doc__,
    start_date=datetime(2026, 10, 1, tzinfo=ZoneInfo("America/Sao_Paulo")),
    schedule="0 8 * * *",
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,          # one writer of ctrl/bronze at a time
    dagrun_timeout=timedelta(hours=4),
    params={
        "full_recheck": Param(False, type="boolean",
                              description="discover: probe the size of EVERY downloaded file (republication check)"),
        "kinds": Param("semestral,mensal,semanal", type="string", pattern=r"^[a-z,]+$",
                       description="download/bronze: subset of semestral,mensal,semanal"),
        "limit": Param(0, type="integer", minimum=0, description="download/bronze: at most N files (0 = all)"),
    },
    default_args={
        "owner": "lambda",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": close_running_attempt,
        "on_retry_callback": close_running_attempt,
    },
    tags=["anp", "postgres", "polars", "bronze", "scraping"],
) as dag:
    preflight = step("preflight", "pipelines.anp.anp00_preflight", timeout_min=5, retries=0)
    discover = step("discover", "pipelines.anp.anp01_discover",
                    "{{ '--full-recheck' if params.full_recheck else '' }}", timeout_min=20, retries=2)
    # first full run: ~5.5 GB, sequential with polite pauses
    download = step("download", "pipelines.anp.anp02_download", SUBSET, timeout_min=180, retries=2,
                    retry_delay=timedelta(minutes=10))
    # none_failed: runs when download was skipped (nothing new) but files from an earlier run still wait for bronze
    bronze = step("bronze", "pipelines.anp.anp03_bronze", SUBSET, timeout_min=120, trigger_rule="none_failed",
                  outlets=[BRONZE])
    finalize = step("finalize", "pipelines.commons.finalize_run", "--pipeline anp", timeout_min=5, retries=0,
                    trigger_rule="all_done")
    preflight >> discover >> download >> bronze >> finalize
