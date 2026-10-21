"""
### rfb_pipeline — Receita Federal CNPJ open data, landing → bronze → silver (SCD2) → gold, on Spark + Delta

**Trigger:** manual, with `ref_month` (YYYY-MM). Months must be applied in order for the SCD2 history.

| Step | Script | Writes |
|---|---|---|
| preflight | `pipelines.rfb.rfb00_preflight` | `audit.health_check` (S3, Spark, metastore, disks, observability) |
| landing | `pipelines.rfb.rfb01_download` | ZIPs on the landing disk, `ctrl.download_ref` / `download_file` |
| `<entity>.extract` | `pipelines.rfb.rfb02_extract` | record-aligned UTF-8 chunks on the SSD scratch + manifest |
| `<entity>.bronze` | `pipelines.rfb.rfb03_bronze` | `rfb_bronze.<entity>` (Delta, raw strings, by `_ref_month`) |
| `<entity>.silver` | `pipelines.rfb.rfb04_silver` | `rfb_silver.<entity>_hist` (SCD2) or code lists (SCD1) |
| `gold.*` | `pipelines.rfb.rfb05_gold` | `rfb_gold.*`: dims, *_atual, fato_mudanca_cadastral, agg |
| maintenance | `pipelines.rfb.rfb06_maintenance` | OPTIMIZE + VACUUM + `audit.delta_table_stats` |
| finalize | `pipelines.commons.finalize_run` | closes `audit.pipeline_run`, run metrics; fails if the run failed |

Every task logs JSON events (Loki, `audit.log_event`), records itself in `audit.task_run`, pushes metrics to the
Pushgateway and evaluates data-quality checks (`audit.dq_event`, BLOCK stops the task, WARN continues).
Exit code 99 = nothing to do (e.g. month not published yet) → the task is **skipped**, not failed.

**Traces:** every task is a span in one trace per run (Tempo, trace id = md5(run_id)); each process also receives
Airflow's own `worker.<task>` span as `TRACEPARENT`, and our span links to it.
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Asset, Param, TaskGroup

REPO = Path(__file__).resolve().parents[2]          # the DAG folder is a symlink into the repository
sys.path.insert(0, str(REPO))
from pipelines.commons.airflow_callbacks import close_running_attempt  # noqa: E402

PY = Path.home() / ".envs" / "env-data" / "bin" / "python"
ENV = {
    "SPARK_HOME": "/opt/spark",
    "SPARK_CONF_DIR": "/etc/spark",
    "PYSPARK_PYTHON": str(PY),
    "PYTHONUNBUFFERED": "1",            # stream task output to the Airflow log while it runs
}
REF = "{{ params.ref_month }}"
LANES = ("estabelecimentos", "empresas", "socios", "simples", "domains")
CRITICAL_PATH = "estabelecimentos"      # largest entity: scheduled first inside the pools
SILVER_TABLE = {"estabelecimentos": "estabelecimento_hist", "empresas": "empresa_hist", "socios": "socio_hist",
                "simples": "simples_hist", "domains": "code_lists"}
GOLD_TARGETS = ("estabelecimento_atual", "socio_atual", "fato_mudanca_cadastral", "agg_estabelecimentos_mensal")


def asset(layer: str, table: str) -> Asset:
    return Asset(name=f"rfb_{layer}.{table}", uri=f"lambda://rfb/{layer}/{table}")


class TracedBashOperator(BashOperator):
    """BashOperator that hands Airflow's current span (`worker.<task>`, when AIRFLOW__TRACES__OTEL_ON) to the process
    as W3C TRACEPARENT; pipelines/commons/tracing.py turns it into a span link."""

    def get_env(self, context) -> dict:
        env = super().get_env(context)
        try:
            from opentelemetry.propagate import inject

            carrier: dict[str, str] = {}
            inject(carrier)
            if carrier.get("traceparent"):
                env["TRACEPARENT"] = carrier["traceparent"]
        except Exception:  # noqa: BLE001 - tracing must never fail a task
            pass
        return env


def step(task_id: str, module: str, args: str, *, pool: str = "default_pool", timeout_min: int = 60,
         retries: int = 1, priority: int = 1, outlets: list | None = None, **kw) -> BashOperator:
    return TracedBashOperator(
        task_id=task_id,
        bash_command=f"{PY} -m {module} {args}".strip(),
        cwd=str(REPO), env=ENV, append_env=True,
        skip_on_exit_code=99,
        pool=pool, priority_weight=priority,
        retries=retries, execution_timeout=timedelta(minutes=timeout_min),
        outlets=outlets or [],
        **kw,
    )


with DAG(
    dag_id="rfb_pipeline",
    description="RFB CNPJ: landing -> bronze -> silver (SCD2) -> gold on Spark + Delta, observability first",
    doc_md=__doc__,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,          # one month at a time: SCD2 needs ordered months
    max_active_tasks=4,
    dagrun_timeout=timedelta(hours=8),
    params={"ref_month": Param("2026-09", type="string", pattern=r"^[0-9]{4}-[0-9]{2}$",
                               description="Reference month to load (YYYY-MM)")},
    default_args={
        "owner": "lambda",
        "retries": 1,
        "retry_delay": timedelta(minutes=3),
        "on_failure_callback": close_running_attempt,
        "on_retry_callback": close_running_attempt,
    },
    tags=["rfb", "lakehouse", "spark", "delta", "scd2"],
) as dag:
    preflight = step("preflight", "pipelines.rfb.rfb00_preflight", "", timeout_min=5, retries=0)
    landing = step("landing", "pipelines.rfb.rfb01_download", f"--ref-month {REF}", timeout_min=180, retries=2,
                   retry_delay=timedelta(minutes=10))
    preflight >> landing

    silver_done = {}
    for lane in LANES:
        prio = 10 if lane == CRITICAL_PATH else 1
        with TaskGroup(group_id=lane, tooltip=f"{lane}: extract -> bronze -> silver"):
            extract = step("extract", "pipelines.rfb.rfb02_extract", f"--ref-month {REF} --entity {lane}",
                           pool="rfb_extract", timeout_min=45, priority=prio)
            bronze = step("bronze", "pipelines.rfb.rfb03_bronze", f"--ref-month {REF} --entity {lane}",
                          pool="rfb_spark", timeout_min=90, priority=prio,
                          outlets=[asset("bronze", lane)])
            silver = step("silver", "pipelines.rfb.rfb04_silver", f"--ref-month {REF} --entity {lane}",
                          pool="rfb_spark", timeout_min=150, priority=prio,
                          outlets=[asset("silver", SILVER_TABLE[lane])])
            landing >> extract >> bronze >> silver
        silver_done[lane] = silver

    with TaskGroup(group_id="gold", tooltip="consumption tables, built as of the month"):
        dims = step("dims", "pipelines.rfb.rfb05_gold", f"--ref-month {REF} --target dims", pool="rfb_spark",
                    timeout_min=20, outlets=[asset("gold", "dims")])
        silver_done["domains"] >> dims
        gold = {}
        for target in GOLD_TARGETS:
            gold[target] = step(target, "pipelines.rfb.rfb05_gold", f"--ref-month {REF} --target {target}",
                                pool="rfb_spark", timeout_min=90, outlets=[asset("gold", target)])
        needs = {
            "estabelecimento_atual": ["estabelecimentos", "empresas", "simples"],
            "socio_atual": ["socios", "empresas"],
            "fato_mudanca_cadastral": ["estabelecimentos", "empresas", "socios", "simples"],
            "agg_estabelecimentos_mensal": ["estabelecimentos", "empresas", "simples"],
        }
        for target, lanes in needs.items():
            [silver_done[lane] for lane in lanes] >> gold[target]
            dims >> gold[target]

    maintenance = step("maintenance", "pipelines.rfb.rfb06_maintenance", f"--ref-month {REF}", pool="rfb_spark",
                       timeout_min=120)
    finalize = step("finalize", "pipelines.commons.finalize_run", "--pipeline rfb", timeout_min=5, retries=0,
                    trigger_rule="all_done")
    list(gold.values()) >> maintenance >> finalize
