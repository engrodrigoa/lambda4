"""
### anp_dbt — ANP silver + data quality (dbt + Cosmos)

Airflow DAG disparado pelo asset `anp_bronze.precos_raw` (task bronze do anp_pipeline): sem arquivo novo, não roda.
Cada modelo vira uma task de run + test: teste com severity error bloqueia o downstream.

| Camada | Modelos |
|---|---|
| silver | `stg_anp__precos` (view) → `anp__precos_coleta` (incremental por mês × grupo) |
| anp_dq | funil `dq_anp__carga_mes`, portão de qualidade `dq_anp__qualidade_lote(_uf)`, `dq_anp__precos_suspeitos` |

`chain_start` liga a execução à carga da bronze de origem; no fim, `publish_silver` emite o asset
`anp_silver.anp__precos_coleta` (levando a origem no extra), que dispara o `anp_dbt_gold`; `chain_end` fecha o run
em `audit.pipeline_run` e falha se algo falhou. Cadeia: `audit.v_anp_cadeia`.
Observabilidade: o Elementary grava cada task em `anp_elementary`; o board Grafana "ANP · Transformações dbt".
Keep the words "airflow" and "dag" in this file: the dag-processor's safe mode skips files without both.
"""
import sys
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from cosmos import DbtTaskGroup

REPO = Path(__file__).resolve().parents[2]          # the DAG folder is a symlink into the repository
sys.path.insert(0, str(REPO))
from pipelines.commons.airflow_dbt import EXECUTION, PROFILE, PROJECT, render  # noqa: E402
from pipelines.commons.assets import ANP_BRONZE, ANP_SILVER  # noqa: E402
from pipelines.commons.chain import chain_end, chain_start, publish  # noqa: E402

with DAG(
    dag_id="anp_dbt",
    description="ANP: bronze -> silver (dbt) + data quality gates (anp_dq), Elementary results",
    doc_md=__doc__,
    start_date=datetime(2026, 11, 1, tzinfo=ZoneInfo("America/Sao_Paulo")),
    schedule=[ANP_BRONZE],
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    dagrun_timeout=timedelta(hours=2),
    default_args={"owner": "lambda", "retries": 1, "retry_delay": timedelta(minutes=5),
                  "execution_timeout": timedelta(minutes=60)},
    tags=["anp", "dbt", "cosmos", "silver", "dq"],
) as dag:
    silver = DbtTaskGroup(
        group_id="silver",
        project_config=PROJECT, profile_config=PROFILE, execution_config=EXECUTION,
        render_config=render(exclude=["tag:gold"]),
    )
    # chain tracing (pipelines/commons/chain.py): link this run to the bronze load(s) it comes from, open/close its
    # audit.pipeline_run, carry the origins in the published asset event; chain_end is the only leaf and fails a
    # failed run (all_done would otherwise make a broken run green)
    start = PythonOperator(task_id="chain_start", python_callable=chain_start, retries=2,
                           execution_timeout=timedelta(minutes=5))
    publish_silver = PythonOperator(task_id="publish_silver", outlets=[ANP_SILVER],
                                    python_callable=publish(ANP_SILVER, consumer="anp_dbt_gold"),
                                    execution_timeout=timedelta(minutes=5))
    end = PythonOperator(task_id="chain_end", python_callable=chain_end, trigger_rule="all_done", retries=0,
                         execution_timeout=timedelta(minutes=5))
    start >> silver >> publish_silver >> end
