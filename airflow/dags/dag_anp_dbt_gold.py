"""
### anp_dbt_gold — ANP gold: modelo dimensional + marts de serving (dbt + Cosmos)

Airflow DAG disparado pelo asset `anp_silver.anp__precos_coleta` (fim do anp_dbt). Criado pausado.

| Tipo | Modelos |
|---|---|
| dimensões | `dim_calendario`, `dim_produto`, `dim_municipio`, `dim_posto` (SCD2 derivado da linha do tempo) |
| fato | `fct_preco_diario` (posto × produto × dia; incremental por mês) |
| serving | `ag_preco_posto_atual`, `ag_preco_municipio_diario` (Streamlit), `ag_preco_semanal_uf` (Grafana/BI) |

Testes: grão, FKs (relationships), SCD2 sem sobreposição e uma versão atual, reconciliação silver → fato → marts.
`chain_start`/`chain_end` registram a cadeia (origem na bronze) e o run em `audit`; `publish_gold` emite `anp_gold`.
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
from pipelines.commons.assets import ANP_GOLD, ANP_SILVER  # noqa: E402
from pipelines.commons.chain import chain_end, chain_start, publish  # noqa: E402

with DAG(
    dag_id="anp_dbt_gold",
    description="ANP: silver -> gold (dimensional model + serving marts) with dbt",
    doc_md=__doc__,
    start_date=datetime(2026, 11, 1, tzinfo=ZoneInfo("America/Sao_Paulo")),
    schedule=[ANP_SILVER],
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    dagrun_timeout=timedelta(hours=2),
    default_args={"owner": "lambda", "retries": 1, "retry_delay": timedelta(minutes=5),
                  "execution_timeout": timedelta(minutes=90)},
    tags=["anp", "dbt", "cosmos", "gold"],
) as dag:
    gold = DbtTaskGroup(
        group_id="gold",
        project_config=PROJECT, profile_config=PROFILE, execution_config=EXECUTION,
        render_config=render(select=["tag:gold"]),
    )
    # chain tracing (pipelines/commons/chain.py): link this run to the bronze load(s) it comes from, open/close its
    # audit.pipeline_run, carry the origins in the published asset event; chain_end is the only leaf and fails a
    # failed run (all_done would otherwise make a broken run green)
    start = PythonOperator(task_id="chain_start", python_callable=chain_start, retries=2,
                           execution_timeout=timedelta(minutes=5))
    # nothing is scheduled on anp_gold (the Streamlit reads the marts): no consumer, so no stalled-chain alert
    publish_gold = PythonOperator(task_id="publish_gold", python_callable=publish(ANP_GOLD), outlets=[ANP_GOLD],
                                  execution_timeout=timedelta(minutes=5))
    end = PythonOperator(task_id="chain_end", python_callable=chain_end, trigger_rule="all_done", retries=0,
                         execution_timeout=timedelta(minutes=5))
    start >> gold >> publish_gold >> end
