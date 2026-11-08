"""Airflow assets shared by the DAGs (imported inside the Airflow virtualenv, never by pipeline code).

One definition per asset: a producer and a consumer that each declared their own `Asset(...)` would silently stop
triggering the day one side is renamed. The `name` is what the Airflow UI shows; the `uri` is the identity.
"""
from __future__ import annotations

from airflow.sdk import Asset

# anp_pipeline (task bronze) -> anp_dbt
ANP_BRONZE = Asset(name="anp_bronze.precos_raw", uri="lambda://anp/bronze/precos_raw")
