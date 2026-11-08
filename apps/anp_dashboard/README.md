# ANP dashboard (Streamlit)

Analytics layer over the ANP gold serving marts. Ported from the datalabs portfolio (`apps/anp_dashboard`) with the
same parameters and visual contract; adapted to lambda4: reads `anp_gold`, no password anywhere.

```bash
# once (server): read-only role + peer map (OS user lambda -> role anp_app_ro over the local socket)
psql -d lambda -v ON_ERROR_STOP=1 -f infra/postgres/anp_app_role.sql
bash infra/postgres/install_anp_app_peer_map.sh                      # sudo: pg_ident + pg_hba + reload
# once: own venv (never the Airflow/data venvs)
uv venv ~/.envs/env-app && uv pip install --python ~/.envs/env-app/bin/python -r apps/anp_dashboard/requirements.txt
# service (on demand), localhost only
sudo install -m 0644 infra/systemd/anp-dashboard.service /etc/systemd/system/ && sudo systemctl daemon-reload
sudo systemctl start anp-dashboard          # http://localhost:8501 via ssh -L 8501:127.0.0.1:8501 lambda@lambdaserver
```

| File | Role |
|---|---|
| `app.py` | layout only: sidebar filters (UF → cidade → bairro, bandeira, busca, janela), product cards, tabs |
| `logic.py` | pure Polars (same as datalabs): filters, recency window, KPIs, matrix pivot, exact weekly roll-up |
| `data.py` | the only SQL, bind parameters, psycopg 3 over the socket as `anp_app_ro`, read-only session (`ANP_APP_DSN`) |
| `.streamlit/config.toml` | theme (dark surface, green accent) |

| Tab | Source (`anp_gold`) | Grain |
|---|---|---|
| Cards + Matriz de preços | `ag_preco_posto_atual` | latest price per station × product (18 months), filtered by the recency window |
| Histórico | `ag_preco_municipio_diario` | collection day (or week) × product, Brasil / UF / city, exact `sum / count` |

Security: `anp_app_ro` has `USAGE` on `anp_gold` and `SELECT` on exactly these two marts (dbt `grants`, re-applied on
every build); no password (TCP login impossible); `default_transaction_read_only` and `statement_timeout = 30s`.
Tests: `tests/test_anp_dashboard.py` (logic always; SQL as the role with
`ANP_APP_SQL_DSN="dbname=lambda host=/var/run/postgresql user=anp_app_ro"`, including "permission denied" outside the marts).
Suspicious prices (outside 0.5–2× the month × product × UF median) never reach the marts.
