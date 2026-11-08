-- Read access to the dbt-owned ANP schemas (silver, gold, data quality, reference seeds, Elementary results)
-- for the read-only roles: dbeaver_ro (owner's DBeaver) and grafana_ro (dashboards). Idempotent: safe to re-run.
--   psql -d lambda -v ON_ERROR_STOP=1 -f infra/postgres/anp_dbt_grants.sql
--
-- Why this file exists: dbt creates its schemas with `CREATE SCHEMA IF NOT EXISTS` and no grants, so a read-only
-- role cannot even resolve `anp_silver.x` ("permission denied for schema"). The schemas are created here, owned by
-- lambda (the role dbt runs as), with USAGE granted and DEFAULT PRIVILEGES so every table/view dbt creates later
-- (including a --full-refresh, which drops and recreates) is readable without re-running this file.
-- dbt_dev is the target's scratch schema: deliberately NOT exposed.

CREATE SCHEMA IF NOT EXISTS anp_silver AUTHORIZATION lambda;
CREATE SCHEMA IF NOT EXISTS anp_gold   AUTHORIZATION lambda;
CREATE SCHEMA IF NOT EXISTS anp_dq     AUTHORIZATION lambda;
CREATE SCHEMA IF NOT EXISTS anp_ref    AUTHORIZATION lambda;
CREATE SCHEMA IF NOT EXISTS anp_elementary AUTHORIZATION lambda;

COMMENT ON SCHEMA anp_silver IS 'ANP silver (dbt dbt_lambda): typed, deduplicated collections, one source per month';
COMMENT ON SCHEMA anp_gold   IS 'ANP gold (dbt dbt_lambda): dimensional model + serving marts (Streamlit, Grafana)';
COMMENT ON SCHEMA anp_dq     IS 'ANP data quality (dbt dbt_lambda): load funnel and price quality gates per batch';
COMMENT ON SCHEMA anp_ref    IS 'ANP reference seeds (dbt dbt_lambda)';
COMMENT ON SCHEMA anp_elementary IS 'Elementary (dbt package): run and test results of every dbt_lambda invocation';

GRANT USAGE ON SCHEMA anp_silver, anp_gold, anp_dq, anp_ref, anp_elementary TO dbeaver_ro, grafana_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA anp_silver, anp_gold, anp_dq, anp_ref, anp_elementary TO dbeaver_ro, grafana_ro;
ALTER DEFAULT PRIVILEGES FOR ROLE lambda IN SCHEMA anp_silver, anp_gold, anp_dq, anp_ref, anp_elementary
    GRANT SELECT ON TABLES TO dbeaver_ro, grafana_ro;
