-- Read-only role of the ANP Streamlit app (apps/anp_dashboard). Idempotent:
--   psql -d lambda -v ON_ERROR_STOP=1 -f infra/postgres/anp_app_role.sql
--
-- Least privilege: USAGE on anp_gold only; SELECT on exactly the two serving marts is granted by dbt
-- (grants config of ag_preco_posto_atual / ag_preco_municipio_diario), re-applied on every build, so a rebuilt mart
-- never loses it and no other table is readable. NO password: the role logs in only over the local socket, mapped
-- from the OS user lambda (infra/postgres/install_anp_app_peer_map.sh); a TCP login (scram) is impossible.
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'anp_app_ro') THEN
    CREATE ROLE anp_app_ro LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT CONNECTION LIMIT 10;
  END IF;
END $$;
COMMENT ON ROLE anp_app_ro IS 'ANP Streamlit app: SELECT on the two anp_gold serving marts only; socket + peer map, no password';
ALTER ROLE anp_app_ro SET default_transaction_read_only = on;
ALTER ROLE anp_app_ro SET statement_timeout = '30s';
GRANT CONNECT ON DATABASE lambda TO anp_app_ro;
CREATE SCHEMA IF NOT EXISTS anp_gold AUTHORIZATION lambda;
GRANT USAGE ON SCHEMA anp_gold TO anp_app_ro;
