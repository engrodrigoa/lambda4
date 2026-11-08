-- Write login for the owner's manual control from DBeaver (fix ctrl rows, clean audit). NOT a superuser:
--   * SELECT/INSERT/UPDATE/DELETE on ctrl and audit (pipeline control + observability);
--   * read-only on everything else (bronze is immutable and Python-owned; silver/gold/dq/elementary are rebuilt by dbt,
--     manual edits there would be overwritten);
--   * no DDL, no TRUNCATE, cannot create roles/databases; timeouts so a forgotten open transaction in DBeaver never
--     blocks a pipeline (anp03 updates ctrl and bronze in one transaction).
-- The PASSWORD is never stored here (kept in ~/.config/lambda/secrets/pg_lambda_rw_password, mode 600); to change it:
--   sudo -u postgres psql -c "\password lambda_rw"
-- Apply (idempotent; re-apply after adding schemas): psql -d lambda -v ON_ERROR_STOP=1 -f infra/postgres/lambda_rw.sql
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'lambda_rw') THEN
    CREATE ROLE lambda_rw LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS NOREPLICATION CONNECTION LIMIT 5;
  END IF;
END $$;
COMMENT ON ROLE lambda_rw IS 'owner manual control from DBeaver: DML on ctrl + audit, read elsewhere; no DDL';
GRANT CONNECT ON DATABASE lambda TO lambda_rw;

-- write: ctrl + audit
GRANT USAGE ON SCHEMA ctrl, audit TO lambda_rw;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA ctrl, audit TO lambda_rw;
GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA ctrl, audit TO lambda_rw;
ALTER DEFAULT PRIVILEGES FOR ROLE lambda IN SCHEMA ctrl, audit GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO lambda_rw;
ALTER DEFAULT PRIVILEGES FOR ROLE lambda IN SCHEMA ctrl, audit GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO lambda_rw;

-- read: every other project schema
GRANT USAGE ON SCHEMA anp_bronze, anp_silver, anp_gold, anp_dq, anp_ref, anp_elementary,
                      analytics, analytics_elementary TO lambda_rw;
GRANT SELECT ON ALL TABLES IN SCHEMA anp_bronze, anp_silver, anp_gold, anp_dq, anp_ref, anp_elementary,
                                     analytics, analytics_elementary TO lambda_rw;
ALTER DEFAULT PRIVILEGES FOR ROLE lambda IN SCHEMA anp_bronze, anp_silver, anp_gold, anp_dq, anp_ref, anp_elementary,
                                                   analytics, analytics_elementary GRANT SELECT ON TABLES TO lambda_rw;

-- safety nets for an interactive client
ALTER ROLE lambda_rw SET idle_in_transaction_session_timeout = '10min';
ALTER ROLE lambda_rw SET lock_timeout = '10s';
ALTER ROLE lambda_rw SET statement_timeout = '15min';
