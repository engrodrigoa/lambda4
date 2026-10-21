-- Read-only login for the owner's DBeaver (audit + analytics). The PASSWORD is never stored here: the owner sets it
-- interactively on the server with:   sudo -u postgres psql -c "\password dbeaver_ro"
-- Apply: sudo -u postgres psql -d lambda -v ON_ERROR_STOP=1 -f infra/postgres/dbeaver_ro.sql
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'dbeaver_ro') THEN
    CREATE ROLE dbeaver_ro LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE CONNECTION LIMIT 5;
  END IF;
END $$;
COMMENT ON ROLE dbeaver_ro IS 'owner read-only access from DBeaver (audit + analytics); password set by the owner';
GRANT CONNECT ON DATABASE lambda TO dbeaver_ro;
GRANT USAGE ON SCHEMA audit, analytics, analytics_elementary TO dbeaver_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA audit, analytics, analytics_elementary TO dbeaver_ro;
ALTER DEFAULT PRIVILEGES FOR ROLE lambda IN SCHEMA audit, analytics, analytics_elementary GRANT SELECT ON TABLES TO dbeaver_ro;
ALTER ROLE dbeaver_ro SET default_transaction_read_only = on;
