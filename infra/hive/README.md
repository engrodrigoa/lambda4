# Hive metastore (embedded in Spark, backed by Postgres)

Spark 4.1 ships the Hive 2.3.10 client + DataNucleus. The metastore DB is `hive_metastore` (role `hive`) on the local Postgres.

Schema files come from Apache Hive tag `rel/release-2.3.10` (`metastore/scripts/upgrade/postgres/`), Apache-2.0.
Initialize ONCE on an empty database, then keep `datanucleus.schema.autoCreateAll=false`:

    psql -h 127.0.0.1 -U hive -d hive_metastore -v ON_ERROR_STOP=1 -f hive-schema-2.3.0.postgres.sql
    # (the script \i-includes hive-txn-schema-2.3.0.postgres.sql: run it from this directory)

Why: with autoCreateAll=true the first Spark session deadlocks inside Postgres (two connections of the same JVM:
one holds an open INSERT transaction, the other waits for an ALTER TABLE lock on the same table).
