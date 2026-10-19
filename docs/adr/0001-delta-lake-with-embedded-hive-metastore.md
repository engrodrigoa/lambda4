# ADR 0001 — Delta Lake as the table format, with an embedded Hive metastore on Postgres

**Status:** accepted (2026-10-18)

## Context

Project 1 (RFB CNPJ full base) needs a lakehouse table format with `MERGE` (for SCD2), time travel and schema
evolution, running on Spark 4.1.3 against S3 (SeaweedFS). Two candidates: Delta Lake and Apache Iceberg.

- The owner's medium-term goal is the Databricks Certified Data Engineer Associate, whose blueprint centres on
  Delta Lake and does not cover Iceberg (exam guide, May 2026).
- Iceberg was already provisioned (JDBC catalog `lambda` in Postgres, `s3://warehouse/`, runtime jar), and the
  owner's day job uses Iceberg, so Iceberg skills are acquired elsewhere.
- Delta Lake 4.1.0 supports Spark 4.1, which is what runs here.
- By 2026 the formats are converging (UniForm, Iceberg v3 in Unity Catalog); the catalog is the real lock-in, not
  the file format.

## Decision

- **Delta Lake 4.1.0** is the format for bronze, silver, gold and the `ctrl` control tables.
- **Named tables** are registered in an **embedded Hive metastore** (Spark's built-in Hive 2.3.10 client) whose
  backing database is `hive_metastore` on the local Postgres. No separate metastore service.
- Iceberg stays installed as the named catalog `lambda` (default catalog is now `spark_catalog` = Delta), so a
  later side-by-side comparison costs nothing.
- Extra jars (Delta, delta-storage, hadoop-aws 3.4.2, AWS SDK v2 bundle 2.29.52) are installed by
  `infra/spark/install_jars.sh`, sha1-verified against Maven Central.
- The metastore schema is created **once, explicitly** with the official Hive 2.3.0 Postgres scripts
  (`infra/hive/`), and `datanucleus.schema.autoCreateAll` is `false`.

## Consequences

- Delta on S3 without a commit coordinator is safe for **one writer per table**. Acceptable for this lab (one
  Airflow DAG writes each table); it is also a concept to study (multi-writer needs a coordinator, e.g. DynamoDB
  log store on AWS or Unity Catalog managed commits).
- DuckDB reads the tables with its `delta` extension (`delta_scan('s3://...')`), so the query layer for DBeaver is
  unchanged.
- Lesson recorded: with `autoCreateAll=true` the first Spark session deadlocks inside Postgres (one connection
  holds an INSERT transaction while another waits for an ALTER TABLE lock). Initialize the schema explicitly.
- The metastore password and S3 keys live in `/etc/spark/spark-defaults.conf` (mode 600) and
  `~/.config/lambda/secrets/`; the repo only holds a template with `${VARS}`.
- A future move to Unity Catalog OSS (closer to Databricks) only changes the catalog layer, not the data.
