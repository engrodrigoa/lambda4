# Grafana Tempo (trace backend, on demand)

Installed 2026-10-21 from the Grafana apt repository: `tempo=3.1.0` (held with `apt-mark hold`; upgrade on purpose).
Single binary (target `all`), local storage on the SSD, everything bound to `127.0.0.1`.

| Item | Value |
|---|---|
| Config | `config.yml` here → `/etc/tempo/config.yml` (`install.sh`) |
| Unit | `tempo.service` (from the package), **disabled at boot**: `sudo systemctl start\|stop tempo` |
| HTTP API / Grafana | `127.0.0.1:3200` (`/ready`, `/api/v2/traces/<id>`, `/api/search?q=<TraceQL>`) |
| OTLP in | gRPC `127.0.0.1:4417` - fed only by Alloy (4317 is Phoenix's, 4318 is Alloy's front door) |
| Data | `/var/lib/tempo/{wal,blocks,generator}` (owner `tempo`), retention 14 days (`backend_worker.compaction.block_retention`) |
| Span metrics | metrics-generator → Prometheus remote write: `traces_spanmetrics_*` (labels `service`, `span_name`, `status_code`, `pipeline`, `layer`), `traces_service_graph_*` |
| Grafana datasource | uid `tempo`, in `../grafana/datasources-observability.yaml` (with Loki ↔ Tempo links) |

`install.sh` is idempotent: package (pinned), config, data dir, unit disabled at boot, Grafana datasources.

Checks:

```bash
curl -s 127.0.0.1:3200/ready
curl -s -G 127.0.0.1:3200/api/search --data-urlencode 'q={resource.service.name="lambda-rfb"}' --data-urlencode limit=5
```

Idle noise: the backend worker logs `error calling scheduler ... no jobs found` every few seconds while there is
nothing to compact. It is harmless.

## Python side (pipelines)

The OpenTelemetry SDK lives in the data env, pinned to the version Airflow already uses:

```bash
uv pip install --python ~/.envs/env-data/bin/python "opentelemetry-sdk==1.44.0" "opentelemetry-exporter-otlp-proto-http==1.44.0"
```

Without it `pipelines/commons/tracing.py` is a no-op. Flags: `LAMBDA_OTEL_ENABLED` (default 1), `LAMBDA_OTEL_ENDPOINT`
(default `http://127.0.0.1:4318`, Alloy).
