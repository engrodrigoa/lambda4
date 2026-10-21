# Alloy: ship pipeline JSON logs to Loki

`lambda-pipelines.alloy` is appended to `/etc/alloy/config.alloy` (check with `alloy fmt`, then restart alloy).
Pipelines write `/var/log/lambda/<pipeline>/<YYYY-MM-DD>.jsonl` (dir created with
`install -d -o lambda -g lambda -m 2775 /var/log/lambda`). Labels: job, host, pipeline, task, level (low cardinality);
query the rest with `| json`, e.g. `{job="lambda-pipelines", pipeline="rfb"} | json | event="file_downloaded"`.

## OTLP: metrics and traces (`airflow-otel.alloy`, `lambda-traces.alloy`)

`otelcol.receiver.otlp "airflow"` on `127.0.0.1:4318` (OTLP/HTTP) is the single OTLP front door:
metrics (Airflow) → `prometheus.remote_write`; traces (Airflow + `pipelines/commons/tracing.py`) →
`otelcol.processor.batch "traces"` → `otelcol.exporter.otlp "tempo"` (OTLP/gRPC `127.0.0.1:4417`, see `infra/tempo/`).
The pipeline log stage also copies `trace_id`/`span_id` into Loki structured metadata, so
`{job="lambda-pipelines"} | trace_id="<id>"` needs no `| json`.

Apply: back up `/etc/alloy/config.alloy`, edit it to match these files, `alloy validate /etc/alloy/config.alloy`,
`sudo systemctl restart alloy`.
