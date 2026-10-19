# Alloy: ship pipeline JSON logs to Loki

`lambda-pipelines.alloy` is appended to `/etc/alloy/config.alloy` (check with `alloy fmt`, then restart alloy).
Pipelines write `/var/log/lambda/<pipeline>/<YYYY-MM-DD>.jsonl` (dir created with
`install -d -o lambda -g lambda -m 2775 /var/log/lambda`). Labels: job, host, pipeline, task, level (low cardinality);
query the rest with `| json`, e.g. `{job="lambda-pipelines", pipeline="rfb"} | json | event="file_downloaded"`.
