# Prometheus additions for the lambda pipelines

Pushgateway (apt `prometheus-pushgateway`), bound to localhost with persistence — `/etc/default/prometheus-pushgateway`:

    ARGS="--web.listen-address=127.0.0.1:9091 --persistence.file=/var/lib/prometheus/pushgateway.data --persistence.interval=1m"

Scrape job, first entry under `scrape_configs:` in `/etc/prometheus/prometheus.yml` (validate with `promtool check config`):

    - job_name: pushgateway
      honor_labels: true        # keep job="lambda_pipeline", pipeline=..., task=... pushed by pipelines/commons/metrics.py
      static_configs: [{targets: ['127.0.0.1:9091']}]

Query pipeline metrics with `{job="lambda_pipeline"}` (not job="pushgateway").
