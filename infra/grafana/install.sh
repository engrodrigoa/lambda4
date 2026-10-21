#!/usr/bin/env bash
# Build the dashboards from code and install them into Grafana's file provisioning (needs sudo).
set -euo pipefail
cd "$(dirname "$0")"
python3 build_dashboards.py
sudo install -d -o grafana -g grafana -m 0755 /var/lib/grafana/dashboards-lambda4
sudo install -o grafana -g grafana -m 0644 dashboards/*.json /var/lib/grafana/dashboards-lambda4/
if ! sudo cmp -s provisioning-lambda4.yaml /etc/grafana/provisioning/dashboards/lambda4-rfb.yaml; then
  sudo install -m 0644 provisioning-lambda4.yaml /etc/grafana/provisioning/dashboards/lambda4-rfb.yaml
  sudo systemctl restart grafana-server      # a new provider needs a restart; later edits reload every 30 s
fi
