#!/usr/bin/env bash
# Install/refresh Grafana Tempo (apt package `tempo`, pinned) as an ON-DEMAND service, the Alloy trace route and the
# Grafana datasources. Idempotent; needs sudo. Run from anywhere: infra/tempo/install.sh
#   start/stop:  sudo systemctl start tempo   /   sudo systemctl stop tempo     (not enabled at boot)
set -euo pipefail
cd "$(dirname "$0")"
TEMPO_VERSION=3.1.0
STAMP=$(date +%Y%m%d%H%M%S)

if ! dpkg-query -W -f='${Version}' tempo 2>/dev/null | grep -qx "$TEMPO_VERSION"; then
  sudo apt-get install -y "tempo=$TEMPO_VERSION"
fi
sudo apt-mark hold tempo >/dev/null                          # upgrade deliberately, like Spark/Delta

# data on the SSD (traces are small; the USB datalake disk is fragile)
sudo install -d -o tempo -g nogroup -m 0750 /var/lib/tempo
if ! sudo cmp -s config.yml /etc/tempo/config.yml; then
  sudo cp -a /etc/tempo/config.yml "/etc/tempo/config.yml.bak-$STAMP"
  sudo install -o root -g root -m 0644 config.yml /etc/tempo/config.yml
fi
# the package enables the unit at boot: lambda4 starts it on demand
sudo systemctl disable tempo >/dev/null 2>&1 || true

# Grafana datasources (Loki with trace links, Alertmanager, Tempo)
DS=/etc/grafana/provisioning/datasources/obs-plus.yaml
if ! sudo cmp -s ../grafana/datasources-observability.yaml "$DS"; then
  sudo cp -a "$DS" "$DS.bak-$STAMP" 2>/dev/null || true
  sudo install -o root -g grafana -m 0640 ../grafana/datasources-observability.yaml "$DS"
  sudo systemctl restart grafana-server
fi

echo "Tempo $TEMPO_VERSION installed (on demand). Alloy route: append ../alloy/lambda-traces.alloy and add"
echo "'traces = [otelcol.processor.batch.traces.input]' to otelcol.receiver.otlp (see ../alloy/README.md)."
