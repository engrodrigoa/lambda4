#!/usr/bin/env bash
# Lets the OS user `lambda` connect over the local socket as the Postgres role `anp_app_ro` (the Streamlit app),
# with no password: a pg_ident map + one pg_hba line placed BEFORE the generic `local all all peer`. Idempotent;
# backs up both files; reloads (no restart, connections stay up). Needs sudo.
set -euo pipefail
CONF=/etc/postgresql/16/main
HBA_LINE="local   lambda          anp_app_ro                              peer map=anp_app"
IDENT_LINE="anp_app         lambda                  anp_app_ro"
stamp=$(date +%Y%m%d%H%M%S)

if ! sudo grep -qF "$IDENT_LINE" "$CONF/pg_ident.conf"; then
  sudo cp "$CONF/pg_ident.conf" "$CONF/pg_ident.conf.bak-$stamp"
  printf '\n# ANP Streamlit app (lambda4 infra/postgres/install_anp_app_peer_map.sh)\n%s\n' "$IDENT_LINE" \
    | sudo tee -a "$CONF/pg_ident.conf" >/dev/null
fi
if ! sudo grep -qF "$HBA_LINE" "$CONF/pg_hba.conf"; then
  sudo cp "$CONF/pg_hba.conf" "$CONF/pg_hba.conf.bak-$stamp"
  # insert right before the first generic "local all all" rule (order matters: first match wins)
  sudo awk -v line="$HBA_LINE" '!done && /^local[[:space:]]+all[[:space:]]+all[[:space:]]/ {
      print "# ANP Streamlit app: OS user lambda -> role anp_app_ro (map anp_app in pg_ident.conf)"; print line; done=1 }
      { print }' "$CONF/pg_hba.conf" | sudo tee "$CONF/pg_hba.conf.new" >/dev/null
  sudo mv "$CONF/pg_hba.conf.new" "$CONF/pg_hba.conf"
  sudo chown postgres:postgres "$CONF/pg_hba.conf" && sudo chmod 640 "$CONF/pg_hba.conf"
fi
sudo -u postgres psql -Atc "select pg_reload_conf()" >/dev/null
sudo -u postgres psql -Atc "select count(*) from pg_hba_file_rules where error is not null" | grep -qx 0 \
  || { echo "pg_hba has errors: check pg_hba_file_rules" >&2; exit 1; }
echo "peer map anp_app ready"
