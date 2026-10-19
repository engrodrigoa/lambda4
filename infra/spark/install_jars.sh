#!/usr/bin/env bash
# Download the extra Spark jars from Maven Central, verify sha1, install into /opt/spark/jars.
# Spark 4.1.x + Delta 4.1.0 + S3A (hadoop-aws must match the bundled hadoop-client: 3.4.2).
set -euo pipefail
M=https://repo1.maven.org/maven2
D=$(mktemp -d); trap 'rm -rf "$D"' EXIT
JARS=(
  io/delta/delta-spark_4.1_2.13/4.1.0/delta-spark_4.1_2.13-4.1.0.jar
  io/delta/delta-storage/4.1.0/delta-storage-4.1.0.jar
  org/apache/hadoop/hadoop-aws/3.4.2/hadoop-aws-3.4.2.jar
  software/amazon/awssdk/bundle/2.29.52/bundle-2.29.52.jar   # ~612 MB, version required by hadoop-aws 3.4.2
)
for u in "${JARS[@]}"; do
  f=$(basename "$u"); curl -fsSL -o "$D/$f" "$M/$u"; curl -fsSL -o "$D/$f.sha1" "$M/$u.sha1"
  echo "$(cat "$D/$f.sha1")  $D/$f" | sha1sum -c -
  install -m 0644 "$D/$f" /opt/spark/jars/
done
