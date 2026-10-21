# Spark install (pinned to 4.1.0 — see docs/adr/0002)

Delta Lake for Spark 4.1 is compiled against **Spark 4.1.0**; later 4.1.x patches break Delta at runtime.

    U=https://archive.apache.org/dist/spark/spark-4.1.0/spark-4.1.0-bin-hadoop3.tgz
    curl -fLO $U && curl -fLO $U.sha512 && sha512sum -c spark-4.1.0-bin-hadoop3.tgz.sha512
    sudo tar -xzf spark-4.1.0-bin-hadoop3.tgz -C /opt && sudo mv /opt/spark-4.1.0-bin-hadoop3 /opt/spark-4.1.0
    sudo chown -R lambda:lambda /opt/spark-4.1.0 && sudo ln -sfn /opt/spark-4.1.0 /opt/spark
    ./install_jars.sh                       # Delta, delta-storage, hadoop-aws 3.4.2, AWS SDK bundle (sha1-verified)
    # plus iceberg-spark-runtime-4.1_2.13-1.12.0, iceberg-aws-bundle-1.12.0, postgresql-42.7.13 (JDBC)
    uv pip install --python ~/.envs/env-data/bin/python "pyspark==4.1.0" "delta-spark==4.1.0"
    sudo systemctl restart spark-master spark-worker spark-history

Config lives in `/etc/spark` (`SPARK_CONF_DIR`), template `spark-defaults.conf.template`; the worker override with the
SSD scratch is `spark-worker-scratch-ssd.conf`. Rollback: `/opt/spark-4.1.3` is kept on the machine.
