"""Who is running: run_id/dag/task/attempt, taken from Airflow when present, generated for manual runs.

Airflow exports AIRFLOW_CTX_* to BashOperator processes, so the same script works under Airflow and by hand.
LAMBDA_RUN_ID lets several manual tasks share one run (e.g. a shell loop over the pipeline steps).
"""
from __future__ import annotations

import os
import socket
import subprocess
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import lru_cache
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


@lru_cache(maxsize=1)
def git_sha() -> str | None:
    try:
        out = subprocess.run(["git", "-C", str(REPO_ROOT), "rev-parse", "--short", "HEAD"],
                             capture_output=True, text=True, timeout=5, check=True)
        return out.stdout.strip() or None
    except (OSError, subprocess.SubprocessError):
        return None


@dataclass(frozen=True)
class RunContext:
    run_id: str
    dag_id: str | None
    trigger: str
    under_airflow: bool
    host: str
    git_sha: str | None

    @classmethod
    def current(cls) -> RunContext:
        airflow_run = os.environ.get("AIRFLOW_CTX_DAG_RUN_ID")
        run_id = os.environ.get("LAMBDA_RUN_ID") or airflow_run
        if not run_id:
            # one id per manual process; exported so child processes join the same run
            run_id = "manual__" + datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
            os.environ["LAMBDA_RUN_ID"] = run_id
        prefix = run_id.split("__", 1)[0]
        trigger = prefix if prefix in {"manual", "scheduled", "backfill", "dataset_triggered", "asset_triggered"} \
            else "manual"
        return cls(
            run_id=run_id,
            dag_id=os.environ.get("AIRFLOW_CTX_DAG_ID"),
            trigger=trigger,
            under_airflow=bool(airflow_run),
            host=socket.gethostname(),
            git_sha=git_sha(),
        )


def airflow_attempt() -> int:
    try:
        return max(1, int(os.environ.get("AIRFLOW_CTX_TRY_NUMBER", "1")))
    except ValueError:
        return 1
