"""Cosmos configuration shared by the dbt DAGs of dbt_lambda (imported inside the Airflow virtualenv only).

One place for how Airflow runs dbt here: project in the repository, profile in the project (socket + peer auth, no
Airflow Connection, no password), dbt from the project's own venv as a subprocess, one run task + one test task per
model (a failing error-severity test blocks the downstream), Elementary's own models left out of the graph (its
on-run-end hooks still record every task in anp_elementary).
"""
from __future__ import annotations

from pathlib import Path

from cosmos import ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import InvocationMode, SourceRenderingBehavior, TestBehavior

REPO = Path(__file__).resolve().parents[2]
DBT_DIR = REPO / "dbt_lambda"

PROJECT = ProjectConfig(DBT_DIR, install_dbt_deps=False)     # `dbt deps` by hand when packages.yml changes
PROFILE = ProfileConfig(profile_name="dbt_lambda", target_name="dev", profiles_yml_filepath=DBT_DIR / "profiles.yml")
EXECUTION = ExecutionConfig(dbt_executable_path=str(DBT_DIR / ".venv" / "bin" / "dbt"),
                            invocation_mode=InvocationMode.SUBPROCESS)


def render(select: list[str] | None = None, exclude: list[str] | None = None) -> RenderConfig:
    return RenderConfig(
        select=select or [],
        exclude=["package:elementary", *(exclude or [])],
        test_behavior=TestBehavior.AFTER_EACH,
        source_rendering_behavior=SourceRenderingBehavior.WITH_TESTS_OR_FRESHNESS,
        # a test with 2+ parents (e.g. ctrl + stg) becomes its own task after ALL of them, not glued to one
        should_detach_multiple_parents_tests=True,
        # required: with a socket host Cosmos builds a host-less postgres:// URI that Airflow 3 rejects
        emit_datasets=False,
    )
