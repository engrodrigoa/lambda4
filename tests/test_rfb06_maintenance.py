"""rfb06: OPTIMIZE only for tables with many small files (pure decision, no Spark)."""
import pytest

from pipelines.rfb.rfb06_maintenance import MIB, OPTIMIZE_MIN_FILES, SMALL_FILE_MB, SPARK_CONF, optimize_decision


@pytest.mark.parametrize(("files", "size_mb", "expected"), [
    (96, 5316, True),            # 2026-09 silver estabelecimento_hist: 96 files of ~55 MiB
    (2688, 4441, True),          # 2026-09 gold estabelecimento_atual: 2688 files of ~1.7 MiB
    (25, 0.01, True),            # ctrl table after many small commits (cheap to compact)
    (36, 3655, False),           # 2026-09 bronze estabelecimentos: ~100 MiB files are fine
    (21, 5316, False),           # silver written with optimized writes (~250 MiB files)
    (OPTIMIZE_MIN_FILES - 1, 1, False),   # too few files to bother
    (0, 0, False),               # empty table
    (None, None, False),
])
def test_optimize_decision_auto(files, size_mb, expected):
    run, why = optimize_decision(files, None if size_mb is None else int(size_mb * MIB))
    assert run is expected and why


def test_optimize_decision_modes_override():
    assert optimize_decision(1, 1, "always")[0] is True
    assert optimize_decision(10_000, 10_000, "never")[0] is False


def test_optimize_sizes_match_the_writers_target():
    from pipelines.commons.spark import DELTA_OPTIMIZED_WRITES, DELTA_TARGET_FILE_MB

    assert int(SPARK_CONF["spark.databricks.delta.optimize.maxFileSize"]) == DELTA_TARGET_FILE_MB * MIB
    assert int(SPARK_CONF["spark.databricks.delta.optimize.minFileSize"]) == SMALL_FILE_MB * MIB
    assert SMALL_FILE_MB < DELTA_TARGET_FILE_MB
    assert DELTA_OPTIMIZED_WRITES["spark.databricks.delta.optimizeWrite.enabled"] == "true"
