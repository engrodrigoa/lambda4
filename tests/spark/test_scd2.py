"""SCD2 engine on local Spark + Delta: baseline, change, type 1, absence, as-of, rollback and re-apply."""
import pytest

from pipelines.commons import scd2
from pipelines.commons.refmonth import RefMonth

pytestmark = pytest.mark.spark
M1, M2 = RefMonth(2026, 1), RefMonth(2026, 2)
SCHEMA = "id string, name string, city string, phone string"


def _spec(lake):
    return scd2.Scd2Spec(table="t.people_hist", location=(lake / "t" / "people_hist").as_uri(), key_cols=("id",),
                         tracked_cols=("name", "city"), type1_cols=("phone",))


def _state(spark):
    rows = spark.table("t.people_hist").select("id", "name", "phone", "valid_from", "valid_to", "is_current",
                                               "change_type", "closed_reason").collect()
    return sorted(tuple(r) for r in rows)


def test_scd2_lifecycle(spark, lake):
    spec = _spec(lake)
    m1 = spark.createDataFrame([("1", "ANA", "SP", "111"), ("2", "BETO", "RJ", "222"), ("3", "CAIO", "MG", None)],
                               SCHEMA)
    scd2.ensure_table(spark, spec, m1.schema)
    r1 = scd2.apply_month(spark, spec, m1, M1, "run_m1")
    assert (r1["new_keys"], r1["changed_keys"], r1["absent_keys"]) == (3, 0, 0)

    m2 = spark.createDataFrame([("1", "ANA MARIA", "SP", "111"),      # tracked change -> new version
                                ("2", "BETO", "RJ", "999"),           # type-1 change -> in place
                                ("4", "DUDA", "BA", "444")],          # new key; key 3 is absent
                               SCHEMA)
    r2 = scd2.apply_month(spark, spec, m2, M2, "run_m2")
    assert (r2["new_keys"], r2["changed_keys"], r2["absent_keys"], r2["type1_updates"], r2["unchanged_keys"]) \
        == (1, 1, 1, 1, 0)

    state = _state(spark)
    assert ("1", "ANA", "111", 202601, 202601, False, "NEW", "CHANGED") in state
    assert ("1", "ANA MARIA", "111", 202602, None, True, "CHANGED", None) in state
    assert ("2", "BETO", "999", 202601, None, True, "NEW", None) in state          # no new version for type 1
    assert ("3", "CAIO", None, 202601, 202601, False, "NEW", "ABSENT") in state
    assert ("4", "DUDA", "444", 202602, None, True, "NEW", None) in state
    assert len(state) == 5

    asof1 = {r["id"]: r["name"] for r in scd2.as_of(spark, spec.table, M1).collect()}
    asof2 = {r["id"]: r["name"] for r in scd2.as_of(spark, spec.table, M2).collect()}
    assert asof1 == {"1": "ANA", "2": "BETO", "3": "CAIO"}
    assert asof2 == {"1": "ANA MARIA", "2": "BETO", "4": "DUDA"}

    # re-running the last month is idempotent: rollback + apply gives the same history
    rb = scd2.rollback(spark, spec, M2)
    assert rb == {"deleted_versions": 2, "reopened_versions": 2}
    assert {r["id"] for r in spark.table(spec.table).where("is_current").collect()} == {"1", "2", "3"}
    scd2.apply_month(spark, spec, m2, M2, "run_m2_again")
    assert _state(spark) == state


def test_null_safe_hash_and_unknown_columns(spark, lake):
    spec = _spec(lake)
    df = spark.createDataFrame([("1", None, "SP", None), ("2", "SP", None, None)], SCHEMA)
    hashes = [r["row_hash"] for r in scd2.prepare_source(df, spec).orderBy("id").collect()]
    assert hashes[0] != hashes[1]                       # (NULL, 'SP') vs ('SP', NULL)
    with pytest.raises(KeyError):
        scd2.prepare_source(df.withColumn("extra", df.id), spec)


def test_rollback_refuses_older_month(spark, lake):
    spec = _spec(lake)
    df = spark.createDataFrame([("1", "ANA", "SP", "1")], SCHEMA)
    scd2.ensure_table(spark, spec, df.schema)
    scd2.apply_month(spark, spec, df, M1, "r1")
    scd2.apply_month(spark, spec, df, M2, "r2")
    with pytest.raises(RuntimeError):
        scd2.rollback(spark, spec, M1)
