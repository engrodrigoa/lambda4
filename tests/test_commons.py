import json

import pytest

from pipelines.commons import audit, dq
from pipelines.commons.ctrl import InMemoryCtrl
from pipelines.commons.logger import log_event
from pipelines.commons.refmonth import RefMonth, month_range
from pipelines.commons.task import EXIT_FAILED, EXIT_OK, EXIT_SKIPPED, TaskSkipped, run_task


# ---------------------------------------------------------------- RefMonth
@pytest.mark.parametrize("raw", ["2026-09", "202609", 202609, "2026/09", " 2026-09 "])
def test_refmonth_parse_forms(raw):
    r = RefMonth.parse(raw)
    assert (r.yyyymm, r.iso, str(r)) == (202609, "2026-09", "2026-09")


def test_refmonth_arithmetic_and_range():
    assert RefMonth.parse("2026-01").prev() == RefMonth(2025, 12)
    assert RefMonth.parse("2026-12").next() == RefMonth(2027, 1)
    assert [m.yyyymm for m in month_range(RefMonth(2026, 11), RefMonth(2027, 2))] == [202611, 202612, 202701, 202702]
    with pytest.raises(ValueError):
        RefMonth.parse("2026-13")
    with pytest.raises(ValueError):
        month_range(RefMonth(2026, 9), RefMonth(2026, 8))


# ---------------------------------------------------------------- TaskRun exit codes and dq severity
def _run(body, **kw):
    return run_task(body, pipeline="test", task_id="t1", layer="other", ref_month=RefMonth(2026, 9), **kw)


def test_task_exit_codes():
    assert _run(lambda t: None) == EXIT_OK

    def skipped(t):
        raise TaskSkipped("nothing new")

    assert _run(skipped) == EXIT_SKIPPED

    def boom(t):
        raise RuntimeError("boom")

    assert _run(boom) == EXIT_FAILED


def test_warn_check_continues_and_block_check_stops(monkeypatch):
    monkeypatch.setitem(dq.SEVERITY, "test.warn", dq.Severity.WARN)
    monkeypatch.setitem(dq.SEVERITY, "test.block", dq.Severity.BLOCK)
    seen = {}

    def body(t):
        t.check("test.warn", False, observed=1, threshold=0)
        seen["after_warn"] = True
        t.check("test.block", False)
        seen["after_block"] = True

    assert _run(body) == EXIT_FAILED
    assert seen == {"after_warn": True}


def test_unregistered_check_fails_loudly():
    assert _run(lambda t: t.check("nope.not_registered", True)) == EXIT_FAILED


def test_counters_are_validated():
    assert _run(lambda t: t.set(not_a_counter=1)) == EXIT_FAILED


# ---------------------------------------------------------------- logging
def test_json_log_line_has_context_and_redacts_secrets(isolated_env):
    def body(t):
        log_event(t.log, "custom_event", "hello", share_token="abc", rows=3)

    _run(body)
    files = (isolated_env / "logs" / "test").glob("*.jsonl")
    lines = [json.loads(x) for f in files for x in f.read_text().splitlines()]
    custom = next(x for x in lines if x["event"] == "custom_event")
    assert custom["run_id"] == "manual__test" and custom["task_id"] == "t1" and custom["ref_month"] == 202609
    assert custom["rows"] == 3 and custom["share_token"] == "***"
    end = next(x for x in lines if x["event"] == "task_end")
    assert end["status"] == "SUCCESS" and end["exit_code"] == 0


# ---------------------------------------------------------------- audit sink
def test_audit_sink_spools_when_database_is_down(tmp_path):
    sink = audit.AuditSink(enabled=True, dsn="host=/nonexistent dbname=x connect_timeout=1",
                           spool_dir=tmp_path / "spool")
    sink.write("dq_event", {"created_at": audit.utcnow_iso(), "run_id": "r", "task_id": "t", "pipeline": "p",
                            "ref_month": 202609, "check_name": "c", "severity": "WARN", "status": "FAIL",
                            "observed": 1, "threshold": 0, "message": "m", "context": {"a": 1}})
    lines = [json.loads(x) for f in (tmp_path / "spool").glob("*.jsonl") for x in f.read_text().splitlines()]
    assert sink.spooled == 1 and lines[0]["op"] == "dq_event" and lines[0]["params"]["context"] == {"a": 1}
    with pytest.raises(KeyError):
        sink.write("drop_everything", {})


# ---------------------------------------------------------------- ctrl (in memory)
def test_inmemory_ctrl_resume_and_validation():
    c = InMemoryCtrl()
    assert c.last_downloaded("rfb") is None
    c.upsert_ref({"pipeline": "rfb", "ref_month": 202608, "status": "DOWNLOADED"})
    c.upsert_ref({"pipeline": "rfb", "ref_month": 202609, "status": "PARTIAL"})
    assert c.last_downloaded("rfb") == 202608
    with pytest.raises(ValueError):
        c.upsert_ref({"pipeline": "rfb", "ref_month": 202610, "status": "WHATEVER"})
    with pytest.raises(KeyError):
        c.upsert_ref({"pipeline": "rfb", "ref_month": 202610, "status": "PENDING", "typo_col": 1})
