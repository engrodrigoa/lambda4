"""Chain tracing (pipelines/commons/chain.py): origin propagation through asset events, run status, audit SQL/view."""
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from pipelines.commons import chain
from pipelines.commons.audit import SQL

T0 = datetime(2026, 11, 9, 11, 0, tzinfo=UTC)
BRONZE = "anp_bronze.precos_raw"
SILVER = "anp_silver.anp__precos_coleta"


class Asset(str):
    @property
    def name(self):
        return str(self)


def ev(asset, dag_id, run_id, ts, extra=None):
    return SimpleNamespace(asset=SimpleNamespace(name=asset), source_dag_id=dag_id, source_run_id=run_id,
                           extra=extra or {}, timestamp=ts)


class FakeSink:
    def __init__(self):
        self.writes = []

    def write(self, op, params):
        self.writes.append((op, params))

    def write_many(self, op, rows):
        self.writes.extend((op, r) for r in rows)

    def flush(self):
        pass


@pytest.fixture
def sink(monkeypatch):
    s = FakeSink()
    monkeypatch.setattr(chain, "get_sink", lambda: s)
    pushed = []
    monkeypatch.setattr(chain, "push_gauges", lambda grouping, gauges: pushed.append((grouping, gauges)) or True)
    s.pushed = pushed
    return s


def ctx(dag_id, run_id, events=None, **extra):
    accessor = {}
    for e in events or []:
        accessor.setdefault(e.asset.name, []).append(e)
    dag_run = SimpleNamespace(dag_id=dag_id, run_id=run_id, start_date=T0,
                              run_type=SimpleNamespace(value="asset_triggered"))
    return {"dag_run": dag_run, "triggering_asset_events": accessor, **extra}


# ------------------------------------------------------------------------------------------------ pure logic
def test_manual_run_starts_its_own_chain():
    links = chain.chain_links([], "anp_dbt", "manual__1")
    assert links == [{"origin_dag_id": "anp_dbt", "origin_run_id": "manual__1", "dag_id": "anp_dbt",
                      "run_id": "manual__1", "upstream_asset": None, "upstream_dag_id": None,
                      "upstream_run_id": None, "triggered_at": None}]


def test_event_without_origins_makes_the_producer_the_origin():
    events = chain.normalize_events({BRONZE: [ev(BRONZE, "anp_pipeline", "sched__1", T0)]})
    (link,) = chain.chain_links(events, "anp_dbt", "asset__1")
    assert (link["origin_dag_id"], link["origin_run_id"]) == ("anp_pipeline", "sched__1")
    assert (link["upstream_asset"], link["upstream_run_id"], link["triggered_at"]) == (BRONZE, "sched__1", T0)


def test_origins_carried_in_extra_pass_through_and_dedupe():
    extra = {"chain_origins": [{"dag_id": "anp_pipeline", "run_id": "b1"}, {"dag_id": "anp_pipeline", "run_id": "b2"}]}
    first = ev(SILVER, "anp_dbt", "s1", T0, extra)
    retry = ev(SILVER, "anp_dbt", "s1", T0 + timedelta(minutes=9), extra)       # cleared + re-published
    events = chain.normalize_events({SILVER: [first, first, retry]})               # aliases repeat events
    assert len(events) == 2
    links = chain.chain_links(events, "anp_dbt_gold", "g1")
    assert sorted(lk["origin_run_id"] for lk in links) == ["b1", "b2"]
    assert all(lk["triggered_at"] == T0 for lk in links)                           # the earliest event made it wait
    assert chain.origins_extra(links) == extra


def test_two_bronze_loads_while_paused_give_two_origins():
    events = chain.normalize_events({BRONZE: [ev(BRONZE, "anp_pipeline", "b1", T0),
                                              ev(BRONZE, "anp_pipeline", "b2", T0 + timedelta(days=1))]})
    links = chain.chain_links(events, "anp_dbt", "asset__x")
    assert [lk["origin_run_id"] for lk in links] == ["b1", "b2"]


def test_run_summary_ignores_itself_and_detects_failures():
    states = {"chain_start": "success", "silver.stg_run": "success", "silver.stg_test": "failed",
              "publish_silver": "upstream_failed", "chain_end": None}
    s = chain.run_summary(states, "chain_end")
    assert s["status"] == "FAILED" and s["tasks_total"] == 4 and s["tasks_ok"] == 2
    assert s["failed_tasks"] == ["publish_silver", "silver.stg_test"]
    assert chain.run_summary({"a": "success", "chain_end": "running"}, "chain_end")["status"] == "SUCCESS"
    assert chain.run_summary({"a": "skipped"}, "chain_end")["status"] == "SKIPPED"


# ------------------------------------------------------------------------------------------------ Airflow callables
def test_chain_start_records_links_run_and_consumed_metric(sink):
    out = chain.chain_start(**ctx("anp_dbt", "asset__1", [ev(BRONZE, "anp_pipeline", "b1", T0)]))
    assert out == {"chain_origins": [{"dag_id": "anp_pipeline", "run_id": "b1"}]}
    ops = [op for op, _ in sink.writes]
    assert ops == ["pipeline_run_start", "pipeline_chain_upsert"]
    run = sink.writes[0][1]
    assert run["pipeline"] == "anp_dbt" and run["trigger"] == "asset_triggered" and run["started_at"] == T0
    assert sink.writes[1][1]["started_at"] == T0 and sink.writes[1][1]["emitted_at"] is None
    now = pytest.approx(datetime.now().timestamp(), abs=60)
    assert sink.pushed == [({"pipeline": "chain", "asset": BRONZE, "consumer": "anp_dbt"},
                            {"lambda_chain_consumed_timestamp_seconds": now})]


def test_publish_puts_origins_in_the_event_extra(sink):
    asset = Asset(SILVER)
    outlet = {asset: SimpleNamespace(extra={})}
    extra = chain.publish(asset, consumer="anp_dbt_gold")(
        **ctx("anp_dbt", "asset__1", [ev(BRONZE, "anp_pipeline", "b1", T0)], outlet_events=outlet))
    assert outlet[asset].extra == extra == {"chain_origins": [{"dag_id": "anp_pipeline", "run_id": "b1"}]}
    (op, row), = sink.writes
    assert op == "pipeline_chain_upsert" and row["emitted_asset"] == SILVER and row["emitted_at"] is not None
    assert sink.pushed[0][0] == {"pipeline": "chain", "asset": SILVER, "consumer": "anp_dbt_gold"}


def test_emit_callback_records_chain_start_and_never_raises(sink):
    cb = chain.emit_callback(SimpleNamespace(name=BRONZE), consumer="anp_dbt")
    cb(ctx("anp_pipeline", "sched__1"))
    (_, row), = sink.writes
    assert (row["origin_run_id"], row["run_id"], row["emitted_asset"]) == ("sched__1", "sched__1", BRONZE)
    cb({})                                                     # broken context: logged, not raised


def test_chain_end_closes_the_run_and_fails_a_failed_run(sink):
    def ti(states):
        return SimpleNamespace(task_id="chain_end",
                               get_task_states=lambda dag_id, run_ids: {run_ids[0]: states})

    ok = chain.chain_end(**ctx("anp_dbt_gold", "g1", ti=ti({"gold.x_run": "success", "chain_end": "running"})))
    assert ok["status"] == "SUCCESS"
    assert sink.writes[-1] == ("pipeline_run_close", {"run_id": "g1", "finished_at": sink.writes[-1][1]["finished_at"],
                                                      "status": "SUCCESS", "tasks_total": 1, "tasks_ok": 1,
                                                      "tasks_failed": 0})
    assert sink.pushed[-1][1]["lambda_pipeline_success"] == 1.0
    with pytest.raises(RuntimeError, match="gold.x_test"):
        chain.chain_end(**ctx("anp_dbt_gold", "g2", ti=ti({"gold.x_test": "failed"})))
    assert sink.writes[-1][1]["status"] == "FAILED" and sink.pushed[-1][1]["lambda_pipeline_success"] == 0.0


def test_pushgateway_base_adds_the_scheme():
    assert chain.pushgateway_base("127.0.0.1:9091") == "http://127.0.0.1:9091"
    assert chain.pushgateway_base("https://pg.example/") == "https://pg.example"


def test_push_gauges_refuses_unsafe_label_values(monkeypatch):
    monkeypatch.setattr(chain, "get_settings", lambda: SimpleNamespace(metrics_enabled=True,
                                                                       pushgateway_url="http://127.0.0.1:1"))
    assert chain.push_gauges({"asset": "a/b"}, {"x": 1.0}) is False


# ------------------------------------------------------------------------------------------------ SQL + view
DDL = Path(__file__).resolve().parents[1] / "infra" / "postgres" / "audit_schema.sql"


@pytest.mark.integration
def test_upsert_and_view_follow_a_chain_in_postgres():
    psycopg = pytest.importorskip("psycopg")
    try:
        conn = psycopg.connect(os.environ.get("LAMBDA_TEST_PG_DSN", "dbname=lambda host=/var/run/postgresql")
                               + " connect_timeout=3", autocommit=False)
    except psycopg.OperationalError as exc:
        pytest.skip(f"postgres unavailable: {exc}")
    try:
        conn.execute(DDL.read_text())
        b, s, g = "test__b", "test__s", "test__g"

        def run(run_id, dag_id, status, start, end=None):
            conn.execute("INSERT INTO audit.pipeline_run (run_id, pipeline, dag_id, status, started_at, finished_at) "
                         "VALUES (%s, %s, %s, %s, %s, %s)", (run_id, dag_id, dag_id, status, start, end))

        def upsert(**kw):
            row = {k: None for k in ("upstream_asset", "upstream_dag_id", "upstream_run_id", "triggered_at",
                                     "started_at", "emitted_asset", "emitted_at")}
            conn.execute(SQL["pipeline_chain_upsert"], row | kw)

        def view():
            return conn.execute("SELECT etapa, espera_silver_s, espera_gold_s, total_s, parada_ha_s IS NOT NULL "
                                "FROM audit.v_anp_cadeia WHERE origin_run_id = %s", (b,)).fetchone()

        base = {"origin_dag_id": "anp_pipeline", "origin_run_id": b}
        run(b, "anp", "SUCCESS", T0, T0 + timedelta(minutes=2))
        upsert(**base, dag_id="anp_pipeline", run_id=b, started_at=T0, emitted_asset=BRONZE,
               emitted_at=T0 + timedelta(minutes=1))
        assert view()[0] == "aguardando silver" and view()[4] is True

        silver = {**base, "dag_id": "anp_dbt", "run_id": s, "upstream_asset": BRONZE, "upstream_dag_id": "anp_pipeline",
                  "upstream_run_id": b, "triggered_at": T0 + timedelta(minutes=1)}
        run(s, "anp_dbt", "RUNNING", T0 + timedelta(minutes=4))
        upsert(**silver, started_at=T0 + timedelta(minutes=4))
        assert view()[:2] == ("silver em execucao", 180)
        upsert(**silver, started_at=T0 + timedelta(minutes=4), emitted_asset=SILVER,     # publish: merge, no dup
               emitted_at=T0 + timedelta(minutes=10))
        assert conn.execute("SELECT count(*) FROM audit.pipeline_chain WHERE run_id = %s", (s,)).fetchone()[0] == 1
        assert view()[0] == "aguardando gold"

        run(g, "anp_dbt_gold", "FAILED", T0 + timedelta(minutes=11), T0 + timedelta(minutes=20))
        upsert(**base, dag_id="anp_dbt_gold", run_id=g, upstream_asset=SILVER, upstream_dag_id="anp_dbt",
               upstream_run_id=s, triggered_at=T0 + timedelta(minutes=10), started_at=T0 + timedelta(minutes=11))
        assert view() == ("falhou na gold", 180, 60, 1200, False)
    finally:
        conn.rollback()
        conn.close()
