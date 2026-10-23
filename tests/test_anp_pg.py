"""anp03 against the real Postgres, inside ONE transaction that is rolled back: the DDL, COPY, quarantine, ctrl in the
same transaction, idempotent reload and the BLOCK on a refused file are exercised, and nothing is left behind.

Skipped when Postgres is unreachable, or when ctrl.source_file already holds ANP rows (a real environment in use)."""
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from pipelines.anp import anp03_bronze as anp03
from pipelines.commons.file_ctrl import PgFileCtrl, new_row
from pipelines.commons.task import EXIT_FAILED, EXIT_OK, run_task
from tests.test_anp import HEADER, ROW, csv_bytes, zip_bytes

pytestmark = pytest.mark.integration
DDL = Path(__file__).resolve().parents[1] / "infra" / "postgres" / "anp_schema.sql"


@pytest.fixture
def conn():
    psycopg = pytest.importorskip("psycopg")
    try:
        c = psycopg.connect("dbname=lambda host=/var/run/postgresql connect_timeout=3", autocommit=False)
    except psycopg.OperationalError as exc:
        pytest.skip(f"postgres unavailable: {exc}")
    try:
        exists = c.execute("SELECT to_regclass('ctrl.source_file') IS NOT NULL").fetchone()[0]
        if exists and c.execute("SELECT count(*) FROM ctrl.source_file WHERE pipeline = 'anp'").fetchone()[0]:
            pytest.skip("ctrl.source_file has ANP rows: not touching a real environment")
        c.execute(DDL.read_text())
        yield c
    finally:
        c.rollback()
        c.close()


def _row(key, path, fmt, sha, kind="semestral", **kw):
    fields = {"period_start": date(2014, 1, 1), "period_end": date(2014, 6, 30), **kw}
    return new_row(pipeline="anp", file_key=key, source_kind=kind, product_group="ca", url="http://x",
                   download_status="DOWNLOADED", local_path=str(path), local_bytes=path.stat().st_size,
                   file_format=fmt, sha256=sha, version=1, **fields)


def _load(conn, tmp_path):
    def body(task):
        anp03.load_files(task, conn=conn, ctrl=PgFileCtrl(conn), scratch=tmp_path / "scratch")

    return run_task(body, pipeline="anp", task_id="anp03_bronze", layer="bronze")


def test_bronze_load_is_transactional_and_idempotent(conn, tmp_path):
    ctrl = PgFileCtrl(conn)
    plain = tmp_path / "s1.csv"
    plain.write_bytes(csv_bytes("02/01/2014", "03/01/2014", extra=["SE;SP;CURTA"]))
    zipped = tmp_path / "s2.zip"
    zipped.write_bytes(zip_bytes("Preços.csv", csv_bytes("01/07/2014")))
    ctrl.save([_row("semestral/ca/2014-S1", plain, "csv", "a" * 64),
               _row("mensal/glp/2014-07", zipped, "zip", "b" * 64, kind="mensal")])

    assert _load(conn, tmp_path) == EXIT_OK
    count = conn.execute("SELECT _source_kind, count(*) FROM anp_bronze.precos_raw GROUP BY 1 ORDER BY 1").fetchall()
    assert count == [("mensal", 1), ("semestral", 2)]
    r = conn.execute("SELECT complemento, cnpj_da_revenda, valor_de_compra, _record_no, _file_sha256 "
                     "FROM anp_bronze.precos_raw WHERE _file_key = 'semestral/ca/2014-S1' "
                     "ORDER BY _record_no").fetchall()
    assert r[0] == ("KM 210;5", " 49.051.667/0001-02", None, 1, "a" * 64)      # raw values, NULL for empty
    q = conn.execute("SELECT _record_no, reason, raw_record FROM anp_bronze.precos_quarantine").fetchall()
    assert q == [(3, "field_count_3", "SE;SP;CURTA")]
    state = ctrl.load("anp")
    assert state["semestral/ca/2014-S1"]["bronze_status"] == "LOADED"
    assert (state["semestral/ca/2014-S1"]["bronze_rows"], state["semestral/ca/2014-S1"]["bronze_quarantined"]) == (2, 1)
    assert not list((tmp_path / "scratch").iterdir())                            # extracted ZIP removed
    events = conn.execute("SELECT event, rows FROM ctrl.source_file_event ORDER BY file_key").fetchall()
    assert events == [("LOADED", 1), ("LOADED", 2)]

    # a new version of the semester: its rows are REPLACED, never appended twice
    plain.write_bytes(csv_bytes("02/01/2014", "03/01/2014", "04/01/2014"))
    ctrl.save([{**state["semestral/ca/2014-S1"], "bronze_status": "PENDING", "sha256": "c" * 64, "version": 2}])
    assert _load(conn, tmp_path) == EXIT_OK
    r = conn.execute("SELECT count(*), min(_file_sha256) FROM anp_bronze.precos_raw "
                     "WHERE _file_key = 'semestral/ca/2014-S1'").fetchone()
    assert r == (3, "c" * 64)
    assert conn.execute("SELECT count(*) FROM anp_bronze.precos_quarantine").fetchone()[0] == 0


def test_refused_file_blocks_after_loading_the_others(conn, tmp_path):
    ctrl = PgFileCtrl(conn)
    good = tmp_path / "good.csv"
    good.write_bytes(csv_bytes("02/01/2014"))
    no_col = tmp_path / "nocol.csv"
    no_col.write_text(";".join(HEADER.split(";")[:-1]) + "\n" + ";".join(ROW.format(d="02/01/2014").split(";")[:-1]))
    ctrl.save([_row("semestral/ca/2014-S1", good, "csv", "a" * 64),
               _row("semestral/ca/2014-S2", no_col, "csv", "d" * 64)])

    assert _load(conn, tmp_path) == EXIT_FAILED                                  # BLOCK: bronze.contract_columns
    state = ctrl.load("anp")
    assert state["semestral/ca/2014-S1"]["bronze_status"] == "LOADED"
    assert state["semestral/ca/2014-S2"]["bronze_status"] == "FAILED"
    assert "bandeira" in state["semestral/ca/2014-S2"]["last_error"]


def test_batch_profile_and_matrix(conn, tmp_path):
    """Survey batches = file x collection month; the matrix pivots the average price per product."""
    ctrl = PgFileCtrl(conn)
    path = tmp_path / "s2.csv"
    etanol = ROW.format(d="15/08/2014").replace("GASOLINA", "ETANOL").replace("2,899", "1,999")
    path.write_bytes(csv_bytes("01/07/2014", "02/07/2014", extra=[
        ROW.format(d="03/07/2014").replace("2,899", "3,101").replace("49.051.667/0001-02", "11.111.111/0001-11"),
        etanol, ROW.format(d="16/08/2014").replace("2,899", "")]))
    ctrl.save([_row("semestral/ca/2014-S2", path, "csv", "e" * 64, period_start=date(2014, 7, 1),
                    period_end=date(2014, 12, 31))])
    assert _load(conn, tmp_path) == EXIT_OK

    prof = {(m, p): (rows, stations, avg, missing) for m, p, rows, stations, avg, missing in conn.execute(
        "SELECT batch_month, produto, rows, stations, price_avg, price_missing FROM anp_bronze.batch_profile "
        "WHERE file_key = 'semestral/ca/2014-S2'").fetchall()}
    assert prof[(201407, "*")] == (3, 2, Decimal("2.9663"), 0)
    assert prof[(201407, "GASOLINA")] == (3, 2, Decimal("2.9663"), 0)
    assert prof[(201408, "*")][0] == 2 and prof[(201408, "*")][3] == 1          # one empty price
    assert prof[(201408, "ETANOL")][2] == Decimal("1.9990")
    m = conn.execute("SELECT batch, rows, stations, brands, gasolina, etanol, glp FROM anp_bronze.v_batch_matrix "
                     "WHERE file_key = 'semestral/ca/2014-S2' ORDER BY batch").fetchall()
    assert m == [("2014-07", 3, 2, 1, Decimal("2.9663"), None, None),
                 ("2014-08", 2, 1, 1, None, Decimal("1.9990"), None)]

    # --reprofile recomputes from bronze without touching the data
    conn.execute("DELETE FROM anp_bronze.batch_profile")

    def body(task):
        anp03.reprofile_files(task, conn=conn, ctrl=PgFileCtrl(conn))

    assert run_task(body, pipeline="anp", task_id="anp03_bronze", layer="bronze") == EXIT_OK
    assert conn.execute("SELECT count(*) FROM anp_bronze.batch_profile WHERE produto = '*'").fetchone()[0] == 2
