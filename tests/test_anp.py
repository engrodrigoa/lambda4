"""ANP pipeline without network or database: contract, page parsing, discovery, downloads, the reader."""
import hashlib
import io
import zipfile
from datetime import date, datetime

import pytest

from pipelines.anp import anp01_discover as anp01
from pipelines.anp import anp02_download as anp02
from pipelines.anp import contract, source
from pipelines.anp.reader import csv_path, read_records
from pipelines.commons.file_ctrl import InMemoryFileCtrl, new_row
from pipelines.commons.http import Fetcher
from pipelines.commons.task import EXIT_OK, EXIT_SKIPPED, TaskRun, run_task

BASE = "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc"
HEADER = ("﻿Regiao - Sigla;Estado - Sigla;Municipio;Revenda;CNPJ da Revenda;Nome da Rua;Numero Rua;Complemento;"
          "Bairro;Cep;Produto;Data da Coleta;Valor de Venda;Valor de Compra;Unidade de Medida;Bandeira")
ROW = ("SE;SP;GUARULHOS;AUTO POSTO X LTDA; 49.051.667/0001-02;RODOVIA DUTRA;S/N;\"KM 210;5\";BONSUCESSO;07178-580;"
       "GASOLINA;{d};2,899;;R$ / litro;PETROBRAS")


def csv_bytes(*dates: str, extra: list[str] | None = None, encoding: str = "utf-8") -> bytes:
    lines = [HEADER] + [ROW.format(d=d) for d in dates] + (extra or [])
    text = "\r\n".join(lines) + "\r\n"
    return text.encode(encoding) if encoding == "utf-8" else text.replace("﻿", "").encode(encoding)


def zip_bytes(member: str, payload: bytes) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr(member, payload)
    return buf.getvalue()


PAGE = f"""<html><body><div id="content">
<p>Metadados (atualizado em 6/3/2026)</p>
<h3>Combustíveis automotivos</h3><ul>
  <li><a href="{BASE}/dsas/ca/ca-2014-01.csv">1º semestre de 2014</a></li>
  <li><a href="{BASE}/dsas/ca/ca-2014-02.csv">2º semestre de 2014</a></li>
  <li><a href="{BASE}/dsas/ca/ca-2015-02.zip">2º semestre de 2015</a></li>
  <li><a href="{BASE}/dsas/ca/precos-semestrais-ca.zip">Todos</a></li></ul>
<h3>GLP</h3><ul><li><a href="{BASE}/dsas/glp/precos-semestrais-glp2021-01.csv">1º semestre de 2021</a></li></ul>
<h3>Mensal</h3><ul>
  <li><a href="{BASE}/dsan/2026/02-cados-abertos-preco-gasolina-etanol.csv">Fevereiro de 2026</a></li>
  <li><a href="{BASE}/dsan/2026/03-dados-abertos-precos-gasolina-etanol.csv">Março de 2026</a></li>
  <li><a href="{BASE}/dsan/2026/05-dados-abertos-precos-gasolina-etanol.csv">Maio de 2026</a></li>
  <li><a href="{BASE}/dsan/2023/precos-glp-12.csv">Dezembro de 2023</a></li>
  <li><a href="/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/dsan/2023/precos-glp-12.csv">dup</a></li>
  <li><a href="{BASE}/dsan/2023/precos-querosene-01.csv">Janeiro de 2023</a></li></ul>
<p>Quatro últimas semanas (atualizado em 20/10/2026)</p><ul>
  <li><a href="{BASE}/qus/ultimas-4-semanas-glp.csv">GLP P13</a></li>
  <li><a href="{BASE}/qus/ultimas-4-semanas-diesel-gnv.csv">Óleo Diesel</a></li></ul>
<a href="{BASE}/metadados.pdf">pdf</a>
</div></body></html>"""


# ----------------------------------------------------------------------------------------------------- contract
@pytest.mark.parametrize("url,text,key,start,end", [
    (f"{BASE}/dsas/ca/ca-2014-01.csv", "1º semestre de 2014", "semestral/ca/2014-S1", date(2014, 1, 1),
     date(2014, 6, 30)),
    (f"{BASE}/dsas/ca/ca-2025-02.zip", "", "semestral/ca/2025-S2", date(2025, 7, 1), date(2025, 12, 31)),
    (f"{BASE}/dsas/glp/precos-semestrais-glp-2022-01.csv", "", "semestral/glp/2022-S1", date(2022, 1, 1),
     date(2022, 6, 30)),
    (f"{BASE}/dsan/2026/02-cados-abertos-preco-gasolina-etanol.csv", "Fevereiro de 2026",
     "mensal/gasolina-etanol/2026-02", date(2026, 2, 1), date(2026, 2, 28)),
    (f"{BASE}/dsan/2026/06-dados-abertos-precos-2026-06-glp.csv", "Junho de 2026", "mensal/glp/2026-06",
     date(2026, 6, 1), date(2026, 6, 30)),
    (f"{BASE}/dsan/2023/precos-diesel-gnv-12.csv", "", "mensal/diesel-gnv/2023-12", date(2023, 12, 1),
     date(2023, 12, 31)),
    (f"{BASE}/qus/ultimas-4-semanas-glp.csv", "GLP P13", "semanal/glp/2026-10-20", date(2026, 9, 23),
     date(2026, 10, 20)),
])
def test_classify(url, text, key, start, end):
    f = contract.classify(url, text, weekly_date=date(2026, 10, 20))
    assert (f.file_key, f.period_start, f.period_end) == (key, start, end)


def test_classify_refuses_what_it_does_not_know():
    assert contract.classify(f"{BASE}/dsan/2023/precos-querosene-01.csv", "Janeiro de 2023") is None
    # the link text says another year than the folder: never guess
    assert contract.classify(f"{BASE}/dsan/2026/precos-glp-01.csv", "Janeiro de 2025") is None
    # weekly files need the page's update date
    assert contract.classify(f"{BASE}/qus/ultimas-4-semanas-glp.csv", "GLP") is None
    assert contract.is_ignored(f"{BASE}/dsas/ca/precos-semestrais-ca.zip")


def test_normalize_header():
    names = [contract.normalize_header(h) for h in HEADER.split(";")]
    assert tuple(names) == contract.COLUMNS
    assert contract.normalize_header(" Região - Sigla ") == "regiao_sigla"


# ----------------------------------------------------------------------------------------------------- page
def test_parse_page():
    cat = source.parse_page(PAGE)
    keys = [f.file_key for f in cat.files]
    assert cat.weekly_date == date(2026, 10, 20)
    assert keys == ["mensal/gasolina-etanol/2026-02", "mensal/gasolina-etanol/2026-03",
                    "mensal/gasolina-etanol/2026-05", "mensal/glp/2023-12", "semanal/diesel-gnv/2026-10-20",
                    "semanal/glp/2026-10-20", "semestral/ca/2014-S1", "semestral/ca/2014-S2", "semestral/ca/2015-S2",
                    "semestral/glp/2021-S1"]
    assert [u.rsplit("/", 1)[1] for u, _ in cat.unclassified] == ["precos-querosene-01.csv"]
    assert cat.ignored == [f"{BASE}/dsas/ca/precos-semestrais-ca.zip"]
    assert cat.duplicates == []                 # same URL twice (absolute and relative) is one link
    assert source.weekly_date_of("sem data") is None


def test_period_gaps():
    gaps = anp01.period_gaps(source.parse_page(PAGE).files)
    assert gaps == ["mensal/gasolina-etanol/2026-04", "semestral/ca/2015-S1"]


# ----------------------------------------------------------------------------------------------------- discovery
def _file(key, kind="semestral", group="ca", start=date(2014, 1, 1), end=date(2014, 6, 30)):
    return contract.SourceFile(key, kind, group, start, end, f"{BASE}/{key}.csv", "")


def test_reconcile_new_changed_unlisted_relisted():
    now = datetime(2026, 10, 22)
    existing = {
        "semestral/ca/2014-S1": new_row(pipeline="anp", file_key="semestral/ca/2014-S1", source_kind="semestral",
                                        product_group="ca", download_status="DOWNLOADED", local_bytes=100,
                                        bronze_status="LOADED", listed=True, version=1),
        "semestral/ca/2013-S2": new_row(pipeline="anp", file_key="semestral/ca/2013-S2", source_kind="semestral",
                                        product_group="ca", download_status="DOWNLOADED", listed=True),
        "semanal/glp/2026-10-13": new_row(pipeline="anp", file_key="semanal/glp/2026-10-13", source_kind="semanal",
                                          product_group="glp", download_status="DOWNLOADED", listed=True),
        "semestral/ca/2014-S2": new_row(pipeline="anp", file_key="semestral/ca/2014-S2", source_kind="semestral",
                                        product_group="ca", listed=False),
    }
    listed = [_file("semestral/ca/2014-S1"), _file("semestral/ca/2014-S2", start=date(2014, 7, 1)),
              _file("mensal/glp/2026-09", "mensal", "glp", date(2026, 9, 1), date(2026, 9, 30))]
    rows, events, report = anp01.reconcile(existing, listed, now=now, run_id="r1",
                                           remote_sizes={"semestral/ca/2014-S1": 120})
    by_key = {r["file_key"]: r for r in rows}
    assert report == {"new": ["mensal/glp/2026-09"], "changed": ["semestral/ca/2014-S1"],
                      "unlisted": ["semestral/ca/2013-S2"], "relisted": ["semestral/ca/2014-S2"]}
    assert by_key["semestral/ca/2014-S1"]["download_status"] == "PENDING"
    assert by_key["semestral/ca/2014-S1"]["remote_bytes"] == 120
    assert by_key["mensal/glp/2026-09"]["download_status"] == "PENDING"
    assert by_key["semanal/glp/2026-10-13"]["listed"] is False       # unlisted, but not reported (weekly cycle)
    assert sorted(e["event"] for e in events) == ["CHANGED_AT_SOURCE", "DISCOVERED", "RELISTED", "UNLISTED",
                                                  "UNLISTED"]


def test_needs_recheck_only_recent_or_full():
    old = new_row(pipeline="anp", file_key="k", source_kind="semestral", download_status="DOWNLOADED",
                  period_end=date(2014, 6, 30))
    recent = {**old, "period_end": date(2026, 9, 30)}
    today = date(2026, 10, 22)
    assert not anp01.needs_recheck(old, today, 120, False) and anp01.needs_recheck(old, today, 120, True)
    assert anp01.needs_recheck(recent, today, 120, False)
    assert not anp01.needs_recheck({**recent, "source_kind": "semanal"}, today, 120, True)
    assert not anp01.needs_recheck({**recent, "download_status": "PENDING"}, today, 120, True)


# ----------------------------------------------------------------------------------------------------- fake source
class _Resp:
    def __init__(self, status, body=b"", ctype="text/csv", total=None):
        self.status_code, self.body = status, body
        self.headers = {"Content-Type": ctype}
        if status == 200:
            self.headers["Content-Length"] = str(len(body))
        if status == 206:
            self.headers["Content-Range"] = f"bytes 0-0/{total}"

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def iter_content(self, chunk_size):
        yield self.body


class FakeSession:
    """url suffix -> body (bytes) or status (int). Ranged requests answer 206 with the body size."""

    def __init__(self, routes):
        self.routes, self.calls = routes, []

    def _answer(self, url, headers=None):
        self.calls.append(url)
        for suffix, answer in self.routes.items():
            if url.endswith(suffix):
                if isinstance(answer, int):
                    return _Resp(answer)
                if headers and "Range" in headers:
                    return _Resp(206, total=len(answer))
                return _Resp(200, answer)
        return _Resp(404)

    def request(self, method, url, headers=None, **kw):
        return self._answer(url, headers)

    def get(self, url, **kw):
        return self._answer(url, kw.get("headers"))


def _fetcher(session):
    return Fetcher(session=session, retries=2, pause_s=0, sleep=lambda s: None)


def _task_run(fn, task_id="anp_test"):
    return run_task(fn, pipeline="anp", task_id=task_id, layer="landing")


def test_discover_then_up_to_date_is_skipped():
    ctrl = InMemoryFileCtrl()
    fetcher = _fetcher(FakeSession({}))
    out = {}

    def body(task: TaskRun):
        out.update(anp01.discover(task, ctrl=ctrl, fetcher=fetcher, html=PAGE, today=date(2026, 10, 22)))

    assert _task_run(body) == EXIT_OK
    assert out["new"] == 10 and out["pending_download"] == 10
    assert {e["event"] for e in ctrl.events} == {"DISCOVERED"}

    # everything downloaded and loaded -> nothing to do -> exit 99 (downstream skipped)
    for r in ctrl.rows.values():
        r.update(download_status="DOWNLOADED", bronze_status="LOADED")
    assert _task_run(body) == EXIT_SKIPPED


# ----------------------------------------------------------------------------------------------------- downloads
def _pending(key, url_suffix, kind="semestral", group="ca"):
    return new_row(pipeline="anp", file_key=key, source_kind=kind, product_group=group, period_start=date(2014, 1, 1),
                   period_end=date(2014, 6, 30), url=f"{BASE}/{url_suffix}")


def test_download_formats_versions_duplicates_and_failures(tmp_path):
    csv1 = csv_bytes("02/01/2014")
    zipped = zip_bytes("Preços semestrais - AUTOMOTIVOS_2014.02.csv", csv_bytes("01/07/2014"))
    ctrl = InMemoryFileCtrl([
        _pending("semestral/ca/2014-S1", "dsas/ca/ca-2014-01.csv"),
        _pending("semestral/ca/2014-S2", "dsas/ca/ca-2014-02.csv"),            # a ZIP behind a .csv name
        _pending("semanal/glp/2026-10-20", "qus/ultimas-4-semanas-glp.csv", "semanal", "glp"),
        _pending("semestral/ca/2015-S1", "dsas/ca/ca-2015-01.csv"),            # 404
        _pending("semestral/ca/2015-S2", "dsas/ca/ca-2015-02.csv"),            # HTML error page
    ])
    session = FakeSession({"ca-2014-01.csv": csv1, "ca-2014-02.csv": zipped, "semanas-glp.csv": csv1,
                           "ca-2015-02.csv": b"<html>oops</html>"})
    landing = tmp_path / "landing"

    def body(task):
        anp02.download_files(task, ctrl=ctrl, fetcher=_fetcher(session), landing_root=landing, free_bytes=10 ** 12)

    assert _task_run(body, "anp02_download") == EXIT_OK
    rows = ctrl.load("anp")
    s1, s2 = rows["semestral/ca/2014-S1"], rows["semestral/ca/2014-S2"]
    sha1 = hashlib.sha256(csv1).hexdigest()
    assert s1["download_status"] == "DOWNLOADED" and s1["sha256"] == sha1 and s1["version"] == 1
    assert s1["local_path"].endswith(f"anp/semestral/ca/2014-S1__{sha1[:12]}.csv") and s1["bronze_status"] == "PENDING"
    assert s2["file_format"] == "zip" and s2["local_path"].endswith(".zip")
    weekly = rows["semanal/glp/2026-10-20"]
    assert weekly["duplicate_of"] == "semestral/ca/2014-S1" and weekly["bronze_status"] == "SKIPPED_DUPLICATE"
    assert rows["semestral/ca/2015-S1"]["download_status"] == "MISSING_AT_SOURCE"
    assert rows["semestral/ca/2015-S2"]["download_status"] == "FAILED"
    assert not list(landing.rglob("*.part")) and not list(landing.rglob("*.download"))
    events = sorted(e["event"] for e in ctrl.events)
    assert events == ["DOWNLOADED", "DOWNLOADED", "DOWNLOAD_FAILED", "DUPLICATE_CONTENT", "MISSING_AT_SOURCE"]

    # republished with the SAME content: no new version, bronze state untouched
    ctrl.rows[("anp", "semestral/ca/2014-S1")].update(download_status="PENDING", bronze_status="LOADED")
    for k in ("semestral/ca/2015-S1", "semestral/ca/2015-S2"):
        ctrl.rows[("anp", k)]["listed"] = False
    assert _task_run(body, "anp02_download") == EXIT_OK
    again = ctrl.load("anp")["semestral/ca/2014-S1"]
    assert again["version"] == 1 and again["bronze_status"] == "LOADED" and again["download_attempts"] == 2

    # nothing pending -> skipped
    assert _task_run(body, "anp02_download") == EXIT_SKIPPED


def test_download_all_failed_fails_the_task(tmp_path):
    ctrl = InMemoryFileCtrl([_pending("semestral/ca/2015-S1", "dsas/ca/ca-2015-01.csv")])

    def body(task):
        anp02.download_files(task, ctrl=ctrl, fetcher=_fetcher(FakeSession({})), landing_root=tmp_path)

    assert _task_run(body, "anp02_download") == 1


def test_is_valid_source_file(tmp_path):
    good, bad, two = tmp_path / "a", tmp_path / "b", tmp_path / "c"
    good.write_bytes(zip_bytes("x.csv", csv_bytes("01/01/2020")))
    bad.write_bytes(b"PK\x03\x04 truncated")
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("a.csv", csv_bytes())
        z.writestr("b.csv", csv_bytes())
    two.write_bytes(buf.getvalue())
    assert anp02.is_valid_source_file(good)
    assert not anp02.is_valid_source_file(bad) and not anp02.is_valid_source_file(two)


# ----------------------------------------------------------------------------------------------------- reader
def test_reader_fast_path(tmp_path):
    p = tmp_path / "f.csv"
    p.write_bytes(csv_bytes("02/01/2014", "03/01/2014"))
    r = read_records(p)
    assert (r.path, r.encoding, r.records, r.quarantine) == ("fast", "utf-8-sig", 2, [])
    assert r.df.columns == ["_record_no", *contract.COLUMNS]
    first = r.df.row(0, named=True)
    assert first["complemento"] == "KM 210;5" and first["cnpj_da_revenda"] == " 49.051.667/0001-02"
    assert first["valor_de_compra"] is None and first["bandeira"] == "PETROBRAS"       # no trailing \r
    assert r.df["_record_no"].to_list() == [1, 2]


def test_reader_quarantines_wrong_field_counts(tmp_path):
    p = tmp_path / "f.csv"
    short = "SE;SP;GUARULHOS;POSTO"
    long_ = ROW.format(d="04/01/2014") + ";SOBRA"
    p.write_bytes(csv_bytes("02/01/2014", extra=[short, ROW.format(d="03/01/2014"), long_]))
    r = read_records(p)
    assert r.path == "python" and r.records == 4 and r.df.height == 2
    assert [(q["_record_no"], q["reason"]) for q in r.quarantine] == [(2, "field_count_4"), (4, "field_count_17")]
    assert r.df["_record_no"].to_list() == [1, 3]
    assert r.df.row(0, named=True)["valor_de_compra"] is None          # same null semantics as the fast path


def test_reader_cp1252_fallback(tmp_path):
    p = tmp_path / "f.csv"
    p.write_bytes(csv_bytes("02/01/2014", encoding="cp1252").replace(b"PETROBRAS", "SÃO JOSÉ".encode("cp1252")))
    r = read_records(p)
    assert r.encoding == "cp1252" and r.df.row(0, named=True)["bandeira"] == "SÃO JOSÉ"


def test_csv_path_extracts_zip_and_cleans_up(tmp_path):
    z = tmp_path / "f.zip"
    z.write_bytes(zip_bytes("Preços.csv", csv_bytes("02/01/2014")))
    scratch = tmp_path / "scratch"
    with csv_path(z, "zip", scratch, "semestral__ca__2014-S1") as p:
        assert p.parent == scratch and read_records(p).records == 1
    assert not list(scratch.iterdir())
    with pytest.raises(RuntimeError), csv_path(z, "zip", scratch, "boom"):
        raise RuntimeError("load failed")
    assert not list(scratch.iterdir())
