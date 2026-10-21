"""rfb02 on fake ZIPs reproducing the real RFB quirks (doubled quotes, ';' and newlines inside quotes)."""
import csv
import io
import json
import zipfile

from pipelines.commons import settings
from pipelines.commons.refmonth import RefMonth
from pipelines.commons.task import EXIT_FAILED, EXIT_OK, run_task
from pipelines.rfb import rfb02_extract as rfb02

REF = RefMonth(2026, 9)
ROWS = [
    '"0001";"GUAJARA-MIRIM"\n',
    '"0002";"BOX ALA  ""D"""\n',          # doubled quotes
    '"0003";"a;b"\n',                     # separator inside quotes
    '"0004";"RIZZ\n"\n',                  # newline inside quotes (multiline record)
    '"0005";"SÃO PAULO"\n',               # latin-1 -> utf-8
]


def _landing(tmp_path, content: str, name: str = "Municipios.zip"):
    month = tmp_path / "landing" / f"ref={REF.yyyymm}"
    month.mkdir(parents=True)
    with zipfile.ZipFile(month / name, "w") as z:
        z.writestr("F.K03200$Z.D60913.MUNICCSV", content.encode("iso-8859-1"))
    return month


def _run(tmp_path, month, entity="municipios"):
    def body(task):
        rfb02.extract_entity(task, REF, entity, month_dir=month, workers=1, target_bytes=40)

    return run_task(body, pipeline="rfb", task_id=f"rfb02_extract.{entity}", layer="extract", ref_month=REF)


def test_extract_counts_transcodes_and_writes_manifest_last(tmp_path, monkeypatch):
    monkeypatch.setenv("LAMBDA_SCRATCH_DIR", str(tmp_path / "scratch"))
    settings.get_settings.cache_clear()
    month = _landing(tmp_path, "".join(ROWS))
    assert _run(tmp_path, month) == EXIT_OK

    out = rfb02.entity_dir(REF, "municipios")
    manifest = json.loads((out / rfb02.MANIFEST).read_text())
    assert manifest["records"] == 5 and manifest["physical_lines"] == 6 and manifest["multiline_records"] == 1
    assert manifest["quarantined_lines"] == 0 and manifest["encoding"] == "utf-8"
    chunks = [c["path"] for f in manifest["files"] for c in f["chunks"]]
    assert len(chunks) > 1                                      # target_bytes=40 forces several chunks
    rows = []
    for path in chunks:
        rows += list(csv.reader(io.StringIO(open(path, encoding="utf-8", newline="").read(), newline=""),
                                delimiter=";"))
    assert [r[1] for r in rows] == ["GUAJARA-MIRIM", 'BOX ALA  "D"', "a;b", "RIZZ\n", "SÃO PAULO"]


def test_extract_quarantines_unbalanced_quote_line(tmp_path, monkeypatch):
    monkeypatch.setenv("LAMBDA_SCRATCH_DIR", str(tmp_path / "scratch"))
    settings.get_settings.cache_clear()
    bad = '"9999";"broken "quote"\n'                             # 5 quotes: odd
    month = _landing(tmp_path, bad + "".join(ROWS) * 20)
    assert _run(tmp_path, month) == EXIT_OK                     # WARN check: the task continues
    out = rfb02.entity_dir(REF, "municipios")
    assert json.loads((out / rfb02.MANIFEST).read_text())["records"] == 100
    q = [json.loads(x) for x in (out / rfb02.QUARANTINE).read_text().splitlines()]
    assert len(q) == 1 and q[0]["reason"] == "unbalanced_quotes" and q[0]["line"] == 1


def test_missing_zip_blocks(tmp_path, monkeypatch):
    monkeypatch.setenv("LAMBDA_SCRATCH_DIR", str(tmp_path / "scratch"))
    settings.get_settings.cache_clear()
    month = tmp_path / "landing" / "empty"
    month.mkdir(parents=True)
    assert _run(tmp_path, month, entity="socios") == EXIT_FAILED
