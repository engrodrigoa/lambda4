"""rfb01 against a fake flaky source: contract, resume, explicit causes, control state."""
import hashlib
import io
import zipfile

import pytest

from pipelines.commons.ctrl import InMemoryCtrl
from pipelines.commons.http import Fetcher
from pipelines.commons.refmonth import RefMonth
from pipelines.commons.task import EXIT_FAILED, EXIT_OK, EXIT_SKIPPED, run_task
from pipelines.rfb import rfb01_download as rfb01
from pipelines.rfb.contract import EXPECTED_FILES, entity_of


def _zip_bytes() -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("x.csv", "a;b\n1;2\n")
    return buf.getvalue()


ZIP = _zip_bytes()
REF = RefMonth(2026, 9)


class _Resp:
    def __init__(self, status, body=b"", ctype="application/zip", length=None):
        self.status_code = status
        self.body = body
        self.headers = {"Content-Type": ctype if status in (200, 206) else "application/xml"}
        if status == 200 and body:
            self.headers["Content-Length"] = str(len(body) if length is None else length)

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def iter_content(self, chunk_size):
        yield self.body


class FakeSession:
    """`script`: URL suffix -> answers consumed in order (last repeats). Ranged GETs (probe) use `probe`."""

    def __init__(self, script=None, probe=None):
        self.script, self.probe, self.calls = script or {}, probe or {}, []

    def _answer(self, url, headers=None):
        self.calls.append(url)
        ranged = bool(headers and "Range" in headers)
        for suffix, answers in (self.probe if ranged else self.script).items():
            if url.endswith(suffix):
                return answers.pop(0) if len(answers) > 1 else answers[0]
        return _Resp(206) if ranged else _Resp(200, ZIP)

    def request(self, method, url, headers=None, **kw):
        return self._answer(url, headers)

    def get(self, url, **kw):
        return self._answer(url, kw.get("headers"))


def _fetcher(session):
    return Fetcher(auth=("tok", ""), session=session, retries=3, pause_s=0, sleep=lambda s: None,
                   retryable=frozenset({404, 500, 503}))


def _run(tmp_path, ctrl, session, ref=REF):
    month_dir = tmp_path / "landing" / f"ref={ref.yyyymm}"
    month_dir.mkdir(parents=True, exist_ok=True)

    def body(task):
        task.set_ref_month(ref)
        rfb01.download_month(task, ref, ctrl=ctrl, month_dir=month_dir, landing_uri="file:///x",
                             fetcher=_fetcher(session), base_dir="/Dados/Cadastros/CNPJ", free_bytes=10 ** 12)

    code = run_task(body, pipeline="rfb", task_id="rfb01_download", layer="landing", ref_month=ref)
    return code, month_dir


def test_contract():
    assert len(EXPECTED_FILES) == 37
    assert entity_of("Estabelecimentos3.zip") == "estabelecimentos" and entity_of("Cnaes.zip") == "cnaes"
    with pytest.raises(KeyError):
        entity_of("Something.zip")


def test_full_month_then_rerun_does_not_contact_source(tmp_path):
    ctrl, session = InMemoryCtrl(), FakeSession()
    code, month_dir = _run(tmp_path, ctrl, session)
    assert code == EXIT_OK
    ref = ctrl.get_ref("rfb", 202609)
    assert ref["status"] == "DOWNLOADED" and ref["files_ok"] == 37 and ref["attempts"] == 1
    assert ref["bytes_total"] == 37 * len(ZIP) and ref["downloaded_at"] is not None
    assert {r["status"] for r in ctrl.files} == {"OK"} and len(ctrl.files) == 37
    assert ctrl.files[0]["sha256"] == hashlib.sha256(ZIP).hexdigest()
    assert not list(month_dir.glob("*.part"))

    session2 = FakeSession()
    code, _ = _run(tmp_path, ctrl, session2)
    assert code == EXIT_OK and session2.calls == []          # complete on disk: source not contacted
    assert ctrl.get_ref("rfb", 202609)["attempts"] == 2
    assert [r["status"] for r in ctrl.files[37:]] == ["SKIPPED_EXISTING"] * 37


def test_flaky_404_is_retried(tmp_path):
    ctrl = InMemoryCtrl()
    session = FakeSession(script={"Empresas0.zip": [_Resp(404), _Resp(404), _Resp(200, ZIP)]})
    code, _ = _run(tmp_path, ctrl, session)
    assert code == EXIT_OK
    row = next(r for r in ctrl.files if r["file_name"] == "Empresas0.zip")
    assert row["status"] == "OK" and row["attempts"] == 3


def test_missing_file_marks_month_partial_and_fails(tmp_path):
    ctrl = InMemoryCtrl()
    session = FakeSession(script={"Socios7.zip": [_Resp(404)]})
    code, _ = _run(tmp_path, ctrl, session)
    assert code == EXIT_FAILED                                   # landing.contract_files_present is BLOCK
    ref = ctrl.get_ref("rfb", 202609)
    assert ref["status"] == "PARTIAL" and ref["last_error"] == "source_incomplete" and ref["files_ok"] == 36
    assert next(r for r in ctrl.files if r["file_name"] == "Socios7.zip")["status"] == "MISSING"

    code, _ = _run(tmp_path, ctrl, FakeSession())               # source recovered: resume fetches only Socios7
    assert code == EXIT_OK and ctrl.get_ref("rfb", 202609)["status"] == "DOWNLOADED"
    assert [r["file_name"] for r in ctrl.files[37:] if r["status"] == "OK"] == ["Socios7.zip"]


def test_truncated_download_is_not_kept(tmp_path):
    ctrl = InMemoryCtrl()
    session = FakeSession(script={"Cnaes.zip": [_Resp(200, ZIP[:10], length=len(ZIP))]})
    code, month_dir = _run(tmp_path, ctrl, session)
    assert code == EXIT_FAILED
    assert not (month_dir / "Cnaes.zip").exists() and not (month_dir / "Cnaes.zip.part").exists()
    assert next(r for r in ctrl.files if r["file_name"] == "Cnaes.zip")["status"] == "CORRUPT"


def test_month_not_published_is_skipped(tmp_path):
    ctrl = InMemoryCtrl()
    session = FakeSession(probe={"2026-09/Cnaes.zip": [_Resp(404)], "2026-08/Cnaes.zip": [_Resp(206)]})
    code, _ = _run(tmp_path, ctrl, session)
    assert code == EXIT_SKIPPED
    ref = ctrl.get_ref("rfb", 202609)
    assert ref["status"] == "NOT_PUBLISHED" and ref["last_error"] == "source_month_not_published"


def test_rejected_link_fails_with_named_cause(tmp_path):
    ctrl = InMemoryCtrl()
    session = FakeSession(probe={"Cnaes.zip": [_Resp(401)]})
    code, _ = _run(tmp_path, ctrl, session)
    assert code == EXIT_FAILED and ctrl.get_ref("rfb", 202609)["last_error"] == "source_auth_failed"


def test_resolve_next_month_from_ctrl(monkeypatch):
    ctrl = InMemoryCtrl()
    assert rfb01.resolve_ref_month("next", ctrl) == RefMonth(2026, 9)     # RFB_START_MONTH default
    ctrl.upsert_ref({"pipeline": "rfb", "ref_month": 202608, "status": "DOWNLOADED"})
    assert rfb01.resolve_ref_month(None, ctrl) == RefMonth(2026, 9)
    assert rfb01.resolve_ref_month("2026-05", ctrl) == RefMonth(2026, 5)
