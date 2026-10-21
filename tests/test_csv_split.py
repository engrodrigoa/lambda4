import csv
import io

from pipelines.commons.csv_split import iter_records, split_stream


def _lines(text: str, enc: str = "iso-8859-1") -> list[bytes]:
    return io.BytesIO(text.encode(enc)).readlines()


def test_records_follow_quote_parity():
    text = '"1";"BOX ALA  ""D""";"x"\n"2";"RIZZ\n";"y"\n"3";"a;b";"z"\n'
    recs = list(iter_records(_lines(text)))
    assert [span for _, span in recs] == [1, 2, 1]
    # every record parses to exactly 3 fields with the standard library reader
    for rec, _ in recs:
        assert len(next(csv.reader(io.StringIO(rec.decode("iso-8859-1")), delimiter=";"))) == 3


def test_runaway_quote_quarantines_only_the_culprit():
    good = "".join(f'"{i}";"ok";"x"\n' for i in range(10))
    text = '"0";"broken "quote";"x"\n' + good         # 7 quotes (odd): would glue everything after it
    dropped = []
    recs = list(iter_records(_lines(text), max_span_lines=3, on_quarantine=lambda n, r, why: dropped.append((n, why))))
    assert dropped == [(1, "unbalanced_quotes")]
    assert len(recs) == 10 and all(span == 1 for _, span in recs)


def test_unbalanced_quote_at_eof():
    text = '"1";"a";"b"\n"2";"open\n'
    dropped = []
    recs = list(iter_records(_lines(text), on_quarantine=lambda n, r, why: dropped.append((n, why))))
    assert len(recs) == 1 and dropped == [(2, "unbalanced_quotes_at_eof")]


def test_split_stream_chunks_at_record_boundaries_and_transcodes(tmp_path):
    rows = ['"1";"SÃO PAULO";"x"\n', '"2";"multi\n";"y"\n'] * 50
    stream = io.BytesIO("".join(rows).encode("iso-8859-1"))
    res = split_stream(stream, tmp_path, "Test0", source_name="Test0.zip", target_bytes=200)
    assert res.records == 100 and res.multiline_records == 50 and res.physical_lines == 150
    assert len(res.chunks) > 1 and sum(c.records for c in res.chunks) == 100
    parsed = []
    for c in res.chunks:
        text = open(c.path, encoding="utf-8", newline="").read()          # UTF-8 now
        parsed += list(csv.reader(io.StringIO(text, newline=""), delimiter=";"))
    assert len(parsed) == 100 and all(len(r) == 3 for r in parsed)
    assert parsed[0][1] == "SÃO PAULO" and parsed[1][1] == "multi\n"


def test_blank_lines_and_missing_final_newline(tmp_path):
    stream = io.BytesIO(b'"1";"a"\n\n"2";"b"')
    res = split_stream(stream, tmp_path, "T", source_name="T.zip")
    assert res.records == 2
    assert open(res.chunks[0].path, "rb").read() == b'"1";"a"\n"2";"b"\n'


def test_stray_quote_does_not_pair_with_a_real_multiline_record():
    from pipelines.commons.csv_split import field_count_validator

    text = '"9";"broken "quote"\n"1";"ok"\n"2";"RIZZ\n"\n"3";"ok"\n'   # 5 quotes, then a legit multiline record
    dropped = []
    recs = list(iter_records(_lines(text), validate=field_count_validator(2),
                             on_quarantine=lambda n, r, why: dropped.append(n)))
    assert dropped == [1]
    assert [span for _, span in recs] == [1, 2, 1]                        # 1, the multiline 2, and 3 survive
