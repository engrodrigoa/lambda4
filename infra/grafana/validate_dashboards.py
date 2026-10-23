"""Run every query of the generated dashboards against the live data sources (no Grafana login needed).

    python infra/grafana/validate_dashboards.py

* SQL (pg-lambda / pg-airflow): executed as role grafana_ro (the role Grafana uses), so permissions are tested too.
* PromQL: /api/v1/query on Prometheus.   * LogQL: /loki/api/v1/query_range on Loki.
Grafana macros/variables are replaced by plausible values. Exit code 1 if any query errors.
"""
from __future__ import annotations

import hashlib
import json
import re
import subprocess
import sys
import urllib.parse
import urllib.request
from pathlib import Path

DASH = Path(__file__).parent / "dashboards"
DBS = {"pg-lambda": "lambda", "pg-airflow": "airflow"}
# plausible values for multi-value variables (${var:singlequote})
VAR_SAMPLES = {"level": "'WARNING','ERROR','CRITICAL'", "kind": "'semestral','mensal','semanal'",
               "product": "'GASOLINA','ETANOL','DIESEL S10'"}


def latest_run_id() -> str:
    out = subprocess.run(["sudo", "-u", "postgres", "psql", "-d", "lambda", "-tAc",
                          "SELECT run_id FROM audit.pipeline_run ORDER BY started_at DESC LIMIT 1"],
                         capture_output=True, text=True, check=True)
    return out.stdout.strip() or "none"


def sql_text(q: str, run_id: str) -> str:
    q = re.sub(r"\$__timeFilter\(([^)]+)\)", r"\1 > now() - interval '30 days'", q)
    q = re.sub(r"\$__timeGroupAlias\(([^,]+),\s*'[^']*'\)", r"date_trunc('hour', \1) AS time", q)
    q = q.replace("$run_id", run_id)
    for var, value in VAR_SAMPLES.items():
        q = q.replace("${" + var + ":singlequote}", value)
    return q


def run_sql(db: str, q: str) -> tuple[bool, str]:
    proc = subprocess.run(["sudo", "-u", "postgres", "psql", "-d", db, "-v", "ON_ERROR_STOP=1", "-tA", "-c",
                           f"SET ROLE grafana_ro; SELECT count(*) FROM ({q}) _q"],
                          capture_output=True, text=True)
    if proc.returncode:
        return False, proc.stderr.strip().splitlines()[-1] if proc.stderr else "error"
    return True, proc.stdout.strip().splitlines()[-1] + " rows"


def http_json(url: str) -> dict:
    with urllib.request.urlopen(url, timeout=30) as r:
        return json.load(r)


def promql(expr: str) -> tuple[bool, str]:
    expr = expr.replace("$__auto", "5m").replace("$__range", "1h").replace("$__interval", "5m")
    try:
        d = http_json("http://127.0.0.1:9090/api/v1/query?" + urllib.parse.urlencode({"query": expr}))
        return d.get("status") == "success", f"{len(d['data']['result'])} series"
    except Exception as exc:  # noqa: BLE001
        return False, str(exc)


def logql(expr: str) -> tuple[bool, str]:
    expr = (expr.replace("$__auto", "1m").replace("$__range", "1h").replace("$task", ".+").replace("$search", ""))
    try:
        d = http_json("http://127.0.0.1:3100/loki/api/v1/query_range?" + urllib.parse.urlencode(
            {"query": expr, "limit": 5, "since": "24h"}))
        return d.get("status") == "success", f"{len(d['data']['result'])} streams/series"
    except urllib.error.HTTPError as exc:
        return False, exc.read().decode()[:300]
    except Exception as exc:  # noqa: BLE001
        return False, str(exc)


def traceql(query: str, run_id: str) -> tuple[bool, str]:
    """A trace id is looked up (404 = the run has no Tempo trace: OK for runs that predate tracing); else a search."""
    query = query.replace("$trace_id", hashlib.md5(run_id.encode()).hexdigest())
    try:
        if re.fullmatch(r"[0-9a-f]{32}", query):
            try:
                d = http_json(f"http://127.0.0.1:3200/api/v2/traces/{query}")
                spans = sum(len(ss["spans"]) for rs in d["trace"]["resourceSpans"] for ss in rs["scopeSpans"])
                return True, f"{spans} spans"
            except urllib.error.HTTPError as exc:
                return exc.code == 404, f"HTTP {exc.code} (no trace for the latest run)"
        d = http_json("http://127.0.0.1:3200/api/search?" + urllib.parse.urlencode({"q": query, "limit": 5}))
        return True, f"{len(d.get('traces', []))} traces"
    except Exception as exc:  # noqa: BLE001
        return False, str(exc)


def targets(board: dict):
    for p in board["panels"]:
        for t in p.get("targets", []):
            yield p["title"], t
    for a in board["annotations"]["list"]:
        if "target" in a:
            yield f"annotation: {a['name']}", {**a["target"], "datasource": a["datasource"]}
    for v in board["templating"]["list"]:
        if v.get("type") == "query":
            ds = v["datasource"]
            yield f"variable: {v['name']}", ({"datasource": ds, "rawSql": v["query"]} if "rawSql" not in v and
                                             ds["uid"].startswith("pg") else {"datasource": ds, "var": v["query"]})


def main() -> int:
    run_id = latest_run_id()
    failures = 0
    for path in sorted(DASH.glob("*.json")):
        board = json.loads(path.read_text())
        print(f"\n== {board['title']}")
        for title, t in targets(board):
            uid = t["datasource"]["uid"]
            if uid in DBS:
                ok, info = run_sql(DBS[uid], sql_text(t["rawSql"], run_id))
            elif uid == "prometheus":
                ok, info = promql(t["expr"])
            elif uid == "tempo":
                ok, info = traceql(t["query"], run_id)
            elif uid == "loki":
                if "var" in t:
                    ok, info = True, "label_values variable (checked by the panels using it)"
                else:
                    ok, info = logql(t["expr"])
            else:
                ok, info = False, f"unknown datasource {uid}"
            failures += not ok
            print(f"  {'OK ' if ok else 'ERR'} {title[:60]:60} {info[:140]}")
    print(f"\n{failures} failing queries")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
