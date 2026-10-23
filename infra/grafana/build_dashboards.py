"""Dashboards as code: generates the Grafana JSON for the lambda4 pipelines (RFB and ANP).

    python infra/grafana/build_dashboards.py          # writes infra/grafana/dashboards/*.json
    infra/grafana/install.sh                          # copies them to Grafana's provisioning folder

Data sources (provisioned by the lab): prometheus, loki, tempo, pg-lambda (audit, ctrl and anp_bronze schemas),
pg-airflow (Airflow metadata DB).
Every query here is also executed by infra/grafana/validate_dashboards.py against the live sources.
"""
from __future__ import annotations

import json
from pathlib import Path

OUT = Path(__file__).parent / "dashboards"
PROM = {"type": "prometheus", "uid": "prometheus"}
LOKI = {"type": "loki", "uid": "loki"}
PG = {"type": "grafana-postgresql-datasource", "uid": "pg-lambda"}
AF = {"type": "grafana-postgresql-datasource", "uid": "pg-airflow"}
TEMPO = {"type": "tempo", "uid": "tempo"}
TAGS = ["lambda4", "rfb"]

STATUS_COLORS = {"SUCCESS": "green", "FAILED": "red", "RUNNING": "blue", "SKIPPED": "text", "PASS": "green",
                 "FAIL": "red", "OK": "green", "WARN": "orange", "BLOCK": "red", "success": "green",
                 "failed": "red", "running": "blue", "queued": "purple", "skipped": "text",
                 "upstream_failed": "orange", "up_for_retry": "yellow", "scheduled": "purple"}


def status_mappings() -> list[dict]:
    options = {k: {"color": v, "index": i} for i, (k, v) in enumerate(STATUS_COLORS.items())}
    return [{"type": "value", "options": options}]


# ----------------------------------------------------------------------------------------------- targets
def sql(query: str, ds=PG, fmt: str = "table", ref: str = "A") -> dict:
    return {"refId": ref, "datasource": ds, "rawSql": " ".join(query.split()), "format": fmt, "rawQuery": True,
            "editorMode": "code"}


def prom(expr: str, legend: str = "", ref: str = "A") -> dict:
    return {"refId": ref, "datasource": PROM, "expr": expr, "legendFormat": legend, "range": True}


def loki(expr: str, legend: str = "", ref: str = "A") -> dict:
    return {"refId": ref, "datasource": LOKI, "expr": expr, "legendFormat": legend, "queryType": "range"}


def traceql(query: str, ref: str = "A", limit: int = 20) -> dict:
    """A trace id (lookup) or a TraceQL search against Tempo."""
    return {"refId": ref, "datasource": TEMPO, "queryType": "traceql", "query": query, "limit": limit,
            "tableType": "traces"}


# ----------------------------------------------------------------------------------------------- layout
class Board:
    def __init__(self, uid: str, title: str, description: str, tags: list[str], time_from: str = "now-24h"):
        self.uid, self.title, self.description, self.tags, self.time_from = uid, title, description, tags, time_from
        self.panels: list[dict] = []
        self.vars: list[dict] = []
        self.annotations: list[dict] = []
        self.x = self.y = self.row_h = 0
        self._id = 0

    def _next_id(self) -> int:
        self._id += 1
        return self._id

    def row(self, title: str) -> None:
        self._newline()
        self.panels.append({"type": "row", "title": title, "id": self._next_id(), "collapsed": False,
                            "gridPos": {"h": 1, "w": 24, "x": 0, "y": self.y}, "panels": []})
        self.y += 1

    def _newline(self) -> None:
        if self.x:
            self.y += self.row_h
            self.x = self.row_h = 0

    def add(self, panel: dict, w: int, h: int) -> None:
        if self.x + w > 24:
            self._newline()
        panel["id"] = self._next_id()
        panel["gridPos"] = {"h": h, "w": w, "x": self.x, "y": self.y}
        self.panels.append(panel)
        self.x += w
        self.row_h = max(self.row_h, h)

    def json(self) -> dict:
        return {
            "uid": self.uid, "title": self.title, "description": self.description, "tags": self.tags,
            "timezone": "browser", "schemaVersion": 41, "version": 1, "editable": True, "graphTooltip": 1,
            "time": {"from": self.time_from, "to": "now"}, "refresh": "30s",
            "templating": {"list": self.vars},
            "annotations": {"list": [{"builtIn": 1, "datasource": {"type": "grafana", "uid": "-- Grafana --"},
                                      "enable": True, "hide": True, "iconColor": "rgba(0, 211, 255, 1)",
                                      "name": "Annotations & Alerts", "type": "dashboard"}, *self.annotations]},
            "links": [{"type": "dashboards", "tags": ["lambda4"], "asDropdown": True, "title": "lambda4 dashboards",
                       "includeVars": False, "keepTime": True}],
            "panels": self.panels,
        }


# ----------------------------------------------------------------------------------------------- panels
def stat(title: str, target: dict, unit: str = "short", desc: str = "", mappings=None, thresholds=None,
         color_mode: str = "value", decimals=None, text_mode: str = "auto") -> dict:
    defaults = {"unit": unit, "mappings": mappings or [], "color": {"mode": "thresholds"},
                "thresholds": {"mode": "absolute", "steps": thresholds or [{"color": "text", "value": None}]}}
    if decimals is not None:
        defaults["decimals"] = decimals
    return {"type": "stat", "title": title, "description": desc, "datasource": target["datasource"],
            "targets": [target], "fieldConfig": {"defaults": defaults, "overrides": []},
            "options": {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                        "colorMode": color_mode, "graphMode": "none", "textMode": text_mode, "justifyMode": "center"}}


def red_if_positive() -> list[dict]:
    return [{"color": "green", "value": None}, {"color": "red", "value": 1}]


def orange_if_positive() -> list[dict]:
    return [{"color": "green", "value": None}, {"color": "orange", "value": 1}]


def table(title: str, target: dict, desc: str = "", overrides=None, status_cols=(), units: dict | None = None,
          sort_by: str | None = None) -> dict:
    ov = list(overrides or [])
    for col in status_cols:
        ov.append({"matcher": {"id": "byName", "options": col},
                   "properties": [{"id": "mappings", "value": status_mappings()},
                                  {"id": "custom.cellOptions", "value": {"type": "color-background"}}]})
    for col, unit in (units or {}).items():
        ov.append({"matcher": {"id": "byName", "options": col}, "properties": [{"id": "unit", "value": unit}]})
    opts = {"showHeader": True, "cellHeight": "sm", "footer": {"show": False}}
    if sort_by:
        opts["sortBy"] = [{"displayName": sort_by, "desc": True}]
    return {"type": "table", "title": title, "description": desc, "datasource": target["datasource"],
            "targets": [target], "fieldConfig": {"defaults": {"custom": {"filterable": True}}, "overrides": ov},
            "options": opts}


def timeseries(title: str, targets: list[dict], unit: str = "short", desc: str = "", stack: bool = False,
               draw: str = "line", points: str = "auto", min0: bool = True) -> dict:
    custom = {"drawStyle": draw, "lineWidth": 1, "fillOpacity": 10, "showPoints": points, "spanNulls": False,
              "stacking": {"mode": "normal" if stack else "none", "group": "A"}}
    defaults = {"unit": unit, "custom": custom, "color": {"mode": "palette-classic"}}
    if min0:
        defaults["min"] = 0
    return {"type": "timeseries", "title": title, "description": desc, "datasource": targets[0]["datasource"],
            "targets": targets, "fieldConfig": {"defaults": defaults, "overrides": []},
            "options": {"legend": {"displayMode": "table", "placement": "right", "calcs": ["lastNotNull", "max"]},
                        "tooltip": {"mode": "multi", "sort": "desc"}}}


def barchart(title: str, target: dict, unit: str = "short", desc: str = "", horizontal: bool = True,
             x_field: str | None = None) -> dict:
    opts = {"orientation": "horizontal" if horizontal else "vertical", "showValue": "auto", "barWidth": 0.7,
            "legend": {"displayMode": "list", "placement": "bottom", "showLegend": False},
            "xTickLabelRotation": 0, "groupWidth": 0.7, "stacking": "none"}
    if x_field:
        opts["xField"] = x_field
    return {"type": "barchart", "title": title, "description": desc, "datasource": target["datasource"],
            "targets": [target], "fieldConfig": {"defaults": {"unit": unit, "color": {"mode": "palette-classic"}},
                                                 "overrides": []}, "options": opts}


def logs(title: str, expr: str, desc: str = "") -> dict:
    return {"type": "logs", "title": title, "description": desc, "datasource": LOKI, "targets": [loki(expr)],
            "options": {"showTime": True, "wrapLogMessage": True, "enableLogDetails": True, "sortOrder": "Descending",
                        "dedupStrategy": "none", "prettifyLogMessage": False}}


def text(title: str, md: str) -> dict:
    return {"type": "text", "title": title, "options": {"mode": "markdown", "content": md}}


def traces(title: str, target: dict, desc: str = "") -> dict:
    return {"type": "traces", "title": title, "description": desc, "datasource": target["datasource"],
            "targets": [target]}


def tempo_table(title: str, target: dict, desc: str = "") -> dict:
    return {"type": "table", "title": title, "description": desc, "datasource": TEMPO, "targets": [target],
            "fieldConfig": {"defaults": {}, "overrides": []}, "options": {"showHeader": True}}


def runs_annotation() -> dict:
    return {"name": "Pipeline runs", "enable": True, "iconColor": "#8AB8FF", "datasource": PG,
            "target": sql("""SELECT started_at AS time, coalesce(finished_at, now()) AS timeend,
                             'rfb ' || coalesce(ref_month::text, '') || ' ' || status AS text, status AS tags
                             FROM audit.pipeline_run WHERE pipeline = 'rfb' AND $__timeFilter(started_at)""")}


RUN_VAR = {"name": "run_id", "label": "Run", "type": "query", "datasource": PG, "refresh": 2, "sort": 0,
           "query": "SELECT run_id FROM audit.pipeline_run WHERE pipeline = 'rfb' ORDER BY started_at DESC LIMIT 100",
           "definition": "SELECT run_id FROM audit.pipeline_run WHERE pipeline = 'rfb' ORDER BY started_at DESC "
                         "LIMIT 100", "includeAll": False, "multi": False, "current": {}}
# the run's trace id is md5(run_id) in Tempo, in audit.v_trace_spans and on every log line (pipelines/commons/tracing)
TRACE_VAR = {"name": "trace_id", "label": "Trace id", "type": "query", "datasource": PG, "refresh": 1, "hide": 2,
             "query": "SELECT md5('$run_id')", "definition": "SELECT md5('$run_id')", "includeAll": False,
             "multi": False, "current": {}}


# ----------------------------------------------------------------------------------------------- 1. pipeline health
def pipeline_health() -> dict:
    b = Board("lambda4-rfb-health", "RFB · Pipeline health & tracing",
              "Health, tracing, volume and data quality of the RFB lakehouse pipeline (source: audit schema, "
              "Pushgateway metrics, node/Spark exporters).", [*TAGS, "pipeline"], time_from="now-7d")
    b.vars.extend([RUN_VAR, TRACE_VAR])
    b.annotations.append(runs_annotation())
    R = "'$run_id'"

    b.row("Run overview — $run_id")
    b.add(stat("Status", sql(f"SELECT status FROM audit.pipeline_run WHERE run_id = {R}"), mappings=status_mappings(),
               color_mode="background", text_mode="value"), 4, 4)
    b.add(stat("Reference month", sql(f"SELECT ref_month::text FROM audit.pipeline_run WHERE run_id = {R}"),
               text_mode="value"), 3, 4)
    b.add(stat("Duration", sql(f"SELECT extract(epoch FROM coalesce(finished_at, now()) - started_at) "
                               f"FROM audit.pipeline_run WHERE run_id = {R}"), unit="s"), 3, 4)
    b.add(stat("Tasks failed", sql(f"SELECT count(*) FROM (SELECT DISTINCT ON (task_id) status FROM audit.task_run "
                                   f"WHERE run_id = {R} ORDER BY task_id, attempt DESC) t WHERE status = 'FAILED'"),
               thresholds=red_if_positive(), color_mode="background"), 3, 4)
    b.add(stat("Rows written (bronze+silver+gold)",
               sql(f"SELECT coalesce(sum(rows_out), 0) FROM audit.task_run WHERE run_id = {R} "
                   "AND layer IN ('bronze','silver','gold')"), unit="short"), 4, 4)
    b.add(stat("DQ BLOCK / WARN fails",
               sql(f"SELECT count(*) FILTER (WHERE severity = 'BLOCK') AS block, "
                   f"count(*) FILTER (WHERE severity = 'WARN') AS warn FROM audit.dq_event "
                   f"WHERE run_id = {R} AND status = 'FAIL'"), thresholds=orange_if_positive(),
               color_mode="background"), 4, 4)
    b.add(stat("Rows quarantined", sql(f"SELECT coalesce(sum(rows_rejected), 0) FROM audit.task_run "
                                       f"WHERE run_id = {R}"), thresholds=orange_if_positive()), 3, 4)

    b.row("Execution trace (run → tasks → timed steps)")
    b.add(traces("Trace of the run (Tempo)", traceql("$trace_id"),
                 desc="OpenTelemetry spans in Tempo (trace id = md5(run_id)): root = the run (closed by finalize), "
                      "children = task attempts (TaskRun), grandchildren = timed steps (log events with seconds / "
                      "latency_ms). Span details: attributes, dq_check events, exceptions, a link to Airflow's "
                      "worker span, and 'Logs for this span' (Loki by trace_id). Needs tempo + alloy running."),
          24, 14)
    b.add(tempo_table("Recent pipeline traces (Tempo)",
                      traceql('{resource.service.name="lambda-rfb" && span.task_id != nil}', limit=30),
                      desc="TraceQL search: the last traces of the RFB pipeline (manual runs included); click a "
                           "trace id to open it."), 12, 9)
    b.add(traces("Trace of the run (audit SQL, works without Tempo)", sql(f"""SELECT "traceID", "spanID",
                 "parentSpanID", "operationName", "serviceName", "startTime", duration FROM audit.v_trace_spans
                 WHERE run_id = {R} ORDER BY "startTime" """),
                 desc="Same ids as the Tempo trace, rebuilt from the audit tables (audit.v_trace_spans): the "
                      "fallback when Tempo is stopped or the run predates tracing."), 12, 9)
    b.add(barchart("Task duration in this run", sql(
        f"""SELECT task_id || CASE WHEN attempt > 1 THEN ' #' || attempt ELSE '' END AS task,
            round(coalesce(duration_s, extract(epoch FROM now() - started_at))) AS seconds
            FROM audit.task_run WHERE run_id = {R} ORDER BY started_at"""), unit="s", x_field="task"), 12, 10)
    b.add(table("Tasks of the run", sql(
        f"""SELECT task_id, layer, entity, attempt, status, started_at, round(duration_s) AS duration_s,
            rows_in, rows_out, rows_rejected, bytes_out, warn_count, block_count, airflow_task_id, spark_app_id,
            error_class, left(error_message, 200) AS error
            FROM audit.task_run WHERE run_id = {R} ORDER BY started_at"""),
        status_cols=("status",), units={"duration_s": "s", "bytes_out": "bytes", "started_at": "dateTimeAsIso"}),
        12, 10)

    b.row("Performance across runs")
    b.add(timeseries("Duration per task", [sql("""SELECT started_at AS time, task_id AS metric, duration_s AS value
        FROM audit.task_run WHERE pipeline = 'rfb' AND duration_s IS NOT NULL AND $__timeFilter(started_at)
        ORDER BY 1""", fmt="time_series")], unit="s", draw="points", points="always"), 12, 8)
    b.add(timeseries("Rows written per task", [sql("""SELECT started_at AS time, task_id AS metric, rows_out AS value
        FROM audit.task_run WHERE pipeline = 'rfb' AND rows_out IS NOT NULL AND $__timeFilter(started_at)
        ORDER BY 1""", fmt="time_series")], draw="points", points="always"), 12, 8)
    b.add(timeseries("Throughput (rows/s) per task", [sql("""SELECT started_at AS time, task_id AS metric,
        rows_out / nullif(duration_s, 0) AS value FROM audit.task_run WHERE pipeline = 'rfb' AND rows_out > 0
        AND $__timeFilter(started_at) ORDER BY 1""", fmt="time_series")], unit="rowsps", draw="points",
        points="always"), 12, 8)
    b.add(timeseries("Last-run duration (Pushgateway gauge)", [prom(
        'lambda_task_duration_seconds{job="lambda_pipeline", pipeline="rfb"}', "{{task}}")], unit="s"), 12, 8)

    b.row("Delta tables — volume and file health")
    latest = """SELECT DISTINCT ON (table_name) table_name, layer, delta_version, operation, rows_total, size_bytes,
                num_files, round(size_bytes / nullif(num_files, 0) / 1048576.0, 1) AS avg_file_mib, rows_written,
                captured_at FROM audit.delta_table_stats WHERE pipeline = 'rfb' ORDER BY table_name, captured_at DESC"""
    b.add(table("Latest state of every Delta table", sql(latest),
                desc="Small files (avg < 32 MiB with many files) ask for OPTIMIZE; rows_written = last operation.",
                units={"size_bytes": "bytes", "avg_file_mib": "decmbytes", "captured_at": "dateTimeAsIso"},
                overrides=[{"matcher": {"id": "byName", "options": "avg_file_mib"},
                            "properties": [{"id": "custom.cellOptions", "value": {"type": "color-text"}},
                                           {"id": "thresholds", "value": {"mode": "absolute", "steps": [
                                               {"color": "orange", "value": None}, {"color": "green", "value": 32}]}},
                                           {"id": "color", "value": {"mode": "thresholds"}}]}]), 24, 9)
    b.add(barchart("Rows per table (latest)", sql(f"SELECT table_name, rows_total FROM ({latest}) t "
                                                  "ORDER BY rows_total DESC NULLS LAST"), x_field="table_name"), 12, 10)
    b.add(barchart("Size per table (latest)", sql(f"SELECT table_name, size_bytes FROM ({latest}) t "
                                                  "ORDER BY size_bytes DESC NULLS LAST"), unit="bytes",
                   x_field="table_name"), 12, 10)
    b.add(timeseries("Table growth (rows)", [sql("""SELECT captured_at AS time, table_name AS metric,
        rows_total AS value FROM audit.delta_table_stats WHERE pipeline = 'rfb' AND rows_total IS NOT NULL
        AND $__timeFilter(captured_at) ORDER BY 1""", fmt="time_series")], draw="points", points="always"), 12, 8)
    b.add(timeseries("Rows written per operation", [sql("""SELECT captured_at AS time,
        table_name || ' ' || operation AS metric, rows_written AS value FROM audit.delta_table_stats
        WHERE pipeline = 'rfb' AND rows_written IS NOT NULL AND $__timeFilter(captured_at) ORDER BY 1""",
        fmt="time_series")], draw="bars"), 12, 8)

    b.row("Data quality and reconciliation")
    b.add(table("Checks of the run", sql(f"""SELECT created_at, task_id, check_name, severity, status, observed,
        threshold, message FROM audit.dq_event WHERE run_id = {R} ORDER BY (status = 'FAIL') DESC, created_at"""),
        status_cols=("status", "severity"), units={"created_at": "dateTimeAsIso"}), 14, 10)
    b.add(table("Reconciliation by entity (rows across layers)", sql(f"""SELECT entity,
        max(rows_out) FILTER (WHERE layer = 'extract') AS extracted,
        max(rows_in) FILTER (WHERE layer = 'bronze') AS bronze_in,
        max(rows_out) FILTER (WHERE layer = 'bronze') AS bronze_out,
        max(rows_in) FILTER (WHERE layer = 'silver') AS silver_in,
        sum(rows_rejected) AS quarantined
        FROM audit.task_run WHERE run_id = {R} AND entity IS NOT NULL GROUP BY entity ORDER BY entity"""),
        desc="extracted == bronze_out + quarantined is a BLOCK check (bronze.rows_match_source)."), 10, 10)
    b.add(timeseries("Failed checks over time", [sql("""SELECT $__timeGroupAlias(created_at, '1h'),
        check_name AS metric, count(*) AS value FROM audit.dq_event WHERE pipeline = 'rfb' AND status = 'FAIL'
        AND $__timeFilter(created_at) GROUP BY 1, 2 ORDER BY 1""", fmt="time_series")], draw="bars"), 12, 8)
    b.add(table("Quarantine (latest per entity and reason)", sql("""SELECT DISTINCT ON (entity, reason) created_at,
        entity, file_name, reason, rows, left(sample, 300) AS sample FROM audit.quarantine_summary
        WHERE pipeline = 'rfb' ORDER BY entity, reason, created_at DESC"""), units={"created_at": "dateTimeAsIso"}),
        12, 8)

    b.row("Platform health at run time")
    b.add(table("Preflight health checks of the run", sql(f"""SELECT component, status, latency_ms, detail::text
        AS detail FROM audit.health_check WHERE run_id = {R} ORDER BY component"""), status_cols=("status",),
        units={"latency_ms": "ms"}), 12, 8)
    b.add(timeseries("Probe latency per component", [sql("""SELECT checked_at AS time, component AS metric,
        latency_ms AS value FROM audit.health_check WHERE pipeline = 'rfb' AND $__timeFilter(checked_at)
        ORDER BY 1""", fmt="time_series")], unit="ms", draw="points", points="always"), 12, 8)
    b.add(timeseries("CPU busy %", [prom('100 * (1 - avg(rate(node_cpu_seconds_total{mode="idle"}[2m])))', "cpu")],
                     unit="percent"), 6, 7)
    b.add(timeseries("Memory used", [prom("node_memory_MemTotal_bytes - node_memory_MemAvailable_bytes", "used")],
                     unit="bytes"), 6, 7)
    b.add(timeseries("Disk throughput (SSD sda, lake USB sdc, landing sdb)", [
        prom('rate(node_disk_read_bytes_total{device=~"sd[a-c]"}[2m])', "read {{device}}"),
        prom('rate(node_disk_written_bytes_total{device=~"sd[a-c]"}[2m])', "write {{device}}", ref="B")],
        unit="Bps"), 6, 7)
    b.add(timeseries("Spark worker cores / memory in use", [
        prom("metrics_worker_coresUsed_Value", "cores used"),
        prom("metrics_worker_memUsed_MB_Value", "memory MB", ref="B")]), 6, 7)
    b.add(timeseries("Free space on the data disks", [prom(
        'node_filesystem_avail_bytes{mountpoint=~"/|/mnt/datalake|/mnt/hdd1tb|/mnt/scratch"}', "{{mountpoint}}")],
        unit="bytes"), 24, 7)
    return b.json()


# ----------------------------------------------------------------------------------------------- 2. airflow
def airflow_board() -> dict:
    b = Board("lambda4-airflow", "Airflow · Orchestration (rfb_pipeline)",
              "Scheduler/executor/pool health (OpenTelemetry metrics) and DAG/task history (Airflow metadata DB).",
              [*TAGS, "airflow"], time_from="now-7d")
    b.annotations.append(runs_annotation())
    DAG = "'rfb_pipeline'"
    b.row("Platform")
    b.add(stat("Scheduler heartbeats / min", prom("sum(rate(airflow_scheduler_heartbeat_total[5m])) * 60"),
               thresholds=[{"color": "red", "value": None}, {"color": "green", "value": 1}], decimals=1,
               color_mode="background"), 4, 4)
    b.add(stat("Executor open slots", prom("sum(airflow_executor_open_slots)")), 4, 4)
    b.add(stat("Running task instances", prom("sum(airflow_executor_running_tasks)")), 4, 4)
    b.add(stat("Tasks starving (pool full)", prom("sum(airflow_scheduler_tasks_starving)"),
               thresholds=orange_if_positive()), 4, 4)
    b.add(stat("DAG import errors", prom("sum(airflow_dag_processing_import_errors)"), thresholds=red_if_positive(),
               color_mode="background"), 4, 4)
    b.add(stat("DAG parse time", prom("max(airflow_dag_processing_total_parse_time)"), unit="s", decimals=2), 4, 4)
    b.add(timeseries("Pool slots (rfb_spark, rfb_extract)", [
        prom('airflow_pool_running_slots{pool_name=~"rfb_.*"}', "running {{pool_name}}"),
        prom('airflow_pool_queued_slots{pool_name=~"rfb_.*"}', "queued {{pool_name}}", ref="B"),
        prom('airflow_pool_open_slots{pool_name=~"rfb_.*"}', "open {{pool_name}}", ref="C")]), 12, 8)
    b.add(timeseries("Executor and scheduler", [
        prom("sum(airflow_executor_running_tasks)", "running"), prom("sum(airflow_executor_queued_tasks)", "queued",
                                                                     ref="B"),
        prom("sum(airflow_scheduler_tasks_executable)", "executable", ref="C")]), 12, 8)

    b.row("DAG runs")
    b.add(table("rfb_pipeline runs", sql(f"""SELECT run_id, state, run_type, conf->>'ref_month' AS ref_month,
        queued_at, start_date, end_date,
        round(extract(epoch FROM coalesce(end_date, now()) - start_date)) AS duration_s,
        triggering_user_name FROM dag_run WHERE dag_id = {DAG} ORDER BY coalesce(start_date, queued_at) DESC
        LIMIT 30""", ds=AF), status_cols=("state",),
        units={"duration_s": "s", "queued_at": "dateTimeAsIso", "start_date": "dateTimeAsIso",
               "end_date": "dateTimeAsIso"}), 24, 8)
    b.add(timeseries("Run duration", [sql(f"""SELECT start_date AS time, 'duration' AS metric,
        extract(epoch FROM end_date - start_date) AS value FROM dag_run WHERE dag_id = {DAG} AND end_date IS NOT NULL
        AND $__timeFilter(start_date) ORDER BY 1""", ds=AF, fmt="time_series")], unit="s", draw="bars"), 12, 8)
    b.add(barchart("Task failures and retries (30 days)", sql(f"""SELECT task_id,
        count(*) FILTER (WHERE state = 'failed') AS failed, count(*) FILTER (WHERE try_number > 1) AS retried
        FROM task_instance_history WHERE dag_id = {DAG} AND start_date > now() - interval '30 days'
        GROUP BY task_id ORDER BY failed DESC, retried DESC""", ds=AF), x_field="task_id"), 12, 8)

    b.row("Task instances (latest run)")
    latest_run = (f"(SELECT run_id FROM dag_run WHERE dag_id = {DAG} "
                  "ORDER BY coalesce(start_date, queued_at) DESC LIMIT 1)")
    b.add(table("Task instances of the latest run", sql(f"""SELECT task_id, state, try_number, max_tries, pool,
        priority_weight, queued_dttm, start_date, end_date, round(duration::numeric) AS duration_s,
        round(extract(epoch FROM start_date - queued_dttm)::numeric, 1) AS queue_wait_s, hostname, pid
        FROM task_instance WHERE dag_id = {DAG} AND run_id = {latest_run} ORDER BY start_date NULLS LAST""", ds=AF),
        status_cols=("state",), units={"duration_s": "s", "queue_wait_s": "s", "queued_dttm": "dateTimeAsIso",
                                       "start_date": "dateTimeAsIso", "end_date": "dateTimeAsIso"}), 24, 10)
    b.add(timeseries("Queue wait per task (pool contention)", [sql(f"""SELECT start_date AS time, task_id AS metric,
        extract(epoch FROM start_date - queued_dttm) AS value FROM task_instance_history WHERE dag_id = {DAG}
        AND queued_dttm IS NOT NULL AND $__timeFilter(start_date) ORDER BY 1""", ds=AF, fmt="time_series")],
        unit="s", draw="points", points="always",
        desc="Time between queued and running: the cost of rfb_spark having 1 slot."), 12, 8)
    b.add(timeseries("Task duration (Airflow view)", [sql(f"""SELECT start_date AS time, task_id AS metric,
        duration AS value FROM task_instance_history WHERE dag_id = {DAG} AND duration IS NOT NULL
        AND $__timeFilter(start_date) ORDER BY 1""", ds=AF, fmt="time_series")], unit="s", draw="points",
        points="always"), 12, 8)
    return b.json()


# ----------------------------------------------------------------------------------------------- 3. logs
def logs_board() -> dict:
    b = Board("lambda4-logs", "Logs · Airflow & pipeline (raw and treated)",
              "Raw Airflow task logs and pipeline JSON events (Loki), plus warnings/errors treated in the audit "
              "schema (Postgres).", [*TAGS, "logs"], time_from="now-24h")
    b.vars += [
        {"name": "task", "label": "Airflow task", "type": "query", "datasource": LOKI, "refresh": 2,
         "query": 'label_values({job="airflow-tasks", dag_id="rfb_pipeline"}, task_id)',
         "definition": 'label_values({job="airflow-tasks", dag_id="rfb_pipeline"}, task_id)',
         "includeAll": True, "allValue": ".+", "multi": True, "current": {"text": "All", "value": "$__all"}},
        {"name": "level", "label": "Level", "type": "custom", "query": "WARNING,ERROR,CRITICAL,INFO",
         "includeAll": False, "multi": True,
         "current": {"text": ["WARNING", "ERROR", "CRITICAL"], "value": ["WARNING", "ERROR", "CRITICAL"]},
         "options": [{"text": v, "value": v, "selected": v != "INFO"}
                     for v in ("WARNING", "ERROR", "CRITICAL", "INFO")]},
        {"name": "search", "label": "Search", "type": "textbox", "query": "", "current": {"text": "", "value": ""}},
    ]
    b.annotations.append(runs_annotation())
    b.row("Treated: what needs attention")
    b.add(timeseries("Pipeline events by level", [loki(
        'sum by (level) (count_over_time({job="lambda-pipelines", pipeline="rfb"} [$__auto]))', "{{level}}")],
        draw="bars", stack=True), 12, 8)
    b.add(timeseries("Pipeline events by type", [loki(
        'sum by (event) (count_over_time({job="lambda-pipelines", pipeline="rfb"} | json | __error__="" [$__auto]))',
        "{{event}}")], draw="bars", stack=True), 12, 8)
    b.add(barchart("Warnings and errors by task (range)", {**loki(
        'sum by (task) (count_over_time({job="lambda-pipelines", pipeline="rfb", level=~"WARNING|ERROR|CRITICAL"} '
        "[$__range]))", "{{task}}"), "queryType": "instant"}), 8, 9)
    b.add(table("Warnings and errors (audit.log_event)", sql("""SELECT ts, level, task_id, event, left(message, 300)
        AS message, run_id FROM audit.log_event WHERE pipeline = 'rfb' AND level IN (${level:singlequote})
        AND $__timeFilter(ts) ORDER BY ts DESC LIMIT 500"""), status_cols=("level",),
        units={"ts": "dateTimeAsIso"},
        overrides=[{"matcher": {"id": "byName", "options": "level"},
                    "properties": [{"id": "mappings", "value": [{"type": "value", "options": {
                        "WARNING": {"color": "orange", "index": 0}, "ERROR": {"color": "red", "index": 1},
                        "CRITICAL": {"color": "dark-red", "index": 2}, "INFO": {"color": "text", "index": 3}}}]}]}]),
        16, 9)
    b.add(table("Task errors with traceback", sql("""SELECT ts, task_id, fields->>'error_class' AS error_class,
        left(message, 300) AS message, right(fields->>'traceback', 1500) AS traceback FROM audit.log_event
        WHERE pipeline = 'rfb' AND event = 'task_error' AND $__timeFilter(ts) ORDER BY ts DESC LIMIT 50"""),
        units={"ts": "dateTimeAsIso"}), 24, 8)

    b.row("Raw: Airflow task logs")
    b.add(timeseries("Airflow task log lines by level", [loki(
        'sum by (level) (count_over_time({job="airflow-tasks", dag_id="rfb_pipeline", task_id=~"$task"} [$__auto]))',
        "{{level}}")], draw="bars", stack=True), 24, 6)
    b.add(logs("Airflow task logs ($task)", '{job="airflow-tasks", dag_id="rfb_pipeline", task_id=~"$task"} '
                                            '|~ "(?i)$search"',
               desc="Exactly what the Airflow UI shows per task attempt (stdout of the scripts included)."), 24, 14)
    b.row("Raw: pipeline JSON events")
    b.add(logs("Pipeline events (JSON, one event per line)", '{job="lambda-pipelines", pipeline="rfb"} '
                                                             '|~ "(?i)$search"',
               desc="Structured events written by pipelines/commons/logger.py; open a line to see every field."),
          24, 14)
    return b.json()


# ----------------------------------------------------------------------------------------------- 4. ANP
ANP_DAG = "'anp_pipeline'"
ANP_RUN_VAR = {**RUN_VAR, "query": "SELECT run_id FROM audit.pipeline_run WHERE pipeline = 'anp' "
                                   "ORDER BY started_at DESC LIMIT 100"}
ANP_RUN_VAR["definition"] = ANP_RUN_VAR["query"]
KIND_VAR = {"name": "kind", "label": "File kind", "type": "custom", "query": "semestral,mensal,semanal",
            "includeAll": True, "multi": True, "current": {"text": "All", "value": "$__all"},
            "options": [{"text": v, "value": v, "selected": False} for v in ("semestral", "mensal", "semanal")]}
PRODUCT_VAR = {"name": "product", "label": "Product", "type": "query", "datasource": PG, "refresh": 1, "sort": 1,
               "query": "SELECT DISTINCT produto FROM anp_bronze.batch_profile WHERE produto NOT IN ('*', 'GLP')",
               "definition": "SELECT DISTINCT produto FROM anp_bronze.batch_profile WHERE produto NOT IN ('*', 'GLP')",
               "includeAll": True, "multi": True, "current": {"text": "All", "value": "$__all"}}
KINDS = "source_kind IN (${kind:singlequote})"
# one value per month and product when several files cover it: the most consolidated source wins (as in silver)
SOURCE_RANK = "CASE source_kind WHEN 'semestral' THEN 1 WHEN 'mensal' THEN 2 ELSE 3 END"
BATCH_TIME = "to_date(batch_month::text, 'YYYYMM')::timestamp"
HISTORY = "25y"                      # survey panels show the whole history whatever the dashboard time range
PRODUCT_COLS = ("gasolina", "gasolina_aditivada", "etanol", "diesel", "diesel_s10", "diesel_s50", "gnv", "glp")


def history(panel: dict) -> dict:
    panel["timeFrom"] = HISTORY
    return panel


def anp_runs_annotation() -> dict:
    return {"name": "ANP runs", "enable": True, "iconColor": "#8AB8FF", "datasource": PG,
            "target": sql("""SELECT started_at AS time, coalesce(finished_at, now()) AS timeend,
                             'anp ' || status AS text, status AS tags
                             FROM audit.pipeline_run WHERE pipeline = 'anp' AND $__timeFilter(started_at)""")}


def anp_board() -> dict:
    b = Board("lambda4-anp", "ANP · Pipeline health & survey batches",
              "ANP fuel price survey. Section 1: pipeline health (tasks, source files control, Airflow, logs). "
              "Section 2: statistical pre-assessment of every survey batch (file x collection month) from "
              "anp_bronze.batch_profile.", ["lambda4", "anp"], time_from="now-7d")
    b.vars.extend([ANP_RUN_VAR, TRACE_VAR, KIND_VAR, PRODUCT_VAR])
    b.annotations.append(anp_runs_annotation())
    R = "'$run_id'"

    # ======================================================================================= 1. pipeline health
    b.row("1 · Pipeline health — run $run_id")
    b.add(stat("Status", sql(f"SELECT status FROM audit.pipeline_run WHERE run_id = {R}"), mappings=status_mappings(),
               color_mode="background", text_mode="value"), 3, 4)
    b.add(stat("Duration", sql(f"SELECT extract(epoch FROM coalesce(finished_at, now()) - started_at) "
                               f"FROM audit.pipeline_run WHERE run_id = {R}"), unit="s"), 3, 4)
    b.add(stat("Tasks failed", sql(f"SELECT count(*) FROM (SELECT DISTINCT ON (task_id) status FROM audit.task_run "
                                   f"WHERE run_id = {R} ORDER BY task_id, attempt DESC) t WHERE status = 'FAILED'"),
               thresholds=red_if_positive(), color_mode="background"), 3, 4)
    b.add(stat("Files downloaded", sql(f"SELECT coalesce(sum(files_ok), 0) FROM audit.task_run WHERE run_id = {R} "
                                       "AND task_id = 'anp02_download'")), 3, 4)
    b.add(stat("Downloaded", sql(f"SELECT coalesce(sum(bytes_out), 0) FROM audit.task_run WHERE run_id = {R} "
                                 "AND task_id = 'anp02_download'"), unit="bytes"), 3, 4)
    b.add(stat("Rows loaded (bronze)", sql(f"SELECT coalesce(sum(rows_out), 0) FROM audit.task_run "
                                           f"WHERE run_id = {R} AND layer = 'bronze'")), 3, 4)
    b.add(stat("DQ BLOCK / WARN fails",
               sql(f"SELECT count(*) FILTER (WHERE severity = 'BLOCK') AS block, "
                   f"count(*) FILTER (WHERE severity = 'WARN') AS warn FROM audit.dq_event "
                   f"WHERE run_id = {R} AND status = 'FAIL'"), thresholds=orange_if_positive(),
               color_mode="background"), 3, 4)
    b.add(stat("Rows quarantined", sql(f"SELECT coalesce(sum(rows_rejected), 0) FROM audit.task_run "
                                       f"WHERE run_id = {R}"), thresholds=orange_if_positive()), 3, 4)
    b.add(table("Tasks of the run", sql(
        f"""SELECT task_id, attempt, status, started_at, round(duration_s) AS duration_s, files_total, files_ok,
            files_failed, files_skipped, rows_in, rows_out, rows_rejected, bytes_out, warn_count, block_count,
            airflow_task_id, error_class, left(error_message, 200) AS error
            FROM audit.task_run WHERE run_id = {R} ORDER BY started_at"""),
        status_cols=("status",), units={"duration_s": "s", "bytes_out": "bytes", "started_at": "dateTimeAsIso"}),
        16, 8)
    b.add(barchart("Task duration in this run", sql(
        f"""SELECT task_id || CASE WHEN attempt > 1 THEN ' #' || attempt ELSE '' END AS task,
            round(coalesce(duration_s, extract(epoch FROM now() - started_at))) AS seconds
            FROM audit.task_run WHERE run_id = {R} ORDER BY started_at"""), unit="s", x_field="task"), 8, 8)
    b.add(table("Checks of the run", sql(f"""SELECT created_at, task_id, check_name, severity, status, observed,
        threshold, left(message, 300) AS message FROM audit.dq_event WHERE run_id = {R}
        ORDER BY (status = 'FAIL') DESC, created_at"""),
        status_cols=("status", "severity"), units={"created_at": "dateTimeAsIso"}), 16, 9)
    b.add(table("Preflight health checks", sql(f"""SELECT component, status, latency_ms, detail::text AS detail
        FROM audit.health_check WHERE run_id = {R} ORDER BY component"""), status_cols=("status",),
        units={"latency_ms": "ms"}), 8, 9)
    b.add(traces("Trace of the run (audit SQL, works without Tempo)", sql(f"""SELECT "traceID", "spanID",
                 "parentSpanID", "operationName", "serviceName", "startTime", duration FROM audit.v_trace_spans
                 WHERE run_id = {R} ORDER BY "startTime" """),
                 desc="Same ids as the Tempo trace (md5(run_id)); with Tempo running, Explore → Tempo → "
                      '{resource.service.name="lambda-anp"} shows the full spans.'), 24, 9)

    b.row("1 · Pipeline health — source files (ctrl)")
    files = "FROM ctrl.source_file WHERE pipeline = 'anp'"
    b.add(stat("Files known", sql(f"SELECT count(*) {files}")), 4, 4)
    b.add(stat("Downloaded", sql(f"SELECT count(*) {files} AND download_status = 'DOWNLOADED'")), 4, 4)
    b.add(stat("Loaded in bronze", sql(f"SELECT count(*) {files} AND bronze_status = 'LOADED'")), 4, 4)
    b.add(stat("Pending (download or bronze)", sql(
        f"SELECT count(*) {files} AND ((listed AND download_status = 'PENDING') OR "
        "(download_status = 'DOWNLOADED' AND bronze_status = 'PENDING'))"), thresholds=orange_if_positive()), 4, 4)
    b.add(stat("Failed / missing at source", sql(
        f"SELECT count(*) {files} AND (download_status IN ('FAILED', 'MISSING_AT_SOURCE') "
        "OR bronze_status = 'FAILED')"), thresholds=red_if_positive(), color_mode="background"), 4, 4)
    b.add(stat("Landing size", sql(f"SELECT coalesce(sum(local_bytes), 0) {files}"), unit="bytes"), 4, 4)
    b.add(table("Files by kind and state", sql(f"""SELECT source_kind, product_group, download_status, bronze_status,
        count(*) AS files, sum(local_bytes) AS bytes, sum(bronze_rows) AS rows, sum(bronze_quarantined) AS quarantined,
        min(period_start) AS from_period, max(period_end) AS to_period, max(loaded_at) AS last_load {files}
        GROUP BY 1, 2, 3, 4 ORDER BY 1, 2, 3, 4"""), status_cols=("download_status", "bronze_status"),
        units={"bytes": "bytes", "last_load": "dateTimeAsIso"},
        overrides=[{"matcher": {"id": "byRegexp", "options": "download_status|bronze_status"},
                    "properties": [{"id": "mappings", "value": [{"type": "value", "options": {
                        "DOWNLOADED": {"color": "green", "index": 0}, "LOADED": {"color": "green", "index": 1},
                        "PENDING": {"color": "blue", "index": 2}, "FAILED": {"color": "red", "index": 3},
                        "MISSING_AT_SOURCE": {"color": "orange", "index": 4},
                        "SKIPPED_DUPLICATE": {"color": "text", "index": 5}}}]}]}]), 12, 9)
    b.add(table("Files needing attention", sql(f"""SELECT file_key, download_status, bronze_status, listed,
        download_attempts, left(last_error, 200) AS last_error, url, updated_at {files}
        AND (download_status IN ('FAILED', 'MISSING_AT_SOURCE') OR bronze_status = 'FAILED' OR NOT listed
             OR (listed AND download_status = 'PENDING') OR duplicate_of IS NOT NULL)
        ORDER BY updated_at DESC LIMIT 200"""), status_cols=("download_status", "bronze_status"),
        units={"updated_at": "dateTimeAsIso"},
        desc="Failed or missing downloads/loads, files no longer linked from the page, pending ones and duplicates."),
        12, 9)
    b.add(timeseries("Control events per day", [sql("""SELECT $__timeGroupAlias(created_at, '1d'),
        event AS metric, count(*) AS value FROM ctrl.source_file_event WHERE pipeline = 'anp'
        AND $__timeFilter(created_at) GROUP BY 1, 2 ORDER BY 1""", fmt="time_series")], draw="bars", stack=True,
        desc="DISCOVERED, DOWNLOADED, LOADED, CHANGED_AT_SOURCE, ... (ctrl.source_file_event)."), 10, 9)
    b.add(table("Recent control events", sql("""SELECT created_at, file_key, event, version, bytes, rows,
        left(detail::text, 300) AS detail, run_id FROM ctrl.source_file_event WHERE pipeline = 'anp'
        AND $__timeFilter(created_at) ORDER BY id DESC LIMIT 300"""),
        units={"created_at": "dateTimeAsIso", "bytes": "bytes"}), 14, 9)

    b.row("1 · Pipeline health — Airflow (anp_pipeline) and logs")
    b.add(table("anp_pipeline runs", sql(f"""SELECT run_id, state, run_type, queued_at, start_date, end_date,
        round(extract(epoch FROM coalesce(end_date, now()) - start_date)) AS duration_s, conf::text AS conf
        FROM dag_run WHERE dag_id = {ANP_DAG} ORDER BY coalesce(start_date, queued_at) DESC LIMIT 30""", ds=AF),
        status_cols=("state",), units={"duration_s": "s", "queued_at": "dateTimeAsIso",
                                       "start_date": "dateTimeAsIso", "end_date": "dateTimeAsIso"}), 12, 8)
    latest_run = (f"(SELECT run_id FROM dag_run WHERE dag_id = {ANP_DAG} "
                  "ORDER BY coalesce(start_date, queued_at) DESC LIMIT 1)")
    b.add(table("Task instances of the latest Airflow run", sql(f"""SELECT task_id, state, try_number, start_date,
        end_date, round(duration::numeric) AS duration_s, hostname FROM task_instance WHERE dag_id = {ANP_DAG}
        AND run_id = {latest_run} ORDER BY start_date NULLS LAST""", ds=AF), status_cols=("state",),
        units={"duration_s": "s", "start_date": "dateTimeAsIso", "end_date": "dateTimeAsIso"}), 12, 8)
    b.add(timeseries("Airflow task log lines by level", [loki(
        'sum by (level) (count_over_time({job="airflow-tasks", dag_id="anp_pipeline"} [$__auto]))', "{{level}}")],
        draw="bars", stack=True), 12, 7)
    b.add(timeseries("Pipeline events by level", [loki(
        'sum by (level) (count_over_time({job="lambda-pipelines", pipeline="anp"} [$__auto]))', "{{level}}")],
        draw="bars", stack=True), 12, 7)
    b.add(logs("Airflow task logs (anp_pipeline)", '{job="airflow-tasks", dag_id="anp_pipeline"}',
               desc="Exactly what the Airflow UI shows per task attempt (stdout of the scripts included)."), 12, 12)
    b.add(table("Pipeline warnings and errors (audit.log_event)", sql("""SELECT ts, level, task_id, event,
        left(message, 300) AS message FROM audit.log_event WHERE pipeline = 'anp'
        AND level IN ('WARNING', 'ERROR', 'CRITICAL') AND $__timeFilter(ts) ORDER BY ts DESC LIMIT 300"""),
        units={"ts": "dateTimeAsIso"}), 12, 12)

    # ======================================================================================= 2. survey batches
    b.row("2 · Survey batches (lotes de pesquisa) — statistical pre-assessment")
    b.add(text("How to read", """A **survey batch** (*lote de pesquisa*) is one source file × one collection month
(`YYYY-MM` of *data da coleta*): a monthly file is one batch, a semester file six, a weekly file one or two.
Statistics are computed at bronze load time (`anp_bronze.batch_profile`, raw text parsed only here: prices with a
decimal comma, dates `dd/mm/yyyy`). Since 2023 the same month exists in a semester, a monthly and weekly files:
compare them here; silver keeps one (semestral > mensal > semanal). **GLP** is priced per 13 kg bottle, the others
per litre (GNV per m³). Filter by *File kind* and *Product* at the top."""), 24, 4)
    matrix = sql(f"""SELECT batch, file_key, source_kind, rows, stations, brands, cities, ufs,
        {', '.join(PRODUCT_COLS)}, other_product_rows, price_missing FROM anp_bronze.v_batch_matrix
        WHERE {KINDS} ORDER BY batch_month DESC, {SOURCE_RANK}, file_key""")
    price_ov = [{"matcher": {"id": "byName", "options": c},
                 "properties": [{"id": "unit", "value": "currencyBRL"}, {"id": "decimals", "value": 3}]}
                for c in PRODUCT_COLS]
    b.add(table("Batch matrix — average sale price per product, stations (CNPJ) and brands per batch", matrix,
                desc="One row per survey batch. Price columns = average valor_de_venda of the product in the batch "
                     "(empty = product not surveyed in that file). stations = distinct CNPJ surveyed, brands = "
                     "distinct bandeiras, price_missing = rows without a parseable price.",
                overrides=price_ov + [{"matcher": {"id": "byName", "options": "price_missing"},
                                       "properties": [{"id": "custom.cellOptions", "value": {"type": "color-text"}},
                                                      {"id": "color", "value": {"mode": "thresholds"}},
                                                      {"id": "thresholds", "value": {"mode": "absolute", "steps": [
                                                          {"color": "green", "value": None},
                                                          {"color": "orange", "value": 1}]}}]}]), 24, 14)
    b.add(stat("Survey batches", sql(f"SELECT count(*) FROM anp_bronze.v_batch_matrix WHERE {KINDS}")), 6, 4)
    b.add(stat("Months covered", sql(f"""SELECT count(DISTINCT batch_month) FROM anp_bronze.batch_profile
        WHERE batch_month > 0 AND {KINDS}""")), 6, 4)
    b.add(stat("First → last month", sql(f"""SELECT min(batch) || ' → ' || max(batch) FROM anp_bronze.v_batch_matrix
        WHERE batch_month > 0 AND {KINDS}"""), text_mode="value"), 6, 4)
    b.add(stat("Rows without a parseable price", sql(f"""SELECT coalesce(sum(price_missing), 0)
        FROM anp_bronze.batch_profile WHERE produto = '*' AND {KINDS}"""), thresholds=orange_if_positive()), 6, 4)

    price_series = f"""SELECT time, produto AS metric, price_avg AS value FROM (
        SELECT DISTINCT ON (batch_month, produto) {BATCH_TIME} AS time, batch_month, produto, price_avg
        FROM anp_bronze.batch_profile WHERE batch_month > 0 AND {KINDS} AND produto {{filter}}
        ORDER BY batch_month, produto, {SOURCE_RANK}, profiled_at DESC) t ORDER BY 1"""
    b.add(history(timeseries("Average sale price per month — fuels (R$/l, GNV R$/m³)", [sql(
        price_series.replace("{filter}", "IN (${product:singlequote})"), fmt="time_series")],
        unit="currencyBRL", min0=False,
        desc="One point per month and product; when several files cover a month the most consolidated source wins "
             "(semestral > mensal > semanal).")), 16, 9)
    b.add(history(timeseries("Average GLP P13 price per month (R$/13 kg)", [sql(
        price_series.replace("{filter}", "= 'GLP'"), fmt="time_series")], unit="currencyBRL", min0=False)), 8, 9)
    whole = f"FROM anp_bronze.batch_profile WHERE produto = '*' AND batch_month > 0 AND {KINDS}"
    family = "source_kind || ' ' || product_group"
    b.add(history(timeseries("Stations surveyed (distinct CNPJ) per batch", [sql(
        f"SELECT {BATCH_TIME} AS time, {family} AS metric, stations AS value {whole} ORDER BY 1",
        fmt="time_series")], draw="points", points="always",
        desc="Per file family: overlapping files of the same month should survey a similar number of stations.")),
        12, 8)
    b.add(history(timeseries("Brands (bandeiras) per batch", [sql(
        f"SELECT {BATCH_TIME} AS time, {family} AS metric, brands AS value {whole} ORDER BY 1",
        fmt="time_series")], draw="points", points="always")), 12, 8)
    b.add(history(timeseries("Collections (rows) per batch", [sql(
        f"SELECT {BATCH_TIME} AS time, {family} AS metric, rows AS value {whole} ORDER BY 1",
        fmt="time_series")], draw="bars")), 12, 8)
    b.add(history(timeseries("Cities and UFs per batch", [
        sql(f"SELECT {BATCH_TIME} AS time, {family} || ' cities' AS metric, cities AS value {whole} ORDER BY 1",
            fmt="time_series"),
        sql(f"SELECT {BATCH_TIME} AS time, {family} || ' UFs' AS metric, ufs AS value {whole} ORDER BY 1",
            fmt="time_series", ref="B")], draw="points", points="always")), 12, 8)
    b.add(table("Price dispersion per batch and product", sql(f"""SELECT left(batch_month::text, 4) || '-' ||
        right(batch_month::text, 2) AS batch, file_key, produto, rows, stations, price_min, price_p50, price_avg,
        price_max, price_stddev, round(100 * price_stddev / nullif(price_avg, 0), 1) AS cv_pct, price_missing,
        buy_price_avg FROM anp_bronze.batch_profile WHERE produto <> '*' AND batch_month > 0 AND {KINDS}
        AND produto IN (${{product:singlequote}}, 'GLP') ORDER BY batch_month DESC, produto, {SOURCE_RANK}"""),
        desc="min / median / average / max sale price, standard deviation and coefficient of variation (cv %) of "
             "every product in every batch. A cv far from its neighbours or min/max outliers point to typing errors "
             "at the source.",
        overrides=[{"matcher": {"id": "byRegexp", "options": "price_.*|buy_price_avg"},
                    "properties": [{"id": "decimals", "value": 3}]},
                   {"matcher": {"id": "byName", "options": "price_missing"},
                    "properties": [{"id": "decimals", "value": 0}]}]), 24, 12)
    b.add(table("Data quality per file", sql(f"""SELECT s.file_key, s.source_kind, s.bronze_rows AS rows,
        s.bronze_quarantined AS quarantined, p.batches, p.no_date_rows, p.price_missing, p.other_product_rows,
        s.version, s.loaded_at FROM ctrl.source_file s LEFT JOIN (
            SELECT file_key, count(*) FILTER (WHERE batch_month > 0) AS batches,
                   coalesce(sum(rows) FILTER (WHERE batch_month = 0), 0) AS no_date_rows,
                   sum(price_missing) AS price_missing, sum(other_product_rows) AS other_product_rows
            FROM anp_bronze.v_batch_matrix GROUP BY file_key) p ON p.file_key = s.file_key
        WHERE s.pipeline = 'anp' AND s.bronze_status = 'LOADED' AND s.{KINDS}
        ORDER BY (coalesce(s.bronze_quarantined, 0) + coalesce(p.no_date_rows, 0) + coalesce(p.price_missing, 0)) DESC,
        s.file_key"""),
        desc="Quarantined records (wrong field count), rows whose date did not parse (batch 'no date'), rows without "
             "a price, rows of products outside the matrix columns.", units={"loaded_at": "dateTimeAsIso"}), 24, 9)
    return b.json()


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    for name, board in (("lambda4-rfb-health", pipeline_health()), ("lambda4-airflow", airflow_board()),
                        ("lambda4-logs", logs_board()), ("lambda4-anp", anp_board())):
        (OUT / f"{name}.json").write_text(json.dumps(board, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        print(f"wrote {OUT / name}.json ({len(board['panels'])} panels)")


if __name__ == "__main__":
    main()
