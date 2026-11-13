# Databricks notebook source
# MAGIC %pip install --quiet --force-reinstall --no-deps /Volumes/keelstone/platform/artifacts/keelstone_holdings-0.1.0-py3-none-any.whl

# COMMAND ----------

dbutils.library.restartPython()  # noqa: F821

# COMMAND ----------

# Code comes from the wheel in the artifacts volume, configs from the same volume:
# serverless cannot reliably read bundle files under /Workspace on this workspace.
"""Measure one run of a client's job (slice 5). Writes a row to keelstone.platform.perf_runs.

  tasks        runtime of every job task (Jobs API)
  pipeline     the latest pipeline update: total time; per flow, rows written and time from
               STARTING to COMPLETED (event log); how each materialized view was refreshed
  tables       Delta files and bytes per table, clustering columns (DESCRIBE DETAIL)
  skew         rows per business date, per account and per security in silver_positions
Serverless has no Spark UI; the event log, Delta details and query history are what is available.
"""
import json
from datetime import datetime, timezone

from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

dbutils.widgets.text("client", "")  # noqa: F821
dbutils.widgets.text("job_run_id", "")  # noqa: F821
dbutils.widgets.text("label", "unlabelled")  # noqa: F821
client = dbutils.widgets.get("client")  # noqa: F821
job_run_id = dbutils.widgets.get("job_run_id")  # noqa: F821
label = dbutils.widgets.get("label")  # noqa: F821
spark = SparkSession.builder.getOrCreate()
S = f"keelstone.{client}"
w = WorkspaceClient()

# tasks ----------------------------------------------------------------------------------
run = w.jobs.get_run(int(job_run_id))
tasks = {}
for t in run.tasks or []:
    if t.start_time and t.end_time and t.state and t.state.result_state:
        tasks[t.task_key] = {"seconds": round((t.end_time - t.start_time) / 1000, 1),
                             "result": t.state.result_state.value}

# pipeline update -------------------------------------------------------------------------
ev = spark.sql(f"SELECT timestamp, event_type, origin.update_id AS u, origin.flow_name AS flow, details "
               f"FROM event_log(TABLE({S}.silver_positions))")
latest = ev.where("event_type = 'create_update'").orderBy(F.col("timestamp").desc()).first()["u"]
rows = [r.asDict() for r in ev.where(F.col("u") == latest).orderBy("timestamp").collect()]
flows, update = {}, {}
for r in rows:
    d = json.loads(r["details"])
    ts = r["timestamp"]
    if r["event_type"] == "create_update":
        update["start"] = ts
    if r["event_type"] == "update_progress" and (d.get("update_progress") or {}).get("state") == "COMPLETED":
        update["end"] = ts
    if r["event_type"] == "flow_progress" and r["flow"] and r["flow"] != "missingFlowName":
        fp = d.get("flow_progress") or {}
        m = fp.get("metrics") or {}
        f = flows.setdefault(r["flow"].split(".")[-1], {"rows": 0, "executor_seconds": 0.0})
        if fp.get("status") in ("STARTING", "RUNNING") and "start" not in f:
            f["start"] = ts
        if fp.get("status") == "COMPLETED":
            f["end"] = ts
        # appends report num_output_rows; AUTO CDC (MERGE) flows report num_upserted_rows
        f["rows"] += int(m.get("num_output_rows") or 0) + int(m.get("num_upserted_rows") or 0)
        f["executor_seconds"] += round(int(m.get("executor_time_ms") or 0) / 1000, 1)
    if r["event_type"] == "planning_information" and r["flow"]:
        pi = d.get("planning_information") or {}
        tech = [t.get("incrementalization_issues") and "complete_recompute" or t.get("maintenance_type")
                for t in pi.get("technique_information") or []]
        flows.setdefault(r["flow"].split(".")[-1], {"rows": 0, "executor_seconds": 0.0})["refresh"] = sorted(
            {t for t in tech if t})
for f in flows.values():
    if "start" in f and "end" in f:
        f["seconds"] = round((f.pop("end") - f.pop("start")).total_seconds(), 1)
    f.pop("start", None); f.pop("end", None)
pipeline = {"update_id": latest, "flows": flows,
            "executor_seconds": round(sum(f.get("executor_seconds", 0) for f in flows.values()), 1),
            "seconds": round((update["end"] - update["start"]).total_seconds(), 1) if "end" in update else None}

# tables ------------------------------------------------------------------------------------
tables = {}
for r in spark.sql(f"SHOW TABLES IN {S}").collect():
    name = r["tableName"]
    try:
        det = spark.sql(f"DESCRIBE DETAIL {S}.{name}").first().asDict()
        tables[name] = {"files": det.get("numFiles"), "bytes": det.get("sizeInBytes"),
                        "cluster_by": det.get("clusteringColumns") or []}
    except Exception:  # noqa: BLE001  materialized views do not support DESCRIBE DETAIL
        tables[name] = {"files": None, "note": "materialized view"}

# skew --------------------------------------------------------------------------------------
pos = spark.table(f"{S}.silver_positions")
def spread(col):
    r = pos.groupBy(col).count().agg(F.min("count"), F.expr("percentile(count, 0.5)"), F.max("count"),
                                     F.count(F.lit(1))).first()
    return {"groups": r[3], "min": r[0], "median": float(r[1]), "max": r[2],
            "max_over_median": round(r[2] / float(r[1]), 2) if r[1] else None}
skew = {c: spread(c) for c in ("business_date", "account_id", "security_id")}

log = spark.table(f"{S}.bronze_file_log").where(F.col("run_id") == f"{job_run_id}-ingest")
ingested = log.agg(F.sum("rows_total"), F.count(F.lit(1))).first()
result = {"label": label, "client": client, "job_run_id": job_run_id,
          "measured_at": datetime.now(timezone.utc).isoformat(),
          "ingest": {"files": ingested[1], "rows": ingested[0],
                     "seconds": tasks.get("ingest_bronze", {}).get("seconds")},
          "tasks": tasks, "pipeline": pipeline, "tables": tables, "skew": skew}
if result["ingest"]["rows"] and result["ingest"]["seconds"]:
    result["ingest"]["rows_per_second"] = round(result["ingest"]["rows"] / result["ingest"]["seconds"])
spark.sql("CREATE SCHEMA IF NOT EXISTS keelstone.platform")
spark.createDataFrame([(label, client, job_run_id, result["measured_at"], json.dumps(result, default=str))],
                      "label string, client string, job_run_id string, measured_at string, result string") \
    .write.mode("append").saveAsTable("keelstone.platform.perf_runs")
dbutils.notebook.exit(json.dumps(result, default=str))  # noqa: F821
