# Databricks notebook source
# MAGIC %pip install --quiet --force-reinstall --no-deps /Volumes/keelstone/platform/artifacts/keelstone_holdings-0.1.0-py3-none-any.whl

# COMMAND ----------

dbutils.library.restartPython()  # noqa: F821

# COMMAND ----------

# Code comes from the wheel in the artifacts volume, configs from the same volume:
# serverless cannot reliably read bundle files under /Workspace on this workspace.
import os
import json
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
dbutils.widgets.text("config_dir", "/Volumes/keelstone/platform/artifacts/config")  # noqa: F821
dbutils.widgets.text("client", "")  # noqa: F821
dbutils.widgets.text("rerun_id", "")  # noqa: F821
CONFIG_DIR = dbutils.widgets.get("config_dir")  # noqa: F821
import keelstone  # noqa: E402

try:  # let foreachBatch workers unpickle our functions even without the wheel
    from pyspark import cloudpickle
    cloudpickle.register_pickle_by_value(keelstone)
except Exception as e:  # pragma: no cover
    print(f"note: register_pickle_by_value unavailable ({e})")
"""Slice 1 validation: compare what bronze did with the ground truth (read from keelstone.qa).

This is the only job allowed to read the QA area. It fails the run if any check fails.
Checks, for every file currently in the landing volume:
  1. it appears exactly once in bronze_file_log, with the expected status;
  2. row counts match ground truth (total, bronze, quarantined) and no row is unaccounted for;
  3. bronze table rows per file match the expected clean rows;
  4. quarantine rows per (file, reason) match the planted corrupt rows;
  5. schema drift is logged with the expected columns for drifted files and only those;
  6. the re-sent duplicate file is logged as duplicate_content of the original;
  7. the idempotency re-run (--rerun-id) wrote nothing.
"""
a = type('Args', (), {"client": dbutils.widgets.get("client"), "rerun_id": dbutils.widgets.get("rerun_id") or None})()

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from keelstone.config import load_client, plan_ingest  # noqa: E402
from keelstone.qa import QA_CATALOG, QA_SCHEMA, ground_truth_path  # noqa: E402

spark = SparkSession.builder.getOrCreate()
client = load_client(a.client, config_dir=CONFIG_DIR)
plans = plan_ingest(a.client, config_dir=CONFIG_DIR)
truth = json.load(open(ground_truth_path(a.client)))
gt_files = truth["files"]
root = client.landing_root + "/"
rel = lambda path: path.replace("dbfs:", "", 1)[len(root):]

failures: list[str] = []
def check(ok: bool, msg: str) -> None:
    if not ok:
        failures.append(msg)

landed = sorted(rel(os.path.join(d, f)) for d, _, fs in os.walk(client.landing_root) for f in fs)
log = [r.asDict() for r in spark.table(plans[0].file_log_table).collect()]
log_by_file = defaultdict(list)
for r in log:
    log_by_file[rel(r["file_path"])].append(r)

bronze_counts = Counter()
for pl in plans:
    if spark.catalog.tableExists(pl.bronze_table):
        for r in spark.table(pl.bronze_table).groupBy("_source_file").count().collect():
            bronze_counts[rel(r[0])] += r[1]
q_counts = defaultdict(Counter)
for r in spark.table(plans[0].quarantine_table).groupBy("file_path", "reason").count().collect():
    q_counts[rel(r[0])][r[1]] += r[2]
drift = defaultdict(list)
for r in spark.table(plans[0].drift_log_table).collect():
    drift[rel(r["file_path"])].append(sorted(r["unexpected_columns"]))

# 1-6: per-file checks
for f in landed:
    exp = gt_files.get(f)
    check(exp is not None, f"{f}: landed file not in ground truth")
    if exp is None:
        continue
    entries = log_by_file.get(f, [])
    check(len(entries) == 1, f"{f}: {len(entries)} file-log entries, expected exactly 1")
    if len(entries) != 1:
        continue
    e = entries[0]
    check(e["status"] == exp["expected_status"], f"{f}: status {e['status']} != {exp['expected_status']}")
    check(e["sha256"] == exp["sha256"], f"{f}: sha256 differs from manifest")
    check(e["rows_total"] == exp["records"], f"{f}: rows_total {e['rows_total']} != {exp['records']}")
    check(e["rows_bronze"] == exp["expected_bronze_rows"],
          f"{f}: rows_bronze {e['rows_bronze']} != {exp['expected_bronze_rows']}")
    exp_q = sum(exp["expected_quarantine"].values())
    check(e["rows_quarantined"] == exp_q, f"{f}: rows_quarantined {e['rows_quarantined']} != {exp_q}")
    if e["status"] == "loaded":
        check(e["rows_bronze"] + e["rows_quarantined"] == e["rows_total"], f"{f}: rows unaccounted for")
    check(bronze_counts[f] == exp["expected_bronze_rows"],
          f"{f}: bronze table has {bronze_counts[f]} rows, expected {exp['expected_bronze_rows']}")
    check(dict(q_counts[f]) == exp["expected_quarantine"],
          f"{f}: quarantine {dict(q_counts[f])} != {exp['expected_quarantine']}")
    want_drift = [exp["expected_drift_columns"]] if exp["expected_drift_columns"] and exp["expected_status"] == "loaded" else []
    check(drift.get(f, []) == want_drift, f"{f}: drift log {drift.get(f, [])} != {want_drift}")
    if exp["duplicate_of"]:
        check((e["duplicate_of"] or "").endswith(exp["duplicate_of"]),
              f"{f}: duplicate_of {e['duplicate_of']} != {exp['duplicate_of']}")

check(not (set(log_by_file) - set(landed)), f"file log has files not in landing: {sorted(set(log_by_file) - set(landed))[:5]}")

# 7: idempotency re-run wrote nothing
if a.rerun_id:
    n_log = spark.table(plans[0].file_log_table).where(F.col("run_id") == a.rerun_id).count()
    n_q = spark.table(plans[0].quarantine_table).where(F.col("run_id") == a.rerun_id).count()
    n_b = sum(spark.table(pl.bronze_table).where(F.col("_run_id") == a.rerun_id).count()
              for pl in plans if spark.catalog.tableExists(pl.bronze_table))
    check(n_log + n_q + n_b == 0, f"re-run {a.rerun_id} wrote {n_log} log, {n_b} bronze, {n_q} quarantine rows")

expected_landed = len(landed)
summary = {
    "client": a.client, "checked_at": datetime.now(timezone.utc).isoformat(),
    "files_landed": expected_landed,
    "files_loaded": sum(1 for r in log if r["status"] == "loaded"),
    "files_duplicate": sum(1 for r in log if r["status"] == "duplicate_content"),
    "rows_total": sum(r["rows_total"] for r in log if r["status"] == "loaded"),
    "rows_bronze": sum(bronze_counts.values()),
    "rows_quarantined": sum(sum(c.values()) for c in q_counts.values()),
    "quarantine_by_reason": dict(sum(q_counts.values(), Counter())),
    "drift_files": len(drift), "rerun_id": a.rerun_id, "failures": failures[:50],
    "n_failures": len(failures), "passed": not failures,
}
print("VALIDATION_SUMMARY " + json.dumps(summary, default=str))
spark.createDataFrame([(a.client, "slice1", summary["checked_at"], summary["passed"], json.dumps(summary, default=str))],
                      "client_id string, check_name string, checked_at string, passed boolean, summary string") \
    .write.mode("append").saveAsTable(f"{QA_CATALOG}.{QA_SCHEMA}.validation_results")
if failures:
    raise RuntimeError(f"VALIDATION FAILED ({len(failures)}): " + "; ".join(failures[:10]))
print("VALIDATION PASSED")
dbutils.notebook.exit(json.dumps(summary, default=str))  # noqa: F821
