# Databricks notebook source
# MAGIC %pip install --quiet --force-reinstall --no-deps /Volumes/keelstone/platform/artifacts/keelstone_holdings-0.1.0-py3-none-any.whl

# COMMAND ----------

dbutils.library.restartPython()  # noqa: F821

# COMMAND ----------

# Code comes from the wheel in the artifacts volume, configs from the same volume:
# serverless cannot reliably read bundle files under /Workspace on this workspace.
import os
import sys
import uuid
dbutils.widgets.text("config_dir", "/Volumes/keelstone/platform/artifacts/config")  # noqa: F821
dbutils.widgets.text("client", "")  # noqa: F821
dbutils.widgets.text("run_id", "")  # noqa: F821
dbutils.widgets.text("expect_no_new_files", "false")  # noqa: F821
CONFIG_DIR = dbutils.widgets.get("config_dir")  # noqa: F821
import keelstone  # noqa: E402

try:  # let foreachBatch workers unpickle our functions even without the wheel
    from pyspark import cloudpickle
    cloudpickle.register_pickle_by_value(keelstone)
except Exception as e:  # pragma: no cover
    print(f"note: register_pickle_by_value unavailable ({e})")
"""Generic bronze ingest for one client: loops over the sources its YAML lists.

--expect-no-new-files turns the run into an idempotency check: it fails if this
run wrote any file-log, bronze or quarantine row.
"""
a = type('Args', (), {"client": dbutils.widgets.get("client"), "run_id": dbutils.widgets.get("run_id") or None, "expect_no_new_files": dbutils.widgets.get("expect_no_new_files").lower() == "true"})()

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from keelstone.config import plan_ingest  # noqa: E402
from keelstone.ingest.bronze import run_source  # noqa: E402

spark = SparkSession.builder.getOrCreate()
run_id = a.run_id or uuid.uuid4().hex
plans = plan_ingest(a.client, config_dir=CONFIG_DIR)
print(f"run_id={run_id} client={a.client} sources={[p.source.source_id for p in plans]}")

for plan in plans:
    if not os.path.isdir(plan.input_path):
        print(f"[{plan.source.source_id}] no landing folder yet at {plan.input_path}; skipped")
        continue
    run_source(spark, plan, run_id)

summary = {}
for plan in plans:
    if not spark.catalog.tableExists(plan.file_log_table):
        continue
    sid = plan.source.source_id
    files = (spark.table(plan.file_log_table)
             .where((F.col("run_id") == run_id) & (F.col("source_id") == sid))
             .groupBy("status").count().collect())
    bronze_rows = (spark.table(plan.bronze_table).where(F.col("_run_id") == run_id).count()
                   if spark.catalog.tableExists(plan.bronze_table) else 0)
    quarantined = (spark.table(plan.quarantine_table)
                   .where((F.col("run_id") == run_id) & (F.col("source_id") == sid)).count())
    summary[sid] = {"files": {r["status"]: r["count"] for r in files},
                    "bronze_rows": bronze_rows, "quarantined_rows": quarantined}
    print(f"[{sid}] {summary[sid]}")

if a.expect_no_new_files:
    written = {s: v for s, v in summary.items()
               if v["files"] or v["bronze_rows"] or v["quarantined_rows"]}
    if written:
        raise RuntimeError(f"IDEMPOTENCY FAILED: re-run wrote data: {written}")
    print("IDEMPOTENCY OK: re-run ingested 0 files and 0 rows")

import json  # noqa: E402

dbutils.notebook.exit(json.dumps({"run_id": run_id, "summary": summary}))  # noqa: F821
