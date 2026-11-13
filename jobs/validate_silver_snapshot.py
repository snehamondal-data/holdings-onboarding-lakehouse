# Databricks notebook source
# MAGIC %pip install --quiet --force-reinstall --no-deps /Volumes/keelstone/platform/artifacts/keelstone_holdings-0.1.0-py3-none-any.whl

# COMMAND ----------

dbutils.library.restartPython()  # noqa: F821

# COMMAND ----------

# Code comes from the wheel in the artifacts volume, configs from the same volume:
# serverless cannot reliably read bundle files under /Workspace on this workspace.
"""Snapshot every silver and gold table (row count + content checksum) into keelstone.qa.silver_snapshots,
so the idempotency check can compare the tables after update, re-run and full refresh."""
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from keelstone.qa import QA_CATALOG, QA_SCHEMA

dbutils.widgets.text("client", "")  # noqa: F821
dbutils.widgets.text("run_tag", "")  # noqa: F821
dbutils.widgets.text("label", "")  # noqa: F821
client = dbutils.widgets.get("client")  # noqa: F821
run_tag = dbutils.widgets.get("run_tag")  # noqa: F821
label = dbutils.widgets.get("label")  # noqa: F821

spark = SparkSession.builder.getOrCreate()
# Every silver and gold table this client's pipeline owns (clients differ: not every client has trades).
SILVER_TABLES = sorted(r["tableName"] for r in spark.sql(f"SHOW TABLES IN keelstone.{client}").collect()
                       if r["tableName"].startswith(("silver_", "gold_")) or r["tableName"] == "unresolved_securities")

taken = datetime.now(timezone.utc).isoformat()
rows = []
for t in SILVER_TABLES:
    df = spark.table(f"keelstone.{client}.{t}")
    cols = sorted(c for c in df.columns if not c.startswith("__"))
    r = df.select(F.count(F.lit(1)).alias("n"),
                  F.sum(F.xxhash64(*[F.col(c) for c in cols]).cast("decimal(38,0)")).cast("string").alias("h")).first()
    rows.append((run_tag, label, t, int(r["n"]), r["h"] or "0", taken))
    print(label, t, r["n"], r["h"])
spark.createDataFrame(rows, "run_tag string, label string, table_name string, n_rows bigint, checksum string, taken_at string") \
    .write.mode("append").saveAsTable(f"{QA_CATALOG}.{QA_SCHEMA}.silver_snapshots")
dbutils.notebook.exit(str({t: n for _, _, t, n, _, _ in rows}))  # noqa: F821
