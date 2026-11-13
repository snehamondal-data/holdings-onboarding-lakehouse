# Databricks notebook source
# MAGIC %pip install --quiet --force-reinstall --no-deps /Volumes/keelstone/platform/artifacts/keelstone_holdings-0.1.0-py3-none-any.whl

# COMMAND ----------

dbutils.library.restartPython()  # noqa: F821

# COMMAND ----------

# Code comes from the wheel in the artifacts volume, configs from the same volume:
# serverless cannot reliably read bundle files under /Workspace on this workspace.
"""Gold validation for one client (reads ground truth from keelstone.qa).

Works on whatever has been delivered so far: "through" is the last business date in the landing
volume, and a late custodian file that has not landed yet makes its book positions pending.
Checks:
  1. every non-matched position is exactly as planted (nothing unexplained, nothing missing);
  2. late files: pending (all their book positions) before they land, then as ground truth after;
  3. matched counts per date equal ground truth;
  4. the tie-out table and the break queue agree with the reconciliation;
  5. positions, FX and exposure add up;
  6. (with snapshots) every silver and gold table identical after update, re-run and full refresh;
  7. isolation: Unity Catalog lineage shows this client's tables read nothing from another client.
"""
import json
import os
from collections import Counter, defaultdict
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from keelstone.config import load_client
from keelstone.qa import QA_CATALOG, QA_SCHEMA, ground_truth_path

dbutils.widgets.text("client", "")  # noqa: F821
dbutils.widgets.text("run_tag", "")  # noqa: F821
dbutils.widgets.text("require_snapshots", "true")  # noqa: F821
dbutils.widgets.text("config_dir", "/Volumes/keelstone/platform/artifacts/config")  # noqa: F821
client_id = dbutils.widgets.get("client")  # noqa: F821
run_tag = dbutils.widgets.get("run_tag")  # noqa: F821
require_snapshots = dbutils.widgets.get("require_snapshots").lower() == "true"  # noqa: F821
CONFIG_DIR = dbutils.widgets.get("config_dir")  # noqa: F821

spark = SparkSession.builder.getOrCreate()
client = load_client(client_id, CONFIG_DIR)
T = lambda name: f"{client.catalog}.{client.schema}.{name}"  # noqa: E731
truth = json.load(open(ground_truth_path(client_id)))
er = truth["expected_recon"]
root = client.landing_root + "/"
landed = {os.path.join(d, f)[len(root):] for d, _, fs in os.walk(client.landing_root) for f in fs}
through = max(truth["files"][f]["business_date"] for f in landed if f in truth["files"])
late_file_of = {(p["source"], p["business_date"]): p["file"] for p in truth["planted"] if p["problem"] == "late_file"}
pending_late = [lf for lf in er["late_files"]
                if lf["business_date"] <= through and late_file_of[(lf["custodian"], lf["business_date"])] not in landed]
arrived_late = [lf for lf in er["late_files"] if lf not in pending_late and lf["business_date"] <= through]
pending_keys = {(lf["custodian"], lf["business_date"]) for lf in pending_late}

failures: list[str] = []
def check(ok: bool, msg: str) -> None:
    if not ok:
        failures.append(msg)

recon = spark.table(T("gold_reconciliation"))
rows = [r.asDict() for r in recon.where(F.col("status") != "matched")
        .select("custodian", F.col("business_date").cast("string").alias("d"), "account_id", "security_id",
                "status", "reason_code", "explanation_basis").collect()]
got = {(r["custodian"], r["d"], r["account_id"], r["security_id"]): r["reason_code"] for r in rows}
max_date = recon.agg(F.max("business_date").cast("string")).first()[0]
check(max_date is not None and max_date <= through, f"recon dates run to {max_date}, landed files to {through}")

# 1. every non-matched position is exactly as planted
want = {(x["custodian"], x["business_date"], x["account_id"], x["security_id"]): x["reason"]
        for x in er["non_matched"] if x["business_date"] <= through and (x["custodian"], x["business_date"]) not in pending_keys}
got_not_pending = {k: v for k, v in got.items() if v != "pending_late_file"}
extra = {k: v for k, v in got_not_pending.items() if want.get(k) != v}
absent = {k: v for k, v in want.items() if got_not_pending.get(k) != v}
check(not extra, f"{len(extra)} unexplained or wrong recon rows, e.g. {list(extra.items())[:5]}")
check(not absent, f"{len(absent)} planted recon rows missing or wrong, e.g. {list(absent.items())[:5]}")
unlabelled = [r for r in rows if r["reason_code"] == "custodian_row_rejected" and not r["explanation_basis"]]
check(not unlabelled, f"{len(unlabelled)} rejected rows without an explanation basis")

# 2. late files
pending = Counter((k[0], k[1]) for k, v in got.items() if v == "pending_late_file")
want_pending = {(lf["custodian"], lf["business_date"]): lf["pending_book_positions"] for lf in pending_late}
check(dict(pending) == want_pending, f"pending {dict(pending)} != expected {want_pending}")
non_matched_on = Counter((x["custodian"], x["business_date"]) for x in er["non_matched"])
for lf in arrived_late:
    key = (lf["custodian"], lf["business_date"])
    by_status = {r[0]: r[1] for r in recon.where((F.col("custodian") == key[0]) &
                 (F.col("business_date") == F.lit(key[1]).cast("date"))).groupBy("status").count().collect()}
    want_status = Counter({"matched": lf["pending_book_positions"] - non_matched_on[key]})
    for x in er["non_matched"]:
        if (x["custodian"], x["business_date"]) == key:
            want_status["explained_upstream" if x["reason"].startswith("custodian_") else "break"] += 1
    check(by_status == dict(+want_status), f"late file {key} after arrival: {by_status} != {dict(+want_status)}")

# 3. matched counts per date
matched = {r[0]: r[1] for r in recon.where(F.col("status") == "matched")
           .groupBy(F.col("business_date").cast("string")).count().collect()}
for d, counts in er["daily"].items():
    if d > through:
        continue
    exp = counts.get("matched", 0) - sum(lf["pending_book_positions"] - non_matched_on[(lf["custodian"], d)]
                                         for lf in pending_late if lf["business_date"] == d)
    check(matched.get(d, 0) == exp, f"{d}: {matched.get(d, 0)} matched, expected {exp}")

# 4. tie-out table and break queue
tie = [r.asDict() for r in spark.table(T("gold_tieout_daily")).orderBy("business_date").collect()]
for r in tie:
    check(r["positions_compared"] == r["matched"] + r["breaks"] + r["pending"] + r["explained_upstream"],
          f"{r['business_date']}: tie-out counts do not add up")
tot = Counter()
for r in tie:
    tot.update({k: r[k] for k in ("positions_compared", "matched", "breaks", "pending", "explained_upstream")})
overall = tot["matched"] / (tot["positions_compared"] - tot["pending"]) if tot["positions_compared"] > tot["pending"] else None
queue = spark.table(T("gold_break_queue")).count()
check(queue == tot["breaks"] + tot["pending"], f"break queue {queue} != breaks + pending {tot['breaks'] + tot['pending']}")

# 5. positions, FX and exposure
pos = spark.table(T("gold_positions"))
book_n = spark.table(T("silver_positions")).where(F.col("position_side") == "book").count()
check(pos.count() == book_n, f"gold_positions {pos.count()} rows != book positions {book_n}")
nulls = pos.where(F.col("market_value_usd").isNull() | F.col("asset_class").isNull()).count()
check(nulls == 0, f"{nulls} gold positions without USD value or asset class")
fx = spark.table(T("silver_fx")).select(F.col("rate_date").alias("business_date"), "currency",
                                        F.col("usd_per_unit").alias("fx_rate"))
fx_bad = (pos.join(fx, ["business_date", "currency"], "left")
          .where(F.col("fx_rate").isNull() |
                 (F.abs(F.col("market_value_usd") - F.col("market_value_local") * F.col("fx_rate")) > 0.01)).count())
check(fx_bad == 0, f"{fx_bad} positions where USD value != local value x FX rate")
pos_by_date = {r[0]: float(r[1]) for r in pos.groupBy("business_date").agg(F.sum("market_value_usd")).collect()}
for name in ("gold_exposure_asset_class", "gold_exposure_currency"):
    for d, v, w in spark.table(T(name)).groupBy("business_date").agg(F.sum("market_value_usd"), F.sum("weight")).collect():
        check(abs(float(v) - pos_by_date.get(d, 0.0)) < 0.05, f"{name} {d}: {v} != positions {pos_by_date.get(d)}")
        check(abs(float(w) - 1.0) < 1e-4, f"{name} {d}: weights sum to {w}")

# 6. idempotency
snaps = defaultdict(dict)
for r in spark.table(f"{QA_CATALOG}.{QA_SCHEMA}.silver_snapshots").where(F.col("run_tag") == run_tag).collect():
    snaps[r["table_name"]][r["label"]] = (r["n_rows"], r["checksum"])
if require_snapshots:
    check(any(t.startswith("gold_") for t in snaps), f"no gold snapshots for run {run_tag}")
    for table, by_label in sorted(snaps.items()):
        check(len(by_label) == 3, f"{table}: expected 3 snapshots, got {sorted(by_label)}")
        check(len(set(by_label.values())) == 1, f"{table}: changed across re-runs {by_label}")

# 7. isolation through Unity Catalog lineage (if system tables are available on this workspace)
lineage = {"available": False}
try:
    leaks = spark.sql(f"""
        SELECT DISTINCT source_table_full_name, target_table_full_name
        FROM system.access.table_lineage
        WHERE target_table_catalog = '{client.catalog}' AND target_table_schema = '{client.schema}'
          AND source_table_catalog = '{client.catalog}'
          AND source_table_schema NOT IN ('{client.schema}')""").collect()
    reads = spark.sql(f"""SELECT count(*) FROM system.access.table_lineage
        WHERE target_table_catalog = '{client.catalog}' AND target_table_schema = '{client.schema}'""").first()[0]
    lineage = {"available": True, "lineage_edges_into_schema": reads,
               "reads_from_other_schemas": [f"{r[0]} -> {r[1]}" for r in leaks]}
    check(not leaks, f"lineage: {client.schema} tables read other schemas: {lineage['reads_from_other_schemas'][:5]}")
except Exception as e:  # noqa: BLE001
    lineage = {"available": False, "reason": f"{type(e).__name__}: {str(e)[:200]}"}

latest = tie[-1] if tie else {}
summary = {"client": client_id, "through": through, "run_tag": run_tag,
           "pending_late_files": sorted(pending_keys), "checked_at": datetime.now(timezone.utc).isoformat(),
           "recon_by_reason": dict(Counter(got.values())), "totals": dict(tot), "tie_out_rate_overall": overall,
           "tie_out_rate_latest": {"business_date": str(latest.get("business_date")), "rate": latest.get("tie_out_rate")},
           "tie_out_rate_min": min((r["tie_out_rate"] for r in tie if r["tie_out_rate"] is not None), default=None),
           "count_attributed_rejections": sum(1 for r in rows if r["explanation_basis"] == "count"),
           "snapshots_checked": require_snapshots, "lineage": lineage,
           "n_failures": len(failures), "failures": failures[:50], "passed": not failures}
print("VALIDATION_SUMMARY " + json.dumps(summary, default=str))
spark.createDataFrame([(client_id, "gold", summary["checked_at"], summary["passed"], json.dumps(summary, default=str))],
                      "client_id string, check_name string, checked_at string, passed boolean, summary string") \
    .write.mode("append").saveAsTable(f"{QA_CATALOG}.{QA_SCHEMA}.validation_results")
if failures:
    raise RuntimeError(f"VALIDATION FAILED ({len(failures)}): " + "; ".join(failures[:10]))
dbutils.notebook.exit(json.dumps(summary, default=str))  # noqa: F821
