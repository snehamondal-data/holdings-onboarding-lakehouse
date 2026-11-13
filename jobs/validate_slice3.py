# Databricks notebook source
# MAGIC %pip install --quiet --force-reinstall --no-deps /Volumes/keelstone/platform/artifacts/keelstone_holdings-0.1.0-py3-none-any.whl

# COMMAND ----------

dbutils.library.restartPython()  # noqa: F821

# COMMAND ----------

# Code comes from the wheel in the artifacts volume, configs from the same volume:
# serverless cannot reliably read bundle files under /Workspace on this workspace.
"""Slice 3 validation (reads ground truth from keelstone.qa).

phase=pending: files delivered through the late file's business date only.
  The late custodian's book positions on that date are pending_late_file; everything up to
  that date matches the ground truth.
phase=final: every file delivered. Every planted reconciliation problem appears with the right
  reason; the late date now ties out; nothing unexplained appears; no pending remains.
Both phases: per-date matched counts equal ground truth; exposure and positions add up; FX applied;
every silver and gold table is identical after update, re-run and full refresh.
"""
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from keelstone.config import load_client
from keelstone.qa import QA_CATALOG, QA_SCHEMA, ground_truth_path

dbutils.widgets.text("client", "")  # noqa: F821
dbutils.widgets.text("run_tag", "")  # noqa: F821
dbutils.widgets.text("phase", "final")  # noqa: F821
dbutils.widgets.text("through", "")  # noqa: F821
dbutils.widgets.text("config_dir", "/Volumes/keelstone/platform/artifacts/config")  # noqa: F821
client_id = dbutils.widgets.get("client")  # noqa: F821
run_tag = dbutils.widgets.get("run_tag")  # noqa: F821
phase = dbutils.widgets.get("phase")  # noqa: F821
through = dbutils.widgets.get("through") or "9999-12-31"  # noqa: F821
CONFIG_DIR = dbutils.widgets.get("config_dir")  # noqa: F821
assert phase in ("pending", "final"), phase

spark = SparkSession.builder.getOrCreate()
client = load_client(client_id, CONFIG_DIR)
T = lambda name: f"{client.catalog}.{client.schema}.{name}"  # noqa: E731
truth = json.load(open(ground_truth_path(client_id)))
er = truth["expected_recon"]
late = er["late_file"]

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
check(max_date <= through, f"recon has dates after {through}: {max_date}")

# 1. every non-matched position is exactly as planted (nothing unexplained, nothing missing)
late_key = (late["custodian"], late["business_date"])
not_yet = lambda x: phase == "pending" and (x["custodian"], x["business_date"]) == late_key  # noqa: E731
want = {(x["custodian"], x["business_date"], x["account_id"], x["security_id"]): x["reason"]
        for x in er["non_matched"] if x["business_date"] <= through and not not_yet(x)}
# Before the late file arrives, all its book positions are pending, whatever they become later.
late_non_matched = sum(1 for x in er["non_matched"] if (x["custodian"], x["business_date"]) == late_key)
got_not_pending = {k: v for k, v in got.items() if v != "pending_late_file"}
extra = {k: v for k, v in got_not_pending.items() if want.get(k) != v}
absent = {k: v for k, v in want.items() if got_not_pending.get(k) != v}
check(not extra, f"{len(extra)} unexplained or wrong recon rows, e.g. {list(extra.items())[:5]}")
check(not absent, f"{len(absent)} planted recon rows missing or wrong, e.g. {list(absent.items())[:5]}")

unlabelled = [k for k, r in zip(got, rows) if r["reason_code"] == "custodian_row_rejected" and not r["explanation_basis"]]
check(not unlabelled, f"{len(unlabelled)} rejected rows without an explanation basis, e.g. {unlabelled[:3]}")

# 2. pending before the late file arrives, cleared after
pending = {k for k, v in got.items() if v == "pending_late_file"}
if phase == "pending":
    check(late["business_date"] <= through < late["delivery_date"], f"pending phase must stop before {late['delivery_date']}")
    check({(k[0], k[1]) for k in pending} == {(late["custodian"], late["business_date"])},
          f"pending rows on {sorted({(k[0], k[1]) for k in pending})}, expected only the late file")
    check(len(pending) == late["pending_book_positions"],
          f"{len(pending)} pending rows, expected {late['pending_book_positions']}")
else:
    check(not pending, f"{len(pending)} rows still pending after every file arrived")
    late_rows = recon.where((F.col("custodian") == late["custodian"]) &
                            (F.col("business_date") == F.lit(late["business_date"]).cast("date")))
    by_status = {r[0]: r[1] for r in late_rows.groupBy("status").count().collect()}
    # After arrival the late file's positions clear or become what the ground truth says.
    want_status = Counter({"matched": late["pending_book_positions"] - late_non_matched})
    for x in er["non_matched"]:
        if (x["custodian"], x["business_date"]) == late_key:
            want_status["explained_upstream" if x["reason"].startswith("custodian_") else "break"] += 1
    check(by_status == dict(+want_status), f"late-file date after arrival: {by_status}, expected {dict(+want_status)}")

# 3. matched counts per date equal ground truth (pending rows are not matched yet)
matched = {r[0]: r[1] for r in recon.where(F.col("status") == "matched")
           .groupBy(F.col("business_date").cast("string")).count().collect()}
for d, counts in er["daily"].items():
    if d > through:
        continue
    exp = counts.get("matched", 0) - ((late["pending_book_positions"] - late_non_matched)
                                      if phase == "pending" and d == late["business_date"] else 0)
    check(matched.get(d, 0) == exp, f"{d}: {matched.get(d, 0)} matched, expected {exp}")

# 4. tie-out table agrees with the reconciliation
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

# 5. positions, FX and exposure add up
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
    ex = spark.table(T(name)).groupBy("business_date").agg(F.sum("market_value_usd"), F.sum("weight")).collect()
    for d, v, w in ex:
        check(abs(float(v) - pos_by_date.get(d, 0.0)) < 0.05, f"{name} {d}: {v} != positions {pos_by_date.get(d)}")
        check(abs(float(w) - 1.0) < 1e-4, f"{name} {d}: weights sum to {w}")

# 6. idempotency: every silver and gold table identical after update, re-run and full refresh
snaps = defaultdict(dict)
for r in spark.table(f"{QA_CATALOG}.{QA_SCHEMA}.silver_snapshots").where(F.col("run_tag") == run_tag).collect():
    snaps[r["table_name"]][r["label"]] = (r["n_rows"], r["checksum"])
check(any(t.startswith("gold_") for t in snaps), f"no gold snapshots for run {run_tag}")
for table, by_label in sorted(snaps.items()):
    check(len(by_label) == 3, f"{table}: expected 3 snapshots, got {sorted(by_label)}")
    check(len(set(by_label.values())) == 1, f"{table}: changed across re-runs {by_label}")

latest = tie[-1] if tie else {}
summary = {"client": client_id, "phase": phase, "through": through, "run_tag": run_tag,
           "checked_at": datetime.now(timezone.utc).isoformat(),
           "recon_by_reason": dict(Counter(got.values())), "pending_rows": len(pending),
           "totals": dict(tot), "tie_out_rate_overall": overall,
           "tie_out_rate_latest": {"business_date": str(latest.get("business_date")), "rate": latest.get("tie_out_rate")},
           "tie_out_rate_min": min((r["tie_out_rate"] for r in tie if r["tie_out_rate"] is not None), default=None),
           "count_attributed_rejections": sum(1 for r in rows if r["explanation_basis"] == "count"),
           "n_failures": len(failures), "failures": failures[:50], "passed": not failures}
print("VALIDATION_SUMMARY " + json.dumps(summary, default=str))
spark.createDataFrame([(client_id, f"slice3_{phase}", summary["checked_at"], summary["passed"], json.dumps(summary, default=str))],
                      "client_id string, check_name string, checked_at string, passed boolean, summary string") \
    .write.mode("append").saveAsTable(f"{QA_CATALOG}.{QA_SCHEMA}.validation_results")
if failures:
    raise RuntimeError(f"VALIDATION FAILED ({len(failures)}): " + "; ".join(failures[:10]))
dbutils.notebook.exit(json.dumps(summary, default=str))  # noqa: F821
