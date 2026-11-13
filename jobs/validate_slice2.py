# Databricks notebook source
# MAGIC %pip install --quiet --force-reinstall --no-deps /Volumes/keelstone/platform/artifacts/keelstone_holdings-0.1.0-py3-none-any.whl

# COMMAND ----------

dbutils.library.restartPython()  # noqa: F821

# COMMAND ----------

# Code comes from the wheel in the artifacts volume, configs from the same volume:
# serverless cannot reliably read bundle files under /Workspace on this workspace.
"""Slice 2 validation (reads ground truth from keelstone.qa; the only kind of job allowed to).

Fails the run if any check fails:
  1. idempotency: every silver table is identical after update, re-run and full refresh;
  2. accounting, per file: bronze rows = silver + silver quarantine + unresolved + deduped;
  3. per file, each of those counts equals the ground truth;
  4. unresolved identifiers are exactly the planted unmapped ones, with the planted row counts;
  5. the units error is caught: units_scale_suspect rows come only from the planted file;
  6. Linden rows from drifted files carry header_version 2, earlier ones 1;
  7. planted quantity and value breaks reach silver with the custodian's values;
  8. no natural key appears with two different contents;
  9. the pipeline's expectation counts (event log) match the routed rows.
"""
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from keelstone.config import load_client, load_source
from keelstone.qa import QA_CATALOG, QA_SCHEMA, ground_truth_path

dbutils.widgets.text("client", "")  # noqa: F821
dbutils.widgets.text("run_tag", "")  # noqa: F821
dbutils.widgets.text("config_dir", "/Volumes/keelstone/platform/artifacts/config")  # noqa: F821
client_id = dbutils.widgets.get("client")  # noqa: F821
run_tag = dbutils.widgets.get("run_tag")  # noqa: F821
CONFIG_DIR = dbutils.widgets.get("config_dir")  # noqa: F821

spark = SparkSession.builder.getOrCreate()
client = load_client(client_id, CONFIG_DIR)
sources = [load_source(s, CONFIG_DIR) for s in client.sources]
truth = json.load(open(ground_truth_path(client_id)))
T = lambda name: f"{client.catalog}.{client.schema}.{name}"  # noqa: E731
root = client.landing_root + "/"
rel = lambda p: p.replace("dbfs:", "", 1)[len(root):] if p else p  # noqa: E731

failures: list[str] = []
def check(ok: bool, msg: str) -> None:
    if not ok:
        failures.append(msg)

def per_file(table: str, file_col: str = "source_file", where=None) -> Counter:
    df = spark.table(table)
    if where is not None:
        df = df.where(where)
    return Counter({rel(r[0]): r[1] for r in df.groupBy(file_col).count().collect()})

# 1. idempotency ------------------------------------------------------------
snaps = defaultdict(dict)
for r in spark.table(f"{QA_CATALOG}.{QA_SCHEMA}.silver_snapshots").where(F.col("run_tag") == run_tag).collect():
    snaps[r["table_name"]][r["label"]] = (r["n_rows"], r["checksum"])
check(bool(snaps), f"no snapshots for run {run_tag}")
for table, by_label in sorted(snaps.items()):
    check(len(by_label) == 3, f"{table}: expected 3 snapshots, got {sorted(by_label)}")
    check(len(set(by_label.values())) == 1, f"{table}: changed across re-runs {by_label}")

# 2 and 3. accounting per file -------------------------------------------------
bronze = Counter()
for s in sources:
    bronze.update(per_file(T(s.bronze_table), "_source_file"))
silver = per_file(T("silver_positions"))
for s in sources:
    if s.silver and not s.silver.is_positions:
        silver.update(per_file(T(s.silver.table)))
quarantine = defaultdict(Counter)
for r in spark.table(T("silver_quarantine")).groupBy("source_file", "quarantine_reason").count().collect():
    quarantine[rel(r[0])][r[1]] += r[2]
unresolved = per_file(T("unresolved_securities"))
deduped = Counter({rel(r[0]): r[1] for r in spark.table(T("silver_dedupe_log"))
                   .groupBy("source_file").agg(F.sum("duplicates_removed")).collect()})

files = sorted(set(bronze) | set(silver) | set(unresolved) | set(deduped) | set(quarantine))
totals = Counter()
for f in files:
    q = dict(quarantine.get(f, {}))
    accounted = silver[f] + sum(q.values()) + unresolved[f] + deduped[f]
    check(bronze[f] == accounted, f"{f}: bronze {bronze[f]} != silver {silver[f]} + quarantine {sum(q.values())} "
                                  f"+ unresolved {unresolved[f]} + deduped {deduped[f]}")
    exp = truth["files"].get(f, {}).get("expected_silver")
    check(exp is not None, f"{f}: not in ground truth")
    if exp:
        got = {"rows": silver[f], "unresolved": unresolved[f], "deduped": deduped[f], "quarantine": q}
        check(got == exp, f"{f}: silver outcome {got} != expected {exp}")
    totals.update(bronze=bronze[f], silver=silver[f], unresolved=unresolved[f], deduped=deduped[f],
                  quarantine=sum(q.values()))
missing = [f for f, v in truth["files"].items() if v["expected_bronze_rows"] and f not in bronze]
check(not missing, f"ground-truth files with no bronze rows: {missing[:5]}")

# 4. unresolved identifiers ---------------------------------------------------
got_ids = {(r["source_id"], r["identifier_type"], r["identifier"]): r["count"] for r in
           spark.table(T("unresolved_securities")).groupBy("source_id", "identifier_type", "identifier").count().collect()}
exp_ids = {(p["source"], p["identifier_type"], p["identifier"]): p["affected_rows"]
           for p in truth["planted"] if p["problem"] == "unmapped_security"}
check(got_ids == exp_ids, f"unresolved identifiers {got_ids} != planted {exp_ids}")

# 5. units error --------------------------------------------------------------
(units,) = [p for p in truth["planted"] if p["problem"] == "units_error"]
units_files = {f for f, c in quarantine.items() if c.get("units_scale_suspect")}
check(units_files == {units["file"]}, f"units_scale_suspect found in {sorted(units_files)}, planted in {units['file']}")

# 6. header versions ------------------------------------------------------------
(drift,) = [p for p in truth["planted"] if p["problem"] == "schema_drift"]
drifted = set(drift["files"])
for r in (spark.table(T("silver_positions")).where(F.col("source_id") == "linden")
          .groupBy("source_file", "header_version").count().collect()):
    want = 2 if rel(r[0]) in drifted else 1
    check(r[1] == want, f"{rel(r[0])}: header_version {r[1]} on {r[2]} rows, expected {want}")

# 7. planted breaks keep custodian values ----------------------------------------
pos = spark.table(T("silver_positions"))
for p in truth["planted"]:
    if p["problem"] not in ("quantity_break", "value_break"):
        continue
    hit = pos.where((F.col("source_id") == p["source"]) & (F.col("account_id") == p["account_id"]) &
                    (F.col("security_id") == p["security_id"]) &
                    (F.col("business_date") == F.lit(p["business_date"]).cast("date"))).collect()
    check(len(hit) == 1, f"{p['id']} {p['problem']}: {len(hit)} silver rows")
    if len(hit) == 1 and p["problem"] == "quantity_break":
        check(float(hit[0]["quantity"]) == p["custodian_quantity"], f"{p['id']}: quantity {hit[0]['quantity']}")
    if len(hit) == 1 and p["problem"] == "value_break":
        check(abs(float(hit[0]["market_value"]) - p["custodian_market_value"]) <= 1.0,
              f"{p['id']}: market_value {hit[0]['market_value']} != {p['custodian_market_value']}")

# 8. key conflicts ----------------------------------------------------------------
n_conflicts = spark.table(T("silver_key_conflicts")).count()
check(n_conflicts == 0, f"{n_conflicts} natural keys with conflicting content")

# 9. expectation counts from the event log (latest update = the full refresh) -------
exp_counts = {}
try:
    ev = spark.sql(f"SELECT origin.update_id AS u, timestamp, details FROM event_log(TABLE({T('silver_positions')})) "
                   "WHERE event_type = 'flow_progress'")
    latest = ev.orderBy(F.col("timestamp").desc()).first()["u"]
    for r in ev.where(F.col("u") == latest).collect():
        dq = (json.loads(r["details"]).get("flow_progress") or {}).get("data_quality") or {}
        for e in dq.get("expectations") or []:
            c = exp_counts.setdefault(e["name"], [0, 0])
            c[0] += e.get("passed_records", 0)
            c[1] += e.get("failed_records", 0)
    checks = spark.table(T("silver_position_checks"))
    want = {"security_resolved": checks.where("security_id IS NULL").count(),
            "units_scale_sane": checks.where("units_suspect").count(),
            "quantity_not_null": 0, "business_date_not_null": 0}
    for name, n_failed in want.items():
        check(name in exp_counts, f"expectation {name} missing from event log")
        if name in exp_counts:
            check(exp_counts[name][1] == n_failed, f"expectation {name}: event log failed {exp_counts[name][1]} != {n_failed}")
except Exception as e:  # noqa: BLE001
    failures.append(f"event log check could not run: {type(e).__name__}: {str(e)[:300]}")

summary = {"client": client_id, "run_tag": run_tag, "checked_at": datetime.now(timezone.utc).isoformat(),
           "files": len(files), "totals": dict(totals),
           "quarantine_by_reason": dict(sum(quarantine.values(), Counter())),
           "unresolved_identifiers": len(got_ids), "expectations": exp_counts,
           "snapshots": {t: v for t, v in sorted(snaps.items())},
           "n_failures": len(failures), "failures": failures[:50], "passed": not failures}
print("VALIDATION_SUMMARY " + json.dumps(summary, default=str))
spark.createDataFrame([(client_id, "slice2", summary["checked_at"], summary["passed"], json.dumps(summary, default=str))],
                      "client_id string, check_name string, checked_at string, passed boolean, summary string") \
    .write.mode("append").saveAsTable(f"{QA_CATALOG}.{QA_SCHEMA}.validation_results")
if failures:
    raise RuntimeError(f"VALIDATION FAILED ({len(failures)}): " + "; ".join(failures[:10]))
dbutils.notebook.exit(json.dumps(summary, default=str))  # noqa: F821
