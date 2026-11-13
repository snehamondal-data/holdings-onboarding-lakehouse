"""Run representative read queries on the SQL warehouse and read their metrics from query history.

  python scripts/probe_queries.py --client scale_test --label S3_baseline

Each query runs three times with a different constant output column (so no run is served from the
result cache); the median time and the
files/bytes read and pruned are written to docs/perf/<label>_probes.json.
"""
import argparse
import json
import statistics
import subprocess
import time
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CLI = str(Path.home() / ".local/bin/databricks")


def api(method: str, path: str, body: dict | None, profile: str) -> dict:
    """One CLI call with a time limit; read-only calls (get) are retried once if the connection hangs."""
    args = [CLI, "api", method, path, "--profile", profile] + (["--json", json.dumps(body)] if body else [])
    for attempt in range(2 if method == "get" else 1):
        try:
            r = subprocess.run(args, capture_output=True, text=True, timeout=60)
        except subprocess.TimeoutExpired:
            if attempt == 0 and method == "get":
                continue
            raise SystemExit(f"databricks api {method} {path} hung for 60 s")
        if r.returncode != 0:
            raise SystemExit(r.stderr or r.stdout)
        return json.loads(r.stdout or "{}")
    raise SystemExit(f"databricks api {method} {path} hung twice")


def warehouse_id(profile: str) -> str:
    out = json.loads(subprocess.run([CLI, "warehouses", "list", "--profile", profile, "-o", "json"],
                                    capture_output=True, text=True).stdout)
    whs = out if isinstance(out, list) else out.get("warehouses", [])
    return next(w["id"] for w in whs if w["name"] == "Serverless Starter Warehouse")


def run_sql(wh: str, sql: str, profile: str) -> str:
    # submit without waiting (returns the id at once), then poll: long-held requests hung on this network
    r = api("post", "/api/2.0/sql/statements", {"warehouse_id": wh, "statement": sql, "wait_timeout": "0s",
                                                "on_wait_timeout": "CONTINUE"}, profile)
    while r.get("status", {}).get("state") in ("PENDING", "RUNNING"):
        time.sleep(2)
        r = api("get", f"/api/2.0/sql/statements/{r['statement_id']}", None, profile)
    if r.get("status", {}).get("state") != "SUCCEEDED":
        raise SystemExit(f"query failed: {r.get('status')}")
    return r["statement_id"]


def metrics(statement_id: str, profile: str) -> dict:
    """Query history metrics for one statement (listed recent queries, matched by id)."""
    keys = ("total_time_ms", "execution_time_ms", "read_bytes", "read_files_count", "pruned_files_count",
            "pruned_bytes", "rows_read_count", "spill_to_disk_bytes", "network_sent_bytes", "result_from_cache")
    for _ in range(30):
        try:
            out = subprocess.run([CLI, "query-history", "list", "--include-metrics", "--max-results", "100",
                                  "--profile", profile, "-o", "json"], capture_output=True, text=True, timeout=60).stdout
        except subprocess.TimeoutExpired:
            continue
        for q in json.loads(out or "{}").get("res", []):
            m = q.get("metrics") or {}
            if q.get("query_id") == statement_id and q.get("is_final") and m.get("total_time_ms") is not None:
                return {k: m.get(k) for k in keys}
        time.sleep(3)
    return {}


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--client", default="scale_test")
    p.add_argument("--label", required=True)
    p.add_argument("--date", default="2026-09-22", help="a business date to filter on")
    p.add_argument("--account", default="SC-CSV-00010")
    p.add_argument("--profile", default="keelstone")
    a = p.parse_args()
    S = f"keelstone.{a.client}"
    queries = {
        "latest_day_exposure": f"SELECT asset_class, sum(market_value_usd) FROM {S}.gold_positions "
                               f"WHERE business_date = DATE'{a.date}' GROUP BY asset_class",
        "one_week_recon_status": f"SELECT status, count(*) FROM {S}.gold_reconciliation "
                                 f"WHERE business_date BETWEEN DATE'2026-08-03' AND DATE'2026-08-07' GROUP BY status",
        "one_day_silver": f"SELECT source_id, count(*), sum(quantity) FROM {S}.silver_positions "
                          f"WHERE business_date = DATE'{a.date}' GROUP BY source_id",
        "one_account_history": f"SELECT business_date, sum(quantity) FROM {S}.silver_positions "
                               f"WHERE account_id = '{a.account}' GROUP BY business_date",
    }
    wh = warehouse_id(a.profile)
    run_sql(wh, "SELECT 1", a.profile)                      # wake the warehouse before timing
    results = {}
    for name, sql in queries.items():
        runs = []
        for _ in range(3):
            # a different output literal each time: the result cache ignores comments and constant-folded
            # predicates, but a different result column cannot be served from it
            n = uuid.uuid4().int % 1_000_000 + 1
            runs.append(metrics(run_sql(wh, sql.replace("SELECT ", f"SELECT {n} AS probe, ", 1), a.profile), a.profile))
        times = [r["total_time_ms"] for r in runs if r.get("total_time_ms") is not None]
        results[name] = {"sql": sql, "median_total_ms": statistics.median(times) if times else None,
                         "runs": runs}
        last = runs[-1]
        assert not any(r.get("result_from_cache") for r in runs), f"{name}: a run came from the result cache"
        print(f"{name:24} median {results[name]['median_total_ms']} ms  files read {last.get('read_files_count')} "
              f"pruned {last.get('pruned_files_count')}  bytes {last.get('read_bytes')}")
    out = ROOT / "docs" / "perf" / f"{a.label}_probes.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps({"label": a.label, "client": a.client, "results": results}, indent=1) + "\n")
    print("wrote", out)


if __name__ == "__main__":
    main()
