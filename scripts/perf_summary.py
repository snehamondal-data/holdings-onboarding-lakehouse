"""Print the slice 5 performance tables (Markdown) from the measurements saved in docs/perf/.

  python scripts/perf_summary.py

Every number comes from a recorded run: <label>_run.json (measure_performance task) and
<label>_probes.json (scripts/probe_queries.py).
"""
import json
from pathlib import Path

PERF = Path(__file__).resolve().parents[1] / "docs" / "perf"
RUNS = [("S1_1M_baseline", "1M", "baseline"), ("S2_5M_baseline", "5M", "baseline"),
        ("S3_15M_baseline", "15M", "baseline"), ("S3_15M_parallel_ingest", "15M", "+ parallel bronze ingest"),
        ("S3_15M_parallel_clustered", "15M", "+ liquid clustering by business_date"),
        ("S3_15M_kept_config", "15M", "kept: parallel ingest, no clustering")]
PROBES = ["latest_day_exposure", "one_week_recon_status", "one_day_silver", "one_account_history"]


def load(name):
    p = PERF / name
    return json.loads(p.read_text()) if p.exists() else None


def layer_seconds(flows: dict, prefix: str) -> float:
    return round(sum(f.get("executor_seconds", 0) for k, f in flows.items()
                     if k.startswith(prefix) or (prefix == "silver_" and k == "unresolved_securities")), 1)


def main() -> None:
    print("| Size | Run | Rows ingested | Bronze s | Bronze rows/s | Pipeline s (wall) | Silver executor s | "
          "Gold executor s | Slowest flow (executor s) | Bronze files per table |")
    print("|---|---|---|---|---|---|---|---|---|---|")
    for label, size, what in RUNS:
        r = load(f"{label}_run.json")
        if not r:
            continue
        flows = r["pipeline"]["flows"]
        slow = max(((k, f.get("executor_seconds", 0)) for k, f in flows.items() if k != "missingFlowName"),
                   key=lambda kv: kv[1])
        bronze_files = sorted({v["files"] for k, v in r["tables"].items()
                               if k.startswith("bronze_scale") and v.get("files") is not None})
        rows = r["ingest"].get("rows")
        print(f"| {size} | {what} | {rows:,} | {r['ingest']['seconds']} | "
              f"{r['ingest'].get('rows_per_second', 'n/a') if rows else 'n/a (no new files)'} | "
              f"{r['pipeline']['seconds']} | {layer_seconds(flows, 'silver_')} | {layer_seconds(flows, 'gold_')} | "
              f"{slow[0]} ({slow[1]:.0f}) | {'-'.join(map(str, bronze_files)) if bronze_files else 'n/a'} |"
              if rows else
              f"| {size} | {what} | 0 (bronze already loaded) | {r['ingest']['seconds']} | n/a | "
              f"{r['pipeline']['seconds']} | {layer_seconds(flows, 'silver_')} | {layer_seconds(flows, 'gold_')} | "
              f"{slow[0]} ({slow[1]:.0f}) | {'-'.join(map(str, bronze_files)) if bronze_files else 'n/a'} |")
    print()
    print("| Size | Run | " + " | ".join(PROBES) + " |")
    print("|---|---|" + "---|" * len(PROBES))
    for label, size, what in RUNS:
        p = load(f"{label}_probes.json")
        if not p:
            continue
        cells = []
        for q in PROBES:
            res = p["results"][q]
            last = res["runs"][-1]
            mb = (last.get("read_bytes") or 0) / 1e6
            cells.append(f"{res['median_total_ms']:.0f} ms, {mb:.1f} MB, {last.get('read_files_count')} files "
                         f"({last.get('pruned_files_count')} pruned)")
        print(f"| {size} | {what} | " + " | ".join(cells) + " |")


if __name__ == "__main__":
    main()
