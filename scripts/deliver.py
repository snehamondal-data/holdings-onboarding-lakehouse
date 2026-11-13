"""Simulate custodians delivering files: upload every generated file whose delivery
date is on or before --through (or all files) to the client's landing volume.

Already-delivered files are tracked in <data>/delivery_state.json and never re-sent,
so playing the days out in steps behaves like real daily arrivals (the late file and
the re-sent duplicate arrive a day after their business date).

  python scripts/deliver.py --client calder_valley --through 2026-07-15
  python scripts/deliver.py --client wrexford --all
  python scripts/deliver.py --client wrexford --ground-truth     # ground truth to keelstone.qa only
  python scripts/deliver.py --client calder_valley --redeliver 2026-08-20   # send a day again
"""
import argparse
import json
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from keelstone.config import load_client  # noqa: E402
from keelstone.qa import ground_truth_path  # noqa: E402

CLI = shutil.which("databricks") or str(Path.home() / ".local/bin/databricks")


def cli(*args: str) -> None:
    print("+ databricks", " ".join(args))
    subprocess.run([CLI, *args], check=True)


def ensure_landing(client, profile: str) -> None:
    """Create the client's schema and landing volume if they do not exist yet (idempotent)."""
    for args in (["schemas", "create", client.schema, client.catalog],
                 ["volumes", "create", client.catalog, client.schema, client.landing_volume, "MANAGED"]):
        r = subprocess.run([CLI, *args, "--profile", profile], capture_output=True, text=True)
        if r.returncode != 0 and "already exists" not in (r.stderr + r.stdout):
            raise SystemExit(r.stderr or r.stdout)


def main() -> None:
    p = argparse.ArgumentParser()
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--through", help="deliver files with delivery_date <= this date (YYYY-MM-DD)")
    g.add_argument("--all", action="store_true")
    g.add_argument("--ground-truth", action="store_true", help="upload ground truth to the QA volume")
    g.add_argument("--redeliver", help="send every file of this business date again (same names, overwritten)")
    p.add_argument("--client", default="calder_valley")
    p.add_argument("--data", default=None, help="default: data/<client>")
    p.add_argument("--profile", default="keelstone")
    p.add_argument("--dry-run", action="store_true")
    a = p.parse_args()

    data = Path(a.data) if a.data else ROOT / "data" / a.client
    client = load_client(a.client)
    if not a.dry_run:
        ensure_landing(client, a.profile)
    if a.ground_truth:
        gt = data / "ground_truth" / "ground_truth.json"
        target = ground_truth_path(a.client)
        steps = [["fs", "mkdir", "dbfs:" + target.rsplit("/", 1)[0], "--profile", a.profile],
                 ["fs", "cp", str(gt), "dbfs:" + target, "--overwrite", "--profile", a.profile]]
        for args in steps:
            print("+ databricks", " ".join(args)) if a.dry_run else cli(*args)
        return

    manifest = json.loads((data / "manifest.json").read_text())
    state_path = data / "delivery_state.json"
    state = json.loads(state_path.read_text()) if state_path.exists() else {"delivered": []}
    if state.get("manifest_seed") not in (None, manifest["params"]["seed"]):
        raise SystemExit("delivery_state.json is from a different dataset; delete it first")
    done = set(state["delivered"])
    if a.redeliver:
        due = [f for f in manifest["files"] if f["business_date"] == a.redeliver and f["path"] in done]
    else:
        due = [f for f in manifest["files"]
               if f["path"] not in done and (a.all or f["delivery_date"] <= a.through)]
    if not due:
        print("nothing new to deliver")
        return
    staging = data / "_staging"
    shutil.rmtree(staging, ignore_errors=True)
    for f in due:
        dst = staging / f["path"]
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(data / "landing" / f["path"], dst)
    print(f"delivering {len(due)} files to {client.landing_root}")
    for f in due[:5]:
        print("  ", f["path"], "(delivered", f["delivery_date"] + ")")
    if a.dry_run:
        return
    cli("fs", "cp", "-r", str(staging), "dbfs:" + client.landing_root, "--profile", a.profile,
        *(["--overwrite"] if a.redeliver else []))
    state = {"manifest_seed": manifest["params"]["seed"],
             "delivered": sorted(done | {f["path"] for f in due})}
    state_path.write_text(json.dumps(state, indent=1))
    shutil.rmtree(staging, ignore_errors=True)


if __name__ == "__main__":
    main()
