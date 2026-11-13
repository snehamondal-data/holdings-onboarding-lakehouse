"""Reset one client's landing zone and bronze so files can be replayed day by day.

Deletes: every file in the client's landing and checkpoint volumes, every bronze_* table in the
client schema, and the local delivery state. Keeps: configs, ground truth (keelstone.qa) and the
silver/gold pipeline (run it next with a full refresh: first_update=full_refresh).
Synthetic data only. Requires --yes.

  python scripts/reset_client.py --client calder_valley --yes
"""
import argparse
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from keelstone.config import load_client  # noqa: E402

CLI = str(Path.home() / ".local/bin/databricks")


def cli(*args: str, profile: str) -> str:
    r = subprocess.run([CLI, *args, "--profile", profile], capture_output=True, text=True)
    if r.returncode != 0:
        sys.exit(f"databricks {' '.join(args)} failed: {r.stderr or r.stdout}")
    return r.stdout


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--client", required=True)
    p.add_argument("--profile", default="keelstone")
    p.add_argument("--yes", action="store_true")
    a = p.parse_args()
    c = load_client(a.client)
    if not a.yes:
        sys.exit("refusing to reset without --yes")
    for root in (c.landing_root, c.checkpoint_root):
        listing = cli("fs", "ls", "dbfs:" + root, profile=a.profile).split()
        for name in listing:
            print(f"rm -r {root}/{name}")
            cli("fs", "rm", "-r", f"dbfs:{root}/{name}", profile=a.profile)
    tables = json.loads(cli("tables", "list", c.catalog, c.schema, "-o", "json", profile=a.profile) or "[]")
    for t in tables if isinstance(tables, list) else tables.get("tables", []):
        if t["name"].startswith("bronze_"):
            print("drop table", t["full_name"])
            cli("tables", "delete", t["full_name"], profile=a.profile)
    state = ROOT / "data" / c.client_id / "delivery_state.json"
    if state.exists():
        state.unlink()
        print("removed", state)
    print("reset done; next: deliver files; the client job's next pipeline update should be a full refresh")


if __name__ == "__main__":
    main()
