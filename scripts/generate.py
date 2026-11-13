"""Generate a client's synthetic dataset into data/<client>/.

  python scripts/generate.py                        # every client
  python scripts/generate.py --client wrexford [--seed 42] [--days 60]
  python scripts/generate.py --client scale_test --rows 5000000
"""
import argparse
import shutil
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from keelstone.generator import DEFAULTS, generate  # noqa: E402
from keelstone.generator.clients import CLIENTS  # noqa: E402
from keelstone.generator.scale import generate_scale  # noqa: E402


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--client", choices=sorted(CLIENTS) + ["scale_test"], default=None)
    p.add_argument("--rows", type=int, default=None, help="scale_test: target custodian + book rows")
    p.add_argument("--seed", type=int, default=DEFAULTS["seed"])
    p.add_argument("--days", type=int, default=DEFAULTS["n_days"])
    p.add_argument("--data", default="data")
    a = p.parse_args()
    if a.client == "scale_test":
        out = Path(a.data) / "scale_test"
        for sub in ("landing", "ground_truth"):
            shutil.rmtree(out / sub, ignore_errors=True)
        truth = generate_scale(out, rows=a.rows or 1_000_000, seed=a.seed, n_days=a.days)
        print("scale_test:", truth["params"], "files", len(truth["files"]))
        return
    for client in [a.client] if a.client else sorted(CLIENTS):
        out = Path(a.data) / client
        for sub in ("landing", "ground_truth"):
            shutil.rmtree(out / sub, ignore_errors=True)
        truth = generate(out, client=client, seed=a.seed, n_days=a.days)
        files = truth["files"]
        print(f"{client}: seed={a.seed} files={len(files)} records={sum(f['records'] for f in files.values())}")
        print("  planted:", dict(Counter(p["problem"] for p in truth["planted"])))
        print("  ground truth:", out / "ground_truth" / "ground_truth.json")


if __name__ == "__main__":
    main()
