"""Generate the synthetic dataset. Usage: python scripts/generate.py [--seed 42] [--days 60] [--out data]"""
import argparse
import shutil
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from keelstone.generator import DEFAULTS, generate  # noqa: E402


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--seed", type=int, default=DEFAULTS["seed"])
    p.add_argument("--days", type=int, default=DEFAULTS["n_days"])
    p.add_argument("--out", default="data")
    a = p.parse_args()
    out = Path(a.out)
    for sub in ("landing", "ground_truth"):
        shutil.rmtree(out / sub, ignore_errors=True)
    truth = generate(out, seed=a.seed, n_days=a.days)
    files = truth["files"]
    print(f"seed={a.seed} files={len(files)} records={sum(f['records'] for f in files.values())}")
    print("planted:", dict(Counter(p["problem"] for p in truth["planted"])))
    print(f"ground truth: {out / 'ground_truth' / 'ground_truth.json'}")


if __name__ == "__main__":
    main()
