"""Run a bundle job with the same variables deploy.py used.

  python scripts/run_job.py <job_key> [phase=pending through=2026-08-11 first_update=full_refresh]
"""
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
from deploy import CLI, TARGET, bundle_vars  # noqa: E402

if __name__ == "__main__":
    job, params = sys.argv[1], sys.argv[2:]
    extra = ["--params", ",".join(params)] if params else []
    sys.exit(subprocess.run([CLI, "bundle", "run", "-t", TARGET, job, *bundle_vars(), *extra], cwd=ROOT).returncode)
