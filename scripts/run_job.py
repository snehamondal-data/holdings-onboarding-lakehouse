"""Run a bundle job with the same variables deploy.py used. Usage: python scripts/run_job.py <job_key>"""
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
from deploy import CLI, bundle_vars  # noqa: E402

if __name__ == "__main__":
    sys.exit(subprocess.run([CLI, "bundle", "run", sys.argv[1], *bundle_vars()], cwd=ROOT).returncode)
