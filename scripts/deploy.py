"""Deploy: build the wheel, deploy the bundle (jobs + notebooks), and upload the
wheel and the YAML configs to the artifacts volume the notebooks install/read from.

Why a volume: on this Free Edition workspace, serverless compute intermittently
cannot read bundle files under /Workspace (403 from workspace storage), while UC
volumes read reliably. Notebooks themselves load fine.
"""
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
from build_wheel import WHEEL_NAME, build  # noqa: E402

CLI = str(Path.home() / ".local/bin/databricks")
PROFILE = "keelstone"
ARTIFACTS = "/Volumes/keelstone/platform/artifacts"


def run(*args: str, ok_if: str | None = None) -> None:
    print("+ databricks", " ".join(args))
    r = subprocess.run([CLI, *args, "--profile", PROFILE], capture_output=True, text=True)
    if r.returncode != 0 and not (ok_if and ok_if in (r.stderr + r.stdout)):
        sys.exit(r.stderr or r.stdout)


def main() -> None:
    wheel = build()
    run("schemas", "create", "platform", "keelstone", "--comment",
        "Shared platform artifacts (code wheel, configs)", ok_if="already exists")
    run("volumes", "create", "keelstone", "platform", "artifacts", "MANAGED", ok_if="already exists")
    run("fs", "cp", str(wheel), f"dbfs:{ARTIFACTS}/{WHEEL_NAME}", "--overwrite")
    run("fs", "cp", "-r", str(ROOT / "config"), f"dbfs:{ARTIFACTS}/config", "--overwrite")
    subprocess.run([CLI, "bundle", "deploy"], cwd=ROOT, check=True)


if __name__ == "__main__":
    main()
