"""Ground truth must never feed the pipeline. Pipeline code (ingest, and later silver
and gold) may not reference the QA schema, the ground truth, or keelstone.qa; only
validation jobs may read it."""
import re
from pathlib import Path

import yaml

from keelstone.config import plan_ingest

REPO = Path(__file__).resolve().parents[1]
FORBIDDEN = [
    re.compile(r"keelstone\.qa\b"),          # import of the QA module, or catalog.schema name
    re.compile(r"ground_truth"),
    re.compile(r"""['"/.]qa['"/.]"""),       # "qa", 'qa', /qa/, .qa.
    re.compile(r"\bQA_(SCHEMA|CATALOG)\b"),
]
PIPELINE_PACKAGES = ["src/keelstone/ingest", "src/keelstone/silver", "src/keelstone/gold"]
PIPELINE_JOB_PREFIXES = ("ingest_", "silver_", "gold_")


def pipeline_files() -> list[Path]:
    files = []
    for pkg in PIPELINE_PACKAGES:
        files += sorted((REPO / pkg).rglob("*.py"))
    files += sorted(p for p in (REPO / "jobs").glob("*.py") if p.name.startswith(PIPELINE_JOB_PREFIXES))
    files += [REPO / "src" / "keelstone" / "config.py"]
    return files


def _hits(path: Path) -> list[str]:
    text = path.read_text()
    if path.name == "config.py":  # the guard that rejects a client configured onto qa is allowed
        text = re.sub(r"RESERVED_SCHEMAS = \{[^}]*\}", "", text)
    return [rx.pattern for rx in FORBIDDEN if rx.search(text)]


def test_pipeline_code_never_references_qa():
    files = pipeline_files()
    assert any(f.name == "ingest_bronze.py" for f in files)
    offenders = {str(f.relative_to(REPO)): h for f in files if (h := _hits(f))}
    assert not offenders, offenders


def test_the_scanner_does_catch_a_qa_read():
    assert _hits(REPO / "jobs" / "validate_slice1.py"), "validation job reads qa, scanner should see it"


def test_pipeline_plans_point_only_at_the_client_schema():
    for path in (REPO / "config" / "clients").glob("*.yml"):
        for p in plan_ingest(path.stem):
            for value in (p.input_path, p.checkpoint_path, p.schema_location, p.bronze_table,
                          p.quarantine_table, p.file_log_table, p.drift_log_table):
                assert ".qa." not in value and "/qa/" not in value, value


def test_only_validation_tasks_get_qa_inputs_in_the_bundle():
    for job_file in (REPO / "resources").glob("*.yml"):
        jobs = yaml.safe_load(job_file.read_text())["resources"]["jobs"]
        for job in jobs.values():
            for task in job["tasks"]:
                nb = task.get("notebook_task", {})
                script = nb.get("notebook_path", "")
                params = " ".join(f"{k} {v}" for k, v in nb.get("base_parameters", {}).items())
                if not Path(script).name.startswith("validate_"):
                    assert "qa" not in params.split() and "ground_truth" not in params, task["task_key"]
