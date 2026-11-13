"""QA area: ground truth and validation results. Only validation jobs and setup may
reference this module; pipeline code (ingest, silver, gold) must not. A test enforces it."""
QA_CATALOG = "keelstone"
QA_SCHEMA = "qa"
GROUND_TRUTH_VOLUME = "ground_truth"


def ground_truth_path(client_id: str) -> str:
    return f"/Volumes/{QA_CATALOG}/{QA_SCHEMA}/{GROUND_TRUTH_VOLUME}/{client_id}/ground_truth.json"
