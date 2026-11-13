from pathlib import Path

import pytest

from keelstone.generator import generate

REPO = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="session")
def generated(tmp_path_factory):
    """One default dataset (seed 42) shared by the generator tests."""
    out = tmp_path_factory.mktemp("gen")
    truth = generate(out)
    return out, truth
