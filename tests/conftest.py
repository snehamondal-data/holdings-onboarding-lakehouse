from pathlib import Path

import pytest

from keelstone.generator import generate

REPO = Path(__file__).resolve().parents[1]
CLIENT_IDS = ("calder_valley", "wrexford")


@pytest.fixture(scope="session")
def generated(tmp_path_factory):
    """Calder Valley's default dataset (seed 42), shared by the generator tests."""
    out = tmp_path_factory.mktemp("gen")
    truth = generate(out)
    return out, truth


@pytest.fixture(scope="session", params=CLIENT_IDS)
def any_client(request, tmp_path_factory):
    """Each client's dataset: (client_id, out_dir, ground truth)."""
    out = tmp_path_factory.mktemp(request.param)
    return request.param, out, generate(out, client=request.param)
