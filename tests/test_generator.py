import hashlib
import json
from collections import Counter
from pathlib import Path

from keelstone.config import load_source
from keelstone.generator import generate
from keelstone.generator.universe import cusip_check_digit, isin_check_digit
from keelstone.ingest.local_reader import read_rows
from keelstone.ingest.rules import quarantine_reason


def _tree_hash(root: Path) -> str:
    h = hashlib.sha256()
    for p in sorted(root.rglob("*")):
        if p.is_file():
            h.update(str(p.relative_to(root)).encode())
            h.update(p.read_bytes())
    return h.hexdigest()


def test_same_seed_gives_byte_identical_output(tmp_path, generated):
    out, _ = generated
    generate(tmp_path / "again")
    assert _tree_hash(out) == _tree_hash(tmp_path / "again")


def test_different_seed_gives_different_data(tmp_path, generated):
    out, _ = generated
    generate(tmp_path / "other", seed=7)
    assert _tree_hash(out / "landing") != _tree_hash(tmp_path / "other" / "landing")


def test_every_planted_problem_type_from_the_spec_is_present(generated):
    _, truth = generated
    kinds = Counter(p["problem"] for p in truth["planted"])
    for k in ("schema_drift", "units_error", "unmapped_security", "duplicate_file", "corrupt_row",
              "quantity_break", "missing_position", "value_break", "late_file"):
        assert kinds[k] >= 1, k
    sides = {p["side"] for p in truth["planted"] if p["problem"] == "missing_position"}
    assert sides == {"missing_at_custodian", "missing_in_book"}
    assert len({p["id"] for p in truth["planted"]}) == len(truth["planted"])


def test_row_level_plants_never_share_a_target(generated):
    _, truth = generated
    keys = [(p["source"], p["business_date"], p["account_id"], p["security_id"])
            for p in truth["planted"] if "account_id" in p]
    assert len(keys) == len(set(keys))


def test_manifest_hashes_match_files(generated):
    out, truth = generated
    manifest = json.loads((out / "manifest.json").read_text())
    assert len(manifest["files"]) == len(truth["files"])
    for f in manifest["files"]:
        data = (out / "landing" / f["path"]).read_bytes()
        assert hashlib.sha256(data).hexdigest() == f["sha256"]


def test_ground_truth_lives_outside_landing(generated):
    out, _ = generated
    assert (out / "ground_truth" / "ground_truth.json").exists()
    assert not list((out / "landing").rglob("*ground_truth*"))


def test_duplicate_file_is_identical_bytes_delivered_next_day(generated):
    out, truth = generated
    (dup,) = [p for p in truth["planted"] if p["problem"] == "duplicate_file"]
    a = (out / "landing" / dup["file"]).read_bytes()
    b = (out / "landing" / dup["duplicate_of"]).read_bytes()
    assert a == b
    f = truth["files"][dup["file"]]
    assert f["delivery_date"] > f["business_date"] and f["expected_status"] == "duplicate_content"


def test_late_file_is_delivered_after_its_business_date(generated):
    _, truth = generated
    (late,) = [p for p in truth["planted"] if p["problem"] == "late_file"]
    f = truth["files"][late["file"]]
    assert f["delivery_date"] > f["business_date"]
    others = [v for k, v in truth["files"].items() if k != late["file"] and v["expected_status"] == "loaded"]
    assert all(v["delivery_date"] == v["business_date"] for v in others)


def test_linden_drift_starts_mid_period_and_units_error_is_in_units(generated):
    out, truth = generated
    (drift,) = [p for p in truth["planted"] if p["problem"] == "schema_drift"]
    linden = sorted(k for k, v in truth["files"].items() if v["source"] == "linden")
    drifted = set(drift["files"])
    assert 0 < len(drifted) < len(linden)
    for k in linden:
        header = (out / "landing" / k).read_text().splitlines()[0]
        assert ("SEC_DESC" in header) == (k in drifted)
    (units,) = [p for p in truth["planted"] if p["problem"] == "units_error"]
    line = (out / "landing" / units["file"]).read_text().splitlines()[1].split("|")
    qty, price, mv = float(line[4]), float(line[5]), float(line[6])
    assert abs(mv - qty * price) < 1.0                     # full units, not thousands


def test_unmapped_identifiers_are_blank_in_master_but_used_by_custodian(any_client):
    _, out, truth = any_client
    master_rel = next(k for k, v in truth["files"].items() if v["source"] == "security_master")
    rows = {l.split(",")[0]: l.split(",") for l in (out / "landing" / master_rel).read_text().splitlines()[1:]}
    col = {"isin": 1, "cusip": 2, "ticker": 3, "exchange": 4}
    fields = {"isin": ("isin",), "cusip": ("cusip",), "ticker_exchange": ("ticker", "exchange")}
    planted = [p for p in truth["planted"] if p["problem"] == "unmapped_security"]
    assert planted
    for p in planted:
        for f in fields[p["identifier_type"]]:
            assert rows[p["security_id"]][col[f]] == ""
        assert p["affected_rows"] > 0
    listed = {(e["source"], e["security_id"]) for e in truth["expected_unresolved"]}
    assert {(p["source"], p["security_id"]) for p in planted} <= listed


def test_local_reader_and_rules_reproduce_expected_bronze_outcome(any_client):
    """Cross-check: what the generator says it planted is what the rules catch."""
    _, out, truth = any_client
    sources = {}
    for rel, f in truth["files"].items():
        src = sources.setdefault(f["source"], load_source(f["source"]))
        rows = read_rows(src, (out / "landing" / rel).read_text())
        assert len(rows) == f["records"], rel
        reasons = Counter(r for r in (quarantine_reason(src, row) for row in rows) if r)
        assert dict(reasons) == f["expected_quarantine"], rel
        drift = sorted({k for row in rows for k in (row["_rescued_data"] or {})})
        assert drift == f["expected_drift_columns"], rel


def test_value_breaks_exceed_tolerance(generated):
    _, truth = generated
    for p in [p for p in truth["planted"] if p["problem"] == "value_break"]:
        assert abs(p["true_market_value"] - p["custodian_market_value"]) >= 1000


def test_identifier_check_digits():
    body = "GB00B03MLX2"
    assert isin_check_digit(body) != isin_check_digit("GB00B03MLX3")
    assert len(cusip_check_digit("ABC12345")) == 1


def test_adding_a_custodian_leaves_existing_custodian_files_unchanged(generated, tmp_path):
    """Onboarding Holloway Custody must not change a byte of Harrow, Linden or Pemberton data."""
    import hashlib
    out, truth = generated
    for rel, f in truth["files"].items():
        if f["source"] in ("harrow", "linden", "pemberton", "prices", "fx"):
            assert hashlib.sha256((out / "landing" / rel).read_bytes()).hexdigest() == f["sha256"]
    # the original custodians' files are fixed points: these hashes come from the slice 1-3 dataset
    h = hashlib.sha256()
    for rel in sorted(r for r, f in truth["files"].items() if f["source"] in ("harrow", "linden", "pemberton")):
        h.update((out / "landing" / rel).read_bytes())
    assert h.hexdigest()[:12] == ORIGINAL_CUSTODIANS_SHA256_PREFIX


ORIGINAL_CUSTODIANS_SHA256_PREFIX = "e4de838b7782"  # slice 1-3 dataset, seed 42


def test_second_client_has_its_own_accounts_and_plants(any_client):
    client_id, _, truth = any_client
    accounts = {x["account_id"] for x in truth["expected_recon"]["non_matched"]}
    prefix = {"calder_valley": "CV-", "wrexford": "WX-"}[client_id]
    assert accounts and all(a.startswith(prefix) for a in accounts)
    kinds = {p["problem"] for p in truth["planted"]}
    assert {"corrupt_row", "unmapped_security", "quantity_break", "value_break", "missing_position",
            "late_file", "duplicate_row"} <= kinds


def test_scale_mode_is_sized_clean_and_matches_its_ground_truth(tmp_path):
    """Scale mode: rows close to the target, gzipped files the bronze rules accept, all ties out."""
    import gzip
    from collections import Counter
    from keelstone.generator.scale import generate_scale
    truth = generate_scale(tmp_path, rows=30_000, n_days=5, n_securities=200, holdings=20)
    assert 0.95 <= truth["params"]["rows_custodian_and_book"] / 30_000 <= 1.2
    assert truth["params"]["position_lots"] > 0 and truth["planted"] == []
    for rel, f in truth["files"].items():
        raw = (tmp_path / "landing" / rel).read_bytes()
        assert hashlib.sha256(raw).hexdigest() == f["sha256"]
        text = gzip.decompress(raw).decode() if rel.endswith(".gz") else raw.decode()
        src = load_source(f["source"])
        rows = read_rows(src, text)
        assert len(rows) == f["records"], rel
        assert not Counter(r for r in (quarantine_reason(src, row) for row in rows) if r), rel
    book = sum(f["records"] for f in truth["files"].values() if f["source"] == "scale_book")
    cust = sum(f["records"] for f in truth["files"].values() if f["source"] in ("scale_csv", "scale_lots"))
    assert book == cust == sum(v["matched"] for v in truth["expected_recon"]["daily"].values())


def test_scale_mode_is_deterministic(tmp_path):
    from keelstone.generator.scale import generate_scale
    a = generate_scale(tmp_path / "a", rows=10_000, n_days=3, n_securities=100, holdings=10)
    b = generate_scale(tmp_path / "b", rows=10_000, n_days=3, n_securities=100, holdings=10)
    assert {k: v["sha256"] for k, v in a["files"].items()} == {k: v["sha256"] for k, v in b["files"].items()}
