from collections import Counter
from datetime import date
from decimal import Decimal

import pytest

from keelstone.config import ConfigError, load_client, load_source, parse_source
from keelstone.ingest.local_reader import read_rows
from keelstone.ingest.rules import quarantine_reason
from keelstone.silver import mapping as M
from keelstone.silver.local import (AMBIGUOUS, identifier_map, map_position, silver_outcomes,
                                    units_suspect)

POSITION_SOURCES = ("harrow", "linden", "pemberton", "book_positions")


def test_every_source_has_a_valid_silver_mapping():
    for sid in load_client("calder_valley").sources:
        s = load_source(sid).silver
        assert s is not None, sid
        if sid in POSITION_SOURCES:
            assert s.is_positions and s.table == "silver_positions"
        else:
            assert not s.is_positions and s.key


def test_identifier_types_and_units_multipliers_come_from_config():
    got = {sid: (load_source(sid).silver.identifier_type, load_source(sid).silver.market_value_multiplier)
           for sid in POSITION_SOURCES}
    assert got == {"harrow": ("isin", 1.0), "linden": ("cusip", 1000.0),
                   "pemberton": ("ticker_exchange", 1.0), "book_positions": ("security_id", 1.0)}


def test_linden_header_versions_drive_bronze_aliases_and_silver_version():
    src = load_source("linden")
    assert [h.version for h in src.header_versions] == [1, 2]
    assert src.column_aliases == {"DESCRIPTION": ("SEC_DESC",), "MKT_VALUE_000": ("MARKET_VALUE_THOUSANDS",)}
    assert src.header_version_for(None) == 1
    assert src.header_version_for({"SEC_DESC", "MARKET_VALUE_THOUSANDS", "SETTLE_STATUS"}) == 2
    sql = M.header_version_expr(src)
    assert "array_contains" in sql and "THEN 2" in sql and sql.endswith("ELSE 1 END")


def test_sql_mapping_is_built_from_config():
    pem = dict(M.position_select(load_source("pemberton"), "c1"))
    assert pem["quantity"].startswith("CAST(aggregate(`lots`")             # tax lots summed
    assert pem["identifier"] == "concat_ws(':', upper(trim(`ticker`)), upper(trim(`exchange`)))"
    lin = dict(M.position_select(load_source("linden"), "c1"))
    assert "* 1000.0 AS DECIMAL(38,2)" in lin["market_value"]
    assert "'dd/MM/yyyy'" in lin["business_date"]
    book = dict(M.position_select(load_source("book_positions"), "c1"))
    assert book["price"] == "CAST(NULL AS DECIMAL(38,6))" and book["market_value"].startswith("CAST(NULL")
    fx = dict(M.reference_select(load_source("fx"), "c1"))
    assert fx["usd_per_unit"] == "CAST(`usd_per_unit` AS DECIMAL(38,8))"


def test_linden_row_maps_to_canonical_position_in_units():
    src = load_source("linden")
    text = ("ACCT|POS_DATE|CUSIP|SEC_DESC|NOMINAL|MKT_PRICE|MARKET_VALUE_THOUSANDS|CCY|SETTLE_STATUS\n"
            "CV-LB-001|05/08/2026|abc123|Desc|6180|46.9385|290.080|usd|SETTLED\n")
    (row,) = read_rows(src, text)
    pos = map_position(src, row, "calder_valley", "f.txt")
    assert pos["business_date"] == date(2026, 8, 5)
    assert pos["market_value"] == Decimal("290080.00")
    assert pos["identifier"] == "ABC123" and pos["currency"] == "USD" and pos["header_version"] == 2
    assert not units_suspect(pos)


def test_pemberton_tax_lots_are_summed():
    src = load_source("pemberton")
    text = ('{"account":"A","position_date":"2026-07-01","ticker":"kas","exchange":"XNYS","currency":"USD",'
            '"price":10.0,"market_value":1250.0,"lots":[{"lot_id":"1","quantity":100},{"lot_id":"2","quantity":25}]}\n')
    (row,) = read_rows(src, text)
    pos = map_position(src, row, "c", "f")
    assert pos["quantity"] == Decimal("125") and pos["identifier"] == "KAS:XNYS"


def test_units_rule_flags_about_1000x_both_ways():
    base = {"quantity": Decimal(100), "price": Decimal("10")}
    assert units_suspect({**base, "market_value": Decimal("1000000")})       # 1000x too big
    assert units_suspect({**base, "market_value": Decimal("1.0")})           # 1000x too small
    assert not units_suspect({**base, "market_value": Decimal("1010")})      # stale price, not units
    assert not units_suspect({**base, "price": None, "market_value": None})  # book rows


def test_identifier_map_marks_ambiguous_and_skips_blanks():
    ids = identifier_map([
        {"security_id": "KS1", "isin": "X1", "cusip": "", "ticker": "AA", "exchange": "XNYS"},
        {"security_id": "KS2", "isin": "X1", "cusip": "C2", "ticker": "", "exchange": "XNYS"},
    ])
    assert ids[("isin", "X1")] is AMBIGUOUS
    assert ids[("cusip", "C2")] == "KS2" and ("cusip", "") not in ids
    assert ids[("ticker_exchange", "AA:XNYS")] == "KS1"
    assert ("ticker_exchange", ":XNYS") not in ids


@pytest.mark.parametrize("silver,msg", [
    ({"target": "positions", "position_side": "custodian", "fields": {"account_id": "a"}}, "needs field"),
    ({"target": "positions", "position_side": "custodian",
      "fields": {"account_id": "a", "business_date": "d", "quantity": "q", "currency": "c", "isin": "a"}},
     "not a canonical"),
    ({"target": "positions", "position_side": "custodian",
      "fields": {"account_id": "a", "business_date": "d", "quantity": "nope", "currency": "c"}}, "unknown column"),
    ({"target": "positions", "position_side": "custodian",
      "fields": {"account_id": "a", "business_date": "d", "quantity": "q", "currency": "c"},
      "identifier": {"type": "sedol", "columns": ["a"]}}, "identifier type"),
    ({"target": "positions", "position_side": "custodian",
      "fields": {"account_id": "a", "business_date": "d", "quantity": "q", "currency": "c"},
      "identifier": {"type": "isin", "columns": ["a"]}, "market_value_multiplier": 0}, "multiplier"),
    ({"target": "prices", "fields": {"a": "string"}, "key": ["zzz"]}, "key"),
    ({"target": "prices", "fields": {"a": "money"}, "key": ["a"]}, "type"),
])
def test_bad_silver_config_is_rejected(silver, msg):
    base = {"source_id": "x", "kind": "custodian", "format": "csv", "landing_subdir": "x",
            "bronze_table": "bronze_x", "columns": ["a", "d", "q", "c"], "options": {"delimiter": ","},
            "rules": {"dates": {"d": "yyyy-MM-dd"}}, "silver": silver}
    with pytest.raises(ConfigError, match=msg):
        parse_source(base)


def test_header_rename_to_unknown_column_is_rejected():
    with pytest.raises(ConfigError, match="rename target"):
        parse_source({"source_id": "x", "kind": "custodian", "format": "csv", "landing_subdir": "x",
                      "bronze_table": "bronze_x", "columns": ["a"], "options": {"delimiter": ","},
                      "header_versions": [{"version": 1, "effective_from": "2026-01-01",
                                           "renames": {"B": "nope"}}]})


def test_local_silver_matches_ground_truth_for_every_file(generated):
    """bronze rows = silver + quarantine + unresolved + deduped, and each part matches ground truth."""
    out, truth = generated
    master_src = load_source("security_master")
    master_file = next(k for k, v in truth["files"].items() if v["source"] == "security_master")
    master_rows = read_rows(master_src, (out / "landing" / master_file).read_text())
    ids = identifier_map(master_rows)
    totals = Counter()
    for rel, f in truth["files"].items():
        exp = f["expected_silver"]
        if f["expected_status"] != "loaded":
            assert exp == {"rows": 0, "unresolved": 0, "deduped": 0, "quarantine": {}}
            continue
        src = load_source(f["source"])
        rows = [r for r in read_rows(src, (out / "landing" / rel).read_text()) if quarantine_reason(src, r) is None]
        got = silver_outcomes(src, rows, "calder_valley", ids, rel)
        assert got == exp, rel
        assert len(rows) == got["rows"] + got["unresolved"] + got["deduped"] + sum(got["quarantine"].values())
        totals.update({k: v for k, v in got.items() if k != "quarantine"})
        totals.update(got["quarantine"])
    assert totals["deduped"] == sum(1 for p in truth["planted"] if p["problem"] == "duplicate_row")
    unmapped = sum(p["affected_rows"] for p in truth["planted"] if p["problem"] == "unmapped_security")
    assert totals["unresolved"] == unmapped
    (units,) = [p for p in truth["planted"] if p["problem"] == "units_error"]
    assert totals[M.UNITS_REASON] == truth["files"][units["file"]]["expected_silver"]["quarantine"][M.UNITS_REASON]


def test_planted_breaks_survive_mapping_with_custodian_values(generated):
    """Quantity and value breaks are custodian facts silver must keep, not fix."""
    out, truth = generated
    for p in truth["planted"]:
        if p["problem"] not in ("quantity_break", "value_break"):
            continue
        rel = next(k for k, v in truth["files"].items()
                   if v["source"] == p["source"] and v["business_date"] == p["business_date"]
                   and v["expected_status"] == "loaded")
        src = load_source(p["source"])
        rows = [map_position(src, r, "c", rel) for r in read_rows(src, (out / "landing" / rel).read_text())
                if quarantine_reason(src, r) is None]
        hits = [r for r in rows if r["account_id"] == p["account_id"]]
        if p["problem"] == "quantity_break":
            assert any(r["quantity"] == p["custodian_quantity"] for r in hits), p["id"]
        else:
            assert any(abs(float(r["market_value"]) - p["custodian_market_value"]) <= 1.0 for r in hits), p["id"]
