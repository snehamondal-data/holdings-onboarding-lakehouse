from collections import Counter

import pytest

from keelstone.config import ConfigError, ReconConfig, load_client, load_source, parse_client
from keelstone.gold.rules import (BREAK_REASONS, UPSTREAM_REASONS, compare, compare_sql, status_of,
                                  status_sql, tie_out_rate)
from keelstone.ingest.local_reader import read_rows
from keelstone.ingest.rules import quarantine_reason
from keelstone.silver.local import AMBIGUOUS, identifier_map, map_position, units_suspect

TOL = load_client("calder_valley").reconciliation


def test_tolerances_come_from_the_client_yaml():
    assert TOL == ReconConfig(quantity_tolerance=0.0, value_abs_tolerance=100.0, value_rel_tolerance=0.0005)
    assert TOL.value_tolerance(50_000) == 100.0 and TOL.value_tolerance(1_000_000) == 500.0
    with pytest.raises(ConfigError, match="negative"):
        parse_client({"client_id": "x", "catalog": "k", "schema": "x", "sources": [],
                      "reconciliation": {"value_tolerance": {"absolute": -1}}})


def test_compare_rule():
    assert compare(100, 100, 1000.0, 1000.4, TOL) is None             # rounding noise
    assert compare(100, 110, 1000.0, 1100.0, TOL) == "quantity_break"  # quantity checked first
    assert compare(100, 100, 100_000.0, 101_500.0, TOL) == "value_break"
    assert compare(100, 100, None, None, TOL) is None                 # nothing to value


def test_sql_rules_mirror_python_rules():
    sql = compare_sql(TOL)
    assert "> 0.0 THEN 'quantity_break'" in sql and "greatest(100.0, 0.0005 * abs(book_market_value))" in sql
    st = status_sql()
    for r in BREAK_REASONS:
        assert f"WHEN reason_code = '{r}' THEN 'break'" in st and status_of(r) == "break"
    assert status_of("pending_late_file") == "pending" and status_of(None) == "matched"
    assert all(status_of(r) == "explained_upstream" for r in UPSTREAM_REASONS)


def test_tie_out_rate_excludes_pending():
    assert tie_out_rate(90, 110, 10) == 0.9
    assert tie_out_rate(0, 5, 5) is None


def test_expected_recon_agrees_with_the_plant_list(any_client):
    """The comparison-derived expectation must contain exactly the planted recon problems."""
    _, _, truth = any_client
    nm = {(x["custodian"], x["business_date"], x["account_id"], x["security_id"]): x["reason"]
          for x in truth["expected_recon"]["non_matched"]}
    planted_breaks = {}
    for p in truth["planted"]:
        key = (p.get("source"), p.get("business_date"), p.get("account_id"), p.get("security_id"))
        if p["problem"] in ("quantity_break", "value_break"):
            planted_breaks[key] = p["problem"]
        elif p["problem"] == "missing_position":
            planted_breaks[key] = p["side"]
        elif p["problem"] == "corrupt_row":
            assert nm[key] == "custodian_row_rejected", p["id"]
    breaks = {k: v for k, v in nm.items() if v in BREAK_REASONS}
    assert breaks == planted_breaks
    unresolved = {(e["source"], e["security_id"]) for e in truth["expected_unresolved"]}
    assert {(k[0], k[3]) for k, v in nm.items() if v == "custodian_identifier_unresolved"} == unresolved
    q = {(k[0], k[1]) for k, v in nm.items() if v == "custodian_row_quarantined"}
    assert q == {(p["source"], p["business_date"]) for p in truth["planted"] if p["problem"] == "units_error"}
    late = truth["expected_recon"]["late_files"]
    assert late and all(lf["pending_book_positions"] > 0 for lf in late)


def _local_recon(client_id, out, truth):
    """Plain-Python reconciliation over the generated files, with the gold rules and YAML tolerances."""
    tol = load_client(client_id).reconciliation
    position_sources = {s for s in load_client(client_id).sources
                        if load_source(s).silver and load_source(s).silver.is_positions}
    files = truth["files"]
    master_rel = next(k for k, v in files.items() if v["source"] == "security_master")
    ids = identifier_map(read_rows(load_source("security_master"), (out / "landing" / master_rel).read_text()))
    prices = {}
    for rel, f in files.items():
        if f["source"] == "prices":
            for r in read_rows(load_source("prices"), (out / "landing" / rel).read_text()):
                prices[(r["price_date"], r["security_id"])] = float(r["close_price"])
    book, cust, upstream, loose = {}, {}, {}, Counter()
    for rel, f in files.items():
        if f["source"] not in position_sources or f["expected_status"] != "loaded":
            continue
        src = load_source(f["source"])
        for row in read_rows(src, (out / "landing" / rel).read_text()):
            if quarantine_reason(src, row):
                # rejected in bronze: key from what can still be read (all planted ones have account+id)
                ident = ":".join((row.get(c) or "").strip().upper() for c in src.silver.identifier_columns)
                sid = ids.get((src.silver.identifier_type, ident))
                acct = (row.get(src.silver.fields["account_id"]) or "").strip()
                if sid and sid is not AMBIGUOUS and acct:
                    upstream[(f["source"], f["business_date"], acct, sid)] = "custodian_row_rejected"
                else:   # key unreadable: gold attributes these by count (account, then date)
                    loose[(f["source"], f["business_date"], acct or None)] += 1
                continue
            pos = map_position(src, row, "c", rel)
            sid = ids.get((pos["identifier_type"], pos["identifier"]))
            key = (pos["account_id"], pos["business_date"].isoformat())
            if src.silver.position_side == "book":
                book[(key[0], sid, key[1])] = float(pos["quantity"])
            elif sid is None or sid is AMBIGUOUS:
                upstream[(f["source"], key[1], key[0], ("qty", float(pos["quantity"])))] = "custodian_identifier_unresolved"
            elif units_suspect(pos):
                upstream[(f["source"], key[1], key[0], sid)] = "custodian_row_quarantined"
            else:
                cust[(key[0], sid, key[1])] = (f["source"], pos)
    acct_cust = {k[0]: v[0] for k, v in cust.items()}
    result = {}
    for k in set(book) | set(cust):
        acct, sid, d = k
        c = cust[k][0] if k in cust else acct_cust[acct]
        if k not in book:
            reason = "missing_in_book"
        elif k not in cust:
            reason = (upstream.get((c, d, acct, sid)) or upstream.get((c, d, acct, ("qty", book[k])))
                      or "missing_at_custodian")
        else:
            pos = cust[k][1]
            bv = round(book[k] * prices[(d, sid)], 2)
            reason = compare(book[k], float(pos["quantity"]), bv, float(pos["market_value"]), tol)
        result[(c, d, acct, sid)] = reason
    for level in ("account", "date"):           # same order as gold's count-based attribution
        for k in sorted(k for k, v in result.items() if v == "missing_at_custodian"):
            c, d, acct, _ = k
            lk = (c, d, acct) if level == "account" else (c, d, None)
            if loose[lk] > 0:
                loose[lk] -= 1
                result[k] = "custodian_row_rejected"
    return result


def test_local_reconciliation_matches_ground_truth(any_client):
    client_id, out, truth = any_client
    got = _local_recon(client_id, out, truth)
    want = {(x["custodian"], x["business_date"], x["account_id"], x["security_id"]): x["reason"]
            for x in truth["expected_recon"]["non_matched"]}
    assert {k: v for k, v in got.items() if v} == want
    daily = Counter()
    for v in truth["expected_recon"]["daily"].values():
        daily.update(v)
    assert sum(1 for v in got.values() if v is None) == daily["matched"]
