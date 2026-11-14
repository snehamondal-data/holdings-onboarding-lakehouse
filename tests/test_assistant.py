"""Onboarding assistant (slice 7): tools, draft validation, dry run, the agent loop with a scripted
fake model (no API calls), and the human approval step."""
import csv
import json
import re
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from keelstone.assistant import agent as A
from keelstone.assistant.draft import dry_run, to_config, validate
from keelstone.assistant.tools import Sample
from keelstone.config import load_source

REPO = Path(__file__).resolve().parents[1]
DATE = "2026-07-08"


@pytest.fixture(scope="module")
def data(tmp_path_factory):
    from keelstone.generator import generate
    out = {}
    for client in ("calder_valley", "wrexford"):
        d = tmp_path_factory.mktemp(client)
        generate(d, client=client)
        out[client] = d
    return out


def sample(data, client, source_id) -> Sample:
    src = load_source(source_id)
    folder = data[client] / "landing" / src.landing_subdir / DATE
    path = next(p for p in sorted(folder.iterdir()) if p.match(src.file_glob))
    master = next((data[client] / "landing" / "reference" / "security_master").rglob("*.csv"))
    return Sample(path.name, path.read_text(), list(csv.DictReader(master.open())))


GOOD_HARROW = {"format": "csv", "file_glob": "*.csv", "delimiter": ",", "decimal_separator": ".",
               "thousands_separator": "", "columns": ["account_id", "as_of_date", "isin", "security_name", "quantity",
                                                      "price", "market_value", "currency"],
               "business_date_column": "as_of_date", "date_format": "yyyy-MM-dd", "account_column": "account_id",
               "quantity_column": "quantity", "price_column": "price", "market_value_column": "market_value",
               "currency_column": "currency", "identifier_type": "isin", "identifier_columns": ["isin"],
               "market_value_multiplier": 1, "numeric_columns": ["quantity", "price", "market_value"],
               "evidence": "sniffed ',', ISIN 100% in master, ratio 1.0"}


# ---- tools ----------------------------------------------------------------------------------
def test_tools_find_the_facts_a_config_needs(data):
    h, hol, q = sample(data, "calder_valley", "harrow"), sample(data, "calder_valley", "holloway"), sample(data, "wrexford", "quayside")
    assert h.sniff_delimiter()["best_guess"] == "," and hol.sniff_delimiter()["best_guess"] == ";"
    assert q.sniff_delimiter()["best_guess"] == "\t"
    assert h.match_identifiers(",", ["isin"])["share_matching_master"]["isin"] > 0.95
    assert h.profile_column(",", "as_of_date")["date_patterns_parse_share"]["yyyy-MM-dd"] == 1.0
    styles = hol.profile_column(";", "Kurswert")["number_styles_parse_share"]
    assert styles["comma_decimal_dot_thousands"] == 1.0 and styles["dot_decimal"] < 0.5
    lin = sample(data, "calder_valley", "linden")
    assert lin.estimate_units("|", "NOMINAL", "MKT_PRICE", "MKT_VALUE_000", ".", "")["suggested_market_value_multiplier"] == 1000
    pem = sample(data, "calder_valley", "pemberton")
    cols = pem.list_columns(None)
    assert "lots[].quantity" in cols["nested_array_fields"]["lots"]
    assert pem.estimate_units(None, "lots[].quantity", "price", "market_value", ".", "")["suggested_market_value_multiplier"] == 1
    assert pem.match_identifiers(None, ["ticker", "exchange"])["share_matching_master"]["ticker_exchange"] > 0.9


def test_tools_record_evidence_and_refuse_unknown_columns(data):
    h = sample(data, "calder_valley", "harrow")
    h.call("read_lines", {"start": 0, "count": 500})
    assert len(json.loads(h.calls[0]["result"].rstrip("."))["lines"]) <= 30 if not h.calls[0]["result"].endswith("...") else True
    with pytest.raises(ValueError, match="no column"):
        h.call("profile_column", {"delimiter": ",", "column": "nope"})


# ---- draft -> config -> validate -> dry run ------------------------------------------------------
def test_a_correct_draft_validates_and_passes_the_dry_run(data):
    s = sample(data, "calder_valley", "harrow")
    cfg = to_config(GOOD_HARROW, "harrow_new", "Harrow (test)", "custodian", "harrow_new", s.text)
    src, err = validate(cfg)
    assert err is None and src.silver.identifier_type == "isin"
    dr = dry_run(src, s.text, s.master_rows)
    assert dr["passed"], dr


def test_dry_run_catches_wrong_units_and_wrong_dates(data):
    s = sample(data, "calder_valley", "harrow")
    wrong_units = to_config({**GOOD_HARROW, "market_value_multiplier": 1000}, "x", "x", "custodian", "x", s.text)
    assert not dry_run(validate(wrong_units)[0], s.text, s.master_rows)["checks"]["units"]
    wrong_date = to_config({**GOOD_HARROW, "date_format": "dd/MM/yyyy"}, "x", "x", "custodian", "x", s.text)
    dr = dry_run(validate(wrong_date)[0], s.text, s.master_rows)
    assert not dr["checks"]["quarantine_rate"] and not dr["passed"]


def test_nested_lots_get_their_struct_type_from_the_sample(data):
    s = sample(data, "calder_valley", "pemberton")
    d = {**GOOD_HARROW, "format": "json", "file_glob": "*.jsonl", "delimiter": None,
         "columns": ["account", "position_date", "ticker", "exchange", "currency", "price", "market_value", "lots"],
         "business_date_column": "position_date", "account_column": "account", "quantity_column": "lots[].quantity",
         "identifier_type": "ticker_exchange", "identifier_columns": ["ticker", "exchange"],
         "numeric_columns": ["price", "market_value", "lots[].quantity"]}
    cfg = to_config(d, "pem_new", "P", "custodian", "pem_new", s.text)
    assert cfg["column_types"]["lots"].startswith("array<struct<lot_id:string,quantity:string")
    src, err = validate(cfg)
    assert err is None and dry_run(src, s.text, s.master_rows)["passed"]


def test_an_invalid_draft_reports_why(data):
    s = sample(data, "calder_valley", "harrow")
    _, err = validate(to_config({**GOOD_HARROW, "date_format": "dd-MMM-yy"}, "x", "x", "custodian", "x", s.text))
    assert "date format" in err
    _, err = validate(to_config({**GOOD_HARROW, "account_column": "nope"}, "x", "x", "custodian", "x", s.text))
    assert "unknown column" in err or "not in columns" in err


# ---- the agent loop with a scripted fake model ------------------------------------------------------
def tool_use(name, args, i):
    return {"type": "tool_use", "id": f"t{i}", "name": name, "input": args}


class ScriptedModel:
    """Replays a fixed list of responses and records what it was sent. No API calls."""

    def __init__(self, turns):
        self.turns, self.sent = list(turns), []

    def create(self, system, tools, messages):
        self.sent.append(json.loads(json.dumps(messages, default=str)))
        content, stop = self.turns.pop(0)
        return SimpleNamespace(content=content, stop_reason=stop,
                               usage=SimpleNamespace(input_tokens=1000, output_tokens=200,
                                                     cache_creation_input_tokens=0, cache_read_input_tokens=0))


def run_agent(data, turns, limit=1.0):
    s = sample(data, "calder_valley", "harrow")
    m = ScriptedModel(turns)
    r = A.draft_source(m, s, "harrow_new", "Harrow (test)", "custodian", "harrow_new", A.Spend(limit))
    return r, m


def test_agent_drafts_after_using_tools(data):
    r, m = run_agent(data, [
        ([tool_use("sniff_delimiter", {}, 1), tool_use("match_identifiers", {"delimiter": ",", "columns": ["isin"]}, 2)], "tool_use"),
        ([tool_use("submit_draft", GOOD_HARROW, 3)], "tool_use")])
    assert r.status == "drafted" and r.dry_run["passed"] and len(r.attempts) == 1
    assert [c["tool"] for c in r.evidence] == ["sniff_delimiter", "match_identifiers"]
    tool_results = m.sent[1][-1]["content"]
    assert {t["tool_use_id"] for t in tool_results} == {"t1", "t2"}       # all results in one message
    assert "approved by a human" in r.yaml_text and r.spend["calls"] == 2


def test_one_retry_after_an_invalid_draft(data):
    bad = {**GOOD_HARROW, "date_format": "dd-MMM-yy"}
    r, m = run_agent(data, [([tool_use("submit_draft", bad, 1)], "tool_use"),
                            ([tool_use("submit_draft", GOOD_HARROW, 2)], "tool_use")])
    assert r.status == "drafted" and [a["valid"] for a in r.attempts] == [False, True]
    retry_msg = m.sent[1][-1]["content"][0]
    assert retry_msg["is_error"] and "submit once more" in retry_msg["content"]


def test_two_invalid_drafts_fall_back_to_a_human(data):
    bad = {**GOOD_HARROW, "date_format": "dd-MMM-yy"}
    r, _ = run_agent(data, [([tool_use("sniff_delimiter", {}, 0)], "tool_use"),
                            ([tool_use("submit_draft", bad, 1)], "tool_use"),
                            ([tool_use("submit_draft", bad, 2)], "tool_use")])
    assert r.status == "needs_human" and r.config is None
    assert r.note.startswith("Needs a human.") and "Here is what I found" in r.note and r.evidence


def test_refusal_and_stopping_without_a_draft_need_a_human(data):
    r, _ = run_agent(data, [([], "refusal")])
    assert r.status == "needs_human" and "declined" in r.note
    r, m = run_agent(data, [([{"type": "text", "text": "done"}], "end_turn"),
                            ([{"type": "text", "text": "still done"}], "end_turn")])
    assert r.status == "needs_human" and "without submitting" in r.note
    assert "submit_draft" in m.sent[1][-1]["content"]                     # one nudge was sent


def test_budget_guard_stops_before_overspending(data):
    with pytest.raises(A.BudgetExceeded):
        run_agent(data, [([tool_use("sniff_delimiter", {}, i)], "tool_use") for i in range(20)], limit=0.02)


def test_cost_accounting_uses_sonnet_prices():
    s = A.Spend(10)
    cost = s.add(SimpleNamespace(input_tokens=1_000_000, output_tokens=100_000, cache_creation_input_tokens=0,
                                 cache_read_input_tokens=2_000_000))
    assert cost == pytest.approx(2.0 + 1.0 + 0.2)


def test_the_assistant_code_cannot_write_files_or_run_commands():
    for f in (REPO / "src" / "keelstone" / "assistant").glob("*.py"):
        src = f.read_text()
        assert not re.search(r"write_text|write_bytes|open\([^)]*['\"][wa]|subprocess|os\.system|shutil", src), f.name


def test_tool_schemas_are_strict_and_read_only():
    from keelstone.assistant.draft import SUBMIT_SCHEMA
    from keelstone.assistant.tools import TOOL_FUNCTIONS, TOOL_SCHEMAS
    names = {t["name"] for t in TOOL_SCHEMAS}
    assert names == set(TOOL_FUNCTIONS) and not names & {"bash", "write", "shell"}
    for t in TOOL_SCHEMAS + [SUBMIT_SCHEMA]:
        assert t["strict"] and t["input_schema"]["additionalProperties"] is False


# ---- human approval -----------------------------------------------------------------------------------
def test_only_approval_writes_config_and_everything_is_audited(data, tmp_path, monkeypatch):
    sys.path.insert(0, str(REPO / "scripts"))
    import onboard_source as O
    monkeypatch.setattr(O, "DRAFTS", tmp_path / "drafts")
    monkeypatch.setattr(O, "SOURCES", tmp_path / "sources")
    monkeypatch.setattr(O, "AUDIT", tmp_path / "audit.jsonl")
    (tmp_path / "sources").mkdir()
    r, _ = run_agent(data, [([tool_use("submit_draft", GOOD_HARROW, 1)], "tool_use")])
    (tmp_path / "drafts" / "harrow_new").mkdir(parents=True)
    (tmp_path / "drafts" / "harrow_new" / "draft.json").write_text(json.dumps(r.to_json(), default=str))
    args = SimpleNamespace(source_id="harrow_new", by="Tester", override_reason="", replace=False, reason="no")
    O.cmd_show(args)
    assert not list((tmp_path / "sources").iterdir())                 # showing writes nothing
    O.cmd_reject(args)
    assert not list((tmp_path / "sources").iterdir())                 # rejecting writes nothing
    O.cmd_approve(args)
    written = tmp_path / "sources" / "harrow_new.yml"
    assert written.exists() and "approved by Tester" in written.read_text()
    assert validate(__import__("yaml").safe_load(written.read_text()))[1] is None
    actions = [json.loads(l)["action"] for l in (tmp_path / "audit.jsonl").read_text().splitlines()]
    assert actions == ["reject", "approve"]
    with pytest.raises(SystemExit, match="exists"):
        O.cmd_approve(args)


def test_approval_refuses_a_failed_dry_run_without_a_reason(data, tmp_path, monkeypatch):
    sys.path.insert(0, str(REPO / "scripts"))
    import onboard_source as O
    monkeypatch.setattr(O, "DRAFTS", tmp_path / "drafts")
    monkeypatch.setattr(O, "SOURCES", tmp_path / "sources")
    monkeypatch.setattr(O, "AUDIT", tmp_path / "audit.jsonl")
    (tmp_path / "sources").mkdir()
    r, _ = run_agent(data, [([tool_use("submit_draft", {**GOOD_HARROW, "market_value_multiplier": 1000}, 1)], "tool_use")])
    assert r.status == "drafted" and not r.dry_run["passed"]
    (tmp_path / "drafts" / "harrow_new").mkdir(parents=True)
    (tmp_path / "drafts" / "harrow_new" / "draft.json").write_text(json.dumps(r.to_json(), default=str))
    args = SimpleNamespace(source_id="harrow_new", by="Tester", override_reason="", replace=False)
    with pytest.raises(SystemExit, match="dry run failed"):
        O.cmd_approve(args)
    assert not (tmp_path / "sources" / "harrow_new.yml").exists()
