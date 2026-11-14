"""The held-out source's YAML (and any copy of it) is unreachable during an eval run.

Replays the eval's agent loop for each held-out source with a scripted model (no API calls) that
calls every tool, including hostile arguments that try to reach config/sources/<id>.yml, while every
way Python can open or list a file is recorded. Nothing may be opened during the loop; path-shaped
arguments must be refused; tools that do not exist must be rejected; and the only task text the model
gets is first_message(kind)."""
import builtins
import csv
import io
import json
import os
import pathlib
from types import SimpleNamespace

import pytest

from keelstone.assistant import agent as A
from keelstone.assistant.tools import MAX_LINES, Sample
from keelstone.config import load_source

REPO = pathlib.Path(__file__).resolve().parents[1]
CASES = [("harrow", "calder_valley"), ("linden", "calder_valley"), ("pemberton", "calder_valley"),
         ("holloway", "calder_valley"), ("linden_wrexford", "wrexford"), ("quayside", "wrexford"),
         ("book_positions", "calder_valley"), ("wrexford_book", "wrexford")]


@pytest.fixture(scope="module")
def data(tmp_path_factory):
    from keelstone.generator import generate
    out = {}
    for client in ("calder_valley", "wrexford"):
        d = tmp_path_factory.mktemp(client)
        generate(d, client=client)
        out[client] = d
    return out


def load(data, source_id, client) -> Sample:
    """What the eval does before the agent starts: read the sample and the security master."""
    src = load_source(source_id)
    folder = data[client] / "landing" / src.landing_subdir / "2026-07-08"
    path = next(p for p in sorted(folder.iterdir()) if p.match(src.file_glob))
    master = next((data[client] / "landing" / "reference" / "security_master").rglob("*.csv"))
    return Sample(path.name, path.read_text(), list(csv.DictReader(master.open())))


@pytest.fixture()
def file_access(monkeypatch):
    seen = []

    def wrap(mod, name):
        orig = getattr(mod, name)

        def rec(*a, **k):
            seen.append((name, str(a[0]) if a else ""))
            return orig(*a, **k)
        monkeypatch.setattr(mod, name, rec)
    for mod, name in [(builtins, "open"), (io, "open"), (os, "open"), (os, "listdir"), (os, "scandir"),
                      (pathlib.Path, "open"), (pathlib.Path, "read_text"), (pathlib.Path, "read_bytes"),
                      (pathlib.Path, "iterdir"), (pathlib.Path, "glob"), (pathlib.Path, "rglob")]:
        wrap(mod, name)
    return seen


class Recorder:
    """Scripted model: one turn of hostile tool calls, then stops. Records everything it is sent."""

    def __init__(self, calls):
        self.turns = [([{"type": "tool_use", "id": f"t{i}", "name": n, "input": a} for i, (n, a) in enumerate(calls)],
                       "tool_use"),
                      ([{"type": "text", "text": "stop"}], "end_turn"), ([{"type": "text", "text": "stop"}], "end_turn")]
        self.sent = []

    def create(self, system, tools, messages):
        self.sent.append(json.dumps({"system": system, "tools": tools, "messages": messages}, default=str))
        content, stop = self.turns.pop(0)
        return SimpleNamespace(content=content, stop_reason=stop, usage=SimpleNamespace(
            input_tokens=0, output_tokens=0, cache_creation_input_tokens=0, cache_read_input_tokens=0))


@pytest.mark.parametrize("source_id,client", CASES)
def test_held_out_yaml_is_unreachable_during_the_agent_loop(data, file_access, source_id, client):
    real = load_source(source_id)
    sample = load(data, source_id, client)
    file_access.clear()                                   # from here on, the agent loop only
    yaml_rel = f"config/sources/{source_id}.yml"
    yaml_abs = str((REPO / yaml_rel).resolve())
    d = real.options.get("delimiter")
    hostile = [("profile_column", {"delimiter": d, "column": yaml_rel}),
               ("profile_column", {"delimiter": d, "column": yaml_abs}),
               ("match_identifiers", {"delimiter": d, "columns": ["../../" + yaml_rel]}),
               ("estimate_units", {"delimiter": d, "quantity_column": yaml_rel, "price_column": "__class__",
                                   "value_column": "__dict__", "decimal_separator": ".", "thousands_separator": ""}),
               ("read_lines", {"start": 0, "count": 100_000}),
               ("load_source", {"source_id": source_id}), ("open", {"file": yaml_abs}),
               ("list_columns", {"delimiter": d}), ("describe_sample", {}), ("sniff_delimiter", {})]
    model = Recorder(hostile)
    r = A.draft_source(model, sample, source_id, "withheld", real.kind, real.landing_subdir, A.Spend(1.0))

    assert file_access == []                              # nothing at all was opened or listed
    results = {i: c for i, c in enumerate(r.evidence)}
    for i in range(4):                                    # every path-shaped argument was refused
        assert "error" in str(results[i]["result"]) or "rows_used\": 0" in str(results[i]["result"]), results[i]
        assert source_id + ".yml" not in str(results[i]["result"]).replace(yaml_rel, "").replace(yaml_abs, "")
    returned = json.loads(model.sent[1])["messages"][-1]["content"]
    unknown = {t["tool_use_id"]: t for t in returned if t.get("is_error") and t["tool_use_id"] in ("t5", "t6")}
    assert set(unknown) == {"t5", "t6"}                   # load_source / open are not tools
    read = next(t for t in returned if t["tool_use_id"] == "t4")
    assert len(json.loads(read["content"])["lines"]) <= MAX_LINES
    first = json.loads(model.sent[0])["messages"][0]["content"]
    assert first == A.first_message(real.kind)            # no source id, display name or file name
    assert source_id not in first and real.display_name not in first


def test_each_eval_case_starts_from_a_fresh_conversation(data):
    sample = load(data, "harrow", "calder_valley")
    m1, m2 = Recorder([("describe_sample", {})]), Recorder([("describe_sample", {})])
    A.draft_source(m1, sample, "harrow", "withheld", "custodian", "harrow", A.Spend(1.0))
    A.draft_source(m2, sample, "harrow", "withheld", "custodian", "harrow", A.Spend(1.0))
    assert len(json.loads(m2.sent[0])["messages"]) == 1   # nothing carried over from the first run
