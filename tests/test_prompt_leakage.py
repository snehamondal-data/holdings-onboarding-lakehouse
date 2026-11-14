"""Leakage guard for the onboarding assistant's eval (slice 7).

Fails if any scored value from any source YAML (longer than one character: a date pattern, an
identifier type, a column name, a multiplier, ...) appears in what the model is told before it looks
at the sample: the system prompt, the tool definitions, or the first user message of an eval case.

One exemption, in the tool definitions only: the answer vocabulary of submit_draft's own enums
(the formats and identifier schemes). The model cannot answer without them, and an enum lists every
option, so it says nothing about which one is right. They may not appear in the prompt or the message.
"""
import re
import sys
from pathlib import Path

import pytest

from keelstone.assistant.agent import SYSTEM, first_message
from keelstone.assistant.draft import SUBMIT_SCHEMA
from keelstone.assistant.tools import TOOL_SCHEMAS
from keelstone.config import load_source

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "scripts"))
import eval_assistant as E  # noqa: E402  (its truth() defines what is scored)


def scored_values() -> dict[str, set[str]]:
    """Every scored value of every position source, as strings longer than one character."""
    out: dict[str, set[str]] = {}
    for path in sorted((REPO / "config" / "sources").glob("*.yml")):
        src = load_source(path.stem)
        if not (src.silver and src.silver.is_positions):
            continue
        for field, value in E.truth(src).items():
            for v in (value if isinstance(value, list) else [value]):
                if v is None:
                    continue
                forms = {str(v)}
                if isinstance(v, float):
                    forms |= {f"{v:g}", str(int(v))} if v == int(v) else {f"{v:g}"}
                for f in forms:
                    if len(f) > 1:
                        out.setdefault(f, set()).add(f"{path.stem}.{field}")
    return out


def definition_text() -> str:
    """What the model is told by the tool definitions: names, descriptions, property names and enum
    values. JSON Schema keywords ("type", "description", "properties", ...) are structure, not content."""
    parts = []
    for tool in TOOL_SCHEMAS + [SUBMIT_SCHEMA]:
        parts += [tool["name"], tool["description"]]
        for name, prop in tool["input_schema"]["properties"].items():
            parts += [name, prop.get("description", ""), *map(str, prop.get("enum", []))]
            parts += [str(e) for e in (prop.get("items") or {}).get("enum", [])]
    return "\n".join(parts)


def enum_vocabulary() -> set[str]:
    vocab = set()
    for prop in SUBMIT_SCHEMA["input_schema"]["properties"].values():
        vocab |= {v.lower() for v in prop.get("enum", []) if len(v) > 1}
    return vocab


def leaks(text: str, values: dict[str, set[str]], exempt: set[str] = frozenset()) -> dict[str, set[str]]:
    found = {}
    for v, where in values.items():
        if v.lower() in exempt:
            continue
        if re.fullmatch(r"\w+", v):          # a token: whole word, any case
            hit = re.search(rf"(?<![A-Za-z0-9_]){re.escape(v)}(?![A-Za-z0-9_])", text, re.I)
        else:                                 # punctuation inside (date patterns, ...): substring, any case
            hit = v.lower() in text.lower()
        if hit:
            found[v] = where
    return found


@pytest.fixture(scope="module")
def first_messages():
    """The first user message of every eval case, built exactly as draft_source builds it."""
    return {source_id: first_message(load_source(source_id).kind) for source_id, _ in E.CASES}


def test_there_is_something_to_check():
    values = scored_values()
    assert {"dd/MM/yyyy", "dd.MM.yyyy", "MKT_VALUE_000", "lots[].quantity", "ticker_exchange", "1000"} <= set(values)


def test_system_prompt_contains_no_scored_value():
    assert leaks(SYSTEM, scored_values()) == {}


def test_tool_definitions_contain_no_scored_value_beyond_the_answer_vocabulary():
    text = definition_text()
    assert "list_columns" in text and "submit_draft" in text          # the extraction sees the content
    assert leaks(text, scored_values(), exempt=enum_vocabulary()) == {}


def test_the_exemption_is_only_the_submit_enums():
    assert enum_vocabulary() == {"csv", "json", "isin", "cusip", "ticker_exchange", "security_id"}


def test_first_user_messages_contain_no_scored_value(first_messages):
    found = {sid: leaks(msg, scored_values()) for sid, msg in first_messages.items()}
    assert all(not v for v in found.values()), found


def test_the_detector_catches_planted_leaks():
    values = scored_values()
    planted = ("Dates look like dd/MM/yyyy; quantities are in the NOMINAL field; the multiplier is 1000; "
               "use lots[].quantity; the scheme is CUSIP.")
    assert {"dd/MM/yyyy", "NOMINAL", "1000", "lots[].quantity", "cusip"} <= set(leaks(planted, values))
