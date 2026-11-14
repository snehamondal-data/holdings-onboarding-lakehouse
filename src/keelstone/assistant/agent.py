"""The onboarding assistant: drafts a new source's YAML config from a sample file (slice 7).

A manual tool loop on Claude (Sonnet 5.5 by default) with read-only tools only. It never writes
config: it returns a DraftResult, and only `scripts/onboard_source.py approve` writes YAML after a
human has seen the draft, the evidence and the dry run.

  status "drafted"       a valid draft (validated by config.py) plus its dry run
  status "needs_human"   no valid draft after one retry, a refusal, or the turn limit; carries
                         what was found so a person can finish it
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field

from keelstone.assistant.draft import SUBMIT_SCHEMA, dry_run, to_config, to_yaml, validate
from keelstone.assistant.tools import TOOL_SCHEMAS, Sample

MODEL = "claude-sonnet-5-5"
# USD per million tokens (Claude Sonnet 5.5): input, output, cache write (1.25x input), cache read
PRICES = {"input": 2.00, "output": 10.00, "cache_write": 2.50, "cache_read": 0.10}
MAX_CALLS = 14

SYSTEM = """You onboard a new data source for an investment-data platform (all data is synthetic).
You get one sample file and read-only tools to inspect it. Work out how the file is laid out and draft
the source's config by calling submit_draft once, when the tool results support every field.

The platform maps every source into one standard holdings model. For each holding: the holder reference,
the business date, the amount held, the per-item quote (if the file has one), the position value (if the
file has one) and the money code, plus an identifier that is looked up in the platform's security master.

How to work: find the layout and the separator, list the fields, profile the fields you need, confirm
which identifier scheme matches the security master, and when the file has both a quote and a position
value, check how the value relates to amount held times quote. Then submit.

Things to get right:
- How numbers are written: which character marks decimals and which, if any, groups thousands. Read the
  values; do not assume the most common convention.
- The date pattern, written with yyyy, MM and dd and the file's own separator. Choose the pattern that
  every sampled value parses with; if more than one does, say so in the evidence.
- If each holding is split into an array of sub-records, map the amount held to the array's field so the
  engine sums it (written array[].field, as list_columns shows).
- The value multiplier: the factor that makes the position value equal amount held times quote.
- Every field, in file order, and which fields hold numbers.
Do not guess when a tool can tell you. Keep the evidence short and cite tool results."""


class BudgetExceeded(RuntimeError):
    pass


@dataclass
class Spend:
    limit_usd: float
    spent_usd: float = 0.0
    calls: int = 0
    tokens: dict = field(default_factory=lambda: {"input": 0, "output": 0, "cache_write": 0, "cache_read": 0})

    def add(self, usage) -> float:
        get = lambda k: int(getattr(usage, k, 0) or 0)  # noqa: E731
        t = {"input": get("input_tokens"), "output": get("output_tokens"),
             "cache_write": get("cache_creation_input_tokens"), "cache_read": get("cache_read_input_tokens")}
        cost = sum(t[k] * PRICES[k] for k in t) / 1e6
        for k in t:
            self.tokens[k] += t[k]
        self.spent_usd += cost
        self.calls += 1
        return cost

    def check(self, projected_next_usd: float) -> None:
        if self.spent_usd + projected_next_usd > self.limit_usd:
            raise BudgetExceeded(f"stopping: ${self.spent_usd:.3f} spent, next call could pass ${self.limit_usd:.2f}")


class AnthropicModel:
    """Claude through the Anthropic SDK (beta endpoint for server-side refusal fallback)."""

    def __init__(self, api_key: str, model: str = MODEL, effort: str = "medium"):
        import anthropic
        self.client = anthropic.Anthropic(api_key=api_key)
        self.model, self.effort = model, effort

    def create(self, system: str, tools: list, messages: list):
        return self.client.beta.messages.create(
            model=self.model, max_tokens=16000, system=system, tools=tools, messages=messages,
            thinking={"type": "adaptive"}, output_config={"effort": self.effort},
            cache_control={"type": "ephemeral"},               # caches the growing prefix each turn
            betas=["server-side-fallback-2026-07-01"], fallbacks="default")


@dataclass
class DraftResult:
    status: str                       # drafted | needs_human
    source_id: str
    config: dict | None = None
    yaml_text: str | None = None
    draft: dict | None = None
    dry_run: dict | None = None
    evidence: list = field(default_factory=list)
    attempts: list = field(default_factory=list)   # each submit_draft and its validation result
    note: str = ""
    spend: dict = field(default_factory=dict)

    def to_json(self) -> dict:
        return {k: getattr(self, k) for k in ("status", "source_id", "config", "yaml_text", "draft", "dry_run",
                                              "evidence", "attempts", "note", "spend")}


KIND_WORDS = {"custodian": "The file comes from a bank that holds the client's assets.",
              "book_of_record": "The file is the client's own record of its holdings."}


def first_message(kind: str) -> str:
    """The only task-specific text the model sees before using its tools: no source id, no display
    name, no file name (its extension would give away the format), no config values."""
    return f"New source to onboard. {KIND_WORDS[kind]} A sample file is loaded; inspect it with the tools, then call submit_draft."


def _block_get(b, k):
    return b.get(k) if isinstance(b, dict) else getattr(b, k, None)


def draft_source(model, sample: Sample, source_id: str, display_name: str, kind: str, landing_subdir: str,
                 spend: Spend, max_calls: int = MAX_CALLS, est_call_usd: float = 0.06) -> DraftResult:
    tools = TOOL_SCHEMAS + [SUBMIT_SCHEMA]
    messages = [{"role": "user", "content": first_message(kind)}]
    result = DraftResult("needs_human", source_id)
    submits, nudged = 0, False
    for _ in range(max_calls):
        spend.check(est_call_usd)
        resp = model.create(SYSTEM, tools, messages)
        spend.add(resp.usage)
        messages.append({"role": "assistant", "content": resp.content})
        if resp.stop_reason == "refusal":
            result.note = "the model declined; needs a human"
            break
        uses = [b for b in resp.content if _block_get(b, "type") == "tool_use"]
        if not uses:
            if nudged:
                result.note = "the model stopped without submitting a draft"
                break
            nudged = True
            messages.append({"role": "user", "content": "Please call submit_draft with your draft now."})
            continue
        results, done = [], False
        for u in uses:
            name, args, uid = _block_get(u, "name"), _block_get(u, "input") or {}, _block_get(u, "id")
            if name == "submit_draft":
                submits += 1
                cfg = to_config(args, source_id, display_name, kind, landing_subdir, sample.text)
                src, err = validate(cfg)
                result.attempts.append({"draft": args, "valid": err is None, "error": err})
                if err is None:
                    result.status, result.config, result.draft = "drafted", cfg, args
                    result.yaml_text = to_yaml(cfg, f"# Drafted by the onboarding assistant ({MODEL}); "
                                                    "approved by a human before use.\n")
                    result.dry_run = dry_run(src, sample.text, sample.master_rows)
                    results.append({"type": "tool_result", "tool_use_id": uid, "content": "Valid. Thank you."})
                    done = True
                elif submits >= 2:
                    result.note = f"draft still invalid after one retry: {err}"
                    results.append({"type": "tool_result", "tool_use_id": uid, "is_error": True, "content": err})
                    done = True
                else:
                    results.append({"type": "tool_result", "tool_use_id": uid, "is_error": True,
                                    "content": f"Invalid draft: {err}. Fix it and submit once more."})
                continue
            try:
                out = sample.call(name, args)
                results.append({"type": "tool_result", "tool_use_id": uid, "content": json.dumps(out, default=str)})
            except Exception as e:  # noqa: BLE001  tool errors go back to the model
                sample.calls.append({"tool": name, "args": args, "result": f"error: {e}"})
                results.append({"type": "tool_result", "tool_use_id": uid, "is_error": True, "content": str(e)})
        messages.append({"role": "user", "content": results})
        if done:
            break
    else:
        result.note = result.note or f"no valid draft within {max_calls} model calls"
    if result.status != "drafted" and not result.note:
        result.note = "no valid draft"
    if result.status == "needs_human":
        result.note = "Needs a human. " + result.note + ". Here is what I found: see evidence."
    result.evidence = sample.calls
    result.spend = {"usd": round(spend.spent_usd, 4), "calls": spend.calls, "tokens": dict(spend.tokens)}
    return result
