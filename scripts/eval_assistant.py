"""Leave-one-out evaluation of the onboarding assistant (slice 7).

For each existing position source, the agent drafts a config from one sample file without seeing
any config (its tools read only the sample and the security master); the draft is scored field by
field against the real config and run through the dry run. One run, Claude Sonnet 5.5.

  python scripts/eval_assistant.py estimate          # token count and projected cost, no model calls
  python scripts/eval_assistant.py run --budget-usd 1.80

Results: docs/eval/assistant_eval.json and the Markdown table it prints.
"""
import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))
from keelstone.assistant.agent import MODEL, PRICES, SYSTEM, AnthropicModel, BudgetExceeded, Spend, draft_source  # noqa: E402
from keelstone.assistant.draft import SUBMIT_SCHEMA  # noqa: E402
from keelstone.assistant.tools import TOOL_SCHEMAS  # noqa: E402
from keelstone.config import load_source  # noqa: E402
from onboard_source import api_key, audit, load_sample  # noqa: E402

DATE = "2026-07-08"            # an ordinary day: before Linden's header change and away from planted rows
CASES = [("harrow", "calder_valley"), ("linden", "calder_valley"), ("pemberton", "calder_valley"),
         ("holloway", "calder_valley"), ("linden_wrexford", "wrexford"), ("quayside", "wrexford"),
         ("book_positions", "calder_valley"), ("wrexford_book", "wrexford")]
FIELDS = ["format", "delimiter", "decimal_separator", "thousands_separator", "date_format", "identifier_type",
          "identifier_columns", "market_value_multiplier", "account_id", "business_date", "quantity", "price",
          "market_value", "currency", "columns", "numeric_columns"]


def sample_path(source_id: str, client: str) -> Path:
    src = load_source(source_id)
    folder = ROOT / "data" / client / "landing" / src.landing_subdir / DATE
    return next(p for p in sorted(folder.iterdir()) if p.match(src.file_glob))


def truth(src) -> dict:
    f = src.silver.fields
    return {"format": src.format, "delimiter": src.options.get("delimiter") if src.format == "csv" else None,
            "decimal_separator": src.decimal_separator, "thousands_separator": src.thousands_separator,
            "date_format": src.dates[f["business_date"]], "identifier_type": src.silver.identifier_type,
            "identifier_columns": list(src.silver.identifier_columns),
            "market_value_multiplier": float(src.silver.market_value_multiplier),
            "account_id": f["account_id"], "business_date": f["business_date"], "quantity": f["quantity"],
            "price": f.get("price"), "market_value": f.get("market_value"), "currency": f["currency"],
            "columns": list(src.columns), "numeric_columns": sorted(src.numeric)}


def drafted_fields(draft_src) -> dict:
    return truth(draft_src)


def estimate() -> None:
    import anthropic
    client = anthropic.Anthropic(api_key=api_key())
    tools = TOOL_SCHEMAS + [SUBMIT_SCHEMA]
    sample = load_sample(sample_path("harrow", "calder_valley"), "calder_valley")
    # a typical late-turn context: the prompt plus about ten tool results of typical size
    filler = json.dumps(sample.read_lines(0, 30))
    msgs = [{"role": "user", "content": "New source to onboard. Inspect it, then call submit_draft."},
            {"role": "assistant", "content": "Inspecting."}, {"role": "user", "content": filler * 4}]
    base = client.messages.count_tokens(model=MODEL, system=SYSTEM, tools=tools,
                                        messages=[{"role": "user", "content": "x"}]).input_tokens
    late = client.messages.count_tokens(model=MODEL, system=SYSTEM, tools=tools, messages=msgs).input_tokens
    calls, out_per_call = 10, 1500        # assumptions: ~10 model calls per source, ~1,500 output tokens each
    # with prompt caching, each call re-reads the previous prefix from cache and writes only the new part
    avg_ctx = (base + late) / 2
    per_source = (calls * avg_ctx * PRICES["cache_read"] + calls * (late - base) / calls * 2 * PRICES["cache_write"]
                  + base * PRICES["cache_write"] + calls * out_per_call * PRICES["output"]) / 1e6
    no_cache = (calls * avg_ctx * PRICES["input"] + calls * out_per_call * PRICES["output"]) / 1e6
    print(json.dumps({"model": MODEL, "system_plus_tools_tokens": base, "late_turn_context_tokens": late,
                      "assumed_calls_per_source": calls, "assumed_output_tokens_per_call": out_per_call,
                      "per_source_usd_with_caching": round(per_source, 3), "per_source_usd_without_caching": round(no_cache, 3),
                      "eval_8_sources_usd_with_caching": round(8 * per_source, 2),
                      "eval_8_sources_usd_worst_case_no_cache": round(8 * no_cache, 2)}, indent=1))


def run(budget: float) -> None:
    model, spend = AnthropicModel(api_key()), Spend(limit_usd=budget)
    rows = []
    for source_id, client in CASES:
        real = load_source(source_id)
        sample_file = sample_path(source_id, client)
        sample = load_sample(sample_file, client)
        before = spend.spent_usd
        try:
            r = draft_source(model, sample, source_id, real.display_name, real.kind, real.landing_subdir, spend)
        except BudgetExceeded as e:
            print("BUDGET STOP:", e)
            rows.append({"source_id": source_id, "status": "not_run_budget", "spend_usd": 0})
            break
        row = {"source_id": source_id, "client": client, "sample": sample_file.name, "status": r.status,
               "attempts": len(r.attempts), "model_calls_total": spend.calls,
               "spend_usd": round(spend.spent_usd - before, 4), "note": r.note}
        want = truth(real)
        if r.status == "drafted":
            from keelstone.assistant.draft import validate
            got = drafted_fields(validate(r.config)[0])
            row["fields"] = {k: {"ok": got[k] == want[k], "got": got[k], "want": want[k]} for k in FIELDS}
            row["dry_run_passed"] = r.dry_run["passed"]
            row["dry_run"] = r.dry_run["metrics"]
        else:
            row["fields"] = {k: {"ok": False, "got": None, "want": want[k]} for k in FIELDS}
            row["dry_run_passed"] = False
        row["evidence_calls"] = len(r.evidence)
        rows.append(row)
        audit("eval_draft", source_id, f"assistant:{MODEL}", status=r.status, dry_run_passed=row["dry_run_passed"],
              spend_usd=row["spend_usd"], note="leave-one-out eval; nothing written to config/")
        ok = sum(v["ok"] for v in row["fields"].values())
        print(f"{source_id:16} {r.status:11} fields {ok}/{len(FIELDS)} dry_run {row['dry_run_passed']} "
              f"${row['spend_usd']:.3f} (total ${spend.spent_usd:.3f})")
    out = ROOT / "docs" / "eval" / "assistant_eval.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps({"model": MODEL, "sample_date": DATE, "total_spend_usd": round(spend.spent_usd, 4),
                               "tokens": spend.tokens, "model_calls": spend.calls, "rows": rows}, indent=1, default=str))
    print("wrote", out)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("mode", choices=["estimate", "run"])
    p.add_argument("--budget-usd", type=float, default=1.80)
    a = p.parse_args()
    estimate() if a.mode == "estimate" else run(a.budget_usd)
