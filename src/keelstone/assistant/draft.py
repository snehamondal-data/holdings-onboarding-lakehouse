"""The draft the assistant submits, how it becomes a source config, and the dry run.

The agent fills submit_draft's schema; everything else here is deterministic code:
  to_config()  turns a draft into exactly the dict config.py's parse_source() validates
               (the nested-lot struct type is read from the sample, not guessed by the model);
  dry_run()    runs the draft through the plain-Python rules engine on the sample and checks
               parse, quarantine, unresolved-identifier and units rates against thresholds.
"""
from __future__ import annotations

import json
import statistics
from collections import Counter

import yaml

from keelstone.config import ConfigError, SourceConfig, parse_source
from keelstone.ingest.local_reader import read_rows
from keelstone.ingest.rules import quarantine_reason
from keelstone.silver.local import AMBIGUOUS, identifier_map, map_position, units_suspect

THRESHOLDS = {"parse_rate_min": 0.98, "quarantine_rate_max": 0.05, "unresolved_rate_max": 0.05,
              "units_suspect_rate_max": 0.01, "units_ratio_band": (0.98, 1.02)}

SUBMIT_SCHEMA = {
    "name": "submit_draft", "strict": True,
    "description": "Submit the drafted source config once the evidence supports every field. The draft is "
                   "validated; if it is invalid you get the errors back and may submit once more.",
    "input_schema": {
        "type": "object", "additionalProperties": False,
        "properties": {
            "format": {"type": "string", "enum": ["csv", "json"]},
            "file_glob": {"type": "string", "description": "e.g. *.csv"},
            "delimiter": {"type": ["string", "null"], "description": "null for JSON Lines"},
            "decimal_separator": {"type": "string", "enum": [".", ","]},
            "thousands_separator": {"type": "string", "enum": ["", ".", ",", " ", "'"]},
            "columns": {"type": "array", "items": {"type": "string"}, "description": "every column/key, in file order"},
            "business_date_column": {"type": "string"},
            "date_format": {"type": "string", "description": "e.g. yyyy-MM-dd, dd/MM/yyyy, yyyyMMdd"},
            "account_column": {"type": "string"},
            "quantity_column": {"type": "string", "description": "a column, or name[].field to sum nested lots"},
            "price_column": {"type": ["string", "null"]},
            "market_value_column": {"type": ["string", "null"]},
            "currency_column": {"type": "string"},
            "identifier_type": {"type": "string", "enum": ["isin", "cusip", "ticker_exchange", "security_id"]},
            "identifier_columns": {"type": "array", "items": {"type": "string"}},
            "market_value_multiplier": {"type": "number"},
            "numeric_columns": {"type": "array", "items": {"type": "string"}},
            "evidence": {"type": "string", "description": "one or two sentences on why, citing tool results"},
        },
        "required": ["format", "file_glob", "delimiter", "decimal_separator", "thousands_separator", "columns",
                     "business_date_column", "date_format", "account_column", "quantity_column", "price_column",
                     "market_value_column", "currency_column", "identifier_type", "identifier_columns",
                     "market_value_multiplier", "numeric_columns", "evidence"],
    },
}


def _lot_struct(sample_text: str, array_col: str) -> str:
    for line in sample_text.splitlines():
        try:
            obj = json.loads(line)
        except ValueError:
            continue
        items = obj.get(array_col) if isinstance(obj, dict) else None
        if isinstance(items, list) and items and isinstance(items[0], dict):
            return "array<struct<" + ",".join(f"{k}:string" for k in items[0]) + ">>"
    return "array<struct<quantity:string>>"


def to_config(draft: dict, source_id: str, display_name: str, kind: str, landing_subdir: str,
              sample_text: str) -> dict:
    """Draft -> the dict a config/sources/<id>.yml would contain."""
    side = "custodian" if kind == "custodian" else "book"
    fields = {"account_id": draft["account_column"], "business_date": draft["business_date_column"],
              "quantity": draft["quantity_column"], "currency": draft["currency_column"]}
    if draft.get("price_column"):
        fields["price"] = draft["price_column"]
    if draft.get("market_value_column"):
        fields["market_value"] = draft["market_value_column"]
    column_types = {}
    for ref in [draft["quantity_column"], *draft.get("numeric_columns", [])]:
        if "[]." in ref:
            arr = ref.split("[].", 1)[0]
            column_types[arr] = _lot_struct(sample_text, arr)
    options = {}
    if draft["format"] == "csv":
        options = {"delimiter": draft["delimiter"], "header": True}
        if draft["decimal_separator"] != ".":
            options["decimal_separator"] = draft["decimal_separator"]
        if draft["thousands_separator"]:
            options["thousands_separator"] = draft["thousands_separator"]
    required = [draft["account_column"], draft["business_date_column"], *draft["identifier_columns"]]
    qty = draft["quantity_column"]
    required.append(qty.split("[].", 1)[0] if "[]." in qty else qty)
    cfg = {
        "source_id": source_id, "display_name": display_name, "kind": kind, "format": draft["format"],
        "landing_subdir": landing_subdir, "file_glob": draft["file_glob"], "options": options,
        "bronze_table": f"bronze_{source_id}", "columns": list(draft["columns"]),
        **({"column_types": column_types} if column_types else {}),
        "rules": {"required": list(dict.fromkeys(required)), "numeric": list(dict.fromkeys(draft["numeric_columns"])),
                  "dates": {draft["business_date_column"]: draft["date_format"]}},
        "silver": {"target": "positions", "position_side": side, "fields": fields,
                   "identifier": {"type": draft["identifier_type"], "columns": list(draft["identifier_columns"])},
                   "market_value_multiplier": draft["market_value_multiplier"], "dedupe": "exact"},
    }
    return cfg


def validate(cfg: dict) -> tuple[SourceConfig | None, str | None]:
    try:
        return parse_source(cfg, where=f"draft {cfg.get('source_id')}"), None
    except (ConfigError, KeyError, TypeError, ValueError) as e:
        return None, str(e)


def to_yaml(cfg: dict, header: str = "") -> str:
    return header + yaml.safe_dump(cfg, sort_keys=False, allow_unicode=True, width=110)


def dry_run(src: SourceConfig, sample_text: str, master_rows: list[dict]) -> dict:
    """Run the draft on the sample with the local rules engine; pass only if every check passes."""
    rows = read_rows(src, sample_text)
    total = len(rows) or 1
    malformed = sum(1 for r in rows if r.get("_corrupt_record") is not None)
    reasons = [quarantine_reason(src, r) for r in rows]
    clean = [r for r, why in zip(rows, reasons) if not why]
    ids = identifier_map(master_rows)
    unresolved, suspect, ratios, mapping_errors = 0, 0, [], []
    for r in clean:
        try:
            pos = map_position(src, r, "dry_run", "sample")
        except Exception as e:  # noqa: BLE001  a wrong mapping shows up here
            mapping_errors.append(f"{type(e).__name__}: {e}")
            continue
        sid = ids.get((pos["identifier_type"], pos["identifier"]))
        if sid is None or sid is AMBIGUOUS:
            unresolved += 1
        if pos["price"] and pos["market_value"] is not None and pos["quantity"]:
            ratios.append(float(pos["market_value"]) / (float(pos["quantity"]) * float(pos["price"])))
            suspect += units_suspect(pos)
    mapped = len(clean) - len(mapping_errors)
    m = {"rows": len(rows), "parse_rate": round(1 - malformed / total, 4),
         "quarantine_rate": round(sum(1 for w in reasons if w) / total, 4),
         "quarantine_reasons": dict(Counter(w for w in reasons if w)),
         "mapping_errors": len(mapping_errors), "mapping_error_example": mapping_errors[:1],
         "unresolved_rate": round(unresolved / (mapped or 1), 4),
         "units_rows": len(ratios), "units_median_ratio": round(statistics.median(ratios), 4) if ratios else None,
         "units_suspect_rate": round(suspect / (len(ratios) or 1), 4)}
    t = THRESHOLDS
    checks = {
        "parse_rate": m["parse_rate"] >= t["parse_rate_min"],
        "quarantine_rate": m["quarantine_rate"] <= t["quarantine_rate_max"],
        "mapping": m["mapping_errors"] == 0 and mapped > 0,
        "unresolved_rate": m["unresolved_rate"] <= t["unresolved_rate_max"],
        "units": (m["units_median_ratio"] is None and "price" not in src.silver.fields)
                 or (m["units_median_ratio"] is not None
                     and t["units_ratio_band"][0] <= m["units_median_ratio"] <= t["units_ratio_band"][1]
                     and m["units_suspect_rate"] <= t["units_suspect_rate_max"]),
    }
    return {"metrics": m, "checks": checks, "passed": all(checks.values()), "thresholds": THRESHOLDS}
