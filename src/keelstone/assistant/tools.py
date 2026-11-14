"""Read-only, typed tools the onboarding assistant may call (slice 7).

Every tool reads only the sample file (held in memory) and the security master rows. None of
them writes anything, runs a shell, or sees any existing source config.
"""
from __future__ import annotations

import csv
import io
import itertools
import json
import math
import re
import statistics
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime

DELIMITERS = [",", ";", "|", "\t"]
# Every order of year, month and day with every common separator (30 patterns), so the candidate
# list is generic rather than a list of the formats our own sources happen to use.
DATE_PATTERNS = [sep.join(order) for order in itertools.permutations(("yyyy", "MM", "dd"))
                 for sep in ("-", "/", ".", "", " ")]
NUMBER_STYLES = {                       # (decimal_separator, thousands_separator)
    "dot_decimal": (".", ""), "dot_decimal_comma_thousands": (".", ","),
    "comma_decimal": (",", ""), "comma_decimal_dot_thousands": (",", "."),
}
_ISIN = re.compile(r"^[A-Z]{2}[A-Z0-9]{9}[0-9]$")
_CUSIP = re.compile(r"^[A-Z0-9]{8}[0-9]$")
_TICKER = re.compile(r"^[A-Z]{1,6}$")
_NUM = re.compile(r"^[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?$")
MAX_LINES = 30


def _py_date(fmt: str) -> str:
    return fmt.replace("yyyy", "%Y").replace("MM", "%m").replace("dd", "%d")


def _to_number(value, dec: str, thou: str):
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    if not isinstance(value, str) or not value.strip():
        return None
    v = value.strip()
    if thou:
        v = v.replace(thou, "")
    if dec != ".":
        if "." in v:                      # a dot left over means this style does not fit
            return None
        v = v.replace(dec, ".")
    return float(v) if _NUM.match(v) else None


@dataclass
class Sample:
    file_name: str
    text: str
    master_rows: list[dict]
    calls: list = field(default_factory=list)       # evidence: every tool call and a short result

    @property
    def lines(self) -> list[str]:
        return self.text.splitlines()

    @property
    def is_json(self) -> bool:
        first = next((l for l in self.lines if l.strip()), "")
        return first.lstrip().startswith("{")

    # ---- parsing helpers -------------------------------------------------------------
    def records(self, delimiter: str | None) -> list[dict]:
        if self.is_json:
            out = []
            for line in self.lines:
                try:
                    obj = json.loads(line)
                    if isinstance(obj, dict):
                        out.append(obj)
                except ValueError:
                    pass
            return out
        if not delimiter:
            raise ValueError("this file is delimited: pass the delimiter")
        reader = csv.reader(io.StringIO(self.text), delimiter=delimiter)
        rows = [r for r in reader if r]
        if not rows:
            return []
        header = rows[0]
        return [dict(zip(header, r)) for r in rows[1:] if len(r) == len(header)]

    def column_values(self, delimiter: str | None, column: str) -> list:
        recs = self.records(delimiter)
        if "[]." in column:
            arr, fld = column.split("[].", 1)
            return [item.get(fld) for r in recs for item in (r.get(arr) or []) if isinstance(item, dict)]
        if recs and column not in recs[0]:
            raise ValueError(f"no column '{column}'; columns are {list(recs[0])}")
        return [r.get(column) for r in recs]

    # ---- the tools -----------------------------------------------------------------------
    def describe_sample(self) -> dict:
        return {"file_name": self.file_name, "bytes": len(self.text.encode()), "lines": len(self.lines),
                "looks_like": "json_lines" if self.is_json else "delimited_text"}

    def read_lines(self, start: int, count: int) -> dict:
        start, count = max(0, start), max(1, min(count, MAX_LINES))
        return {"start": start, "lines": [l[:300] for l in self.lines[start:start + count]],
                "total_lines": len(self.lines)}

    def sniff_delimiter(self) -> dict:
        if self.is_json:
            return {"note": "one object per line: no separator"}
        sample = self.lines[:60]
        out = {}
        for d in DELIMITERS:
            counts = [len(next(csv.reader([l], delimiter=d))) for l in sample if l.strip()]
            mode, freq = Counter(counts).most_common(1)[0]
            out[repr(d)] = {"fields_per_line": mode, "share_of_lines_with_that_count": round(freq / len(counts), 3)}
        best = max(DELIMITERS, key=lambda d: (out[repr(d)]["fields_per_line"] > 1,
                                              out[repr(d)]["share_of_lines_with_that_count"],
                                              out[repr(d)]["fields_per_line"]))
        return {"candidates": out, "best_guess": best}

    def list_columns(self, delimiter: str | None) -> dict:
        recs = self.records(delimiter)
        if not recs:
            return {"columns": [], "rows": 0}
        cols = list(recs[0])
        nested = {}
        for k, v in recs[0].items():
            if isinstance(v, list) and v and isinstance(v[0], dict):
                nested[k] = [f"{k}[].{f}" for f in v[0]]
        return {"columns": cols, "nested_array_fields": nested, "rows": len(recs)}

    def profile_column(self, delimiter: str | None, column: str) -> dict:
        values = self.column_values(delimiter, column)
        present = [v for v in values if v not in (None, "")]
        texts = [str(v).strip() for v in present]
        n = len(values) or 1
        styles = {}
        for name, (dec, thou) in NUMBER_STYLES.items():
            ok = sum(1 for v in present if _to_number(v, dec, thou) is not None)
            styles[name] = round(ok / (len(present) or 1), 3)
        dates = {}
        for p in DATE_PATTERNS:
            ok = 0
            for t in texts[:500]:
                try:
                    datetime.strptime(t, _py_date(p))
                    ok += 1
                except ValueError:
                    pass
            dates[p] = round(ok / (min(len(texts), 500) or 1), 3)
        shapes = {"isin_like": sum(1 for t in texts if _ISIN.match(t.upper())),
                  "cusip_like": sum(1 for t in texts if _CUSIP.match(t.upper())),
                  "ticker_like": sum(1 for t in texts if _TICKER.match(t.upper()))}
        return {"column": column, "rows": len(values), "null_share": round(1 - len(present) / n, 3),
                "distinct": len(set(texts)), "sample_values": texts[:8],
                "number_styles_parse_share": styles, "date_patterns_parse_share": {k: v for k, v in dates.items() if v},
                "identifier_shape_counts": {k: v for k, v in shapes.items() if v}}

    def match_identifiers(self, delimiter: str | None, columns: list[str]) -> dict:
        if not columns or len(columns) > 2:
            raise ValueError("pass one identifier column, or two for ticker and exchange")
        vals = [self.column_values(delimiter, c) for c in columns]
        keys = {"isin": set(), "cusip": set(), "security_id": set(), "ticker_exchange": set()}
        for r in self.master_rows:
            for k in ("isin", "cusip", "security_id"):
                if (r.get(k) or "").strip():
                    keys[k].add(r[k].strip().upper())
            if (r.get("ticker") or "").strip() and (r.get("exchange") or "").strip():
                keys["ticker_exchange"].add(f"{r['ticker'].strip().upper()}:{r['exchange'].strip().upper()}")
        if len(columns) == 1:
            ids = [str(v).strip().upper() for v in vals[0] if v not in (None, "")]
            shares = {k: round(sum(1 for i in ids if i in keys[k]) / (len(ids) or 1), 3)
                      for k in ("isin", "cusip", "security_id")}
        else:
            ids = [f"{str(a).strip().upper()}:{str(b).strip().upper()}" for a, b in zip(*vals) if a and b]
            shares = {"ticker_exchange": round(sum(1 for i in ids if i in keys["ticker_exchange"]) / (len(ids) or 1), 3)}
        return {"columns": columns, "values_checked": len(ids), "share_matching_master": shares}

    def estimate_units(self, delimiter: str | None, quantity_column: str, price_column: str, value_column: str,
                       decimal_separator: str, thousands_separator: str) -> dict:
        recs = self.records(delimiter)
        ratios = []
        for r in recs:
            def get(col):
                if "[]." in col:
                    arr, fld = col.split("[].", 1)
                    nums = [_to_number(x.get(fld), decimal_separator, thousands_separator)
                            for x in (r.get(arr) or []) if isinstance(x, dict)]
                    return sum(nums) if nums and None not in nums else None
                return _to_number(r.get(col), decimal_separator, thousands_separator)
            q, p, v = get(quantity_column), get(price_column), get(value_column)
            if q and p and v is not None:
                ratios.append(v / (q * p))
        if not ratios:
            return {"rows_used": 0, "note": "no rows with quantity, price and value all numeric in that style"}
        med = statistics.median(ratios)
        suggestion = 1.0 / med if med else None
        # nearest power of ten (a generic rule, not a list of the multipliers our sources use)
        nice = 10.0 ** round(math.log10(suggestion)) if suggestion and suggestion > 0 else None
        return {"rows_used": len(ratios), "median_value_over_qty_x_price": round(med, 6),
                "suggested_market_value_multiplier": nice,
                "share_within_1pct_of_median": round(sum(1 for x in ratios if abs(x - med) <= 0.01 * abs(med)) / len(ratios), 3)}

    def call(self, name: str, args: dict) -> dict:
        fn = TOOL_FUNCTIONS[name]
        result = fn(self, **args)
        self.calls.append({"tool": name, "args": args, "result": _short(result)})
        return result


def _short(result: dict) -> str:
    s = json.dumps(result, default=str)
    return s if len(s) <= 400 else s[:400] + "..."


TOOL_FUNCTIONS = {
    "describe_sample": Sample.describe_sample, "read_lines": Sample.read_lines,
    "sniff_delimiter": Sample.sniff_delimiter, "list_columns": Sample.list_columns,
    "profile_column": Sample.profile_column, "match_identifiers": Sample.match_identifiers,
    "estimate_units": Sample.estimate_units,
}

_NULLABLE_DELIM = {"type": ["string", "null"], "description": "the field separator, or null when each line is one object"}


def _tool(name, description, props, required):
    return {"name": name, "description": description, "strict": True,
            "input_schema": {"type": "object", "properties": props, "required": required, "additionalProperties": False}}


TOOL_SCHEMAS = [
    _tool("describe_sample", "Size, line count, and whether the sample looks like one object per line or delimited text.", {}, []),
    _tool("read_lines", f"Read raw lines from the sample (at most {MAX_LINES} per call).",
          {"start": {"type": "integer"}, "count": {"type": "integer"}}, ["start", "count"]),
    _tool("sniff_delimiter", "For each candidate separator, how many fields per line and how consistently.", {}, []),
    _tool("list_columns", "Field names (the header row, or each object's keys), including fields inside arrays of "
          "sub-records, written array[].field.",
          {"delimiter": _NULLABLE_DELIM}, ["delimiter"]),
    _tool("profile_column", "Sample values, share of empty values, which number styles and date patterns parse, "
          "and identifier shapes.",
          {"delimiter": _NULLABLE_DELIM, "column": {"type": "string"}}, ["delimiter", "column"]),
    _tool("match_identifiers", "Share of a field's identifiers found in the security master under each identifier "
          "scheme; for a two-part identifier (symbol and venue) pass both fields.",
          {"delimiter": _NULLABLE_DELIM, "columns": {"type": "array", "items": {"type": "string"}}}, ["delimiter", "columns"]),
    _tool("estimate_units", "Median of value / (amount held x quote) and the multiplier that would bring it to 1. "
          "For an amount summed over sub-records, pass array[].field.",
          {"delimiter": _NULLABLE_DELIM, "quantity_column": {"type": "string"}, "price_column": {"type": "string"},
           "value_column": {"type": "string"}, "decimal_separator": {"type": "string", "enum": [".", ","]},
           "thousands_separator": {"type": "string", "enum": ["", ".", ",", " ", "'"]}},
          ["delimiter", "quantity_column", "price_column", "value_column", "decimal_separator", "thousands_separator"]),
]
