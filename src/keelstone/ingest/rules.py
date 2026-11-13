"""Bronze quarantine rules, built from a source's YAML.

Two renderings of the same ordered rule list:
  * quarantine_reason_sql(src): a Spark SQL expression that returns the first
    failing rule's reason code, or NULL when the row is clean (used on Databricks);
  * quarantine_reason(src, row): the same rules in plain Python (used by local
    tests and by the dry-run reader).
Reason codes: malformed_record, missing_required:<col>, not_numeric:<col>, bad_date:<col>.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime

from keelstone.config import SourceConfig

_JAVA_TO_PY_DATE = {"yyyy": "%Y", "MM": "%m", "dd": "%d"}
# Numbers Spark's try_cast(... AS DOUBLE) accepts for our generated data.
_NUMBER = re.compile(r"^\s*[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?\s*$")


@dataclass(frozen=True)
class Rule:
    reason: str
    kind: str          # malformed | required | numeric | date
    column: str | None = None
    nested_field: str | None = None
    date_format: str | None = None


def _is_array(src: SourceConfig, col: str) -> bool:
    return src.column_types.get(col, "string").lower().startswith("array")


def build_rules(src: SourceConfig) -> list[Rule]:
    rules = [Rule("malformed_record", "malformed")]
    for c in src.required:
        rules.append(Rule(f"missing_required:{c}", "required", c))
    for c in src.numeric:
        if "[]." in c:
            arr, fld = c.split("[].", 1)
            rules.append(Rule(f"not_numeric:{c}", "numeric", arr, nested_field=fld))
        else:
            rules.append(Rule(f"not_numeric:{c}", "numeric", c))
    for c, fmt in src.dates.items():
        rules.append(Rule(f"bad_date:{c}", "date", c, date_format=fmt))
    return rules


def _sql_condition(src: SourceConfig, r: Rule) -> str:
    c = f"`{r.column}`" if r.column else None
    if r.kind == "malformed":
        return "`_corrupt_record` IS NOT NULL"
    if r.kind == "required":
        if _is_array(src, r.column):
            return f"({c} IS NULL OR size({c}) = 0)"
        return f"({c} IS NULL OR trim({c}) = '')"
    if r.kind == "numeric":
        if r.nested_field:
            f = f"x.`{r.nested_field}`"
            return f"coalesce(exists({c}, x -> {f} IS NOT NULL AND try_cast({f} AS DOUBLE) IS NULL), false)"
        return f"({c} IS NOT NULL AND trim({c}) <> '' AND try_cast({c} AS DOUBLE) IS NULL)"
    if r.kind == "date":
        return (f"({c} IS NOT NULL AND trim({c}) <> '' "
                f"AND try_to_timestamp({c}, '{r.date_format}') IS NULL)")
    raise ValueError(r.kind)


def quarantine_reason_sql(src: SourceConfig) -> str:
    whens = " ".join(f"WHEN {_sql_condition(src, r)} THEN '{r.reason}'" for r in build_rules(src))
    return f"CASE {whens} ELSE NULL END"


def _blank(v) -> bool:
    return v is None or (isinstance(v, str) and v.strip() == "")


def _is_number(v) -> bool:
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return True
    return isinstance(v, str) and bool(_NUMBER.match(v))


def _py_date_format(fmt: str) -> str:
    out = fmt
    for j, p in _JAVA_TO_PY_DATE.items():
        out = out.replace(j, p)
    return out


def _is_date(v: str, fmt: str) -> bool:
    try:
        datetime.strptime(v.strip(), _py_date_format(fmt))
        return True
    except ValueError:
        return False


def quarantine_reason(src: SourceConfig, row: dict) -> str | None:
    for r in build_rules(src):
        if r.kind == "malformed":
            if row.get("_corrupt_record") is not None:
                return r.reason
        elif r.kind == "required":
            v = row.get(r.column)
            if _is_array(src, r.column):
                if not v:
                    return r.reason
            elif _blank(v):
                return r.reason
        elif r.kind == "numeric":
            v = row.get(r.column)
            if r.nested_field:
                for item in v or []:
                    x = (item or {}).get(r.nested_field)
                    if x is not None and not _is_number(x):
                        return r.reason
            elif not _blank(v) and not _is_number(v):
                return r.reason
        elif r.kind == "date":
            v = row.get(r.column)
            if not _blank(v) and not _is_date(v, r.date_format):
                return r.reason
    return None
