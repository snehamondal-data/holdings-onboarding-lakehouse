"""Plain-Python mirror of silver (mapping, dedupe, resolution, units rule) for local
tests and dry runs. Spark on Databricks is the source of truth; this mirrors the
same config so the mapping can be checked without Spark."""
from __future__ import annotations

import hashlib
import json
from collections import Counter
from datetime import datetime
from decimal import ROUND_HALF_UP, Decimal

from keelstone.config import SourceConfig
from keelstone.ingest.rules import _py_date_format
from keelstone.silver.mapping import UNITS_RATIO_BANDS, UNITS_REASON

AMBIGUOUS = object()


def _num(v) -> float | None:
    if v is None or (isinstance(v, str) and not v.strip()):
        return None
    return float(v)


def _ref(row: dict, ref: str):
    if "[]." in ref:
        arr, fld = ref.split("[].", 1)
        return sum(_num(x.get(fld)) or 0.0 for x in (row.get(arr) or []))
    return row.get(ref)


def _date(src: SourceConfig, col: str, v: str):
    return datetime.strptime(v.strip(), _py_date_format(src.dates[col])).date()


def row_hash(src: SourceConfig, row: dict, source_file: str) -> str:
    payload = json.dumps([row.get(c) for c in src.columns] + [source_file], sort_keys=True, default=str)
    return hashlib.sha256(payload.encode()).hexdigest()


def identifier(src: SourceConfig, row: dict) -> str:
    parts = [(row.get(c) or "").strip().upper() for c in src.silver.identifier_columns]
    return ":".join(parts)


def map_position(src: SourceConfig, row: dict, client_id: str, source_file: str = "") -> dict:
    s, f = src.silver, src.silver.fields
    mv = _num(_ref(row, f["market_value"])) if "market_value" in f else None
    price = _num(_ref(row, f["price"])) if "price" in f else None
    return {
        "client_id": client_id, "source_id": src.source_id, "position_side": s.position_side,
        "account_id": row[f["account_id"]].strip(),
        "business_date": _date(src, f["business_date"], row[f["business_date"]]),
        "quantity": Decimal(str(_num(_ref(row, f["quantity"])))).quantize(Decimal("0.000001")),
        "price": None if price is None else Decimal(str(price)).quantize(Decimal("0.000001")),
        "market_value": None if mv is None else Decimal(str(mv * s.market_value_multiplier)).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP),
        "currency": row[f["currency"]].strip().upper(),
        "identifier_type": s.identifier_type, "identifier": identifier(src, row),
        "header_version": src.header_version_for((row.get("_rescued_data") or {}).keys()),
        "source_file": source_file, "row_hash": row_hash(src, row, source_file),
    }


def units_suspect(pos: dict) -> bool:
    q, p, mv = pos["quantity"], pos["price"], pos["market_value"]
    if p is None or mv is None or q == 0 or p == 0:
        return False
    ratio = abs(float(mv) / (float(q) * float(p)))
    return any(lo <= ratio <= hi for lo, hi in UNITS_RATIO_BANDS)


def identifier_map(master_rows: list[dict]) -> dict:
    seen: dict = {}
    for r in master_rows:
        sid = r["security_id"]
        cands = [("security_id", sid.strip().upper())]
        if (r.get("isin") or "").strip():
            cands.append(("isin", r["isin"].strip().upper()))
        if (r.get("cusip") or "").strip():
            cands.append(("cusip", r["cusip"].strip().upper()))
        if (r.get("ticker") or "").strip() and (r.get("exchange") or "").strip():
            cands.append(("ticker_exchange", f"{r['ticker'].strip().upper()}:{r['exchange'].strip().upper()}"))
        for k in cands:
            seen.setdefault(k, set()).add(sid)
    return {k: (next(iter(v)) if len(v) == 1 else AMBIGUOUS) for k, v in seen.items()}


def silver_outcomes(src: SourceConfig, rows: list[dict], client_id: str, ids: dict,
                    source_file: str = "") -> dict:
    """What silver does with the clean bronze rows of one file: counts per destination."""
    out = {"rows": 0, "unresolved": 0, "deduped": 0, "quarantine": Counter()}
    hashes = Counter(row_hash(src, r, source_file) for r in rows)
    out["deduped"] = sum(n - 1 for n in hashes.values() if src.silver.dedupe == "exact")
    seen = set()
    for r in rows:
        h = row_hash(src, r, source_file)
        if src.silver.dedupe == "exact":
            if h in seen:
                continue
            seen.add(h)
        if not src.silver.is_positions:
            out["rows"] += 1
            continue
        pos = map_position(src, r, client_id, source_file)
        sid = ids.get((pos["identifier_type"], pos["identifier"]))
        if sid is None or sid is AMBIGUOUS:
            out["unresolved"] += 1
        elif units_suspect(pos):
            out["quarantine"][UNITS_REASON] += 1
        else:
            out["rows"] += 1
    out["quarantine"] = dict(out["quarantine"])
    return out
