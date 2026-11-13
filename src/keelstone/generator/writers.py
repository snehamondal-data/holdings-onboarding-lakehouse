"""Render each source's daily file in its own format.

Every writer returns (text, row_reasons, silver_tags), one entry per data record:
  row_reasons  the bronze quarantine reason the generator planted, or None if clean;
  silver_tags  what silver should do with a clean row: None (into silver),
               "deduped", "unresolved" or "units_scale_suspect".
The ground truth's expected per-file counts are built from these lists.
"""
from __future__ import annotations

import json

from keelstone.generator.plants import CORRUPT_REASON
from keelstone.generator.universe import World

HARROW_HEADER = ["account_id", "as_of_date", "isin", "security_name", "quantity", "price",
                 "market_value", "currency"]
LINDEN_HEADER = ["ACCT", "POS_DATE", "CUSIP", "DESCRIPTION", "NOMINAL", "MKT_PRICE",
                 "MKT_VALUE_000", "CCY"]
LINDEN_DRIFT_HEADER = ["ACCT", "POS_DATE", "CUSIP", "SEC_DESC", "NOMINAL", "MKT_PRICE",
                       "MARKET_VALUE_THOUSANDS", "CCY", "SETTLE_STATUS"]
LINDEN_DRIFT_COLUMNS = sorted(set(LINDEN_DRIFT_HEADER) - set(LINDEN_HEADER))


def _p(x: float) -> str:
    return f"{x:.4f}"


def _m(x: float) -> str:
    return f"{x:.2f}"


def harrow_file(world: World, d: int, recs: list[dict], unresolved: set = frozenset()) -> tuple:
    day = world.days[d]
    lines, reasons, tags = [",".join(HARROW_HEADER)], [], []
    for r in recs:
        sec = world.securities[r["security_id"]]
        fields = [r["account"], day.isoformat(), sec.isin, sec.name, str(r["quantity"]),
                  _p(r["price"]), _m(r["market_value"]), r["currency"]]
        kind = r["corrupt"]
        if kind == "unquoted_comma_in_name":
            fields[3] = f"{sec.name}, Class A"               # unquoted comma: one field too many
        elif kind == "non_numeric_quantity":
            fields[4] = fields[4].replace("0", "O", 1)        # letter O typed for zero
        elif kind == "empty_identifier":
            fields[2] = ""
        line = ",".join(fields)
        copies = 2 if r["duplicate"] else 1
        for copy in range(copies):
            lines.append(line)
            reasons.append(CORRUPT_REASON[kind] if kind else None)
            tags.append("deduped" if copy else ("unresolved" if r["security_id"] in unresolved else None))
    return "\n".join(lines) + "\n", reasons, tags


def linden_file(world: World, d: int, recs: list[dict], drift: bool, units_error: bool,
                unresolved: set = frozenset()) -> tuple:
    day = world.days[d]
    header = LINDEN_DRIFT_HEADER if drift else LINDEN_HEADER
    lines, reasons, tags = ["|".join(header)], [], []
    for r in recs:
        sec = world.securities[r["security_id"]]
        mv = r["market_value"] if units_error else r["market_value"] / 1000.0
        fields = [r["account"], day.strftime("%d/%m/%Y"), sec.cusip, sec.name, str(r["quantity"]),
                  _p(r["price"]), f"{mv:.3f}" if not units_error else _m(mv), r["currency"]]
        if drift:
            fields.append("SETTLED")
        kind = r["corrupt"]
        if kind == "swapped_day_month":
            fields[1] = day.strftime("%m/%d/%Y")              # US-style date in a DD/MM/YYYY feed
        elif kind == "truncated_row":
            fields = fields[:5]
        elif kind == "non_numeric_nominal":
            fields[4] = "N/A"
        lines.append("|".join(fields))
        reasons.append(CORRUPT_REASON[kind] if kind else None)
        tags.append("unresolved" if r["security_id"] in unresolved
                    else ("units_scale_suspect" if units_error else None))
    return "\n".join(lines) + "\n", reasons, tags


def pemberton_file(world: World, d: int, recs: list[dict], unresolved: set = frozenset()) -> tuple:
    day = world.days[d]
    lines, reasons, tags = [], [], []
    for r in recs:
        sec = world.securities[r["security_id"]]
        lots = [dict(l) for l in r["lots"]]
        kind = r["corrupt"]
        if kind == "non_numeric_lot_quantity":
            lots[0]["quantity"] = "n/a"
        obj = {"account": r["account"], "position_date": day.isoformat(), "ticker": sec.ticker,
               "exchange": sec.exchange, "currency": r["currency"], "price": r["price"],
               "market_value": r["market_value"], "lots": lots}
        line = json.dumps(obj, separators=(",", ":"))
        if kind == "truncated_json":
            line = line[: len(line) // 2]
        lines.append(line)
        reasons.append(CORRUPT_REASON[kind] if kind else None)
        tags.append("unresolved" if r["security_id"] in unresolved else None)
    return "\n".join(lines) + "\n", reasons, tags


def _csv(header: list[str], rows: list[list]) -> tuple:
    lines = [",".join(header)] + [",".join(str(v) for v in row) for row in rows]
    return "\n".join(lines) + "\n", [None] * len(rows), [None] * len(rows)


def book_positions_file(world: World, d: int) -> tuple:
    day = world.days[d].isoformat()
    rows = []
    for acct, held in world.positions[d].items():
        for sid, lots in held.items():
            rows.append([day, acct, world.accounts[acct].upper(), sid,
                         sum(l.quantity for l in lots), world.securities[sid].currency])
    return _csv(["as_of_date", "account_id", "custodian", "security_id", "quantity", "currency"], rows)


def book_trades_file(world: World, d: int) -> tuple:
    rows = [[t.trade_id, t.trade_date.isoformat(), t.account_id, t.custodian.upper(), t.security_id,
             t.side, t.quantity, _p(t.price), t.currency] for t in world.trades[d]]
    return _csv(["trade_id", "trade_date", "account_id", "custodian", "security_id", "side",
                 "quantity", "price", "currency"], rows)


def security_master_file(world: World, blanks: dict[str, tuple[str, ...]]) -> tuple:
    rows = []
    for sid, s in sorted(world.securities.items()):
        blank = blanks.get(sid, ())
        rows.append([sid, "" if "isin" in blank else s.isin, "" if "cusip" in blank else s.cusip,
                     "" if "ticker" in blank else s.ticker, "" if "exchange" in blank else s.exchange,
                     s.name, s.asset_class, s.currency])
    return _csv(["security_id", "isin", "cusip", "ticker", "exchange", "name", "asset_class",
                 "currency"], rows)


def prices_file(world: World, d: int) -> tuple:
    day = world.days[d].isoformat()
    rows = [[day, sid, _p(p), world.securities[sid].currency] for sid, p in sorted(world.prices[d].items())]
    return _csv(["price_date", "security_id", "close_price", "currency"], rows)


def fx_file(world: World, d: int) -> tuple:
    day = world.days[d].isoformat()
    rows = [[day, c, f"{v:.6f}"] for c, v in sorted(world.fx[d].items())]
    return _csv(["rate_date", "currency", "usd_per_unit"], rows)
