"""Generic synthetic custodians, driven by a file-layout description (generator side only).

Used for every custodian added after the original three (Harrow, Linden, Pemberton): a fourth
custodian for Calder Valley and both custodians of the second client. Each one gets its own
random stream (seeded from the client seed and the source id), so adding a custodian never
changes the data of the existing ones.
"""
from __future__ import annotations

import random
from dataclasses import dataclass, field


from keelstone.generator.universe import LOT_STEP, Lot, Trade, World


@dataclass(frozen=True)
class Layout:
    """How a delimited file looks. columns: (header name, field). Fields: account, business_date,
    isin, cusip, ticker, exchange, security_id, name, quantity, price, market_value, currency, custodian."""
    columns: tuple
    delimiter: str = ","
    date_format: str = "%Y-%m-%d"           # strftime pattern
    decimal: str = "."
    thousands: str = ""
    value_divisor: float = 1.0             # 1000 when the bank reports market value in thousands
    file_pattern: str = "{source}_{ymd}.csv"

    def column_for(self, fld: str) -> str:
        return next(c for c, f in self.columns if f == fld)

    def has(self, fld: str) -> bool:
        return any(f == fld for _, f in self.columns)


@dataclass(frozen=True)
class CustodianSpec:
    source_id: str
    account_prefix: str
    n_accounts: int
    layout: Layout
    identifier_fields: tuple                # e.g. ("isin",) or ("ticker", "exchange")
    holdings_per_account: int = 40
    subdir: str = ""
    late_day: int | None = None            # its file for this day index arrives a business day late
    corrupt_rows: tuple = ("unquoted_delimiter_in_name", "non_numeric_quantity", "impossible_date",
                           "empty_identifier")
    unmapped: int = 1
    duplicate_rows: int = 2
    quantity_breaks: int = 1
    value_breaks: int = 1
    missing_at_custodian: int = 1
    missing_in_book: int = 1


@dataclass(frozen=True)
class BookSpec:
    source_id: str
    layout: Layout
    subdir: str


def stream(seed, *parts) -> random.Random:
    return random.Random(":".join(str(p) for p in (seed, *parts)))


def add_accounts(rng: random.Random, world: World, spec: CustodianSpec) -> list[str]:
    """Simulate this custodian's accounts over the same days, securities and prices."""
    sids = sorted(world.securities)
    accts = [f"{spec.account_prefix}-{k:03d}" for k in range(1, spec.n_accounts + 1)]
    counter = {a: 0 for a in accts}

    def new_lot(a, sid, qty, acquired, price):
        counter[a] += 1
        return Lot(f"{a}-L{counter[a]:05d}", qty, acquired, round(qty * price * rng.uniform(0.7, 1.2), 2))

    def lot_qty(sid):
        return LOT_STEP[world.securities[sid].asset_class] * rng.randint(10, 500)

    held = {}
    for a in accts:
        world.accounts[a] = spec.source_id
        held[a] = {sid: [new_lot(a, sid, lot_qty(sid), world.days[0], world.prices[0][sid])]
                   for sid in sorted(rng.sample(sids, spec.holdings_per_account))}
    code = spec.source_id.upper()[:4]
    for d, day in enumerate(world.days):
        seq = 0
        for a in accts:
            h = held[a]
            for _ in range(rng.randint(1, 2)):
                if rng.random() < 0.6 or len(h) < spec.holdings_per_account * 0.6:
                    sid = rng.choice([s for s in sids if s not in h]) if rng.random() < 0.3 else rng.choice(sorted(h))
                    qty = lot_qty(sid)
                    h.setdefault(sid, []).append(new_lot(a, sid, qty, day, world.prices[d][sid]))
                    side = "BUY"
                else:
                    sid = rng.choice(sorted(h))
                    qty = sum(l.quantity for l in h.pop(sid))
                    side = "SELL"
                seq += 1
                world.trades[d].append(Trade(f"T{day:%Y%m%d}{code}{seq:04d}", day, a, spec.source_id, sid, side,
                                             qty, world.prices[d][sid], world.securities[sid].currency))
            world.positions[d][a] = {s: [Lot(l.lot_id, l.quantity, l.acquired, l.cost_basis) for l in lots]
                                     for s, lots in sorted(h.items())}
        world.positions[d] = dict(sorted(world.positions[d].items()))
    return accts


def build_view(world: World, source_id: str) -> list[list[dict]]:
    out = []
    for d, by_acct in enumerate(world.positions):
        recs = []
        for a, held in by_acct.items():
            if world.accounts[a] != source_id:
                continue
            for sid, lots in held.items():
                qty = sum(l.quantity for l in lots)
                p = world.prices[d][sid]
                recs.append({"account": a, "security_id": sid, "currency": world.securities[sid].currency,
                             "quantity": qty, "price": p, "market_value": round(qty * p, 2),
                             "corrupt": None, "duplicate": False})
        out.append(recs)
    return out


def corrupt_reason(spec: CustodianSpec, kind: str) -> str:
    L = spec.layout
    return {"unquoted_delimiter_in_name": "malformed_record",
            "non_numeric_quantity": f"not_numeric:{L.column_for('quantity')}",
            "impossible_date": f"bad_date:{L.column_for('business_date')}",
            "empty_identifier": f"missing_required:{L.column_for(spec.identifier_fields[0])}"}[kind]


@dataclass
class ExtraPlanter:
    rng: random.Random
    world: World
    spec: CustodianSpec
    master_blanks: dict                     # shared with the base planter: security_id -> blanked fields
    planted: list                           # shared plant list (ids continue across custodians)
    view: list = field(default_factory=list)
    used: set = field(default_factory=set)
    unmapped_sids: set = field(default_factory=set)

    def __post_init__(self):
        self.view = build_view(self.world, self.spec.source_id)

    def _add(self, problem: str, **kw) -> dict:
        e = {"id": f"P{len(self.planted) + 1:03d}", "problem": problem, **kw}
        self.planted.append(e)
        return e

    def _where(self, d: int, r: dict) -> dict:
        return {"source": self.spec.source_id, "business_date": self.world.days[d].isoformat(),
                "account_id": r["account"], "security_id": r["security_id"]}

    def _pick(self, min_day: int = 0, accept=None):
        for _ in range(10_000):
            d = self.rng.randrange(min_day, len(self.world.days))
            if d == self.spec.late_day or not self.view[d]:
                continue
            i = self.rng.randrange(len(self.view[d]))
            r = self.view[d][i]
            key = (d, r["account"], r["security_id"])
            if key in self.used or r["security_id"] in self.unmapped_sids or r["security_id"] in self.master_blanks:
                continue
            if accept and not accept(d, r):
                continue
            self.used.add(key)
            return d, i, r
        raise RuntimeError("could not find a plant target")

    def run(self) -> None:
        s, w = self.spec, self.world
        # Unmapped: a security only this custodian holds, so no other custodian is affected.
        others = {sid for a, held in ((a, h) for pos in w.positions for a, h in pos.items())
                  if w.accounts[a] != s.source_id for sid in held}
        mine = sorted({r["security_id"] for day in self.view for r in day} - others - set(self.master_blanks))
        for sid in self.rng.sample(mine, s.unmapped):
            self.unmapped_sids.add(sid)
            self.master_blanks[sid] = s.identifier_fields
            sec = w.securities[sid]
            self._add("unmapped_security", source=s.source_id, security_id=sid,
                      identifier_type="ticker_exchange" if len(s.identifier_fields) == 2 else s.identifier_fields[0],
                      identifier=":".join(getattr(sec, f) for f in s.identifier_fields),
                      expected_reason="unmapped_identifier", checked_in_slice=2,
                      details="identifier removed from the security master")
        for kind in s.corrupt_rows:
            d, i, r = self._pick()
            r["corrupt"] = kind
            self._add("corrupt_row", **self._where(d, r), corruption=kind,
                      expected_reason=corrupt_reason(s, kind), checked_in_slice=1,
                      details="row must be quarantined in bronze with this reason")
        for _ in range(s.duplicate_rows):
            d, i, r = self._pick()
            r["duplicate"] = True
            self._add("duplicate_row", **self._where(d, r), expected_reason="duplicate_row", checked_in_slice=2,
                      details="identical row appears twice in the file; silver keeps one")
        for _ in range(s.quantity_breaks):
            d, i, r = self._pick()
            step = LOT_STEP[w.securities[r["security_id"]].asset_class]
            book_qty = r["quantity"]
            r["quantity"] += max(step, int(r["quantity"] * self.rng.uniform(0.1, 0.3)) // step * step)
            r["market_value"] = round(r["quantity"] * r["price"], 2)
            self._add("quantity_break", **self._where(d, r), book_quantity=book_qty,
                      custodian_quantity=r["quantity"], expected_reason="quantity_break", checked_in_slice=3,
                      details="custodian units differ from the book")
        for _ in range(s.missing_at_custodian):
            d, i, r = self._pick()
            self.view[d].pop(i)
            self._add("missing_position", **self._where(d, r), side="missing_at_custodian",
                      book_quantity=r["quantity"], expected_reason="missing_at_custodian", checked_in_slice=3,
                      details="in the book of record, absent from the custodian file")
        for _ in range(s.missing_in_book):
            d = self.rng.choice([x for x in range(len(w.days)) if x != s.late_day and self.view[x]])
            acct = self.rng.choice(sorted({x["account"] for x in self.view[d]}))
            sid = self.rng.choice([x for x in sorted(w.securities) if x not in w.positions[d][acct]
                                   and x not in self.master_blanks])
            qty = LOT_STEP[w.securities[sid].asset_class] * self.rng.randint(10, 200)
            p = w.prices[d][sid]
            rec = {"account": acct, "security_id": sid, "currency": w.securities[sid].currency, "quantity": qty,
                   "price": p, "market_value": round(qty * p, 2), "corrupt": None, "duplicate": False}
            self.view[d].append(rec)
            self.view[d].sort(key=lambda x: (x["account"], x["security_id"]))
            self.used.add((d, acct, sid))
            self._add("missing_position", **self._where(d, rec), side="missing_in_book", custodian_quantity=qty,
                      expected_reason="missing_in_book", checked_in_slice=3,
                      details="held at the custodian, absent from the book of record")

        def big_move(d, r):
            p, prev = w.prices[d][r["security_id"]], w.prices[d - 1][r["security_id"]]
            return abs(p / prev - 1) >= 0.01 and abs(r["quantity"] * (p - prev)) >= 1000

        for _ in range(s.value_breaks):
            d, i, r = self._pick(min_day=1, accept=big_move)
            true_mv = r["market_value"]
            r["price"] = w.prices[d - 1][r["security_id"]]
            r["market_value"] = round(r["quantity"] * r["price"], 2)
            self._add("value_break", **self._where(d, r), true_price=w.prices[d][r["security_id"]],
                      custodian_price=r["price"], true_market_value=true_mv,
                      custodian_market_value=r["market_value"], expected_reason="value_break", checked_in_slice=3,
                      details="custodian valued the position at the previous day's price")


def _num(x: float, decimals: int, L: Layout) -> str:
    s = f"{x:,.{decimals}f}"                       # 1,234.50
    return s.replace(",", "\0").replace(".", L.decimal).replace("\0", L.thousands)


def render(world: World, d: int, recs: list[dict], spec: CustodianSpec, unresolved: set) -> tuple:
    """Delimited custodian file; returns (text, bronze reasons, silver tags) per record."""
    L, day = spec.layout, world.days[d]
    lines, reasons, tags = [L.delimiter.join(c for c, _ in L.columns)], [], []
    for r in recs:
        sec = world.securities[r["security_id"]]
        vals = {"account": r["account"], "business_date": day.strftime(L.date_format), "isin": sec.isin,
                "cusip": sec.cusip, "ticker": sec.ticker, "exchange": sec.exchange, "security_id": sec.security_id,
                "name": sec.name, "quantity": _num(r["quantity"], 0, L), "price": _num(r["price"], 4, L),
                "market_value": _num(r["market_value"] / L.value_divisor, 3 if L.value_divisor != 1 else 2, L),
                "currency": r["currency"], "custodian": spec.source_id.upper()}
        kind = r["corrupt"]
        if kind == "unquoted_delimiter_in_name":
            vals["name"] = f"{sec.name}{L.delimiter} Class A"
        elif kind == "non_numeric_quantity":
            vals["quantity"] = vals["quantity"].replace("0", "O", 1) if "0" in vals["quantity"] else vals["quantity"] + "O"
        elif kind == "impossible_date":
            vals["business_date"] = L.date_format.replace("%d", "31").replace("%m", "02").replace("%Y", str(day.year))
        elif kind == "empty_identifier":
            vals[spec.identifier_fields[0]] = ""
        line = L.delimiter.join(vals[f] for _, f in L.columns)
        for copy in range(2 if r["duplicate"] else 1):
            lines.append(line)
            reasons.append(corrupt_reason(spec, kind) if kind else None)
            tags.append("deduped" if copy else ("unresolved" if r["security_id"] in unresolved else None))
    return "\n".join(lines) + "\n", reasons, tags


def render_book(world: World, d: int, book: BookSpec) -> tuple:
    L, day = book.layout, world.days[d]
    lines = [L.delimiter.join(c for c, _ in L.columns)]
    for a, held in world.positions[d].items():
        for sid, lots in held.items():
            vals = {"account": a, "business_date": day.strftime(L.date_format), "security_id": sid,
                    "custodian": world.accounts[a].upper(), "quantity": _num(sum(l.quantity for l in lots), 0, L),
                    "currency": world.securities[sid].currency}
            lines.append(L.delimiter.join(vals[f] for _, f in L.columns))
    n = len(lines) - 1
    return "\n".join(lines) + "\n", [None] * n, [None] * n


