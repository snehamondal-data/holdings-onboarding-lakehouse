"""Plant every problem from PROJECT_SPEC section 3 into the custodian views, and
record each one as ground truth.

The custodian "view" starts as an exact copy of the book of record (the truth);
every difference between them is planted here, on purpose, and written down.
"""
from __future__ import annotations

import random
from dataclasses import dataclass, field

from keelstone.generator.universe import CUSTODIANS, LOT_STEP, World


@dataclass(frozen=True)
class PlantSpec:
    """Where the file-level problems go (day indexes) and how many row-level ones."""
    linden_drift_from_day: int = 24       # Linden header changes from this day onward
    linden_units_error_day: int = 39      # Linden sends values in units, not thousands
    harrow_resend_day: int = 9            # Harrow re-sends this day's file, same bytes, new name
    pemberton_late_day: int = 29          # Pemberton file for this day arrives a day late
    unmapped: tuple = (("harrow", 2), ("linden", 2), ("pemberton", 1))
    harrow_duplicate_rows: int = 4
    quantity_breaks: int = 4
    missing_at_custodian: int = 2         # in the book, at no custodian
    missing_in_book: int = 2              # at a custodian, not in the book
    value_breaks: int = 3
    corrupt_rows: dict = field(default_factory=lambda: {
        "harrow": ("unquoted_comma_in_name", "non_numeric_quantity", "empty_identifier"),
        "linden": ("swapped_day_month", "truncated_row", "non_numeric_nominal"),
        "pemberton": ("truncated_json", "non_numeric_lot_quantity"),
    })


CORRUPT_REASON = {
    "unquoted_comma_in_name": "malformed_record",
    "non_numeric_quantity": "not_numeric:quantity",
    "empty_identifier": "missing_required:isin",
    "swapped_day_month": "bad_date:POS_DATE",
    "truncated_row": "malformed_record",
    "non_numeric_nominal": "not_numeric:NOMINAL",
    "truncated_json": "malformed_record",
    "non_numeric_lot_quantity": "not_numeric:lots[].quantity",
}

IDENTIFIER_FIELD = {"harrow": ("isin",), "linden": ("cusip",), "pemberton": ("ticker", "exchange")}


def _record(world: World, d: int, acct: str, sid: str, lots: list[dict], price: float) -> dict:
    qty = sum(l["quantity"] for l in lots)
    return {
        "account": acct, "security_id": sid, "currency": world.securities[sid].currency,
        "quantity": qty, "price": price, "market_value": round(qty * price, 2), "lots": lots,
        "corrupt": None, "duplicate": False,
    }


def build_views(world: World) -> dict[str, list[list[dict]]]:
    """views[custodian][day] = list of position records, sorted by account then security."""
    views = {c: [] for c in CUSTODIANS}
    for d, by_acct in enumerate(world.positions):
        per = {c: [] for c in CUSTODIANS}
        for acct, held in by_acct.items():
            cust = world.accounts[acct]
            for sid, lots in held.items():
                ld = [{"lot_id": l.lot_id, "quantity": l.quantity,
                       "acquired": l.acquired.isoformat(), "cost_basis": l.cost_basis} for l in lots]
                per[cust].append(_record(world, d, acct, sid, ld, world.prices[d][sid]))
        for c in CUSTODIANS:
            views[c].append(per[c])
    return views


class Planter:
    def __init__(self, rng: random.Random, world: World, spec: PlantSpec):
        self.rng, self.world, self.spec = rng, world, spec
        self.views = build_views(world)
        self.master_blanks: dict[str, tuple[str, ...]] = {}     # security_id -> blanked fields
        self.planted: list[dict] = []
        self.used: set = set()                                  # (custodian, day, account, sid)
        self.unmapped_sids: set = set()

    # ---- helpers ---------------------------------------------------------
    def _add(self, problem: str, **kw) -> dict:
        entry = {"id": f"P{len(self.planted) + 1:03d}", "problem": problem, **kw}
        self.planted.append(entry)
        return entry

    def _special_day(self, cust: str) -> set:
        s = self.spec
        return {"harrow": {s.harrow_resend_day}, "linden": {s.linden_units_error_day},
                "pemberton": {s.pemberton_late_day}}[cust]

    def _pick(self, cust: str | None = None, min_day: int = 0, accept=None):
        n_days = len(self.world.days)
        for _ in range(10_000):
            c = cust or self.rng.choice(CUSTODIANS)
            d = self.rng.randrange(min_day, n_days)
            if d in self._special_day(c):
                continue
            recs = self.views[c][d]
            i = self.rng.randrange(len(recs))
            r = recs[i]
            key = (c, d, r["account"], r["security_id"])
            if key in self.used or r["security_id"] in self.unmapped_sids:
                continue
            if accept and not accept(c, d, r):
                continue
            self.used.add(key)
            return c, d, i, r
        raise RuntimeError("could not find a plant target")

    def _where(self, c: str, d: int, r: dict) -> dict:
        return {"source": c, "business_date": self.world.days[d].isoformat(),
                "account_id": r["account"], "security_id": r["security_id"]}

    # ---- the planted problems -------------------------------------------
    def plant_unmapped(self) -> None:
        for cust, n in self.spec.unmapped:
            held = sorted({r["security_id"] for r in self.views[cust][0]} - self.unmapped_sids)
            for sid in self.rng.sample(held, n):
                self.unmapped_sids.add(sid)
                fields = IDENTIFIER_FIELD[cust]
                self.master_blanks[sid] = fields
                sec = self.world.securities[sid]
                ident = " ".join(getattr(sec, f) for f in fields)
                self._add("unmapped_security", source=cust, security_id=sid,
                          identifier_type="+".join(fields), identifier=ident,
                          expected_reason="unmapped_security", checked_in_slice=2,
                          details="identifier removed from the security master; every row of it "
                                  "from this custodian must land in the unresolved queue")

    def plant_corrupt_rows(self) -> None:
        for cust, kinds in self.spec.corrupt_rows.items():
            for kind in kinds:
                accept = None
                if kind == "swapped_day_month":
                    accept = lambda c, d, r: self.world.days[d].day > 12
                c, d, i, r = self._pick(cust, accept=accept)
                r["corrupt"] = kind
                self._add("corrupt_row", **self._where(c, d, r), corruption=kind,
                          expected_reason=CORRUPT_REASON[kind], checked_in_slice=1,
                          details="row must be quarantined in bronze with this reason; the true "
                                  "position is therefore absent from this custodian's clean data")

    def plant_duplicate_rows(self) -> None:
        for _ in range(self.spec.harrow_duplicate_rows):
            c, d, i, r = self._pick("harrow")
            r["duplicate"] = True
            self._add("duplicate_row", **self._where(c, d, r), expected_reason="duplicate_row",
                      checked_in_slice=2, details="identical row appears twice in the file; bronze "
                                                  "keeps both, silver keeps one")

    def plant_quantity_breaks(self) -> None:
        for _ in range(self.spec.quantity_breaks):
            c, d, i, r = self._pick()
            step = LOT_STEP[self.world.securities[r["security_id"]].asset_class]
            delta = max(step, int(r["quantity"] * self.rng.uniform(0.1, 0.3)) // step * step)
            last = r["lots"][-1]
            if c != "pemberton" and self.rng.random() < 0.5 and last["quantity"] > delta:
                delta = -delta
            book_qty = r["quantity"]
            last["quantity"] += delta
            r["quantity"] += delta
            r["market_value"] = round(r["quantity"] * r["price"], 2)
            self._add("quantity_break", **self._where(c, d, r), book_quantity=book_qty,
                      custodian_quantity=r["quantity"], expected_reason="quantity_break",
                      checked_in_slice=3, details="custodian units differ from the book")

    def plant_missing(self) -> None:
        for _ in range(self.spec.missing_at_custodian):
            c, d, i, r = self._pick()
            self.views[c][d].pop(i)
            self._add("missing_position", **self._where(c, d, r), side="missing_at_custodian",
                      book_quantity=r["quantity"], expected_reason="missing_at_custodian",
                      checked_in_slice=3, details="in the book of record, absent from the custodian file")
        for _ in range(self.spec.missing_in_book):
            c = self.rng.choice(CUSTODIANS)
            d = self.rng.choice([x for x in range(len(self.world.days)) if x not in self._special_day(c)])
            recs = self.views[c][d]
            acct = self.rng.choice(sorted({x["account"] for x in recs}))
            held = set(self.world.positions[d][acct])
            candidates = [s for s in sorted(self.world.securities)
                          if s not in held and s not in self.unmapped_sids]
            sid = self.rng.choice(candidates)
            sec = self.world.securities[sid]
            qty = LOT_STEP[sec.asset_class] * self.rng.randint(10, 200)
            lot = {"lot_id": f"{acct}-X{len(self.planted):04d}", "quantity": qty,
                   "acquired": self.world.days[d].isoformat(),
                   "cost_basis": round(qty * self.world.prices[d][sid], 2)}
            rec = _record(self.world, d, acct, sid, [lot], self.world.prices[d][sid])
            recs.append(rec)
            recs.sort(key=lambda x: (x["account"], x["security_id"]))
            self.used.add((c, d, acct, sid))
            self._add("missing_position", **self._where(c, d, rec), side="missing_in_book",
                      custodian_quantity=qty, expected_reason="missing_in_book",
                      checked_in_slice=3, details="held at the custodian, absent from the book of record")

    def plant_value_breaks(self) -> None:
        prices = self.world.prices

        def big_move(c, d, r):
            p, prev = prices[d][r["security_id"]], prices[d - 1][r["security_id"]]
            return abs(p / prev - 1) >= 0.01 and abs(r["quantity"] * (p - prev)) >= 1000

        for _ in range(self.spec.value_breaks):
            c, d, i, r = self._pick(min_day=1, accept=big_move)
            stale = prices[d - 1][r["security_id"]]
            true_mv = r["market_value"]
            r["price"] = stale
            r["market_value"] = round(r["quantity"] * stale, 2)
            self._add("value_break", **self._where(c, d, r), true_price=prices[d][r["security_id"]],
                      custodian_price=stale, true_market_value=true_mv,
                      custodian_market_value=r["market_value"], expected_reason="value_break",
                      checked_in_slice=3, details="custodian valued the position at the previous day's price")

    def run(self) -> None:
        self.plant_unmapped()
        self.plant_corrupt_rows()
        self.plant_duplicate_rows()
        self.plant_quantity_breaks()
        self.plant_missing()
        self.plant_value_breaks()
