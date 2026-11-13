"""The true state of the (fictional) world: securities, prices, FX, accounts,
trades and the positions they produce, with tax lots.

Everything comes from one random.Random passed in by the caller, iterated in a
fixed order, so the same seed always gives the same world.
"""
from __future__ import annotations

import math
import random
import string
from dataclasses import dataclass, field
from datetime import date, timedelta

CUSTODIANS = ("harrow", "linden", "pemberton")
ACCOUNT_PREFIX = {"harrow": "CV-HT", "linden": "CV-LB", "pemberton": "CV-PC"}
CURRENCY_COUNTRY = {"USD": "US", "GBP": "GB", "EUR": "DE"}
CURRENCY_EXCHANGES = {"USD": ("XNYS", "XNAS"), "GBP": ("XLON",), "EUR": ("XETR",)}
ASSET_CLASSES = (("equity", 0.60), ("etf", 0.10), ("govt_bond", 0.15), ("corp_bond", 0.15))
DAILY_VOL = {"equity": 0.018, "etf": 0.010, "govt_bond": 0.003, "corp_bond": 0.004}
START_PRICE = {"equity": (10, 400), "etf": (20, 300), "govt_bond": (85, 110), "corp_bond": (80, 115)}
LOT_STEP = {"equity": 10, "etf": 10, "govt_bond": 100, "corp_bond": 100}

# Fictional name parts (no real companies).
_NAME_A = ("Ashgrove", "Brindle", "Cobalt", "Dunmore", "Elderfield", "Fenwick", "Galloway",
           "Hartwell", "Ironbridge", "Juniper", "Kestrel", "Larchmont", "Marrow", "Northcote",
           "Oakhaven", "Pellham", "Quarry", "Redfern", "Stonecroft", "Thistle", "Underhill",
           "Vale", "Westbury", "Yarrow")
_NAME_B = ("Holdings", "Industries", "Energy", "Logistics", "Foods", "Systems", "Minerals",
           "Health", "Telecom", "Materials", "Capital", "Motors", "Utilities", "Retail")
_SUFFIX = {"USD": "Inc", "GBP": "Plc", "EUR": "AG"}


def business_days(start: date, n: int) -> list[date]:
    out, d = [], start
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d)
        d += timedelta(days=1)
    return out


def isin_check_digit(body: str) -> str:
    digits = "".join(str(int(c, 36)) for c in body)
    total = 0
    for i, ch in enumerate(reversed(digits)):
        v = int(ch)
        if i % 2 == 0:
            v *= 2
            if v > 9:
                v -= 9
        total += v
    return str((10 - total % 10) % 10)


def cusip_check_digit(body: str) -> str:
    total = 0
    for i, c in enumerate(body):
        v = int(c) if c.isdigit() else ord(c) - 55
        if i % 2 == 1:
            v *= 2
        total += v // 10 + v % 10
    return str((10 - total % 10) % 10)


@dataclass(frozen=True)
class Security:
    security_id: str
    isin: str
    cusip: str
    ticker: str
    exchange: str
    name: str
    asset_class: str
    currency: str


@dataclass
class Lot:
    lot_id: str
    quantity: int
    acquired: date
    cost_basis: float


@dataclass(frozen=True)
class Trade:
    trade_id: str
    trade_date: date
    account_id: str
    custodian: str
    security_id: str
    side: str
    quantity: int
    price: float
    currency: str


@dataclass
class World:
    days: list[date]
    securities: dict[str, Security]
    prices: list[dict[str, float]]                 # prices[day_index][security_id]
    fx: list[dict[str, float]]                     # fx[day_index][currency] = USD per unit
    accounts: dict[str, str]                       # account_id -> custodian
    trades: list[list[Trade]]                      # trades[day_index]
    # positions[day_index][account_id][security_id] = list of lots (end of day)
    positions: list[dict[str, dict[str, list[Lot]]]] = field(default_factory=list)


def _unique(rng: random.Random, seen: set, make) -> str:
    while True:
        v = make()
        if v not in seen:
            seen.add(v)
            return v


def make_securities(rng: random.Random, n: int) -> dict[str, Security]:
    alnum = string.digits + string.ascii_uppercase
    seen_isin, seen_cusip, seen_ticker = set(), set(), set()
    out = {}
    for i in range(n):
        sid = f"KS{100001 + i:06d}"
        ccy = rng.choices(("USD", "GBP", "EUR"), (0.70, 0.15, 0.15))[0]
        ac = rng.choices([a for a, _ in ASSET_CLASSES], [w for _, w in ASSET_CLASSES])[0]
        country = CURRENCY_COUNTRY[ccy]
        isin = _unique(rng, seen_isin, lambda: (lambda b: b + isin_check_digit(b))(
            country + "".join(rng.choice(alnum) for _ in range(9))))
        cusip = _unique(rng, seen_cusip, lambda: (lambda b: b + cusip_check_digit(b))(
            "".join(rng.choice(alnum) for _ in range(8))))
        prefix = "B" if ac.endswith("bond") else ""
        ticker = _unique(rng, seen_ticker, lambda: prefix + "".join(
            rng.choice(string.ascii_uppercase) for _ in range(rng.choice((3, 4)))))
        exchange = rng.choice(CURRENCY_EXCHANGES[ccy])
        issuer = f"{rng.choice(_NAME_A)} {rng.choice(_NAME_B)}"
        if ac.endswith("bond"):
            name = f"{issuer} {rng.choice((2.5, 3.25, 4.0, 4.75, 5.5))}% {rng.randint(2028, 2045)}"
        elif ac == "etf":
            name = f"{rng.choice(_NAME_A)} {rng.choice(('Global', 'Value', 'Income', 'Growth'))} Index Fund"
        else:
            name = f"{issuer} {_SUFFIX[ccy]}"
        out[sid] = Security(sid, isin, cusip, ticker, exchange, name, ac, ccy)
    return out


def make_prices(rng: random.Random, securities: dict[str, Security], n_days: int) -> list[dict[str, float]]:
    cur = {}
    for sid, s in securities.items():
        lo, hi = START_PRICE[s.asset_class]
        cur[sid] = round(rng.uniform(lo, hi), 4)
    out = []
    for d in range(n_days):
        if d > 0:
            cur = {sid: round(p * math.exp(rng.gauss(0, DAILY_VOL[securities[sid].asset_class])), 4)
                   for sid, p in cur.items()}
        out.append(dict(cur))
    return out


def make_fx(rng: random.Random, n_days: int) -> list[dict[str, float]]:
    cur = {"USD": 1.0, "GBP": 1.27, "EUR": 1.09}
    out = []
    for d in range(n_days):
        if d > 0:
            cur = {c: (1.0 if c == "USD" else round(v * math.exp(rng.gauss(0, 0.004)), 6))
                   for c, v in cur.items()}
        out.append(dict(cur))
    return out


def _lot_qty(rng: random.Random, sec: Security) -> int:
    step = LOT_STEP[sec.asset_class]
    return step * rng.randint(10, 500)


def simulate(rng: random.Random, start: date, n_days: int, n_securities: int,
             accounts_per_custodian: int, holdings_per_account: int) -> World:
    days = business_days(start, n_days)
    securities = make_securities(rng, n_securities)
    prices = make_prices(rng, securities, n_days)
    fx = make_fx(rng, n_days)
    sids = sorted(securities)

    accounts = {}
    for c in CUSTODIANS:
        for k in range(1, accounts_per_custodian + 1):
            accounts[f"{ACCOUNT_PREFIX[c]}-{k:03d}"] = c

    lot_counter = {a: 0 for a in accounts}

    def new_lot(acct: str, sid: str, qty: int, acquired: date, price: float) -> Lot:
        lot_counter[acct] += 1
        return Lot(f"{acct}-L{lot_counter[acct]:05d}", qty, acquired,
                   round(qty * price * rng.uniform(0.7, 1.2), 2))

    book: dict[str, dict[str, list[Lot]]] = {}
    for acct in sorted(accounts):
        held = {}
        for sid in sorted(rng.sample(sids, holdings_per_account)):
            sec = securities[sid]
            lots = []
            for _ in range(rng.randint(1, 3)):
                acquired = start - timedelta(days=rng.randint(30, 1000))
                lots.append(new_lot(acct, sid, _lot_qty(rng, sec), acquired, prices[0][sid]))
            held[sid] = lots
        book[acct] = held

    world = World(days, securities, prices, fx, accounts, trades=[])
    for d, day in enumerate(days):
        todays = []
        seq = 0
        for acct in sorted(accounts):
            held = book[acct]
            for _ in range(rng.randint(1, 3)):
                action = rng.choices(("buy_more", "sell_part", "new", "close"), (0.4, 0.3, 0.15, 0.15))[0]
                if action in ("sell_part", "close") and len(held) <= holdings_per_account * 0.6:
                    action = "new"
                if action == "new" and len(held) >= holdings_per_account * 1.4:
                    action = "buy_more"
                price = prices[d]
                if action == "buy_more":
                    sid = rng.choice(sorted(held))
                    qty = _lot_qty(rng, securities[sid])
                    held[sid].append(new_lot(acct, sid, qty, day, price[sid]))
                    side = "BUY"
                elif action == "new":
                    sid = rng.choice([s for s in sids if s not in held])
                    qty = _lot_qty(rng, securities[sid])
                    held[sid] = [new_lot(acct, sid, qty, day, price[sid])]
                    side = "BUY"
                else:
                    sid = rng.choice(sorted(held))
                    total = sum(l.quantity for l in held[sid])
                    step = LOT_STEP[securities[sid].asset_class]
                    if action == "close" or total <= 2 * step:
                        qty = total
                    else:
                        qty = max(step, int(total * rng.uniform(0.1, 0.5)) // step * step)
                    remaining = qty
                    lots = held[sid]
                    while remaining > 0:                       # FIFO
                        if lots[0].quantity <= remaining:
                            remaining -= lots[0].quantity
                            lots.pop(0)
                        else:
                            lots[0].quantity -= remaining
                            remaining = 0
                    if not lots:
                        del held[sid]
                    side = "SELL"
                seq += 1
                sec = securities[sid]
                todays.append(Trade(f"T{day:%Y%m%d}{seq:04d}", day, acct, accounts[acct], sid, side,
                                    qty, price[sid], sec.currency))
        world.trades.append(todays)
        world.positions.append({a: {s: [Lot(l.lot_id, l.quantity, l.acquired, l.cost_basis) for l in lots]
                                    for s, lots in sorted(h.items())}
                                for a, h in sorted(book.items())})
    return world
