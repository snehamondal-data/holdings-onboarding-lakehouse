"""Scale mode of the generator: the same universe and file layouts, sized by a row target.

Writes the scale_test client's files day by day (nothing kept beyond each account's current
holdings, so tens of millions of rows fit in memory), gzipped like a real bulk feed:
  scale_csv/<date>/scale_csv_<ymd>.csv.gz        Harrow layout (ISIN), one row per position
  scale_lots/<date>/scale_lots_<ymd>.jsonl.gz    Pemberton layout (ticker + exchange), nested tax lots
  book_of_record/<date>/scale_book_<ymd>.csv.gz  book of record, one row per position
  reference/...                                  security master, prices, FX (plain CSV)
The data is clean (no planted problems): the scale runs measure speed, and the existing
validations then check every row is accounted for and every position ties out.
"""
from __future__ import annotations

import gzip
import hashlib
import json
import math
import random
from datetime import date
from pathlib import Path

from keelstone.generator import writers as W
from keelstone.generator.universe import LOT_STEP, simulate

HARROW_HEADER = W.HARROW_HEADER
BOOK_HEADER = ["as_of_date", "account_id", "custodian", "security_id", "quantity", "currency"]


def _gz(text: str) -> bytes:
    return gzip.compress(text.encode("utf-8"), compresslevel=6, mtime=0)


def generate_scale(out_dir: Path | str, rows: int, seed: int = 42, start: date = date(2026, 7, 1),
                   n_days: int = 60, n_securities: int = 2000, holdings: int = 100) -> dict:
    """rows: target for custodian + book rows over the period (each position appears once at its
    custodian and once in the book, every day)."""
    out = Path(out_dir)
    rng = random.Random(seed)
    world = simulate(rng, start, n_days, n_securities, 0, holdings)      # universe only
    sids = sorted(world.securities)
    n_accounts = max(2, math.ceil(rows / (2 * holdings * n_days)))
    accounts = [(f"SC-CSV-{i:05d}", "scale_csv") if i % 2 == 0 else (f"SC-LOT-{i:05d}", "scale_lots")
                for i in range(n_accounts)]
    srng = random.Random(f"{seed}:scale:{rows}")

    def lot_qty(sid):
        return LOT_STEP[world.securities[sid].asset_class] * srng.randint(10, 500)

    held: dict = {}
    for acct, _ in accounts:
        held[acct] = {sid: [lot_qty(sid) for _ in range(srng.randint(1, 3))]
                      for sid in sorted(srng.sample(sids, holdings))}

    files, daily, lots_total = {}, {}, 0

    def emit(rel: str, data: bytes, records: int, source: str, d: date) -> None:
        path = out / "landing" / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        files[rel] = {"source": source, "business_date": d.isoformat(), "delivery_date": d.isoformat(),
                      "sha256": hashlib.sha256(data).hexdigest(), "records": records,
                      "expected_status": "loaded", "expected_bronze_rows": records, "expected_quarantine": {},
                      "expected_drift_columns": [], "duplicate_of": None,
                      "expected_silver": {"rows": records, "unresolved": 0, "deduped": 0, "quarantine": {}}}

    for d, day in enumerate(world.days):
        ymd, iso = day.strftime("%Y%m%d"), day.isoformat()
        prices = world.prices[d]
        if d > 0:                                   # one trade per account per day
            for acct, _ in accounts:
                h = held[acct]
                r = srng.random()
                if r < 0.4:
                    sid = srng.choice(sorted(h))
                    h[sid].append(lot_qty(sid))
                elif r < 0.7 and len(h) > holdings * 0.8:
                    h.pop(srng.choice(sorted(h)))
                else:
                    sid = srng.choice(sids)
                    h.setdefault(sid, []).append(lot_qty(sid))
        csv_lines, lot_lines, book_lines = [",".join(HARROW_HEADER)], [], [",".join(BOOK_HEADER)]
        for acct, cust in accounts:
            for sid, lots in sorted(held[acct].items()):
                sec, p = world.securities[sid], prices[sid]
                qty = sum(lots)
                mv = round(qty * p, 2)
                book_lines.append(f"{iso},{acct},{cust.upper()},{sid},{qty},{sec.currency}")
                if cust == "scale_csv":
                    csv_lines.append(f"{acct},{iso},{sec.isin},{sec.name},{qty},{p:.4f},{mv:.2f},{sec.currency}")
                else:
                    lots_total += len(lots)
                    lot_lines.append(json.dumps({
                        "account": acct, "position_date": iso, "ticker": sec.ticker, "exchange": sec.exchange,
                        "currency": sec.currency, "price": p, "market_value": mv,
                        "lots": [{"lot_id": f"{acct}-{sid}-{k}", "quantity": q, "acquired": iso, "cost_basis": round(q * p, 2)}
                                 for k, q in enumerate(lots, 1)]}, separators=(",", ":")))
        emit(f"scale_csv/{iso}/scale_csv_{ymd}.csv.gz", _gz("\n".join(csv_lines) + "\n"), len(csv_lines) - 1, "scale_csv", day)
        emit(f"scale_lots/{iso}/scale_lots_{ymd}.jsonl.gz", _gz("\n".join(lot_lines) + "\n"), len(lot_lines), "scale_lots", day)
        emit(f"book_of_record/{iso}/scale_book_{ymd}.csv.gz", _gz("\n".join(book_lines) + "\n"), len(book_lines) - 1,
             "scale_book", day)
        for name, source, rendered in (
                (f"reference/prices/{iso}/prices_{ymd}.csv", "prices", W.prices_file(world, d)),
                (f"reference/fx/{iso}/fx_{ymd}.csv", "fx", W.fx_file(world, d))):
            emit(name, rendered[0].encode(), len(rendered[1]), source, day)
        if d == 0:
            text, recs, _ = W.security_master_file(world, {})
            emit(f"reference/security_master/{iso}/security_master_{ymd}.csv", text.encode(), len(recs),
                 "security_master", day)
        daily[iso] = {"matched": len(book_lines) - 1}

    params = {"client": "scale_test", "seed": seed, "rows_target": rows, "n_days": n_days,
              "n_securities": n_securities, "holdings_per_account": holdings, "accounts": n_accounts,
              "rows_custodian_and_book": sum(f["records"] for f in files.values()
                                             if f["source"] in ("scale_csv", "scale_lots", "scale_book")),
              "position_lots": lots_total}
    truth = {"note": "Synthetic ground truth for the scale test (clean data: no planted problems).",
             "params": params, "planted": [], "files": dict(sorted(files.items())), "expected_unresolved": [],
             "expected_recon": {"breaks": [], "explained_upstream": [], "late_files": [], "daily": daily,
                                "non_matched": []}}
    manifest = {"params": params, "files": [{"path": k, **{f: v[f] for f in ("source", "business_date",
                                                                              "delivery_date", "sha256", "records")}}
                                            for k, v in sorted(files.items())]}
    (out / "ground_truth").mkdir(parents=True, exist_ok=True)
    (out / "manifest.json").write_text(json.dumps(manifest, indent=1) + "\n")
    (out / "ground_truth" / "ground_truth.json").write_text(json.dumps(truth) + "\n")
    return truth
