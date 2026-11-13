"""Synthetic data generator (fixed seed). All companies and people are fictional.

generate(out, client) writes, for one synthetic client:
  <out>/landing/<source subdir>/<business date>/<file>   files exactly as the sources send them
  <out>/manifest.json                                    every file with its delivery date and sha256
  <out>/ground_truth/ground_truth.json                   every planted problem and the expected
                                                         bronze outcome per file (never loaded
                                                         into the lakehouse tables)
"""
from __future__ import annotations

import hashlib
import json
import random
from dataclasses import asdict
from datetime import date, timedelta
from pathlib import Path

from keelstone.generator import writers as W
from keelstone.generator.clients import CLIENTS
from keelstone.generator.extra import ExtraPlanter, add_accounts, render, render_book, stream
from keelstone.generator.plants import IDENTIFIER_FIELD, Planter, PlantSpec
from keelstone.generator.universe import CUSTODIANS, simulate

GENERATOR_VERSION = "2"
DEFAULTS = dict(seed=42, start=date(2026, 7, 1), n_days=60, n_securities=300,
                accounts_per_custodian=4, holdings_per_account=45)

SUBDIR = {
    "harrow": "harrow", "linden": "linden", "pemberton": "pemberton",
    "book_positions": "book_of_record/positions", "book_trades": "book_of_record/trades",
    "security_master": "reference/security_master", "prices": "reference/prices", "fx": "reference/fx",
}


def _next_business_day(d: date) -> date:
    d += timedelta(days=1)
    while d.weekday() >= 5:
        d += timedelta(days=1)
    return d


def _silver_expectation(reasons: list, tags: list) -> dict:
    """What silver should do with each clean bronze row of a file."""
    out = {"rows": 0, "unresolved": 0, "deduped": 0, "quarantine": {}}
    for reason, tag in zip(reasons, tags):
        if reason:                       # quarantined in bronze, never reaches silver
            continue
        if tag is None:
            out["rows"] += 1
        elif tag in ("unresolved", "deduped"):
            out[tag] += 1
        else:
            out["quarantine"][tag] = out["quarantine"].get(tag, 0) + 1
    return out


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


RECON_BREAKS = ("quantity_break", "missing_at_custodian", "missing_in_book", "value_break")
RECON_UPSTREAM = ("custodian_row_rejected", "custodian_identifier_unresolved", "custodian_row_quarantined")


def _unresolved_sids(blanks: dict, id_fields: tuple) -> set:
    """Securities this custodian cannot resolve: any of its identifier fields is blank in the master."""
    return {sid for sid, blanked in blanks.items() if set(blanked) & set(id_fields)}


def _expected_recon(world, views: dict, id_fields: dict, blanks: dict, units: tuple | None,
                    late: list) -> dict:
    """Book of record vs each custodian, key by key, from the true world and the planted views.

    Derived by comparison (not from the plant list), so a test can cross-check the two."""
    non_matched, daily = [], {}
    unresolved = {c: _unresolved_sids(blanks, f) for c, f in id_fields.items()}
    for d, day in enumerate(world.days):
        counts: dict[str, int] = {}
        for c in views:
            book = {(a, sid): sum(l.quantity for l in lots)
                    for a, held in world.positions[d].items() if world.accounts[a] == c
                    for sid, lots in held.items()}
            cust = {(r["account"], r["security_id"]): r for r in views[c][d]}
            for key in sorted(set(book) | set(cust)):
                r = cust.get(key)
                if r is None:
                    reason = "missing_at_custodian"
                elif key not in book:
                    reason = "missing_in_book"
                elif r["corrupt"]:
                    reason = "custodian_row_rejected"
                elif key[1] in unresolved[c]:
                    reason = "custodian_identifier_unresolved"
                elif units and (c, d) == units:
                    reason = "custodian_row_quarantined"
                elif r["quantity"] != book[key]:
                    reason = "quantity_break"
                elif abs(r["price"] - world.prices[d][key[1]]) > 1e-9:
                    reason = "value_break"
                else:
                    reason = None
                counts[reason or "matched"] = counts.get(reason or "matched", 0) + 1
                if reason:
                    non_matched.append({"business_date": day.isoformat(), "custodian": c, "account_id": key[0],
                                        "security_id": key[1], "reason": reason})
        daily[day.isoformat()] = dict(sorted(counts.items()))
    late_files = [{"custodian": c, "business_date": world.days[d].isoformat(),
                   "delivery_date": _next_business_day(world.days[d]).isoformat(),
                   "pending_book_positions": sum(1 for a, held in world.positions[d].items()
                                                 if world.accounts[a] == c for _ in held)}
                  for c, d in late]
    return {"note": "Final state after every file has arrived. Before a late file arrives, the book "
                    "positions of its custodian and date show as pending_late_file instead.",
            "breaks": list(RECON_BREAKS), "explained_upstream": list(RECON_UPSTREAM),
            "late_files": late_files, "daily": daily, "non_matched": non_matched}


def _expected_unresolved(world, views: dict, id_fields: dict, blanks: dict) -> list:
    """Rows each custodian must send to unresolved_securities, per identifier."""
    out = []
    for c, fields in id_fields.items():
        for sid in sorted(_unresolved_sids(blanks, fields)):
            rows = sum(1 for day in views[c] for r in day if r["security_id"] == sid and not r["corrupt"])
            if rows:
                sec = world.securities[sid]
                out.append({"source": c, "security_id": sid,
                            "identifier_type": "ticker_exchange" if len(fields) == 2 else fields[0],
                            "identifier": ":".join(getattr(sec, f) for f in fields), "rows": rows})
    return out


def generate(out_dir: Path | str, client: str = "calder_valley", seed: int = DEFAULTS["seed"],
             start: date = DEFAULTS["start"],
             n_days: int = DEFAULTS["n_days"], n_securities: int = DEFAULTS["n_securities"],
             accounts_per_custodian: int = DEFAULTS["accounts_per_custodian"],
             holdings_per_account: int = DEFAULTS["holdings_per_account"],
             spec: PlantSpec | None = None) -> dict:
    spec = spec or PlantSpec()
    for name in ("linden_drift_from_day", "linden_units_error_day", "harrow_resend_day", "pemberton_late_day"):
        if not 0 < getattr(spec, name) < n_days - 1:
            raise ValueError(f"{name}={getattr(spec, name)} must fall inside 1..{n_days - 2}")

    cs = CLIENTS[client]
    rng = random.Random(seed)
    # The universe (securities, prices, FX) comes first from the main stream, so every client
    # shares it; the original three custodians are simulated only for clients that have them.
    world = simulate(rng, start, n_days, n_securities, accounts_per_custodian if cs.base else 0,
                     holdings_per_account)
    planted: list = []
    blanks: dict = {}
    planter = None
    if cs.base:
        planter = Planter(rng, world, spec)
        planter.run()
        planted, blanks = planter.planted, planter.master_blanks
    # Extra custodians: their own random streams, so they never change the original data.
    for es in cs.extras:
        add_accounts(stream(seed, client, es.source_id), world, es)
    extra_planters = []
    for es in cs.extras:
        ep = ExtraPlanter(stream(seed, client, es.source_id, "plants"), world, es, blanks, planted)
        ep.run()
        extra_planters.append(ep)
    views = {c: planter.views[c] for c in CUSTODIANS} if cs.base else {}
    views.update({ep.spec.source_id: ep.view for ep in extra_planters})
    id_fields = {c: IDENTIFIER_FIELD[c] for c in CUSTODIANS} if cs.base else {}
    id_fields.update({es.source_id: es.identifier_fields for es in cs.extras})
    subdir = dict(SUBDIR)
    subdir.update({es.source_id: es.subdir for es in cs.extras})
    if cs.book:
        subdir[cs.book.source_id] = cs.book.subdir

    def add_plant(problem, **kw):
        e = {"id": f"P{len(planted) + 1:03d}", "problem": problem, **kw}
        planted.append(e)
        return e

    out = Path(out_dir)
    files: dict[str, dict] = {}

    def emit(source: str, d: int, name: str, rendered: tuple[str, list], delivery: date | None = None,
             drift_columns: list | None = None) -> str:
        text, reasons, tags = rendered
        bdate = world.days[d]
        rel = f"{subdir[source]}/{bdate.isoformat()}/{name}"
        path = out / "landing" / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
        q: dict[str, int] = {}
        for r in reasons:
            if r:
                q[r] = q.get(r, 0) + 1
        files[rel] = {
            "source": source, "business_date": bdate.isoformat(),
            "delivery_date": (delivery or bdate).isoformat(), "sha256": _sha(text),
            "records": len(reasons), "expected_status": "loaded",
            "expected_bronze_rows": len(reasons) - sum(q.values()),
            "expected_quarantine": dict(sorted(q.items())),
            "expected_drift_columns": drift_columns or [], "duplicate_of": None,
            "expected_silver": _silver_expectation(reasons, tags),
        }
        return rel

    unresolved = {c: _unresolved_sids(blanks, f) for c, f in id_fields.items()}
    drift_files = []
    late: list = []
    units = None
    for d, day in enumerate(world.days):
        ymd = day.strftime("%Y%m%d")
        if cs.base:
            h = emit("harrow", d, f"harrow_positions_{ymd}.csv",
                     W.harrow_file(world, d, planter.views["harrow"][d], unresolved["harrow"]))
            if d == spec.harrow_resend_day:
                text = (out / "landing" / h).read_text(encoding="utf-8")
                rel = h.replace(".csv", "_resend.csv")
                (out / "landing" / rel).write_text(text, encoding="utf-8")
                files[rel] = dict(files[h], delivery_date=_next_business_day(day).isoformat(),
                                  expected_status="duplicate_content", expected_bronze_rows=0,
                                  expected_quarantine={}, duplicate_of=h,
                                  expected_silver={"rows": 0, "unresolved": 0, "deduped": 0, "quarantine": {}})
                add_plant("duplicate_file", source="harrow", business_date=day.isoformat(), file=rel,
                          duplicate_of=h, expected_reason="duplicate_content", checked_in_slice=1,
                          details="same bytes re-sent under a new name the next day; must load once")
            drift = d >= spec.linden_drift_from_day
            is_units = d == spec.linden_units_error_day
            lrel = emit("linden", d, f"linden_holdings_{ymd}.txt",
                        W.linden_file(world, d, planter.views["linden"][d], drift, is_units, unresolved["linden"]),
                        drift_columns=W.LINDEN_DRIFT_COLUMNS if drift else None)
            if drift:
                drift_files.append(lrel)
            if is_units:
                units = ("linden", d)
                add_plant("units_error", source="linden", business_date=day.isoformat(), file=lrel,
                          rows=len(planter.views["linden"][d]), scale_factor=1000,
                          expected_reason="units_scale_suspect", checked_in_slice=2,
                          details="MKT_VALUE_000 holds full values, not thousands; applying the "
                                  "configured x1000 makes them 1000 times too large")
            is_late = d == spec.pemberton_late_day
            prel = emit("pemberton", d, f"pemberton_positions_{ymd}.jsonl",
                        W.pemberton_file(world, d, planter.views["pemberton"][d], unresolved["pemberton"]),
                        delivery=_next_business_day(day) if is_late else None)
            if is_late:
                late.append(("pemberton", d))
                add_plant("late_file", source="pemberton", business_date=day.isoformat(), file=prel,
                          delivery_date=_next_business_day(day).isoformat(),
                          expected_reason="pending_late_file", checked_in_slice=3,
                          details="file arrives one business day late; its positions show as "
                                  "pending until then, not broken")
        for ep in extra_planters:
            es = ep.spec
            is_late = d == es.late_day
            erel = emit(es.source_id, d, es.layout.file_pattern.format(source=es.source_id, ymd=ymd),
                        render(world, d, ep.view[d], es, unresolved[es.source_id]),
                        delivery=_next_business_day(day) if is_late else None)
            if is_late:
                late.append((es.source_id, d))
                add_plant("late_file", source=es.source_id, business_date=day.isoformat(), file=erel,
                          delivery_date=_next_business_day(day).isoformat(),
                          expected_reason="pending_late_file", checked_in_slice=3,
                          details="file arrives one business day late; its positions show as "
                                  "pending until then, not broken")
        if cs.book:
            emit(cs.book.source_id, d, cs.book.layout.file_pattern.format(source=cs.book.source_id, ymd=ymd),
                 render_book(world, d, cs.book))
        else:
            emit("book_positions", d, f"book_positions_{ymd}.csv", W.book_positions_file(world, d))
            emit("book_trades", d, f"book_trades_{ymd}.csv", W.book_trades_file(world, d))
        if d == 0:
            emit("security_master", d, f"security_master_{ymd}.csv", W.security_master_file(world, blanks))
        emit("prices", d, f"prices_{ymd}.csv", W.prices_file(world, d))
        emit("fx", d, f"fx_{ymd}.csv", W.fx_file(world, d))

    if cs.base:
        add_plant("schema_drift", source="linden",
                  business_date=world.days[spec.linden_drift_from_day].isoformat(), files=drift_files,
                  renamed={"DESCRIPTION": "SEC_DESC", "MKT_VALUE_000": "MARKET_VALUE_THOUSANDS"},
                  added=["SETTLE_STATUS"], expected_drift_columns=W.LINDEN_DRIFT_COLUMNS,
                  expected_reason="schema_drift", checked_in_slice=1,
                  details="header changes mid-period; ingestion keeps loading and logs the drift "
                          "for every affected file")

    expected_unresolved = _expected_unresolved(world, views, id_fields, blanks)
    rows_by = {(e["source"], e["security_id"]): e["rows"] for e in expected_unresolved}
    for p in planted:
        if p["problem"] == "unmapped_security":
            p["affected_rows"] = rows_by.get((p["source"], p["security_id"]), 0)

    params = dict(client=client, seed=seed, start=start.isoformat(), n_days=n_days, n_securities=n_securities,
                  accounts_per_custodian=accounts_per_custodian,
                  holdings_per_account=holdings_per_account, generator_version=GENERATOR_VERSION,
                  plant_spec=asdict(spec) if cs.base else None,
                  extra_custodians=[es.source_id for es in cs.extras])
    manifest = {"params": params,
                "files": [{"path": k, **{f: v[f] for f in ("source", "business_date", "delivery_date",
                                                          "sha256", "records")}}
                          for k, v in sorted(files.items())]}
    truth = {"note": "Synthetic ground truth. Never loaded into lakehouse tables; read only by "
                     "validation jobs and tests.",
             "params": params, "planted": planted, "files": dict(sorted(files.items())),
             "expected_unresolved": expected_unresolved,
             "expected_recon": _expected_recon(world, views, id_fields, blanks, units, late)}
    (out / "ground_truth").mkdir(parents=True, exist_ok=True)
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    (out / "ground_truth" / "ground_truth.json").write_text(json.dumps(truth, indent=2) + "\n",
                                                            encoding="utf-8")
    return truth
