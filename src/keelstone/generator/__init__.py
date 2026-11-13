"""Synthetic data generator (fixed seed). All companies and people are fictional.

generate() writes:
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
from keelstone.generator.plants import Planter, PlantSpec
from keelstone.generator.universe import simulate

GENERATOR_VERSION = "1"
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


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def generate(out_dir: Path | str, seed: int = DEFAULTS["seed"], start: date = DEFAULTS["start"],
             n_days: int = DEFAULTS["n_days"], n_securities: int = DEFAULTS["n_securities"],
             accounts_per_custodian: int = DEFAULTS["accounts_per_custodian"],
             holdings_per_account: int = DEFAULTS["holdings_per_account"],
             spec: PlantSpec | None = None) -> dict:
    spec = spec or PlantSpec()
    for name in ("linden_drift_from_day", "linden_units_error_day", "harrow_resend_day", "pemberton_late_day"):
        if not 0 < getattr(spec, name) < n_days - 1:
            raise ValueError(f"{name}={getattr(spec, name)} must fall inside 1..{n_days - 2}")

    rng = random.Random(seed)
    world = simulate(rng, start, n_days, n_securities, accounts_per_custodian, holdings_per_account)
    planter = Planter(rng, world, spec)
    planter.run()

    out = Path(out_dir)
    files: dict[str, dict] = {}

    def emit(source: str, d: int, name: str, rendered: tuple[str, list], delivery: date | None = None,
             drift_columns: list | None = None) -> str:
        text, reasons = rendered
        bdate = world.days[d]
        rel = f"{SUBDIR[source]}/{bdate.isoformat()}/{name}"
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
        }
        return rel

    drift_files = []
    for d, day in enumerate(world.days):
        ymd = day.strftime("%Y%m%d")
        h = emit("harrow", d, f"harrow_positions_{ymd}.csv", W.harrow_file(world, d, planter.views["harrow"][d]))
        if d == spec.harrow_resend_day:
            text = (out / "landing" / h).read_text(encoding="utf-8")
            rel = h.replace(".csv", "_resend.csv")
            (out / "landing" / rel).write_text(text, encoding="utf-8")
            files[rel] = dict(files[h], delivery_date=_next_business_day(day).isoformat(),
                              expected_status="duplicate_content", expected_bronze_rows=0,
                              expected_quarantine={}, duplicate_of=h)
            planter._add("duplicate_file", source="harrow", business_date=day.isoformat(), file=rel,
                         duplicate_of=h, expected_reason="duplicate_content", checked_in_slice=1,
                         details="same bytes re-sent under a new name the next day; must load once")
        drift = d >= spec.linden_drift_from_day
        units = d == spec.linden_units_error_day
        lrel = emit("linden", d, f"linden_holdings_{ymd}.txt",
                    W.linden_file(world, d, planter.views["linden"][d], drift, units),
                    drift_columns=W.LINDEN_DRIFT_COLUMNS if drift else None)
        if drift:
            drift_files.append(lrel)
        if units:
            planter._add("units_error", source="linden", business_date=day.isoformat(), file=lrel,
                         rows=len(planter.views["linden"][d]), scale_factor=1000,
                         expected_reason="units_scale_error", checked_in_slice=2,
                         details="MKT_VALUE_000 holds full values, not thousands; applying the "
                                 "configured x1000 makes them 1000 times too large")
        late = d == spec.pemberton_late_day
        prel = emit("pemberton", d, f"pemberton_positions_{ymd}.jsonl",
                    W.pemberton_file(world, d, planter.views["pemberton"][d]),
                    delivery=_next_business_day(day) if late else None)
        if late:
            planter._add("late_file", source="pemberton", business_date=day.isoformat(), file=prel,
                         delivery_date=_next_business_day(day).isoformat(),
                         expected_reason="pending_late_file", checked_in_slice=3,
                         details="file arrives one business day late; its positions show as "
                                 "pending until then, not broken")
        emit("book_positions", d, f"book_positions_{ymd}.csv", W.book_positions_file(world, d))
        emit("book_trades", d, f"book_trades_{ymd}.csv", W.book_trades_file(world, d))
        if d == 0:
            emit("security_master", d, f"security_master_{ymd}.csv",
                 W.security_master_file(world, planter.master_blanks))
        emit("prices", d, f"prices_{ymd}.csv", W.prices_file(world, d))
        emit("fx", d, f"fx_{ymd}.csv", W.fx_file(world, d))

    planter._add("schema_drift", source="linden",
                 business_date=world.days[spec.linden_drift_from_day].isoformat(), files=drift_files,
                 renamed={"DESCRIPTION": "SEC_DESC", "MKT_VALUE_000": "MARKET_VALUE_THOUSANDS"},
                 added=["SETTLE_STATUS"], expected_drift_columns=W.LINDEN_DRIFT_COLUMNS,
                 expected_reason="schema_drift", checked_in_slice=1,
                 details="header changes mid-period; ingestion keeps loading and logs the drift "
                         "for every affected file")

    # How many custodian rows each unmapped identifier affects (for slice 2).
    for p in planter.planted:
        if p["problem"] == "unmapped_security":
            p["affected_rows"] = sum(1 for day_recs in planter.views[p["source"]]
                                     for r in day_recs
                                     if r["security_id"] == p["security_id"] and not r["corrupt"])

    params = dict(seed=seed, start=start.isoformat(), n_days=n_days, n_securities=n_securities,
                  accounts_per_custodian=accounts_per_custodian,
                  holdings_per_account=holdings_per_account, generator_version=GENERATOR_VERSION,
                  plant_spec=asdict(spec))
    manifest = {"params": params,
                "files": [{"path": k, **{f: v[f] for f in ("source", "business_date", "delivery_date",
                                                          "sha256", "records")}}
                          for k, v in sorted(files.items())]}
    truth = {"note": "Synthetic ground truth. Never loaded into lakehouse tables; read only by "
                     "validation jobs and tests.",
             "params": params, "planted": planter.planted, "files": dict(sorted(files.items()))}
    (out / "ground_truth").mkdir(parents=True, exist_ok=True)
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    (out / "ground_truth" / "ground_truth.json").write_text(json.dumps(truth, indent=2) + "\n",
                                                            encoding="utf-8")
    return truth
