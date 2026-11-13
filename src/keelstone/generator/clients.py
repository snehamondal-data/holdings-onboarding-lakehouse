"""Synthetic clients and the extra custodians each one has (generator side only; all fictional).

calder_valley  Calder Valley Teachers' Retirement System: the original three custodians
               (Harrow, Linden, Pemberton; see plants.py and writers.py) plus a fourth,
               Holloway Custody, onboarded in slice 4.
wrexford       Wrexford County Firefighters' Pension Fund, onboarded in slice 4: Linden Bank in a
               different layout (Linden's "global custody" CSV) and Quayside Trust Company, with
               its own book of record. It shares Keelstone's security universe, but every file it
               receives lives under its own landing volume.
"""
from __future__ import annotations

from dataclasses import dataclass

from keelstone.generator.extra import BookSpec, CustodianSpec, Layout

HOLLOWAY = CustodianSpec(
    source_id="holloway", account_prefix="CV-HC", n_accounts=3, subdir="holloway",
    identifier_fields=("isin",),
    layout=Layout(
        columns=(("Depot", "account"), ("Stichtag", "business_date"), ("ISIN", "isin"),
                 ("Bezeichnung", "name"), ("Stueck", "quantity"), ("Kurs", "price"),
                 ("Kurswert", "market_value"), ("Waehrung", "currency")),
        delimiter=";", date_format="%d.%m.%Y", decimal=",", thousands=".",
        file_pattern="holloway_depot_{ymd}.csv"),
)

LINDEN_WREXFORD = CustodianSpec(
    source_id="linden_wrexford", account_prefix="WX-LB", n_accounts=3, subdir="linden",
    identifier_fields=("isin",),
    layout=Layout(
        columns=(("AccountNo", "account"), ("ValuationDate", "business_date"), ("ISIN", "isin"),
                 ("SecurityName", "name"), ("Units", "quantity"), ("ClosePrice", "price"),
                 ("MarketValueLocal", "market_value"), ("Currency", "currency")),
        delimiter=",", date_format="%Y%m%d", file_pattern="linden_gc_{ymd}.csv"),
)

QUAYSIDE = CustodianSpec(
    source_id="quayside", account_prefix="WX-QT", n_accounts=3, subdir="quayside",
    identifier_fields=("cusip",), late_day=17,
    layout=Layout(
        columns=(("portfolio", "account"), ("as_at", "business_date"), ("cusip", "cusip"),
                 ("description", "name"), ("shares", "quantity"), ("px", "price"),
                 ("mv_000", "market_value"), ("ccy", "currency")),
        delimiter="\t", date_format="%m/%d/%Y", value_divisor=1000.0,
        file_pattern="quayside_holdings_{ymd}.tsv"),
)

WREXFORD_BOOK = BookSpec(
    source_id="wrexford_book", subdir="book_of_record",
    layout=Layout(
        columns=(("valuation_date", "business_date"), ("portfolio", "account"), ("custodian_code", "custodian"),
                 ("ks_security_id", "security_id"), ("units", "quantity"), ("ccy", "currency")),
        delimiter="|", date_format="%Y-%m-%d", file_pattern="wrexford_book_{ymd}.txt"),
)


@dataclass(frozen=True)
class ClientSpec:
    client_id: str
    base: bool                    # has the original three custodians and their planted problems
    extras: tuple = ()
    book: BookSpec | None = None  # None: the original book of record writer (positions + trades)


CLIENTS = {
    "calder_valley": ClientSpec("calder_valley", base=True, extras=(HOLLOWAY,)),
    "wrexford": ClientSpec("wrexford", base=False, extras=(LINDEN_WREXFORD, QUAYSIDE), book=WREXFORD_BOOK),
}
