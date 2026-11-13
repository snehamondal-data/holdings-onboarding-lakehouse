"""Reconciliation rules (plain Python, shared by the gold pipeline and local tests).

Each book-vs-custodian comparison gets a status and a reason code:
  matched              quantities agree and values agree within tolerance
  break                quantity_break, value_break, missing_at_custodian, missing_in_book
  pending              pending_late_file: the custodian's file for that date has not arrived
  explained_upstream   the custodian row exists but was held back upstream, so it is already
                       in another queue: custodian_row_rejected (bronze quarantine),
                       custodian_row_quarantined (silver quarantine),
                       custodian_identifier_unresolved (unresolved_securities)
"""
from __future__ import annotations

from keelstone.config import ReconConfig

BREAK_REASONS = ("quantity_break", "value_break", "missing_at_custodian", "missing_in_book")
PENDING_REASONS = ("pending_late_file",)
UPSTREAM_REASONS = ("custodian_row_rejected", "custodian_row_quarantined", "custodian_identifier_unresolved")
STATUS_OF = {**{r: "break" for r in BREAK_REASONS}, **{r: "pending" for r in PENDING_REASONS},
             **{r: "explained_upstream" for r in UPSTREAM_REASONS}}


def status_of(reason: str | None) -> str:
    return "matched" if reason is None else STATUS_OF[reason]


def compare(book_qty: float, cust_qty: float, book_value: float | None, cust_value: float | None,
            tol: ReconConfig) -> str | None:
    """Reason for a key present on both sides, or None if it ties out."""
    if abs(cust_qty - book_qty) > tol.quantity_tolerance:
        return "quantity_break"
    if book_value is not None and cust_value is not None:
        if abs(cust_value - book_value) > tol.value_tolerance(book_value):
            return "value_break"
    return None


def compare_sql(tol: ReconConfig) -> str:
    """The same rule as compare(), over columns book_quantity, custodian_quantity,
    book_market_value, custodian_market_value."""
    return (f"CASE WHEN abs(custodian_quantity - book_quantity) > {tol.quantity_tolerance!r} THEN 'quantity_break' "
            f"WHEN book_market_value IS NOT NULL AND custodian_market_value IS NOT NULL AND "
            f"abs(custodian_market_value - book_market_value) > greatest({tol.value_abs_tolerance!r}, "
            f"{tol.value_rel_tolerance!r} * abs(book_market_value)) THEN 'value_break' END")


def status_sql(reason_col: str = "reason_code") -> str:
    whens = " ".join(f"WHEN {reason_col} = '{r}' THEN '{s}'" for r, s in STATUS_OF.items())
    return f"CASE WHEN {reason_col} IS NULL THEN 'matched' {whens} END"


def tie_out_rate(matched: int, total: int, pending: int) -> float | None:
    """Share of compared positions that tie out; pending ones are not yet comparable."""
    denom = total - pending
    return None if denom <= 0 else matched / denom
