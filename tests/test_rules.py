from keelstone.config import load_source
from keelstone.ingest.local_reader import read_rows
from keelstone.ingest.rules import quarantine_reason, quarantine_reason_sql


def test_rules_in_order_first_failure_wins():
    src = load_source("harrow")
    ok = dict(account_id="A", as_of_date="2026-07-01", isin="X", security_name="n",
              quantity="10", price="1.5", market_value="15.00", currency="USD", _corrupt_record=None)
    assert quarantine_reason(src, ok) is None
    assert quarantine_reason(src, {**ok, "_corrupt_record": "x"}) == "malformed_record"
    assert quarantine_reason(src, {**ok, "isin": ""}) == "missing_required:isin"
    assert quarantine_reason(src, {**ok, "quantity": "12O0"}) == "not_numeric:quantity"
    assert quarantine_reason(src, {**ok, "as_of_date": "07/01/2026"}) == "bad_date:as_of_date"
    assert quarantine_reason(src, {**ok, "price": None}) is None   # optional and blank is fine


def test_nested_lot_rule():
    src = load_source("pemberton")
    row = dict(account="A", position_date="2026-07-01", ticker="T", exchange="XNYS",
               lots=[{"quantity": "5"}, {"quantity": "n/a"}], _corrupt_record=None)
    assert quarantine_reason(src, row) == "not_numeric:lots[].quantity"
    assert quarantine_reason(src, {**row, "lots": []}) == "missing_required:lots"


def test_sql_expression_is_built_from_config():
    sql = quarantine_reason_sql(load_source("linden"))
    assert sql.startswith("CASE WHEN `_corrupt_record` IS NOT NULL THEN 'malformed_record'")
    assert "try_to_timestamp(`POS_DATE`, 'dd/MM/yyyy')" in sql
    assert "THEN 'not_numeric:NOMINAL'" in sql
    sql_p = quarantine_reason_sql(load_source("pemberton"))
    assert "exists(`lots`, x -> x.`quantity`" in sql_p


def test_csv_field_count_mismatch_is_malformed():
    src = load_source("harrow")
    text = ("account_id,as_of_date,isin,security_name,quantity,price,market_value,currency\n"
            "A,2026-07-01,X,Name, Class A,10,1.0,10.00,USD\n"
            "A,2026-07-01,X,Name,10,1.0,10.00,USD\n")
    rows = read_rows(src, text)
    assert [quarantine_reason(src, r) for r in rows] == ["malformed_record", None]


def test_drifted_header_is_rescued_and_aliased():
    src = load_source("linden")
    text = ("ACCT|POS_DATE|CUSIP|SEC_DESC|NOMINAL|MKT_PRICE|MARKET_VALUE_THOUSANDS|CCY|SETTLE_STATUS\n"
            "L1|05/08/2026|C1|Desc|10|2.0|0.020|USD|SETTLED\n")
    (row,) = read_rows(src, text)
    assert row["MKT_VALUE_000"] == "0.020" and row["DESCRIPTION"] == "Desc"
    assert sorted(row["_rescued_data"]) == ["MARKET_VALUE_THOUSANDS", "SEC_DESC", "SETTLE_STATUS"]
    assert quarantine_reason(src, row) is None
