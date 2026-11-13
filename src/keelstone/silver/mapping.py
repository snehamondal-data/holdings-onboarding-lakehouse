"""Config -> Spark SQL expressions for silver (plain Python strings, testable locally).

Every source's YAML `silver:` section maps its bronze columns into either the one
canonical position model or a typed reference table. Nothing here names a source.

Canonical position model (silver_positions):
  client_id, source_id, position_side, account_id, security_id, business_date,
  quantity, price, market_value (local currency, after the units multiplier), currency,
  identifier_type, identifier, header_version, source_file, row_hash, load_ts
Natural key: client_id, source_id, account_id, security_id, business_date
"""
from __future__ import annotations

from keelstone.config import SourceConfig
from keelstone.ingest.rules import number_sql

POSITION_KEY = ("client_id", "source_id", "account_id", "security_id", "business_date")
UNRESOLVED_KEY = ("client_id", "source_id", "account_id", "business_date", "identifier_type", "identifier")
QUARANTINE_KEY = ("row_hash",)

# A row is "about 1000x off" when market value / (quantity x price) falls in one of these bands.
UNITS_RATIO_BANDS = ((500.0, 2000.0), (0.0005, 0.002))
UNITS_REASON = "units_scale_suspect"

QTY_TYPE = "DECIMAL(38,6)"
PRICE_TYPE = "DECIMAL(38,6)"
VALUE_TYPE = "DECIMAL(38,2)"


def q(col: str) -> str:
    return f"`{col}`"


def _lit(v: str) -> str:
    return "'" + str(v).replace("'", "\\'") + "'"


def column_ref(ref: str, src: SourceConfig | None = None) -> str:
    """A bronze column, or the sum of a field over a nested array ("lots[].quantity").
    With src, numbers are first rewritten from the source's decimal/thousands separators."""
    num = (lambda e: number_sql(src, e)) if src is not None else (lambda e: e)  # noqa: E731
    if "[]." in ref:
        arr, fld = ref.split("[].", 1)
        return (f"aggregate({q(arr)}, CAST(0 AS DOUBLE), "
                f"(acc, x) -> acc + coalesce(CAST({num('x.' + q(fld))} AS DOUBLE), 0D))")
    return num(q(ref))


def date_expr(src: SourceConfig, column: str) -> str:
    return f"to_date(try_to_timestamp({q(column)}, {_lit(src.dates[column])}))"


def row_hash_expr(src: SourceConfig) -> str:
    """Identical bronze rows in the same file hash the same (exact duplicates)."""
    cols = ", ".join(q(c) for c in src.columns)
    return f"sha2(to_json(struct({cols}, `_source_file`)), 256)"


def header_version_expr(src: SourceConfig) -> str:
    if not src.header_versions:
        return "CAST(NULL AS INT)"
    keys = "coalesce(json_object_keys(`_rescued_data`), array())"
    whens = []
    for hv in sorted(src.header_versions, key=lambda h: -h.version):
        if hv.renames:
            cond = " AND ".join(f"array_contains({keys}, {_lit(k)})" for k in sorted(hv.renames))
            whens.append(f"WHEN {cond} THEN {hv.version}")
    base = src.header_versions[0].version
    return f"CASE {' '.join(whens)} ELSE {base} END" if whens else str(base)


def identifier_expr(src: SourceConfig) -> str:
    cols = [f"upper(trim({q(c)}))" for c in src.silver.identifier_columns]
    return cols[0] if len(cols) == 1 else f"concat_ws(':', {', '.join(cols)})"


def position_select(src: SourceConfig, client_id: str) -> list[tuple[str, str]]:
    """(alias, sql) pairs that turn one bronze row into one canonical position row."""
    s = src.silver
    f = s.fields
    mult = s.market_value_multiplier
    mv = (f"CAST(CAST({column_ref(f['market_value'], src)} AS DOUBLE) * {mult!r} AS {VALUE_TYPE})"
          if "market_value" in f else f"CAST(NULL AS {VALUE_TYPE})")
    price = (f"CAST({column_ref(f['price'], src)} AS {PRICE_TYPE})" if "price" in f
             else f"CAST(NULL AS {PRICE_TYPE})")
    return [
        ("client_id", _lit(client_id)),
        ("source_id", _lit(src.source_id)),
        ("position_side", _lit(s.position_side)),
        ("account_id", f"trim({q(f['account_id'])})"),
        ("business_date", date_expr(src, f["business_date"])),
        ("quantity", f"CAST({column_ref(f['quantity'], src)} AS {QTY_TYPE})"),
        ("price", price),
        ("market_value", mv),
        ("currency", f"upper(trim({q(f['currency'])}))"),
        ("identifier_type", _lit(s.identifier_type)),
        ("identifier", identifier_expr(src)),
        ("header_version", header_version_expr(src)),
        ("source_file", "`_source_file`"),
        ("row_hash", row_hash_expr(src)),
        ("load_ts", "`_load_ts`"),
    ]


def reference_select(src: SourceConfig, client_id: str) -> list[tuple[str, str]]:
    out = [("client_id", _lit(client_id)), ("source_id", _lit(src.source_id))]
    for col, typ in src.silver.fields.items():
        if typ == "date":
            out.append((col, date_expr(src, col)))
        elif typ == "string":
            out.append((col, f"nullif(trim({q(col)}), '')"))
        else:
            out.append((col, f"CAST({number_sql(src, q(col))} AS {typ.upper()})"))
    out += [("source_file", "`_source_file`"), ("row_hash", row_hash_expr(src)), ("load_ts", "`_load_ts`")]
    return out


def units_suspect_expr() -> str:
    ratio = ("CAST(market_value AS DOUBLE) / (CAST(quantity AS DOUBLE) * CAST(price AS DOUBLE))")
    bands = " OR ".join(f"abs({ratio}) BETWEEN {lo!r} AND {hi!r}" for lo, hi in UNITS_RATIO_BANDS)
    return (f"coalesce(price IS NOT NULL AND market_value IS NOT NULL AND quantity <> 0 "
            f"AND price <> 0 AND ({bands}), false)")


IDENTIFIER_MAP_SQL = """
WITH ids AS (
  SELECT 'isin' AS identifier_type, upper(trim(isin)) AS identifier, security_id FROM {master} WHERE nullif(trim(isin), '') IS NOT NULL
  UNION ALL
  SELECT 'cusip', upper(trim(cusip)), security_id FROM {master} WHERE nullif(trim(cusip), '') IS NOT NULL
  UNION ALL
  SELECT 'ticker_exchange', concat_ws(':', upper(trim(ticker)), upper(trim(exchange))), security_id
    FROM {master} WHERE nullif(trim(ticker), '') IS NOT NULL AND nullif(trim(exchange), '') IS NOT NULL
  UNION ALL
  SELECT 'security_id', upper(trim(security_id)), security_id FROM {master} WHERE security_id IS NOT NULL
)
SELECT identifier_type, identifier,
       CASE WHEN count(DISTINCT security_id) = 1 THEN min(security_id) END AS security_id,
       count(DISTINCT security_id) AS n_matches
FROM ids GROUP BY identifier_type, identifier
"""
