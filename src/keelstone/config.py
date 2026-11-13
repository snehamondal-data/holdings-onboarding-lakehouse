"""Load and validate client and source configs (plain Python, no Spark).

A new custodian is a new file in config/sources/; a new client is a new file in
config/clients/. Nothing in the ingest code is specific to any one source.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_DIR = REPO_ROOT / "config"

SUPPORTED_FORMATS = {"csv", "json"}
SUPPORTED_KINDS = {"custodian", "book_of_record", "reference"}
# Any day/month/year order with one consistent separator (or none), e.g. dd.MM.yyyy, yyyyMMdd.
_DATE_FORMAT = re.compile(r"^(yyyy|MM|dd)([-./]?)(yyyy|MM|dd)\2(yyyy|MM|dd)$")


def date_format_ok(fmt: str) -> bool:
    m = _DATE_FORMAT.match(fmt)
    return bool(m) and sorted((m.group(1), m.group(3), m.group(4))) == ["MM", "dd", "yyyy"]
# Schemas that ingest must never touch (ground truth lives there).
RESERVED_SCHEMAS = {"qa"}
_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class ConfigError(ValueError):
    pass


IDENTIFIER_TYPES = {"isin": 1, "cusip": 1, "ticker_exchange": 2, "security_id": 1}
POSITION_FIELDS_REQUIRED = ("account_id", "business_date", "quantity", "currency")
POSITION_FIELDS_OPTIONAL = ("price", "market_value")
REFERENCE_TYPES = {"string", "date", "decimal(38,6)", "decimal(38,8)", "decimal(38,2)", "bigint"}


@dataclass(frozen=True)
class HeaderVersion:
    version: int
    effective_from: str
    renames: dict           # header as received -> configured column
    added: tuple = ()


@dataclass(frozen=True)
class SilverConfig:
    target: str                          # "positions" or a reference table name
    fields: dict                         # positions: canonical -> bronze column; reference: column -> type
    position_side: str | None = None     # custodian | book (positions only)
    identifier_type: str | None = None
    identifier_columns: tuple = ()
    market_value_multiplier: float = 1.0
    key: tuple = ()                      # reference targets: natural key
    dedupe: str = "exact"

    @property
    def is_positions(self) -> bool:
        return self.target == "positions"

    @property
    def table(self) -> str:
        return f"silver_{self.target}"


@dataclass(frozen=True)
class SourceConfig:
    source_id: str
    display_name: str
    kind: str
    format: str
    landing_subdir: str
    file_glob: str
    bronze_table: str
    columns: tuple[str, ...]
    options: dict = field(default_factory=dict)
    column_types: dict = field(default_factory=dict)
    column_aliases: dict = field(default_factory=dict)
    required: tuple[str, ...] = ()
    numeric: tuple[str, ...] = ()
    dates: dict = field(default_factory=dict)
    header_versions: tuple = ()
    silver: SilverConfig | None = None
    decimal_separator: str = "."
    thousands_separator: str = ""

    def header_version_for(self, rescued_keys) -> int | None:
        """Highest declared header version whose renamed headers all appear in the row."""
        if not self.header_versions:
            return None
        keys = set(rescued_keys or ())
        best = self.header_versions[0].version
        for hv in self.header_versions:
            if hv.renames and set(hv.renames) <= keys:
                best = max(best, hv.version)
        return best

    def schema_ddl(self) -> str:
        """All columns are strings unless the config says otherwise (nested JSON)."""
        cols = [f"`{c}` {self.column_types.get(c, 'string')}" for c in self.columns]
        cols.append("`_corrupt_record` string")
        return ", ".join(cols)


@dataclass(frozen=True)
class ReconConfig:
    quantity_tolerance: float = 0.0
    value_abs_tolerance: float = 100.0
    value_rel_tolerance: float = 0.0005

    def value_tolerance(self, book_value: float) -> float:
        return max(self.value_abs_tolerance, self.value_rel_tolerance * abs(book_value))


@dataclass(frozen=True)
class ClientConfig:
    client_id: str
    display_name: str
    catalog: str
    schema: str
    landing_volume: str
    checkpoint_volume: str
    sources: tuple[str, ...]
    reconciliation: ReconConfig = ReconConfig()

    @property
    def landing_root(self) -> str:
        return f"/Volumes/{self.catalog}/{self.schema}/{self.landing_volume}"

    @property
    def checkpoint_root(self) -> str:
        return f"/Volumes/{self.catalog}/{self.schema}/{self.checkpoint_volume}"

    def table(self, name: str) -> str:
        return f"{self.catalog}.{self.schema}.{name}"


@dataclass(frozen=True)
class StreamPlan:
    """Everything one generic Auto Loader stream needs for one source."""
    client_id: str
    source: SourceConfig
    input_path: str
    checkpoint_path: str
    schema_location: str
    bronze_table: str
    quarantine_table: str
    file_log_table: str
    drift_log_table: str
    reader_options: dict


def _load_yaml(path: Path) -> dict:
    if not path.exists():
        raise ConfigError(f"config file not found: {path}")
    with path.open() as fh:
        data = yaml.safe_load(fh)
    if not isinstance(data, dict):
        raise ConfigError(f"{path}: expected a mapping at the top level")
    return data


def _need(d: dict, key: str, where: str):
    if key not in d or d[key] in (None, ""):
        raise ConfigError(f"{where}: missing required key '{key}'")
    return d[key]


def _check_ident(value: str, what: str, where: str) -> None:
    if not _IDENT.match(value):
        raise ConfigError(f"{where}: {what} '{value}' is not a valid identifier")


def parse_source(d: dict, where: str = "source") -> SourceConfig:
    sid = _need(d, "source_id", where)
    _check_ident(sid, "source_id", where)
    fmt = _need(d, "format", where)
    if fmt not in SUPPORTED_FORMATS:
        raise ConfigError(f"{where}: format '{fmt}' not in {sorted(SUPPORTED_FORMATS)}")
    kind = _need(d, "kind", where)
    if kind not in SUPPORTED_KINDS:
        raise ConfigError(f"{where}: kind '{kind}' not in {sorted(SUPPORTED_KINDS)}")
    columns = tuple(_need(d, "columns", where))
    if len(set(columns)) != len(columns):
        raise ConfigError(f"{where}: duplicate column names")
    for c in columns:
        _check_ident(c, "column", where)
    bronze_table = _need(d, "bronze_table", where)
    _check_ident(bronze_table, "bronze_table", where)
    subdir = _need(d, "landing_subdir", where)
    if subdir.startswith("/") or ".." in subdir.split("/"):
        raise ConfigError(f"{where}: landing_subdir must be relative to the landing volume")

    rules = d.get("rules") or {}
    required = tuple(rules.get("required") or ())
    numeric = tuple(rules.get("numeric") or ())
    dates = dict(rules.get("dates") or {})
    known = set(columns)
    for c in required:
        if c not in known:
            raise ConfigError(f"{where}: required column '{c}' is not in columns")
    for c in numeric:
        base = c.split("[]")[0]
        if base not in known:
            raise ConfigError(f"{where}: numeric column '{c}' is not in columns")
    for c, f in dates.items():
        if c not in known:
            raise ConfigError(f"{where}: date column '{c}' is not in columns")
        if not date_format_ok(f):
            raise ConfigError(f"{where}: date format '{f}' must order yyyy, MM and dd with one separator")
    aliases = {k: list(v) for k, v in (d.get("column_aliases") or {}).items()}
    header_versions = _parse_header_versions(d.get("header_versions") or [], known, where)
    for hv in header_versions:
        for received, configured in hv.renames.items():
            aliases.setdefault(configured, [])
            if received not in aliases[configured]:
                aliases[configured].append(received)
    for c, names in aliases.items():
        if c not in known:
            raise ConfigError(f"{where}: alias target '{c}' is not in columns")
        for n in names:
            _check_ident(n, "alias", where)
    column_types = dict(d.get("column_types") or {})
    for c in column_types:
        if c not in known:
            raise ConfigError(f"{where}: column_types key '{c}' is not in columns")
    options = dict(d.get("options") or {})
    if fmt == "csv" and "delimiter" not in options:
        raise ConfigError(f"{where}: csv sources need options.delimiter")
    dec = options.get("decimal_separator", ".")
    thou = options.get("thousands_separator", "")
    if dec not in (".", ",") or thou not in ("", ".", ",", " ", "'") or dec == thou:
        raise ConfigError(f"{where}: decimal_separator must be '.' or ',' and differ from thousands_separator")
    if fmt == "csv" and options["delimiter"] in (dec, thou):
        raise ConfigError(f"{where}: the delimiter cannot also be a decimal or thousands separator")

    return SourceConfig(
        source_id=sid,
        display_name=d.get("display_name", sid),
        kind=kind,
        format=fmt,
        landing_subdir=subdir.strip("/"),
        file_glob=d.get("file_glob", "*"),
        bronze_table=bronze_table,
        columns=columns,
        options=options,
        column_types=column_types,
        column_aliases={k: tuple(v) for k, v in aliases.items()},
        required=required,
        numeric=numeric,
        dates=dates,
        header_versions=header_versions,
        silver=_parse_silver(d.get("silver"), known, column_types, dates, where),
        decimal_separator=dec,
        thousands_separator=thou,
    )


def _parse_header_versions(raw: list, known: set, where: str) -> tuple:
    out, seen = [], set()
    for hv in raw:
        v = hv.get("version")
        if not isinstance(v, int) or v in seen:
            raise ConfigError(f"{where}: header_versions need unique integer versions")
        seen.add(v)
        renames = dict(hv.get("renames") or {})
        for received, configured in renames.items():
            _check_ident(received, "renamed header", where)
            if configured not in known:
                raise ConfigError(f"{where}: header rename target '{configured}' is not in columns")
        out.append(HeaderVersion(v, str(_need(hv, "effective_from", where)), renames,
                                 tuple(hv.get("added") or ())))
    if [h.version for h in out] != sorted(h.version for h in out):
        raise ConfigError(f"{where}: header_versions must be listed in version order")
    return tuple(out)


def _column_ref_ok(ref: str, known: set, column_types: dict) -> bool:
    if "[]." in ref:
        arr = ref.split("[].", 1)[0]
        return arr in known and column_types.get(arr, "").lower().startswith("array")
    return ref in known


def _parse_silver(d: dict | None, known: set, column_types: dict, dates: dict, where: str):
    if d is None:
        return None
    where = f"{where} (silver)"
    target = _need(d, "target", where)
    _check_ident(target, "target", where)
    fields = dict(_need(d, "fields", where))
    dedupe = d.get("dedupe", "exact")
    if dedupe not in ("exact", "none"):
        raise ConfigError(f"{where}: dedupe must be 'exact' or 'none'")
    if target == "positions":
        side = _need(d, "position_side", where)
        if side not in ("custodian", "book"):
            raise ConfigError(f"{where}: position_side must be custodian or book")
        for f in POSITION_FIELDS_REQUIRED:
            if f not in fields:
                raise ConfigError(f"{where}: positions mapping needs field '{f}'")
        for f, ref in fields.items():
            if f not in POSITION_FIELDS_REQUIRED + POSITION_FIELDS_OPTIONAL:
                raise ConfigError(f"{where}: '{f}' is not a canonical position field")
            if not _column_ref_ok(ref, known, column_types):
                raise ConfigError(f"{where}: field '{f}' maps to unknown column '{ref}'")
        if fields["business_date"] not in dates:
            raise ConfigError(f"{where}: business_date column needs a format in rules.dates")
        ident = _need(d, "identifier", where)
        itype = _need(ident, "type", where)
        if itype not in IDENTIFIER_TYPES:
            raise ConfigError(f"{where}: identifier type '{itype}' not in {sorted(IDENTIFIER_TYPES)}")
        icols = tuple(_need(ident, "columns", where))
        if len(icols) != IDENTIFIER_TYPES[itype] or any(c not in known for c in icols):
            raise ConfigError(f"{where}: identifier '{itype}' needs {IDENTIFIER_TYPES[itype]} known column(s)")
        mult = float(d.get("market_value_multiplier", 1))
        if mult <= 0:
            raise ConfigError(f"{where}: market_value_multiplier must be positive")
        return SilverConfig(target, fields, side, itype, icols, mult, (), dedupe)
    key = tuple(_need(d, "key", where))
    for c, t in fields.items():
        if c not in known:
            raise ConfigError(f"{where}: field '{c}' is not in columns")
        if t not in REFERENCE_TYPES:
            raise ConfigError(f"{where}: type '{t}' for '{c}' not in {sorted(REFERENCE_TYPES)}")
        if t == "date" and c not in dates:
            raise ConfigError(f"{where}: date field '{c}' needs a format in rules.dates")
    if not key or any(k not in fields for k in key):
        raise ConfigError(f"{where}: key must be a non-empty subset of fields")
    return SilverConfig(target, fields, key=key, dedupe=dedupe)


def parse_client(d: dict, where: str = "client") -> ClientConfig:
    cid = _need(d, "client_id", where)
    _check_ident(cid, "client_id", where)
    schema = _need(d, "schema", where)
    _check_ident(schema, "schema", where)
    if schema in RESERVED_SCHEMAS:
        raise ConfigError(f"{where}: schema '{schema}' is reserved for QA ground truth")
    catalog = _need(d, "catalog", where)
    _check_ident(catalog, "catalog", where)
    sources = tuple(_need(d, "sources", where))
    if len(set(sources)) != len(sources):
        raise ConfigError(f"{where}: a source is listed twice")
    rc = d.get("reconciliation") or {}
    vt = rc.get("value_tolerance") or {}
    recon = ReconConfig(float(rc.get("quantity_tolerance", 0)), float(vt.get("absolute", 100)),
                        float(vt.get("relative", 0.0005)))
    if min(recon.quantity_tolerance, recon.value_abs_tolerance, recon.value_rel_tolerance) < 0:
        raise ConfigError(f"{where}: reconciliation tolerances must not be negative")
    return ClientConfig(
        client_id=cid,
        display_name=d.get("display_name", cid),
        catalog=catalog,
        schema=schema,
        landing_volume=d.get("landing_volume", "landing"),
        checkpoint_volume=d.get("checkpoint_volume", "_checkpoints"),
        sources=sources,
        reconciliation=recon,
    )


def load_source(source_id: str, config_dir: Path = DEFAULT_CONFIG_DIR) -> SourceConfig:
    path = Path(config_dir) / "sources" / f"{source_id}.yml"
    src = parse_source(_load_yaml(path), where=str(path))
    if src.source_id != source_id:
        raise ConfigError(f"{path}: source_id '{src.source_id}' does not match file name")
    return src


def load_client(client_id: str, config_dir: Path = DEFAULT_CONFIG_DIR) -> ClientConfig:
    path = Path(config_dir) / "clients" / f"{client_id}.yml"
    client = parse_client(_load_yaml(path), where=str(path))
    if client.client_id != client_id:
        raise ConfigError(f"{path}: client_id '{client.client_id}' does not match file name")
    return client


def reader_options(src: SourceConfig) -> dict:
    """Auto Loader options derived only from config."""
    opts = {
        "cloudFiles.format": src.format,
        "cloudFiles.schemaEvolutionMode": "rescue",
        "rescuedDataColumn": "_rescued_data",
        "columnNameOfCorruptRecord": "_corrupt_record",
        "mode": "PERMISSIVE",
        "pathGlobFilter": src.file_glob,
        "recursiveFileLookup": "true",
    }
    if src.format == "csv":
        opts["header"] = str(bool(src.options.get("header", True))).lower()
        opts["sep"] = src.options["delimiter"]
        opts["quote"] = src.options.get("quote", '"')
    return opts


def plan_ingest(client_id: str, config_dir: Path = DEFAULT_CONFIG_DIR) -> list[StreamPlan]:
    """One StreamPlan per source the client lists, built from YAML alone."""
    client = load_client(client_id, config_dir)
    plans = []
    tables = set()
    for sid in client.sources:
        src = load_source(sid, config_dir)
        if src.bronze_table in tables:
            raise ConfigError(f"client {client_id}: two sources write {src.bronze_table}")
        tables.add(src.bronze_table)
        cp = f"{client.checkpoint_root}/bronze/{sid}"
        plans.append(StreamPlan(
            client_id=client.client_id,
            source=src,
            input_path=f"{client.landing_root}/{src.landing_subdir}",
            checkpoint_path=f"{cp}/checkpoint",
            schema_location=f"{cp}/schema",
            bronze_table=client.table(src.bronze_table),
            quarantine_table=client.table("bronze_quarantine"),
            file_log_table=client.table("bronze_file_log"),
            drift_log_table=client.table("bronze_schema_drift_log"),
            reader_options=reader_options(src),
        ))
    return plans
