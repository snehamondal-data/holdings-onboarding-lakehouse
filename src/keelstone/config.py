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
SUPPORTED_DATE_FORMATS = {"yyyy-MM-dd", "dd/MM/yyyy", "MM/dd/yyyy", "yyyyMMdd"}
# Schemas that ingest must never touch (ground truth lives there).
RESERVED_SCHEMAS = {"qa"}
_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class ConfigError(ValueError):
    pass


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

    def schema_ddl(self) -> str:
        """All columns are strings unless the config says otherwise (nested JSON)."""
        cols = [f"`{c}` {self.column_types.get(c, 'string')}" for c in self.columns]
        cols.append("`_corrupt_record` string")
        return ", ".join(cols)


@dataclass(frozen=True)
class ClientConfig:
    client_id: str
    display_name: str
    catalog: str
    schema: str
    landing_volume: str
    checkpoint_volume: str
    sources: tuple[str, ...]

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
        if f not in SUPPORTED_DATE_FORMATS:
            raise ConfigError(f"{where}: date format '{f}' not in {sorted(SUPPORTED_DATE_FORMATS)}")
    aliases = dict(d.get("column_aliases") or {})
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
    )


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
    return ClientConfig(
        client_id=cid,
        display_name=d.get("display_name", cid),
        catalog=catalog,
        schema=schema,
        landing_volume=d.get("landing_volume", "landing"),
        checkpoint_volume=d.get("checkpoint_volume", "_checkpoints"),
        sources=sources,
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
