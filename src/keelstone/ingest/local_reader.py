"""Plain-Python reader that mimics how bronze parses a file (for local tests and
dry runs). It is a preview, not the source of truth: Spark on Databricks is.

CSV: header row gives the names; a data row whose field count differs from the
header is malformed. JSON Lines: a line that is not a JSON object is malformed.
Columns not in the config are rescued (reported, not lost); config aliases fill
the configured column.
"""
from __future__ import annotations

import csv
import io
import json

from keelstone.config import SourceConfig


def read_rows(src: SourceConfig, text: str) -> list[dict]:
    if src.format == "csv":
        return _read_csv(src, text)
    return _read_jsonl(src, text)


def _apply_names(src: SourceConfig, record: dict) -> dict:
    row = {c: None for c in src.columns}
    rescued = {}
    alias_of = {a: c for c, names in src.column_aliases.items() for a in names}
    for k, v in record.items():
        if k in row:
            row[k] = v
        else:
            rescued[k] = v
            if k in alias_of and row[alias_of[k]] is None:
                row[alias_of[k]] = v
    row["_rescued_data"] = rescued or None
    row["_corrupt_record"] = None
    return row


def _read_csv(src: SourceConfig, text: str) -> list[dict]:
    reader = csv.reader(io.StringIO(text), delimiter=src.options["delimiter"])
    lines = text.splitlines()
    rows = list(reader)
    if not rows:
        return []
    header, out = rows[0], []
    for i, fields in enumerate(rows[1:], start=1):
        if not fields:
            continue
        if len(fields) != len(header):
            bad = {c: None for c in src.columns}
            bad.update(_rescued_data=None, _corrupt_record=lines[i] if i < len(lines) else "")
            out.append(bad)
            continue
        out.append(_apply_names(src, {h: (v if v != "" else None) for h, v in zip(header, fields)}))
    return out


def _read_jsonl(src: SourceConfig, text: str) -> list[dict]:
    out = []
    for line in text.splitlines():
        if not line.strip():
            continue
        try:
            rec = json.loads(line)
            if not isinstance(rec, dict):
                raise ValueError("not an object")
        except ValueError:
            bad = {c: None for c in src.columns}
            bad.update(_rescued_data=None, _corrupt_record=line)
            out.append(bad)
            continue
        out.append(_apply_names(src, rec))
    return out
