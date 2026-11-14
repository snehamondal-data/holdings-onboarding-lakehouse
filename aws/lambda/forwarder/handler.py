"""S3 landing zone -> Databricks landing volume forwarder (AWS Lambda, Python 3.13).

Triggered by s3:ObjectCreated on incoming/<client>/<source landing_subdir>/<business date>/<file>.
For each object:
  1. find the client and source in the packaged config (rendered from the YAML at build time);
  2. check it: file name matches the source's file_glob, non-empty, readable (gzip opens, first line
     decodes as UTF-8);
  3. bad   -> copy to quarantine/<same key> tagged with the reason, delete the original;
     good  -> SHA-256, then PUT into /Volumes/<catalog>/<schema>/<landing>/<same path> through the
              Databricks Files API with overwrite=false (409 = already there = duplicate, skipped);
  4. any Databricks failure raises, so Lambda retries and then sends the event to the SQS DLQ.
Uses only the standard library and boto3 (present in the Lambda runtime).
"""
from __future__ import annotations

import fnmatch
import gzip
import hashlib
import io
import json
import logging
import os
import re
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

log = logging.getLogger()
log.setLevel(logging.INFO)

CONFIG_PATH = Path(__file__).with_name("landing_config.json")
INCOMING, QUARANTINE = "incoming/", "quarantine/"
_token_cache: dict = {}


class ForwardError(Exception):
    """Databricks could not take the file; raising lets Lambda retry and then dead-letter it."""


def load_config(path: Path = CONFIG_PATH) -> dict:
    return json.loads(path.read_text())


def route(key: str, config: dict) -> tuple[dict | None, dict | None, str, str]:
    """(client, source, path inside the landing volume, reason if it cannot be routed)."""
    if not key.startswith(INCOMING):
        return None, None, "", "not under incoming/"
    rest = key[len(INCOMING):]
    client_id, _, rel = rest.partition("/")
    client = config["clients"].get(client_id)
    if client is None:
        return None, None, rel, f"unknown client '{client_id}'"
    for sid in client["sources"]:
        src = config["sources"][sid]
        prefix = src["landing_subdir"].rstrip("/") + "/"
        if rel.startswith(prefix):
            name = rel.rsplit("/", 1)[-1]
            if not fnmatch.fnmatch(name, src["file_glob"]):
                return client, src, rel, f"file name '{name}' does not match {sid} pattern '{src['file_glob']}'"
            return client, src, rel, ""
    return client, None, rel, f"no source of client '{client_id}' lands under '{rel.split('/', 1)[0]}/'"


def check_content(data: bytes, name: str) -> str:
    """Reason the bytes are unusable, or '' if they look like a readable file."""
    if not data:
        return "empty file"
    try:
        raw = gzip.GzipFile(fileobj=io.BytesIO(data)).read() if name.endswith(".gz") else data
    except (OSError, EOFError) as e:
        return f"unreadable gzip: {e}"
    if not raw.strip():
        return "empty after decompression"
    try:
        raw.splitlines()[0].decode("utf-8")
    except UnicodeDecodeError:
        return "first line is not UTF-8 text"
    return ""


def databricks_token(ssm, parameter: str) -> str:
    if parameter not in _token_cache:
        _token_cache[parameter] = ssm.get_parameter(Name=parameter, WithDecryption=True)["Parameter"]["Value"]
    return _token_cache[parameter]


def put_file(host: str, token: str, volume_path: str, data: bytes, sha256: str, opener=urllib.request.urlopen) -> str:
    """PUT the bytes into the volume; returns 'forwarded' or 'duplicate'. Raises ForwardError otherwise."""
    url = f"{host.rstrip('/')}/api/2.0/fs/files{urllib.parse.quote(volume_path)}?overwrite=false"
    req = urllib.request.Request(url, data=data, method="PUT", headers={
        "Authorization": f"Bearer {token}", "Content-Type": "application/octet-stream"})
    try:
        with opener(req, timeout=30) as resp:
            if 200 <= resp.status < 300:
                return "forwarded"
            raise ForwardError(f"HTTP {resp.status}")
    except urllib.error.HTTPError as e:
        if e.code == 409:
            return "duplicate"
        raise ForwardError(f"HTTP {e.code}: {e.read()[:200]!r}") from e
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        raise ForwardError(f"cannot reach Databricks: {e}") from e


_TAG_OK = re.compile(r"[^A-Za-z0-9 +\-=._:/@]")


def tag_value(reason: str) -> str:
    """S3 tag values allow only letters, digits, spaces and + - = . _ : / @ (max 256)."""
    return _TAG_OK.sub("", reason)[:256]


def quarantine(s3, bucket: str, key: str, reason: str) -> str:
    target = QUARANTINE + key[len(INCOMING):] if key.startswith(INCOMING) else QUARANTINE + key
    s3.copy_object(Bucket=bucket, Key=target, CopySource={"Bucket": bucket, "Key": key},
                   TaggingDirective="REPLACE", Tagging=urllib.parse.urlencode({"reason": tag_value(reason)}),
                   MetadataDirective="REPLACE", Metadata={"quarantine-reason": reason[:1000]})
    s3.delete_object(Bucket=bucket, Key=key)
    return target


def process(record: dict, s3, ssm, config: dict, host: str, parameter: str, opener=urllib.request.urlopen) -> dict:
    bucket = record["s3"]["bucket"]["name"]
    key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])
    client, src, rel, reason = route(key, config)
    data = b""
    if not reason:
        data = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        reason = check_content(data, rel.rsplit("/", 1)[-1])
    if reason:
        target = quarantine(s3, bucket, key, reason)
        log.warning(json.dumps({"key": key, "outcome": "quarantined", "reason": reason, "to": target}))
        return {"key": key, "outcome": "quarantined", "reason": reason}
    sha = hashlib.sha256(data).hexdigest()
    volume_path = f"{client['landing_root']}/{rel}"
    outcome = put_file(host, databricks_token(ssm, parameter), volume_path, data, sha, opener)
    log.info(json.dumps({"key": key, "outcome": outcome, "sha256": sha, "bytes": len(data), "to": volume_path}))
    return {"key": key, "outcome": outcome, "sha256": sha, "volume_path": volume_path}


def handler(event, context):  # pragma: no cover - thin AWS entry point
    import boto3
    s3, ssm = boto3.client("s3"), boto3.client("ssm")
    config = load_config()
    host, parameter = os.environ["DATABRICKS_HOST"], os.environ["TOKEN_PARAMETER"]
    return [process(r, s3, ssm, config, host, parameter) for r in event.get("Records", [])]
