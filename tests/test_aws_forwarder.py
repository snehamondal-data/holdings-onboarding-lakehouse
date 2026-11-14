"""Unit tests for the S3 -> Databricks forwarder Lambda. No AWS or Databricks calls: S3, SSM and
HTTP are stubbed."""
import gzip
import hashlib
import io
import json
import sys
import urllib.error
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "aws" / "lambda" / "forwarder"))
sys.path.insert(0, str(REPO / "aws"))
import handler as H  # noqa: E402
from build_lambda_config import build  # noqa: E402

CONFIG = build(["calder_valley", "wrexford"])


class FakeS3:
    def __init__(self, objects: dict):
        self.objects = dict(objects)
        self.tags = {}
        self.meta = {}

    def get_object(self, Bucket, Key):
        return {"Body": io.BytesIO(self.objects[Key])}

    def copy_object(self, Bucket, Key, CopySource, TaggingDirective, Tagging, MetadataDirective, Metadata):
        # enforce S3's tag rules, as the real service does (InvalidTag otherwise)
        import re
        import urllib.parse
        for _, value in urllib.parse.parse_qsl(Tagging):
            if re.search(r"[^A-Za-z0-9 +\-=._:/@]", value) or len(value) > 256:
                raise ValueError(f"InvalidTag: {value!r}")
        self.objects[Key] = self.objects[CopySource["Key"]]
        self.tags[Key], self.meta[Key] = Tagging, Metadata

    def delete_object(self, Bucket, Key):
        del self.objects[Key]


class FakeSSM:
    calls = 0

    def get_parameter(self, Name, WithDecryption):
        FakeSSM.calls += 1
        assert WithDecryption
        return {"Parameter": {"Value": "test-token"}}


class Resp:
    def __init__(self, status):
        self.status = status

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def opener_returning(status=200, error=None):
    sent = []

    def opener(req, timeout):
        sent.append(req)
        if error is not None:
            raise error
        return Resp(status)
    opener.sent = sent
    return opener


def event(key):
    return {"s3": {"bucket": {"name": "landing"}, "object": {"key": key}}}


def run(key, data, opener):
    s3 = FakeS3({key: data})
    out = H.process(event(key), s3, FakeSSM(), CONFIG, "https://example.cloud.databricks.com", "/p", opener)
    return out, s3


GOOD_KEY = "incoming/wrexford/quayside/2026-09-22/quayside_holdings_20260922.tsv"
GOOD = b"portfolio\tas_at\tcusip\n WX-QT-001\t09/22/2026\tABC\n"


def setup_function():
    H._token_cache.clear()


def test_config_is_rendered_from_the_yaml():
    assert CONFIG["clients"]["wrexford"]["landing_root"] == "/Volumes/keelstone/wrexford/landing"
    assert CONFIG["sources"]["quayside"] == {"landing_subdir": "quayside", "file_glob": "*.tsv", "format": "csv"}


def test_good_file_is_forwarded_to_the_same_path_in_the_volume():
    op = opener_returning(200)
    out, s3 = run(GOOD_KEY, GOOD, op)
    assert out["outcome"] == "forwarded"
    assert out["sha256"] == hashlib.sha256(GOOD).hexdigest()
    req = op.sent[0]
    assert req.method == "PUT"
    assert req.full_url == ("https://example.cloud.databricks.com/api/2.0/fs/files/Volumes/keelstone/wrexford/"
                            "landing/quayside/2026-09-22/quayside_holdings_20260922.tsv?overwrite=false")
    assert req.headers["Authorization"] == "Bearer test-token" and req.data == GOOD
    assert GOOD_KEY in s3.objects                       # good files stay; lifecycle removes them


def test_duplicate_is_skipped_not_failed():
    err = urllib.error.HTTPError("u", 409, "Conflict", {}, io.BytesIO(b"ALREADY_EXISTS"))
    out, _ = run(GOOD_KEY, GOOD, opener_returning(error=err))
    assert out["outcome"] == "duplicate"


@pytest.mark.parametrize("err", [urllib.error.HTTPError("u", 503, "Unavailable", {}, io.BytesIO(b"")),
                                 urllib.error.HTTPError("u", 401, "Unauthorized", {}, io.BytesIO(b"")),
                                 urllib.error.URLError("Name or service not known"), TimeoutError("timed out")])
def test_databricks_failure_raises_so_lambda_retries_then_dead_letters(err):
    with pytest.raises(H.ForwardError):
        run(GOOD_KEY, GOOD, opener_returning(error=err))


@pytest.mark.parametrize("key,data,reason", [
    ("incoming/wrexford/quayside/2026-09-22/notes.txt", GOOD, "does not match quayside pattern '*.tsv'"),
    ("incoming/wrexford/quayside/2026-09-22/quayside_holdings_20260922.tsv", b"", "empty file"),
    ("incoming/wrexford/quayside/2026-09-22/quayside_holdings_20260922.tsv", b"\n \n", "empty after decompression"),
    ("incoming/wrexford/quayside/2026-09-22/quayside_holdings_20260922.tsv", b"\xff\xfe\xfa\n", "not UTF-8"),
    ("incoming/nobody/quayside/2026-09-22/x.tsv", GOOD, "unknown client 'nobody'"),
    ("incoming/wrexford/harrow/2026-09-22/harrow_positions_20260922.csv", GOOD, "no source of client 'wrexford'"),
])
def test_bad_files_are_moved_to_quarantine_with_the_reason(key, data, reason):
    op = opener_returning(200)
    out, s3 = run(key, data, op)
    assert out["outcome"] == "quarantined" and reason in out["reason"]
    target = "quarantine/" + key[len("incoming/"):]
    assert key not in s3.objects and target in s3.objects
    assert reason[:20] in s3.meta[target]["quarantine-reason"]
    assert "reason=" in s3.tags[target]
    assert not op.sent                                   # nothing reaches Databricks


def test_gzip_files_are_checked_after_decompression():
    key = "incoming/calder_valley/harrow/2026-07-01/harrow_positions_20260701.csv"
    gz_cfg = json.loads(json.dumps(CONFIG))
    gz_cfg["sources"]["harrow"]["file_glob"] = "*.csv.gz"
    bad_key = key.replace("harrow_positions", "harrow_resend")
    s3 = FakeS3({key + ".gz": gzip.compress(b"a,b\n1,2\n"), bad_key + ".gz": b"not gzip"})
    ok = H.process(event(key + ".gz"), s3, FakeSSM(), gz_cfg, "https://h", "/p", opener_returning(200))
    bad = H.process(event(bad_key + ".gz"), s3, FakeSSM(), gz_cfg, "https://h", "/p", opener_returning(200))
    assert ok["outcome"] == "forwarded" and bad["outcome"] == "quarantined" and "gzip" in bad["reason"]


def test_url_encoded_keys_from_s3_events_are_decoded():
    key = "incoming/wrexford/quayside/2026-09-22/quayside holdings 20260922.tsv"     # S3 sends spaces as '+'
    s3 = FakeS3({key: GOOD})
    op = opener_returning(200)
    out = H.process(event(key.replace(" ", "+")), s3, FakeSSM(), CONFIG, "https://h", "/p", op)
    assert out["outcome"] == "forwarded" and op.sent[0].full_url.endswith("quayside%20holdings%2020260922.tsv?overwrite=false")


def test_token_is_read_once_per_container():
    FakeSSM.calls = 0
    for _ in range(3):
        run(GOOD_KEY, GOOD, opener_returning(200))
    assert FakeSSM.calls == 1


def test_handler_source_contains_no_secrets_or_hosts():
    src = (REPO / "aws" / "lambda" / "forwarder" / "handler.py").read_text()
    assert "dapi" not in src and ".cloud.databricks.com" not in src.replace("example.cloud.databricks.com", "")
