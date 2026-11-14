"""End-to-end checks of the AWS landing zone (slice 6). Each step prints what happened; times are UTC.

  python aws/e2e.py upload-day --client wrexford_e2e --date 2026-09-22   # one day via S3, wait for the volume
  python aws/e2e.py duplicate  --client wrexford_e2e --date 2026-09-22   # same object again -> skipped (409)
  python aws/e2e.py bad        --client wrexford_e2e                     # empty + misnamed -> quarantine/
  python aws/e2e.py outage     --client wrexford_e2e --date 2026-09-22   # (stack deployed with SIMULATE_OUTAGE=true)
  python aws/e2e.py logs                                                 # the forwarder's outcomes

Results are appended to data/aws_e2e.json (gitignored) and summarised in the README.
"""
import argparse
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from keelstone.config import load_client  # noqa: E402

AWS = str(Path.home() / ".local/bin/aws")
DBX = str(Path.home() / ".local/bin/databricks")
LOG = ROOT / "data" / "aws_e2e.json"


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def aws(*args, profile: str, region: str, check=True) -> dict | str:
    r = subprocess.run([AWS, *args, "--profile", profile, "--region", region, "--output", "json"],
                       capture_output=True, text=True, timeout=300)
    if check and r.returncode != 0:
        raise SystemExit(f"aws {' '.join(args[:3])} failed: {r.stderr[:400]}")
    try:
        return json.loads(r.stdout) if r.stdout.strip() else {}
    except json.JSONDecodeError:
        return r.stdout


def bucket(profile: str, region: str) -> str:
    acct = aws("sts", "get-caller-identity", profile=profile, region=region)["Account"]
    return f"keelstone-landing-{acct}-{region}"


def in_volume(path: str) -> bool:
    r = subprocess.run([DBX, "fs", "ls", "dbfs:" + path.rsplit("/", 1)[0], "--profile", "keelstone"],
                       capture_output=True, text=True, timeout=120)
    return path.rsplit("/", 1)[1] in r.stdout.split()


def record(step: str, **kw) -> None:
    data = json.loads(LOG.read_text()) if LOG.exists() else []
    data.append({"step": step, "at": now(), **kw})
    LOG.write_text(json.dumps(data, indent=1))
    print(json.dumps(data[-1], indent=1))


def day_files(client_id: str, date: str) -> list[dict]:
    manifest = json.loads((ROOT / "data" / client_id / "manifest.json").read_text())
    return [f for f in manifest["files"] if f["business_date"] == date]


def upload(local: Path, key: str, b: str, profile: str, region: str) -> None:
    aws("s3", "cp", str(local), f"s3://{b}/{key}", "--only-show-errors", profile=profile, region=region)


def upload_day(a) -> None:
    c, b = load_client(a.client), bucket(a.profile, a.region)
    files = day_files(a.client, a.date)
    t0 = time.time()
    for f in files:
        upload(ROOT / "data" / a.client / "landing" / f["path"], f"incoming/{a.client}/{f['path']}", b, a.profile, a.region)
    uploaded = time.time()
    arrived = {}
    while len(arrived) < len(files) and time.time() - t0 < 600:
        for f in files:
            if f["path"] not in arrived and in_volume(f"{c.landing_root}/{f['path']}"):
                arrived[f["path"]] = round(time.time() - t0, 1)
        time.sleep(5)
    state = ROOT / "data" / a.client / "delivery_state.json"     # keep deliver.py's view consistent
    st = json.loads(state.read_text()) if state.exists() else {"delivered": []}
    st["delivered"] = sorted(set(st["delivered"]) | {f["path"] for f in files})
    state.write_text(json.dumps(st, indent=1))
    record("upload_day", client=a.client, date=a.date, files=len(files), upload_started=t0,
           upload_seconds=round(uploaded - t0, 1), seconds_to_volume=arrived,
           all_in_volume=len(arrived) == len(files))


def duplicate(a) -> None:
    f = day_files(a.client, a.date)[0]
    upload(ROOT / "data" / a.client / "landing" / f["path"], f"incoming/{a.client}/{f['path']}",
           bucket(a.profile, a.region), a.profile, a.region)
    time.sleep(20)
    record("duplicate", client=a.client, key=f["path"], forwarder=outcomes(a, since_seconds=120, key=f["path"]))


def bad(a) -> None:
    b = bucket(a.profile, a.region)
    tmp = ROOT / "data" / "_bad"
    tmp.mkdir(exist_ok=True)
    empty, misnamed = tmp / "quayside_holdings_20260922.tsv", tmp / "notes.txt"
    empty.write_bytes(b"")
    misnamed.write_text("not a custodian file\n")
    keys = [f"incoming/{a.client}/quayside/2026-09-22/bad_empty/{empty.name}",
            f"incoming/{a.client}/quayside/2026-09-22/{misnamed.name}"]
    upload(empty, keys[0], b, a.profile, a.region)
    upload(misnamed, keys[1], b, a.profile, a.region)
    time.sleep(25)
    result = {}
    for k in keys:
        q = "quarantine/" + k[len("incoming/"):]
        head = aws("s3api", "head-object", "--bucket", b, "--key", q, profile=a.profile, region=a.region, check=False)
        tags = aws("s3api", "get-object-tagging", "--bucket", b, "--key", q, profile=a.profile, region=a.region, check=False)
        still = aws("s3api", "head-object", "--bucket", b, "--key", k, profile=a.profile, region=a.region, check=False)
        result[k] = {"in_quarantine": bool(head), "reason": (head or {}).get("Metadata", {}).get("quarantine-reason"),
                     "tags": (tags or {}).get("TagSet"), "original_removed": not still}
    record("bad_files", client=a.client, result=result)


def outage(a) -> None:
    f = day_files(a.client, a.date)[-1]
    b = bucket(a.profile, a.region)
    # a valid source path (so it passes the checks and reaches the forward), in its own date folder
    subdir, day, name = f["path"].rsplit("/", 2)
    key = f"incoming/{a.client}/{subdir}/{day}-outage-test/{name}"
    q = aws("sqs", "get-queue-url", "--queue-name", "keelstone-landing-forwarder-dlq", profile=a.profile, region=a.region)
    t0 = time.time()
    upload(ROOT / "data" / a.client / "landing" / f["path"], key, b, a.profile, a.region)
    msg, others = None, []
    while time.time() - t0 < 900 and not msg:
        r = aws("sqs", "receive-message", "--queue-url", q["QueueUrl"], "--wait-time-seconds", "20",
                "--max-number-of-messages", "10", "--message-attribute-names", "All", profile=a.profile, region=a.region)
        for m in (r or {}).get("Messages", []):
            try:
                mkey = json.loads(m["Body"])["Records"][0]["s3"]["object"]["key"]
            except (KeyError, ValueError, IndexError):
                mkey = None
            aws("sqs", "delete-message", "--queue-url", q["QueueUrl"], "--receipt-handle", m["ReceiptHandle"],
                profile=a.profile, region=a.region)
            if mkey == key:
                msg = m
            else:
                others.append(mkey)       # an earlier test's event; noted, not counted
    attrs = {k: v.get("StringValue") for k, v in ((msg or {}).get("MessageAttributes") or {}).items()}
    body_key = None
    if msg:
        try:
            body_key = json.loads(msg["Body"])["Records"][0]["s3"]["object"]["key"]
        except (KeyError, ValueError, IndexError):
            pass
    record("outage", client=a.client, key=key, dead_lettered=bool(msg), other_dead_letters=others,
           seconds_to_dlq=round(time.time() - t0, 1),
           dlq_error=attrs.get("ErrorMessage"), dlq_event_key=body_key)


def outcomes(a, since_seconds=3600, key=None) -> list:
    r = aws("logs", "filter-log-events", "--log-group-name", "/aws/lambda/keelstone-landing-forwarder",
            "--start-time", str(int((time.time() - since_seconds) * 1000)), "--filter-pattern", '"outcome"',
            profile=a.profile, region=a.region, check=False)
    out = []
    for e in (r or {}).get("events", []):
        try:
            j = json.loads(e["message"].split("\t")[-1])
        except ValueError:
            continue
        if key is None or j.get("key", "").endswith(key):
            out.append({k: j.get(k) for k in ("key", "outcome", "reason", "sha256")})
    return out


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("step", choices=["upload-day", "duplicate", "bad", "outage", "logs"])
    p.add_argument("--client", default="wrexford_e2e")
    p.add_argument("--date", default="2026-09-22")
    p.add_argument("--profile", default="keelstone-aws")
    p.add_argument("--region", default="us-east-2")
    a = p.parse_args()
    if a.step == "logs":
        print(json.dumps(outcomes(a), indent=1))
    else:
        {"upload-day": upload_day, "duplicate": duplicate, "bad": bad, "outage": outage}[a.step](a)
