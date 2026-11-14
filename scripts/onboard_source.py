"""Onboard a new source with the AI assistant and a human approval step (slice 7).

  python scripts/onboard_source.py draft   --client wrexford --source-id acme_custody --kind custodian \
                                           --display-name "Acme Custody (fictional)" --landing-subdir acme \
                                           --sample path/to/sample.csv [--budget-usd 0.30]
  python scripts/onboard_source.py show    --source-id acme_custody
  python scripts/onboard_source.py approve --source-id acme_custody --by "Sneha"
  python scripts/onboard_source.py reject  --source-id acme_custody --by "Sneha" --reason "..."

The assistant only drafts (into data/onboarding/<id>/, outside config/). Only `approve`, run by a
person, writes config/sources/<id>.yml, and only for a draft that validates; a draft whose dry run
failed needs --override-reason. Every action is appended to onboarding/audit.jsonl.
"""
import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from keelstone.assistant.agent import MODEL, AnthropicModel, Spend, draft_source  # noqa: E402
from keelstone.assistant.draft import dry_run, validate  # noqa: E402
from keelstone.assistant.tools import Sample  # noqa: E402

DRAFTS = ROOT / "data" / "onboarding"
AUDIT = ROOT / "onboarding" / "audit.jsonl"
SOURCES = ROOT / "config" / "sources"


def audit(action: str, source_id: str, actor: str, **details) -> None:
    AUDIT.parent.mkdir(exist_ok=True)
    entry = {"at": datetime.now(timezone.utc).isoformat(timespec="seconds"), "action": action,
             "source_id": source_id, "actor": actor, **details}
    with AUDIT.open("a") as fh:
        fh.write(json.dumps(entry, default=str) + "\n")


def api_key() -> str:
    for line in (ROOT / ".env").read_text().splitlines():
        if line.strip().startswith("ANTHROPIC_API_KEY"):
            return line.split("=", 1)[1].strip().strip('"').strip("'")
    raise SystemExit("ANTHROPIC_API_KEY not found in .env")


def master_rows(client_id: str) -> list[dict]:
    import csv
    path = next((ROOT / "data" / client_id / "landing" / "reference" / "security_master").rglob("*.csv"))
    return list(csv.DictReader(path.open()))


def load_sample(path: Path, client_id: str) -> Sample:
    raw = path.read_bytes()
    text = (__import__("gzip").decompress(raw) if path.name.endswith(".gz") else raw).decode("utf-8")
    return Sample(path.name, text, master_rows(client_id))


def cmd_draft(a) -> None:
    sample = load_sample(Path(a.sample), a.client)
    spend = Spend(limit_usd=a.budget_usd)
    result = draft_source(AnthropicModel(api_key()), sample, a.source_id, a.display_name, a.kind,
                          a.landing_subdir, spend)
    out = DRAFTS / a.source_id
    out.mkdir(parents=True, exist_ok=True)
    (out / "draft.json").write_text(json.dumps({**result.to_json(), "client": a.client, "sample": str(a.sample)},
                                               indent=1, default=str))
    audit("draft", a.source_id, f"assistant:{MODEL}", status=result.status, sample=Path(a.sample).name,
          dry_run_passed=(result.dry_run or {}).get("passed"), spend=result.spend, note=result.note)
    print(f"{result.status}: {result.note or 'see: python scripts/onboard_source.py show --source-id ' + a.source_id}")


def load_draft(source_id: str) -> dict:
    p = DRAFTS / source_id / "draft.json"
    if not p.exists():
        raise SystemExit(f"no draft for {source_id}; run `draft` first")
    return json.loads(p.read_text())


def cmd_show(a) -> None:
    d = load_draft(a.source_id)
    print(f"== {a.source_id}: {d['status']}  (spend ${d['spend'].get('usd')}, {d['spend'].get('calls')} model calls)")
    if d.get("note"):
        print("note:", d["note"])
    print("\n== evidence (tool calls)")
    for c in d["evidence"]:
        print(f"- {c['tool']}({json.dumps(c['args'])}) -> {c['result'][:160]}")
    for i, att in enumerate(d["attempts"], 1):
        print(f"\n== submit #{i}: {'valid' if att['valid'] else 'INVALID: ' + str(att['error'])}")
        print("   evidence:", att["draft"].get("evidence"))
    if d.get("yaml_text"):
        print("\n== draft YAML\n" + d["yaml_text"])
    if d.get("dry_run"):
        dr = d["dry_run"]
        print("== dry run on the sample:", "PASS" if dr["passed"] else "FAIL")
        for k, ok in dr["checks"].items():
            print(f"   {'ok  ' if ok else 'FAIL'} {k}")
        print("   metrics:", json.dumps(dr["metrics"]))


def shown(path: Path) -> str:
    return str(path.relative_to(ROOT)) if path.is_relative_to(ROOT) else str(path)


def cmd_approve(a) -> None:
    d = load_draft(a.source_id)
    if d["status"] != "drafted" or not d.get("config"):
        raise SystemExit("nothing to approve: the assistant did not produce a valid draft")
    src, err = validate(d["config"])
    if err:
        raise SystemExit(f"draft no longer validates: {err}")
    if not d["dry_run"]["passed"] and not a.override_reason:
        raise SystemExit("the dry run failed; pass --override-reason if you approve it anyway")
    target = SOURCES / f"{a.source_id}.yml"
    if target.exists() and not a.replace:
        raise SystemExit(f"{target} exists; pass --replace to overwrite")
    target.write_text(d["yaml_text"].replace("approved by a human before use.",
                                             f"approved by {a.by} on {datetime.now(timezone.utc):%Y-%m-%d}."))
    audit("approve", a.source_id, f"human:{a.by}", wrote=shown(target),
          sha256=hashlib.sha256(target.read_bytes()).hexdigest(), dry_run_passed=d["dry_run"]["passed"],
          override_reason=a.override_reason)
    print(f"wrote {shown(target)}; add '{a.source_id}' to the client's sources list to start ingesting")


def cmd_reject(a) -> None:
    load_draft(a.source_id)
    audit("reject", a.source_id, f"human:{a.by}", reason=a.reason)
    print("rejected; nothing written to config/")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)
    d = sub.add_parser("draft")
    for arg in ("--client", "--sample", "--source-id", "--display-name", "--landing-subdir"):
        d.add_argument(arg, required=True)
    d.add_argument("--kind", choices=["custodian", "book_of_record"], required=True)
    d.add_argument("--budget-usd", type=float, default=0.30)
    s = sub.add_parser("show")
    s.add_argument("--source-id", required=True)
    ap = sub.add_parser("approve")
    ap.add_argument("--source-id", required=True)
    ap.add_argument("--by", required=True)
    ap.add_argument("--override-reason", default="")
    ap.add_argument("--replace", action="store_true")
    rj = sub.add_parser("reject")
    rj.add_argument("--source-id", required=True)
    rj.add_argument("--by", required=True)
    rj.add_argument("--reason", required=True)
    a = p.parse_args()
    {"draft": cmd_draft, "show": cmd_show, "approve": cmd_approve, "reject": cmd_reject}[a.cmd](a)
