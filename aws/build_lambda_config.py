"""Render the client and source YAMLs the forwarder needs into one JSON file (no YAML parser in Lambda).

  python aws/build_lambda_config.py [client ...]      # default: every client
"""
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from keelstone.config import load_client, load_source  # noqa: E402


def build(client_ids: list[str]) -> dict:
    out = {"clients": {}, "sources": {}}
    for cid in client_ids:
        c = load_client(cid)
        out["clients"][cid] = {"landing_root": c.landing_root, "sources": list(c.sources)}
        for sid in c.sources:
            s = load_source(sid)
            out["sources"][sid] = {"landing_subdir": s.landing_subdir, "file_glob": s.file_glob, "format": s.format}
    return out


if __name__ == "__main__":
    ids = sys.argv[1:] or sorted(p.stem for p in (ROOT / "config" / "clients").glob("*.yml"))
    target = ROOT / "aws" / "lambda" / "forwarder" / "landing_config.json"
    target.write_text(json.dumps(build(ids), indent=1, sort_keys=True) + "\n")
    print("wrote", target, "for", ids)
