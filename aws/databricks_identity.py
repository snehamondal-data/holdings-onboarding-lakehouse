"""Databricks identity for the forwarder Lambda, and its token in AWS SSM.

  python aws/databricks_identity.py create --client wrexford_e2e     # service principal + grants + token -> SSM
  python aws/databricks_identity.py revoke                           # delete token, principal, SSM parameter

Least privilege: a service principal `keelstone-landing-forwarder` that may only use the catalog
and the client's schema and read/write that client's landing volume. Its token (7 days) is created
on its behalf and written straight into SSM Parameter Store as a SecureString: it is never printed,
never written to the repo, and passes through a 0600 temp file only for the AWS CLI call.
--own-token falls back to a 7-day token for the signed-in user (documented as a fallback).
"""
import argparse
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from keelstone.config import load_client  # noqa: E402

DBX = str(Path.home() / ".local/bin/databricks")
AWS = str(Path.home() / ".local/bin/aws")
SP_NAME = "keelstone-landing-forwarder"
PARAM = "/keelstone/databricks/token"
STATE = ROOT / "data" / "aws_state.json"
LIFETIME = 7 * 24 * 3600


def dbx(*args, profile="keelstone", ok_if=None) -> dict:
    r = subprocess.run([DBX, *args, "--profile", profile, "-o", "json"], capture_output=True, text=True)
    if r.returncode != 0:
        if ok_if and ok_if in (r.stderr + r.stdout):
            return {}
        raise SystemExit(f"databricks {' '.join(args[:3])} failed: {(r.stderr or r.stdout)[:500]}")
    return json.loads(r.stdout or "{}")


def put_secret(token: str, aws_profile: str, region: str) -> None:
    fd, path = tempfile.mkstemp(suffix=".json")
    try:
        os.chmod(path, 0o600)
        with os.fdopen(fd, "w") as fh:
            json.dump({"Name": PARAM, "Value": token, "Type": "SecureString", "Overwrite": True,
                       "Description": "Databricks token for the keelstone landing forwarder (7 days)"}, fh)
        r = subprocess.run([AWS, "ssm", "put-parameter", "--cli-input-json", f"file://{path}",
                            "--profile", aws_profile, "--region", region], capture_output=True, text=True)
        if r.returncode != 0:
            raise SystemExit(f"ssm put-parameter failed: {r.stderr[:300]}")
    finally:
        os.remove(path)


def grant_workspace_access(sp_id: str) -> None:
    """The Files API refuses identities without an access entitlement (HTTP 403). workspace-access is
    the minimum; what the principal can read or write is still limited by the Unity Catalog grants."""
    dbx("service-principals", "patch", sp_id, "--json", json.dumps({
        "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
        "Operations": [{"op": "add", "path": "entitlements", "value": [{"value": "workspace-access"}]}]}))


def create(client_id: str, own_token: bool, aws_profile: str, region: str) -> None:
    c = load_client(client_id)
    state = {"client": client_id, "parameter": PARAM}
    if own_token:
        tok = dbx("tokens", "create", "--lifetime-seconds", str(LIFETIME), "--comment", "keelstone forwarder (fallback)")
        state.update(kind="own_token", token_id=tok["token_info"]["token_id"])
        put_secret(tok["token_value"], aws_profile, region)
    else:
        sps = dbx("service-principals", "list", "--filter", f'displayName eq "{SP_NAME}"')
        sp = (sps if isinstance(sps, list) else sps.get("Resources", []))
        sp = sp[0] if sp else dbx("service-principals", "create", "--display-name", SP_NAME)
        app = sp["applicationId"]
        state.update(kind="service_principal", sp_id=sp["id"], application_id=app)
        grant_workspace_access(sp["id"])
        dbx("permissions", "update", "authorization", "tokens", "--json", json.dumps(
            {"access_control_list": [{"service_principal_name": app, "permission_level": "CAN_USE"}]}))
        for kind, name, privs in (("catalog", c.catalog, ["USE_CATALOG"]),
                                  ("schema", f"{c.catalog}.{c.schema}", ["USE_SCHEMA"]),
                                  ("volume", f"{c.catalog}.{c.schema}.{c.landing_volume}", ["READ_VOLUME", "WRITE_VOLUME"])):
            dbx("grants", "update", kind, name, "--json", json.dumps({"changes": [{"principal": app, "add": privs}]}))
        tok = dbx("token-management", "create-obo-token", app, "--lifetime-seconds", str(LIFETIME),
                  "--comment", f"keelstone landing forwarder for {client_id}")
        state["token_id"] = tok["token_info"]["token_id"]
        put_secret(tok["token_value"], aws_profile, region)
    STATE.parent.mkdir(exist_ok=True)
    STATE.write_text(json.dumps(state, indent=1))
    print(f"{state['kind']}: token {state['token_id'][:6]}... stored in SSM {PARAM} (SecureString); not printed")


def revoke(aws_profile: str, region: str) -> None:
    """Revoke the token, delete the service principal, delete the SSM parameter. Works without the local
    state file too: it also finds the principal by name and its tokens by creator."""
    state = json.loads(STATE.read_text()) if STATE.exists() else {}
    if state.get("kind") == "own_token":
        dbx("tokens", "delete", state["token_id"], ok_if="does not exist")
    sps = dbx("service-principals", "list", "--filter", f'displayName eq "{SP_NAME}"')
    for sp in (sps if isinstance(sps, list) else sps.get("Resources", [])):
        toks = dbx("token-management", "list", "--created-by-username", sp["applicationId"])
        for t in (toks if isinstance(toks, list) else toks.get("token_infos", [])):
            dbx("token-management", "delete", t["token_id"], ok_if="does not exist")
            print("revoked token", t["token_id"][:6] + "...")
        dbx("service-principals", "delete", sp["id"], ok_if="does not exist")
        print("deleted service principal", SP_NAME)
    r = subprocess.run([AWS, "ssm", "delete-parameter", "--name", PARAM, "--profile", aws_profile, "--region", region],
                       capture_output=True, text=True)
    if r.returncode != 0 and "ParameterNotFound" not in r.stderr:
        raise SystemExit(r.stderr[:300])
    STATE.unlink(missing_ok=True)
    print("revoked Databricks token and identity; deleted SSM parameter", PARAM)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("action", choices=["create", "revoke"])
    p.add_argument("--client", default="wrexford_e2e")
    p.add_argument("--own-token", action="store_true")
    p.add_argument("--aws-profile", default="keelstone-aws")
    p.add_argument("--region", default="us-east-2")
    a = p.parse_args()
    create(a.client, a.own_token, a.aws_profile, a.region) if a.action == "create" else revoke(a.aws_profile, a.region)
