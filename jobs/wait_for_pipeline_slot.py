# Databricks notebook source
# MAGIC %pip install --quiet --force-reinstall --no-deps /Volumes/keelstone/platform/artifacts/keelstone_holdings-0.1.0-py3-none-any.whl

# COMMAND ----------

dbutils.library.restartPython()  # noqa: F821

# COMMAND ----------

# Code comes from the wheel in the artifacts volume, configs from the same volume:
# serverless cannot reliably read bundle files under /Workspace on this workspace.
"""Wait until no other client's silver+gold pipeline is running.

Free Edition allows one active pipeline per type, so client pipelines run one at a time:
each client job waits here before starting its own pipeline (pipeline tasks also retry
as a backstop if two jobs pass this check at the same moment)."""
import time

from databricks.sdk import WorkspaceClient

dbutils.widgets.text("client", "")  # noqa: F821
dbutils.widgets.text("max_wait_minutes", "45")  # noqa: F821
client_id = dbutils.widgets.get("client")  # noqa: F821
max_wait = int(dbutils.widgets.get("max_wait_minutes"))  # noqa: F821

ACTIVE = {"QUEUED", "CREATED", "WAITING_FOR_RESOURCES", "INITIALIZING", "RESETTING", "SETTING_UP_TABLES",
          "RUNNING", "STOPPING"}
w = WorkspaceClient()
own_suffix = f"keelstone-silver-gold-{client_id}"


def busy() -> list[str]:
    out = []
    for p in w.pipelines.list_pipelines(filter="name LIKE '%keelstone-silver-gold-%'"):
        if p.name.endswith(own_suffix):
            continue
        for u in (w.pipelines.get(p.pipeline_id).latest_updates or [])[:1]:
            if u.state and u.state.value in ACTIVE:
                out.append(f"{p.name} ({u.state.value})")
    return out


deadline = time.time() + max_wait * 60
waited = 0
while (others := busy()):
    if time.time() > deadline:
        raise RuntimeError(f"pipeline slot still busy after {max_wait} min: {others}")
    print(f"waiting for {others}")
    time.sleep(30)
    waited += 30
dbutils.notebook.exit(f"slot free after {waited}s")  # noqa: F821
