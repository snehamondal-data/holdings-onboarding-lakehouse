# Databricks notebook source
# MAGIC %pip install --quiet --force-reinstall --no-deps /Volumes/keelstone/platform/artifacts/keelstone_holdings-0.1.0-py3-none-any.whl

# COMMAND ----------

dbutils.library.restartPython()  # noqa: F821

# COMMAND ----------

# Code comes from the wheel in the artifacts volume, configs from the same volume:
# serverless cannot reliably read bundle files under /Workspace on this workspace.
import os
import sys
dbutils.widgets.text("config_dir", "/Volumes/keelstone/platform/artifacts/config")  # noqa: F821
dbutils.widgets.text("client", "")  # noqa: F821
CONFIG_DIR = dbutils.widgets.get("config_dir")  # noqa: F821
import keelstone  # noqa: E402

try:  # let foreachBatch workers unpickle our functions even without the wheel
    from pyspark import cloudpickle
    cloudpickle.register_pickle_by_value(keelstone)
except Exception as e:  # pragma: no cover
    print(f"note: register_pickle_by_value unavailable ({e})")
"""Create the catalog, client schema, QA schema and volumes (idempotent)."""
a = type('Args', (), {"client": dbutils.widgets.get("client")})()

from pyspark.sql import SparkSession  # noqa: E402

from keelstone.config import load_client  # noqa: E402
from keelstone.qa import GROUND_TRUTH_VOLUME, QA_CATALOG, QA_SCHEMA  # noqa: E402

spark = SparkSession.builder.getOrCreate()
c = load_client(a.client, config_dir=CONFIG_DIR)
stmts = [
    f"CREATE CATALOG IF NOT EXISTS {c.catalog}",
    f"CREATE SCHEMA IF NOT EXISTS {c.catalog}.{c.schema} COMMENT 'Client {c.client_id} (synthetic data)'",
    f"CREATE VOLUME IF NOT EXISTS {c.catalog}.{c.schema}.{c.landing_volume} COMMENT 'Landing zone'",
    f"CREATE VOLUME IF NOT EXISTS {c.catalog}.{c.schema}.{c.checkpoint_volume} COMMENT 'Auto Loader state'",
    f"CREATE SCHEMA IF NOT EXISTS {QA_CATALOG}.{QA_SCHEMA} COMMENT 'Ground truth and validation; pipelines never read it'",
    f"CREATE VOLUME IF NOT EXISTS {QA_CATALOG}.{QA_SCHEMA}.{GROUND_TRUTH_VOLUME}",
]
for s in stmts:
    print(s)
    spark.sql(s)
print("setup done")
