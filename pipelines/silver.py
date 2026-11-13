# Lakeflow declarative pipeline source for silver. All logic lives in the project
# wheel (keelstone.silver.pipeline) and is driven by the YAML configs; this file
# only reads the pipeline configuration and registers the datasets.
try:
    from pyspark import pipelines as dp
except ImportError:  # older runtimes
    import dlt as dp

from keelstone.silver.pipeline import define

define(dp, spark,  # noqa: F821  (spark is provided by the pipeline runtime)
       client_id=spark.conf.get("keelstone.client"),  # noqa: F821
       config_dir=spark.conf.get("keelstone.config_dir"))  # noqa: F821
