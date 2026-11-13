"""Generic bronze ingest: one Auto Loader stream per source, built only from config.

Nothing here names a particular source. For each StreamPlan:
  * Auto Loader reads new files once (checkpoint), every column as a string,
    unknown columns rescued into _rescued_data instead of failing;
  * foreachBatch hashes each new file's bytes; a file whose bytes were already
    loaded (a re-send under a new name) is logged as duplicate_content and skipped;
  * rows failing the config's rules go to bronze_quarantine with a reason;
  * clean rows go to the source's bronze table with source file, file hash and load time;
  * every file gets one row in bronze_file_log; header drift goes to bronze_schema_drift_log.
Each Delta write uses txnAppId/txnVersion so a retried micro-batch is not written twice.
"""
from __future__ import annotations

from dataclasses import asdict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from keelstone.config import StreamPlan
from keelstone.ingest.rules import quarantine_reason_sql

META_COLS = ["_rescued_data", "_corrupt_record", "_source_file", "_file_modified", "_file_size"]

FILE_LOG_DDL = """
  client_id STRING, source_id STRING, file_path STRING, file_name STRING,
  file_modified TIMESTAMP, file_size BIGINT, sha256 STRING, status STRING, duplicate_of STRING,
  rows_total BIGINT, rows_bronze BIGINT, rows_quarantined BIGINT,
  run_id STRING, batch_id BIGINT, load_ts TIMESTAMP"""
QUARANTINE_DDL = """
  client_id STRING, source_id STRING, file_path STRING, reason STRING, raw_record STRING,
  corrupt_record STRING, run_id STRING, batch_id BIGINT, load_ts TIMESTAMP"""
DRIFT_DDL = """
  client_id STRING, source_id STRING, file_path STRING, unexpected_columns ARRAY<STRING>,
  aliased_columns ARRAY<STRING>, run_id STRING, batch_id BIGINT, load_ts TIMESTAMP"""


def _norm(path: str) -> str:
    return path[len("dbfs:"):] if path.startswith("dbfs:") else path


def ensure_shared_tables(spark: SparkSession, plan: StreamPlan) -> None:
    for table, ddl in ((plan.file_log_table, FILE_LOG_DDL), (plan.quarantine_table, QUARANTINE_DDL),
                       (plan.drift_log_table, DRIFT_DDL)):
        spark.sql(f"CREATE TABLE IF NOT EXISTS {table} ({ddl}) USING DELTA")


def read_stream(spark: SparkSession, plan: StreamPlan) -> DataFrame:
    src = plan.source
    reader = spark.readStream.format("cloudFiles")
    for k, v in plan.reader_options.items():
        reader = reader.option(k, v)
    df = (reader.option("cloudFiles.schemaLocation", plan.schema_location)
          .schema(src.schema_ddl())
          .load(plan.input_path)
          .withColumn("_source_file", F.col("_metadata.file_path"))
          .withColumn("_file_modified", F.col("_metadata.file_modification_time"))
          .withColumn("_file_size", F.col("_metadata.file_size")))
    for col, names in src.column_aliases.items():
        df = df.withColumn(col, F.coalesce(F.col(col), *[
            F.get_json_object(F.col("_rescued_data"), f"$.{n}") for n in names]))
    return df


def make_batch_writer(plan_dict: dict, reason_sql: str, run_id: str):
    """Return the foreachBatch function. It closes over plain data only, so it
    pickles cleanly into the Spark Connect foreachBatch worker."""
    client_id = plan_dict["client_id"]
    source_id = plan_dict["source"]["source_id"]
    data_cols = list(plan_dict["source"]["columns"])
    bronze_table = plan_dict["bronze_table"]
    q_table, log_table, drift_table = (plan_dict["quarantine_table"], plan_dict["file_log_table"],
                                       plan_dict["drift_log_table"])
    aliases = sorted(a for names in plan_dict["source"]["column_aliases"].values() for a in names)

    def write_batch(batch_df: DataFrame, batch_id: int) -> None:
        from pyspark.sql import functions as F  # noqa: local import for the worker process

        spark = batch_df.sparkSession
        df = (batch_df.withColumn("_quarantine_reason", F.expr(reason_sql))
              .withColumn("_run_id", F.lit(run_id)).withColumn("_batch_id", F.lit(batch_id))
              .withColumn("_load_ts", F.current_timestamp()))

        per_file = (df.groupBy("_source_file", "_file_modified", "_file_size")
                    .agg(F.count(F.lit(1)).alias("rows_total"),
                         F.sum(F.when(F.col("_quarantine_reason").isNull(), 1).otherwise(0)).alias("rows_ok"))
                    .collect())
        if not per_file:
            return
        paths = [r["_source_file"] for r in per_file]
        hashes = {_norm(r[0]): r[1] for r in spark.read.format("binaryFile").load(paths)
                  .select("path", F.sha2(F.col("content"), 256)).collect()}
        known = {r[0]: r[1] for r in spark.table(log_table)
                 .where((F.col("source_id") == source_id) & (F.col("status") == "loaded"))
                 .select("sha256", "file_path").collect()}

        decisions = []          # (path, sha, status, duplicate_of)
        for r in sorted(per_file, key=lambda r: (r["_file_modified"], r["_source_file"])):
            p = r["_source_file"]
            sha = hashes[_norm(p)]
            # known[sha] == p only on a retried micro-batch whose file log already landed.
            if sha in known and _norm(known[sha]) != _norm(p):
                decisions.append((p, sha, "duplicate_content", known[sha]))
            else:
                known[sha] = p
                decisions.append((p, sha, "loaded", None))
        status_df = spark.createDataFrame(
            decisions, "_source_file string, _file_sha256 string, _file_status string, _duplicate_of string")
        df = df.join(F.broadcast(status_df), "_source_file")
        loaded = df.where(F.col("_file_status") == "loaded")

        w = lambda d, table: (d.write.format("delta").mode("append").option("mergeSchema", "true")
                              .option("txnAppId", f"{table}:{source_id}").option("txnVersion", batch_id)
                              .saveAsTable(table))

        good = loaded.where(F.col("_quarantine_reason").isNull())
        w(good.select(*data_cols, *META_COLS, "_file_sha256", "_run_id", "_batch_id", "_load_ts"),
          bronze_table)

        bad = loaded.where(F.col("_quarantine_reason").isNotNull())
        w(bad.select(F.lit(client_id).alias("client_id"), F.lit(source_id).alias("source_id"),
                     F.col("_source_file").alias("file_path"), F.col("_quarantine_reason").alias("reason"),
                     F.to_json(F.struct(*[F.col(c) for c in data_cols], "_rescued_data")).alias("raw_record"),
                     F.col("_corrupt_record").alias("corrupt_record"),
                     F.col("_run_id").alias("run_id"), F.col("_batch_id").alias("batch_id"),
                     F.col("_load_ts").alias("load_ts")),
          q_table)

        dec = {d[0]: d for d in decisions}
        log_rows = []
        for r in per_file:
            p = r["_source_file"]
            _, sha, status, dup_of = dec[p]
            ok = r["rows_ok"] if status == "loaded" else 0
            bad_n = (r["rows_total"] - r["rows_ok"]) if status == "loaded" else 0
            log_rows.append((client_id, source_id, _norm(p), _norm(p).rsplit("/", 1)[-1],
                             r["_file_modified"], r["_file_size"], sha, status,
                             _norm(dup_of) if dup_of else None, r["rows_total"], ok, bad_n,
                             run_id, batch_id))
        log_df = spark.createDataFrame(log_rows, ",".join(FILE_LOG_DDL.split(",")[:-1]))
        w(log_df.withColumn("load_ts", F.current_timestamp()), log_table)

        drift = (loaded.where(F.col("_rescued_data").isNotNull())
                 .select("_source_file", F.explode(F.json_object_keys("_rescued_data")).alias("col"))
                 .where(F.col("col") != "_file_path")
                 .groupBy("_source_file").agg(F.array_sort(F.collect_set("col")).alias("unexpected_columns")))
        w(drift.select(F.lit(client_id).alias("client_id"), F.lit(source_id).alias("source_id"),
                       F.col("_source_file").alias("file_path"), "unexpected_columns",
                       F.array_intersect("unexpected_columns",
                                         F.array(*[F.lit(a) for a in aliases]) if aliases
                                         else F.array().cast("array<string>")).alias("aliased_columns"),
                       F.lit(run_id).alias("run_id"), F.lit(batch_id).cast("bigint").alias("batch_id"),
                       F.current_timestamp().alias("load_ts")),
          drift_table)

    return write_batch


def start_source(spark: SparkSession, plan: StreamPlan, run_id: str):
    """Start the availableNow stream for one source and return it (not yet awaited)."""
    plan_dict = asdict(plan)
    writer = make_batch_writer(plan_dict, quarantine_reason_sql(plan.source), run_id)
    return (read_stream(spark, plan).writeStream
            .foreachBatch(writer)
            .option("checkpointLocation", plan.checkpoint_path)
            .trigger(availableNow=True)
            .queryName(f"bronze_{plan.client_id}_{plan.source.source_id}")
            .start())


def run_source(spark: SparkSession, plan: StreamPlan, run_id: str) -> None:
    """Process every new file for one source, then stop (availableNow)."""
    ensure_shared_tables(spark, plan)
    start_source(spark, plan, run_id).awaitTermination()


def run_sources(spark: SparkSession, plans: list, run_id: str, parallel: bool) -> None:
    """All sources of a client: one after another, or (performance.parallel_ingest) all at once.
    Each stream keeps its own checkpoint and bronze table; shared tables only receive appends."""
    if not plans:
        return
    if not parallel:
        for plan in plans:
            run_source(spark, plan, run_id)
        return
    ensure_shared_tables(spark, plans[0])
    queries = [start_source(spark, plan, run_id) for plan in plans]
    errors = []
    for q in queries:
        try:
            q.awaitTermination()
        except Exception as e:  # noqa: BLE001  wait for every stream, then report
            errors.append(f"{q.name}: {e}")
    if errors:
        raise RuntimeError("bronze streams failed: " + "; ".join(errors))
