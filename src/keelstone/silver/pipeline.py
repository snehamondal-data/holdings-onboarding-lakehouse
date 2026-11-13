"""Silver as a Lakeflow declarative pipeline, built only from config.

define(dp, spark, client_id, config_dir) registers, for the client's sources:
  silver_<reference>          typed reference tables (security master, prices, fx, trades),
                              AUTO CDC on each table's key
  silver_identifier_map       ISIN, CUSIP, ticker:exchange and security_id -> one security_id
  silver_position_checks      every mapped position row with its outcome; expectations here
                              are what the event log counts
  silver_positions            canonical positions, AUTO CDC (MERGE) on the natural key
  silver_quarantine           rows failing a silver rule (units_scale_suspect), keyed by row hash
  unresolved_securities       positions whose identifier maps to no single security
  silver_dedupe_log           exact duplicate rows removed, per file
  silver_key_conflicts        same natural key with different content (must stay empty)

AUTO CDC (SCD type 1) is the pipeline's MERGE: re-running on the same bronze data,
incrementally or as a full refresh, leaves the tables unchanged.
"""
from __future__ import annotations

from keelstone.config import load_client, load_source
from keelstone.silver.mapping import (IDENTIFIER_MAP_SQL, POSITION_KEY, QUARANTINE_KEY, UNITS_REASON,
                                      UNRESOLVED_KEY, position_select, reference_select, row_hash_expr,
                                      units_suspect_expr)

AUDIT_COLS = ["outcome", "quarantine_reason", "units_suspect", "n_matches"]


def _auto_cdc(dp):
    return getattr(dp, "create_auto_cdc_flow", None) or dp.apply_changes


def _mv(dp):
    """Materialized view decorator: pyspark.pipelines has materialized_view, older dlt uses table."""
    return getattr(dp, "materialized_view", None) or dp.table


def define(dp, spark, client_id: str, config_dir: str) -> None:
    from pyspark.sql import functions as F

    client = load_client(client_id, config_dir)
    sources = [load_source(s, config_dir) for s in client.sources]
    silvered = [s for s in sources if s.silver is not None]
    positions = [s for s in silvered if s.silver.is_positions]
    references = [s for s in silvered if not s.silver.is_positions]
    cdc = _auto_cdc(dp)
    mv = _mv(dp)
    bronze = lambda src: client.table(src.bronze_table)  # noqa: E731
    seq = F.struct("load_ts", "row_hash")

    # ---- reference tables -------------------------------------------------
    for src in references:
        def ref_view(src=src):
            df = spark.readStream.table(bronze(src))
            return df.selectExpr(*[f"{sql} AS `{alias}`" for alias, sql in reference_select(src, client_id)])
        dp.view(name=f"v_silver_{src.source_id}")(ref_view)
        dp.create_streaming_table(src.silver.table, comment=f"{src.display_name}, typed (synthetic data)")
        cdc(target=src.silver.table, source=f"v_silver_{src.source_id}", keys=list(src.silver.key),
            sequence_by=seq, stored_as_scd_type=1)

    # ---- security master resolution -------------------------------------
    @mv(name="silver_identifier_map",
        comment="Every identifier type mapped to one security_id; n_matches > 1 is ambiguous")
    def silver_identifier_map():
        return spark.sql(IDENTIFIER_MAP_SQL.format(master="silver_security_master"))

    # ---- positions ----------------------------------------------------------
    def mapped(src):
        df = spark.readStream.table(bronze(src))
        return df.selectExpr(*[f"{sql} AS `{alias}`" for alias, sql in position_select(src, client_id)])

    @dp.table(name="silver_position_checks",
              comment="Every mapped position row with its silver outcome (audit; expectations counted here)")
    @dp.expect("quantity_not_null", "quantity IS NOT NULL")
    @dp.expect("business_date_not_null", "business_date IS NOT NULL")
    @dp.expect("security_resolved", "security_id IS NOT NULL")
    @dp.expect("units_scale_sane", "NOT units_suspect")
    def silver_position_checks():
        rows = None
        for src in positions:
            rows = mapped(src) if rows is None else rows.unionByName(mapped(src))
        ids = spark.read.table("silver_identifier_map")
        j = rows.join(F.broadcast(ids), ["identifier_type", "identifier"], "left")
        j = j.withColumn("units_suspect", F.expr(units_suspect_expr()))
        outcome = (F.when(F.col("security_id").isNull(), F.lit("unresolved"))
                   .when(F.col("units_suspect"), F.lit("quarantine"))
                   .otherwise(F.lit("silver")))
        reason = (F.when(F.col("security_id").isNull(),
                         F.when(F.coalesce(F.col("n_matches"), F.lit(0)) > 1, F.lit("ambiguous_identifier"))
                         .otherwise(F.lit("unmapped_identifier")))
                  .when(F.col("units_suspect"), F.lit(UNITS_REASON)))
        return j.withColumn("outcome", outcome).withColumn("quarantine_reason", reason)

    def by_outcome(name: str, value: str):
        def v():
            return spark.readStream.table("silver_position_checks").where(F.col("outcome") == value)
        dp.view(name=name)(v)

    by_outcome("v_silver_ok", "silver")
    by_outcome("v_silver_quarantine", "quarantine")
    by_outcome("v_silver_unresolved", "unresolved")

    dp.create_streaming_table("silver_positions",
                              comment="Canonical positions, one row per client/source/account/security/date")
    cdc(target="silver_positions", source="v_silver_ok", keys=list(POSITION_KEY), sequence_by=seq,
        stored_as_scd_type=1, except_column_list=AUDIT_COLS)

    dp.create_streaming_table("silver_quarantine", comment="Rows that failed a silver rule, with the reason")
    cdc(target="silver_quarantine", source="v_silver_quarantine", keys=list(QUARANTINE_KEY), sequence_by=seq,
        stored_as_scd_type=1, except_column_list=["outcome", "n_matches"])

    dp.create_streaming_table("unresolved_securities",
                              comment="Positions whose identifier maps to no single security; never dropped")
    cdc(target="unresolved_securities", source="v_silver_unresolved", keys=list(UNRESOLVED_KEY),
        sequence_by=seq, stored_as_scd_type=1, except_column_list=["outcome", "units_suspect"])

    # ---- dedupe log and conflict check ---------------------------------------
    @mv(name="silver_dedupe_log",
                          comment="Exact duplicate bronze rows collapsed by silver, per file")
    def silver_dedupe_log():
        logs = None
        for src in silvered:
            if src.silver.dedupe != "exact":
                continue
            d = (spark.read.table(bronze(src))
                 .select(F.expr(row_hash_expr(src)).alias("row_hash"), F.col("_source_file").alias("source_file"))
                 .groupBy("source_file", "row_hash").count().where(F.col("count") > 1)
                 .select(F.lit(client_id).alias("client_id"), F.lit(src.source_id).alias("source_id"),
                         "source_file", "row_hash", (F.col("count") - 1).cast("bigint").alias("duplicates_removed")))
            logs = d if logs is None else logs.unionByName(d)
        return logs

    @mv(name="silver_key_conflicts",
                          comment="Natural keys seen with different content; must be empty")
    def silver_key_conflicts():
        return (spark.read.table("silver_position_checks").where(F.col("security_id").isNotNull())
                .groupBy(*POSITION_KEY).agg(F.countDistinct("row_hash").alias("versions"))
                .where(F.col("versions") > 1))

    # ---- gold (same pipeline) ------------------------------------------------
    from keelstone.gold.pipeline import define_gold
    define_gold(dp, spark, client, sources, mv)
