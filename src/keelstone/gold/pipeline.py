"""Gold, in the same Lakeflow declarative pipeline as silver (materialized views).

  gold_reconciliation         book of record vs custodians per account, security, date:
                              status (matched | break | pending | explained_upstream) + reason_code
  gold_break_queue            what ops must act on: breaks and pending
  gold_positions              the trusted position book per client and date: the book of record,
                              valued at closing prices, converted to USD, with its recon status
  gold_exposure_asset_class   market value in USD by asset class, per client and date
  gold_exposure_currency      market value (local and USD) by currency, per client and date
  gold_tieout_daily           counts per status and the tie-out rate, per client and date

Materialized views are recomputed from silver, so re-running never changes them for the same data.
"""
from __future__ import annotations

from keelstone.config import ClientConfig, SourceConfig
from keelstone.gold.rules import compare_sql, status_sql
from keelstone.silver.mapping import identifier_expr

KEY = ["client_id", "account_id", "security_id", "business_date"]


def define_gold(dp, spark, client: ClientConfig, sources: list[SourceConfig], mv) -> None:
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    tol = client.reconciliation
    custodians = [s for s in sources if s.silver and s.silver.is_positions and s.silver.position_side == "custodian"]

    def rejected_rows():
        """Bronze-quarantined custodian rows, with whatever key parts can still be read."""
        files = (spark.read.table("silver_position_checks")
                 .select(F.col("source_file").alias("file_path"), F.col("business_date")).distinct())
        ids = spark.read.table("silver_identifier_map").select("identifier_type", "identifier", "security_id")
        out = None
        for src in custodians:
            schema = ", ".join(f"`{c}` {src.column_types.get(c, 'string')}" for c in src.columns)
            acct = src.silver.fields["account_id"]
            df = (spark.read.table(client.table("bronze_quarantine"))
                  .where(F.col("source_id") == src.source_id)
                  .select("file_path", F.from_json("raw_record", schema).alias("r"))
                  .select("file_path", "r.*")
                  .select("file_path",
                          F.expr(f"nullif(trim(`{acct}`), '')").alias("account_id"),
                          F.expr(f"nullif({identifier_expr(src)}, '')").alias("identifier"))
                  .withColumn("identifier_type", F.lit(src.silver.identifier_type))
                  .withColumn("custodian", F.lit(src.source_id)))
            out = df if out is None else out.unionByName(df)
        if out is None:
            return None
        return (out.join(files, "file_path", "left")
                .join(ids, ["identifier_type", "identifier"], "left")
                .select("custodian", "business_date", "account_id", "security_id"))

    @mv(name="gold_reconciliation", comment="Book of record vs custodians: status and reason per position")
    def gold_reconciliation():
        pos = spark.read.table("silver_positions")
        checks = spark.read.table("silver_position_checks").where(F.col("position_side") == "custodian")
        prices = spark.read.table("silver_prices").select(
            F.col("price_date").alias("business_date"), "security_id", "close_price")

        book = pos.where(F.col("position_side") == "book").select(
            *KEY, F.col("quantity").alias("book_quantity"), F.col("currency").alias("book_currency"))
        cust = pos.where(F.col("position_side") == "custodian").select(
            *KEY, F.col("source_id").alias("custodian_source"), F.col("quantity").alias("custodian_quantity"),
            F.col("price").alias("custodian_price"), F.col("market_value").alias("custodian_market_value"))
        # Which custodian holds each account, and which (custodian, date) files have arrived.
        acct = checks.select("account_id", F.col("source_id").alias("account_custodian")).distinct()
        arrived = checks.select(F.col("source_id").alias("custodian"), "business_date").distinct() \
            .withColumn("file_arrived", F.lit(True))

        j = (book.join(cust, KEY, "full_outer")
             .join(acct, "account_id", "left")
             .withColumn("custodian", F.coalesce("custodian_source", "account_custodian"))
             .join(prices, ["business_date", "security_id"], "left")
             .withColumn("book_market_value", F.round(F.col("book_quantity") * F.col("close_price"), 2)
                         .cast("decimal(38,2)"))
             .join(arrived, ["custodian", "business_date"], "left"))

        # Upstream explanations for book positions with no custodian row in silver.
        squar = (spark.read.table("silver_quarantine").select(*KEY).distinct()
                 .withColumn("in_silver_quarantine", F.lit(True)))
        unres = (spark.read.table("unresolved_securities")
                 .select("account_id", "business_date", F.col("quantity").alias("book_quantity"),
                         F.col("source_id").alias("custodian")).distinct()
                 .withColumn("in_unresolved", F.lit(True)))
        j = j.join(squar, KEY, "left").join(unres, ["account_id", "business_date", "book_quantity", "custodian"], "left")

        rej = rejected_rows()
        if rej is not None:
            exact = (rej.where(F.col("security_id").isNotNull()).select("custodian", "business_date", "account_id",
                                                                       "security_id").distinct()
                     .withColumn("rejected_exact", F.lit(True)))
            j = j.join(exact, ["custodian", "business_date", "account_id", "security_id"], "left")
        else:
            j = j.withColumn("rejected_exact", F.lit(None).cast("boolean"))

        book_only = F.col("custodian_quantity").isNull()
        cust_only = F.col("book_quantity").isNull()
        reason = (F.when(cust_only, F.lit("missing_in_book"))
                  .when(book_only & F.col("file_arrived").isNull(), F.lit("pending_late_file"))
                  .when(book_only & F.col("in_silver_quarantine"), F.lit("custodian_row_quarantined"))
                  .when(book_only & F.col("in_unresolved"), F.lit("custodian_identifier_unresolved"))
                  .when(book_only & F.col("rejected_exact"), F.lit("custodian_row_rejected"))
                  .when(book_only, F.lit("missing_at_custodian"))
                  .otherwise(F.expr(compare_sql(tol))))
        j = j.withColumn("reason_code", reason).withColumn("explanation_basis",
                                                           F.when(F.col("reason_code") == "custodian_row_rejected",
                                                                  F.lit("exact")))

        # Rejected rows whose security could not be read: attribute by count to the remaining
        # missing_at_custodian rows of the same custodian, date and account (or date only).
        if rej is not None:
            loose = rej.where(F.col("security_id").isNull())
            for part in (["custodian", "business_date", "account_id"], ["custodian", "business_date"]):
                n = (loose.where(F.col("account_id").isNotNull()) if len(part) == 3
                     else loose.where(F.col("account_id").isNull())).groupBy(*part).agg(F.count(F.lit(1)).alias("n_loose"))
                w = Window.partitionBy(*part, "reason_code").orderBy("security_id")   # ranks within candidates
                cand = F.col("reason_code") == "missing_at_custodian"
                j = (j.withColumn("_rn", F.when(cand, F.row_number().over(w)))
                     .join(n, part, "left")
                     .withColumn("_hit", F.coalesce(cand & (F.col("_rn") <= F.col("n_loose")), F.lit(False)))
                     .withColumn("reason_code", F.when(F.col("_hit"), F.lit("custodian_row_rejected"))
                                 .otherwise(F.col("reason_code")))
                     .withColumn("explanation_basis", F.when(F.col("_hit"), F.lit("count"))
                                 .otherwise(F.col("explanation_basis")))
                     .drop("_rn", "n_loose", "_hit"))
        return (j.withColumn("status", F.expr(status_sql()))
                .withColumn("quantity_difference", F.col("custodian_quantity") - F.col("book_quantity"))
                .withColumn("value_difference", F.col("custodian_market_value") - F.col("book_market_value"))
                .select(*KEY, "custodian", "status", "reason_code", "explanation_basis",
                        "book_quantity", "custodian_quantity", "quantity_difference", "close_price",
                        "book_market_value", "custodian_price", "custodian_market_value", "value_difference",
                        "book_currency"))

    @mv(name="gold_break_queue", comment="Breaks and pending positions for the operations team")
    def gold_break_queue():
        return spark.read.table("gold_reconciliation").where(F.col("status").isin("break", "pending"))

    @mv(name="gold_positions",
        comment="Trusted position book per client and date: book of record valued in local currency and USD")
    def gold_positions():
        pos = spark.read.table("silver_positions").where(F.col("position_side") == "book")
        master = spark.read.table("silver_security_master").select("security_id", "name", "asset_class")
        prices = spark.read.table("silver_prices").select(F.col("price_date").alias("business_date"),
                                                          "security_id", "close_price")
        fx = spark.read.table("silver_fx").select(F.col("rate_date").alias("business_date"), "currency",
                                                  "usd_per_unit")
        recon = spark.read.table("gold_reconciliation").select(*KEY, "custodian", "status", "reason_code")
        return (pos.select(*KEY, "quantity", "currency")
                .join(master, "security_id", "left")
                .join(prices, ["business_date", "security_id"], "left")
                .join(fx, ["business_date", "currency"], "left")
                .withColumn("market_value_local", F.round(F.col("quantity") * F.col("close_price"), 2).cast("decimal(38,2)"))
                .withColumn("market_value_usd",
                            F.round(F.col("market_value_local") * F.col("usd_per_unit"), 2).cast("decimal(38,2)"))
                .join(recon, KEY, "left")
                .withColumnRenamed("status", "recon_status").withColumnRenamed("reason_code", "recon_reason"))

    @mv(name="gold_exposure_asset_class", comment="Market value in USD by asset class, per client and date")
    def gold_exposure_asset_class():
        g = (spark.read.table("gold_positions").groupBy("client_id", "business_date", "asset_class")
             .agg(F.sum("market_value_usd").alias("market_value_usd"), F.count(F.lit(1)).alias("positions")))
        w = Window.partitionBy("client_id", "business_date")
        return (g.withColumn("total_usd", F.sum("market_value_usd").over(w))
                .withColumn("weight", F.round(F.expr("try_divide(market_value_usd, total_usd)"), 6))
                .drop("total_usd"))

    @mv(name="gold_exposure_currency", comment="Market value by currency (local and USD), per client and date")
    def gold_exposure_currency():
        g = (spark.read.table("gold_positions").groupBy("client_id", "business_date", "currency")
             .agg(F.sum("market_value_local").alias("market_value_local"),
                  F.sum("market_value_usd").alias("market_value_usd"), F.count(F.lit(1)).alias("positions")))
        w = Window.partitionBy("client_id", "business_date")
        return (g.withColumn("total_usd", F.sum("market_value_usd").over(w))
                .withColumn("weight", F.round(F.expr("try_divide(market_value_usd, total_usd)"), 6))
                .drop("total_usd"))

    @mv(name="gold_tieout_daily", comment="Reconciliation counts and tie-out rate per client and date")
    def gold_tieout_daily():
        r = spark.read.table("gold_reconciliation")
        cnt = lambda s: F.sum(F.when(F.col("status") == s, 1).otherwise(0)).cast("bigint")  # noqa: E731
        return (r.groupBy("client_id", "business_date")
                .agg(F.count(F.lit(1)).alias("positions_compared"), cnt("matched").alias("matched"),
                     cnt("break").alias("breaks"), cnt("pending").alias("pending"),
                     cnt("explained_upstream").alias("explained_upstream"))
                .withColumn("tie_out_rate", F.round(F.expr("try_divide(matched, positions_compared - pending)"), 6)))
