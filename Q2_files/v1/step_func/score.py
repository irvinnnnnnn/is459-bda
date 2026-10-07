"""
IS459 G2 - Q2 customer risk scoring (EMR Spark step).

Reads curated/ for the window ending on run_date, keeps only our bank's view,
rolls accounts up to customers (Entity ID), applies the rules, and writes:
  results/customer_scores/run_date=<d>/
  results/rule_hits/run_date=<d>/
  state/cp_first_seen/run_date=<d>/      (counterparty history for R7)

Usage:
  spark-submit score.py --bucket <bucket|/local/path> --run-date 2022-09-07 --bank-id 12 \
                        [--window-days 7] [--recent-days 3]
"""
import argparse
import math
from datetime import date, timedelta

from pyspark.sql import SparkSession, functions as F, Window
from pyspark.sql.utils import AnalysisException

RULES = {   # rule_id: (name, points)
    "R1": ("Rapid pass-through", 25),
    "R2": ("Round-trip (U-turn)", 20),
    "R3": ("Cash structuring", 20),
    "R4": ("Fan-in", 15),
    "R5": ("Fan-out", 15),
    "R6": ("Linked to High-risk customers", 15),
    "R7": ("New-counterparty burst", 10),
    "R8": ("Activity spike", 10),
    "R9": ("Cash/crypto concentration", 10),
    "R13": ("Own-account shuffling", 10),
}
HIGH_PCT, MEDIUM_PCT = 0.01, 0.10


def parse():
    p = argparse.ArgumentParser()
    p.add_argument("--bucket", required=True)
    p.add_argument("--run-date", required=True)
    p.add_argument("--bank-id", required=True)
    p.add_argument("--window-days", type=int, default=7)
    p.add_argument("--recent-days", type=int, default=3)
    return p.parse_args()


def base_path(bucket):
    return bucket if bucket.startswith("/") else f"s3://{bucket}"


def latest_before(spark, path, part_col, run_date):
    """Return the partition of `path` with the largest part_col < run_date, or None."""
    try:
        df = spark.read.parquet(path)
    except AnalysisException:
        return None
    prev = df.where(F.col(part_col) < run_date).agg(F.max(part_col)).first()[0]
    return None if prev is None else df.where(F.col(part_col) == prev)


def main():
    a = parse()
    base, bank = base_path(a.bucket), str(int(a.bank_id))
    run = date.fromisoformat(a.run_date)
    start = (run - timedelta(days=a.window_days - 1)).isoformat()
    recent_start = (run - timedelta(days=a.recent_days - 1)).isoformat()
    run_s = run.isoformat()

    spark = SparkSession.builder.appName(f"aml-q2-score-{run_s}").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")  # keep date/run_date as strings

    cur = (spark.read.parquet(f"{base}/curated/")
           .where((F.col("date") >= start) & (F.col("date") <= run_s))
           .where((F.col("from_bank") == bank) | (F.col("to_bank") == bank)))     # our bank's view

    # ---------- customers = entities owning accounts at our bank
    acc = spark.read.csv(f"{base}/reference/accounts/", header=True)
    etype = F.regexp_replace(F.col("Entity Name"), r"\s*#\d+$", "")
    etype = F.when(etype.isin("Country", "Individual", "Direct"), "Other").otherwise(etype)
    customers = (acc.where(F.col("Bank ID").cast("int") == int(bank))
                 .groupBy(F.col("Entity ID").alias("entity_id"))
                 .agg(F.first(etype).alias("entity_type"), F.count("*").alias("n_accounts")))

    # ---------- one row per (customer, transaction side)
    common = ["txn_id", "txn_ts", "date", "payment_format", "is_laundering"]
    out_legs = cur.where(F.col("from_bank") == bank).select(
        F.col("from_entity_id").alias("entity_id"), F.lit("out").alias("direction"),
        F.col("to_entity_id").alias("cp_entity"), F.col("to_bank").alias("cp_bank"),
        F.col("amount_paid_usd").alias("amount"), F.col("payment_currency").alias("currency"), *common,
        (F.col("from_entity_id") == F.col("to_entity_id")).alias("internal"))
    in_legs = cur.where(F.col("to_bank") == bank).select(
        F.col("to_entity_id").alias("entity_id"), F.lit("in").alias("direction"),
        F.col("from_entity_id").alias("cp_entity"), F.col("from_bank").alias("cp_bank"),
        F.col("amount_received_usd").alias("amount"), F.col("receiving_currency").alias("currency"), *common,
        (F.col("from_entity_id") == F.col("to_entity_id")).alias("internal"))
    legs = out_legs.unionByName(in_legs).where(F.col("entity_id").isNotNull()).cache()
    ext = legs.where(~F.col("internal"))

    dates_in_window = [r[0] for r in cur.select("date").distinct().collect()]
    recent_days_present = max(1, sum(d >= recent_start for d in dates_in_window))
    prior_days_present = sum(d < recent_start for d in dates_in_window)
    behaviour_rules_on = prior_days_present >= (a.window_days - a.recent_days)

    is_in, is_out = F.col("direction") == "in", F.col("direction") == "out"
    is_recent = F.col("date") >= recent_start
    cash_crypto = F.col("payment_format").isin("Cash", "Bitcoin") | (F.col("currency") == "Bitcoin")

    base_feats = ext.groupBy("entity_id").agg(
        F.sum(F.when(is_in, F.col("amount")).otherwise(0)).alias("inflow_usd"),
        F.sum(F.when(is_out, F.col("amount")).otherwise(0)).alias("outflow_usd"),
        F.sum("amount").alias("total_value"),
        F.countDistinct(F.when(is_in, F.col("cp_entity"))).alias("distinct_senders"),
        F.countDistinct(F.when(is_out, F.col("cp_entity"))).alias("distinct_recipients"),
        F.sum(F.when(cash_crypto, F.col("amount")).otherwise(0)).alias("cash_crypto_value"),
        F.sum(F.when(is_recent, F.col("amount")).otherwise(0)).alias("recent_value"),
        F.sum(F.when(~is_recent, F.col("amount")).otherwise(0)).alias("prior_value"),
        F.sum(F.when((F.col("payment_format") == "Cash") & F.col("amount").between(8000, 9999.99), 1)
              .otherwise(0)).alias("near_threshold_cash_n"),
        F.sum(F.when((F.col("payment_format") == "Cash") & F.col("amount").between(8000, 9999.99),
                     F.col("amount")).otherwise(0)).alias("near_threshold_cash_value"),
    )

    # R1: median hours from a receipt to the next outgoing payment
    w = Window.partitionBy("entity_id").orderBy("txn_ts", "direction").rowsBetween(1, Window.unboundedFollowing)
    lag = (ext.withColumn("next_out_ts", F.min(F.when(is_out, F.col("txn_ts"))).over(w))
              .where(is_in & F.col("next_out_ts").isNotNull())
              .withColumn("lag_h", (F.col("next_out_ts").cast("long") - F.col("txn_ts").cast("long")) / 3600.0)
              .groupBy("entity_id").agg(F.percentile_approx("lag_h", 0.5).alias("median_hours_to_forward")))

    # R2: paid a counterparty and >= 80% came back from the same counterparty within 7 days
    o = ext.where(is_out & (F.col("amount") >= 1000)).select(
        "entity_id", "cp_entity", F.col("txn_id").alias("o_id"), F.col("amount").alias("o_amt"), F.col("txn_ts").alias("o_ts"))
    i = ext.where(is_in).select(
        "entity_id", "cp_entity", F.col("amount").alias("i_amt"), F.col("txn_ts").alias("i_ts"))
    rt = (o.join(i, ["entity_id", "cp_entity"])
           .where((F.col("i_ts") > F.col("o_ts")) &
                  (F.col("i_ts").cast("long") - F.col("o_ts").cast("long") <= 7 * 86400) &
                  (F.col("i_amt") >= 0.8 * F.col("o_amt")))
           .groupBy("entity_id").agg(F.countDistinct("o_id").alias("round_trips")))

    # R6: counterparties who were High in the previous run
    prev_scores = latest_before(spark, f"{base}/results/customer_scores/", "run_date", run_s)
    if prev_scores is not None:
        prev_tiers = prev_scores.select("entity_id", F.col("tier").alias("prev_tier"))
        prev_high = prev_tiers.where(F.col("prev_tier") == "High").select(F.col("entity_id").alias("cp_entity"))
        exposure = (ext.join(prev_high, "cp_entity").where(F.col("cp_entity") != F.col("entity_id"))
                       .groupBy("entity_id").agg(F.countDistinct("cp_entity").alias("high_risk_counterparties")))
    else:
        prev_tiers = None
        exposure = None

    # R7: counterparties first seen in the recent period (history kept in state/)
    pairs = ext.groupBy("entity_id", "cp_entity").agg(F.min("date").alias("first_seen"))
    prev_hist = latest_before(spark, f"{base}/state/cp_first_seen/", "run_date", run_s)
    if prev_hist is not None:
        pairs = (pairs.unionByName(prev_hist.select("entity_id", "cp_entity", "first_seen"))
                      .groupBy("entity_id", "cp_entity").agg(F.min("first_seen").alias("first_seen")))
    pairs = pairs.cache()
    pairs.write.mode("overwrite").parquet(f"{base}/state/cp_first_seen/run_date={run_s}/")
    recent_cps = ext.where(is_recent).select("entity_id", "cp_entity").distinct()
    newness = (recent_cps.join(pairs, ["entity_id", "cp_entity"], "left")
               .groupBy("entity_id").agg(
                   F.count("*").alias("recent_counterparties"),
                   F.sum(F.when(F.col("first_seen") >= recent_start, 1).otherwise(0)).alias("new_counterparties")))

    # R13: transfers between the customer's own different accounts
    own = (legs.where(F.col("internal") & is_out & (F.col("amount") >= 5000))
               .groupBy("entity_id").agg(F.count("*").alias("own_account_transfers")))

    # ---------- assemble features
    f = customers.join(base_feats, "entity_id", "inner")
    for extra in [lag, rt, newness, own] + ([exposure] if exposure is not None else []):
        f = f.join(extra, "entity_id", "left")
    if exposure is None:
        f = f.withColumn("high_risk_counterparties", F.lit(0))
    f = (f.fillna(0, ["round_trips", "recent_counterparties", "new_counterparties",
                      "own_account_transfers", "high_risk_counterparties"])
          .withColumn("pass_through_ratio",
                      F.when(F.col("inflow_usd") > 0, F.col("outflow_usd") / F.col("inflow_usd")))
          .withColumn("new_counterparty_share",
                      F.when(F.col("recent_counterparties") > 0,
                             F.col("new_counterparties") / F.col("recent_counterparties")).otherwise(0))
          .withColumn("spike_ratio",
                      F.when((F.col("prior_value") > 0) & F.lit(prior_days_present > 0),
                             (F.col("recent_value") / recent_days_present) /
                             (F.col("prior_value") / max(prior_days_present, 1))))
          .withColumn("cash_crypto_share",
                      F.when(F.col("total_value") > 0, F.col("cash_crypto_value") / F.col("total_value")).otherwise(0)))

    # peer percentiles within entity type (used for fan-in/out and as a tie-breaker)
    peer = Window.partitionBy("entity_type")
    for c in ["distinct_senders", "distinct_recipients"]:
        f = f.withColumn(f"{c}_pct", F.percent_rank().over(peer.orderBy(c)))
    composite_cols = ["pass_through_ratio", "distinct_senders", "distinct_recipients",
                      "new_counterparty_share", "spike_ratio", "cash_crypto_share", "total_value"]
    for c in composite_cols:
        f = f.withColumn(f"_p_{c}", F.percent_rank().over(peer.orderBy(F.coalesce(F.col(c), F.lit(0)))))
    f = f.withColumn("peer_composite", sum(F.col(f"_p_{c}") for c in composite_cols) / len(composite_cols))

    money = lambda c: F.format_number(F.col(c), 0)
    checks = {
        "R1": ((F.col("inflow_usd") >= 10000) & F.col("pass_through_ratio").between(0.9, 1.1) &
               (F.col("median_hours_to_forward") <= 48),
               F.format_string("%s%% of $%s received was sent on, median %sh later",
                               F.round(F.col("pass_through_ratio") * 100, 0).cast("int"),
                               money("inflow_usd"), F.round("median_hours_to_forward", 1))),
        "R2": (F.col("round_trips") >= 1,
               F.format_string("%s payment(s) came back from the same counterparty within 7 days", F.col("round_trips"))),
        "R3": ((F.col("near_threshold_cash_n") >= 3) & (F.col("near_threshold_cash_value") >= 10000),
               F.format_string("%s cash payments of $8,000-9,999 totalling $%s",
                               F.col("near_threshold_cash_n"), money("near_threshold_cash_value"))),
        "R4": ((F.col("distinct_senders") >= 10) |
               ((F.col("distinct_senders_pct") >= 0.99) & (F.col("distinct_senders") >= 5)),
               F.format_string("Received from %s different senders", F.col("distinct_senders"))),
        "R5": ((F.col("distinct_recipients") >= 10) |
               ((F.col("distinct_recipients_pct") >= 0.99) & (F.col("distinct_recipients") >= 5)),
               F.format_string("Paid %s different recipients", F.col("distinct_recipients"))),
        "R6": (F.col("high_risk_counterparties") >= 2,
               F.format_string("Transacts with %s customers rated High last run", F.col("high_risk_counterparties"))),
        "R7": (F.lit(behaviour_rules_on) & (F.col("new_counterparties") >= 5) & (F.col("new_counterparty_share") >= 0.7),
               F.format_string("%s of %s recent counterparties are new",
                               F.col("new_counterparties"), F.col("recent_counterparties"))),
        "R8": (F.lit(behaviour_rules_on) & (F.col("spike_ratio") >= 5) & (F.col("recent_value") >= 10000),
               F.format_string("Daily value up %sx on the previous days ($%s recently)",
                               F.round("spike_ratio", 1), money("recent_value"))),
        "R9": ((F.col("cash_crypto_share") >= 0.5) & (F.col("total_value") >= 10000),
               F.format_string("%s%% of $%s moved in cash or Bitcoin",
                               F.round(F.col("cash_crypto_share") * 100, 0).cast("int"), money("total_value"))),
        "R13": (F.col("own_account_transfers") >= 3,
                F.format_string("%s transfers of $5k+ between own accounts", F.col("own_account_transfers"))),
    }
    hit_structs = [
        F.when(F.coalesce(cond, F.lit(False)),
               F.struct(F.lit(rid).alias("rule_id"), F.lit(RULES[rid][0]).alias("rule_name"),
                        F.lit(RULES[rid][1]).alias("points"), ev.alias("evidence")))
        for rid, (cond, ev) in checks.items()]
    f = (f.withColumn("hits", F.filter(F.array(*hit_structs), lambda x: x.isNotNull()))
          .withColumn("hits", F.expr("array_sort(hits, (l, r) -> case when l.points > r.points then -1 "
                                     "when l.points < r.points then 1 else 0 end)"))
          .withColumn("points", F.coalesce(F.aggregate("hits", F.lit(0), lambda acc, x: acc + x.points), F.lit(0)))
          .withColumn("score", F.round(F.col("points") + F.col("peer_composite"), 4)))

    # ---------- tiers: top 1% High, next 9% Medium (must have at least one rule triggered)
    n = f.count()
    n_high, n_med = max(1, math.ceil(HIGH_PCT * n)), max(1, math.ceil(MEDIUM_PCT * n))
    rank_w = Window.orderBy(F.col("score").desc(), F.col("entity_id"))
    f = (f.withColumn("rank", F.row_number().over(rank_w))
          .withColumn("score_percentile", F.round(1 - (F.col("rank") - 1) / F.lit(n), 4))
          .withColumn("tier", F.when((F.col("rank") <= n_high) & (F.col("points") > 0), "High")
                               .when((F.col("rank") <= n_med) & (F.col("points") > 0), "Medium")
                               .otherwise("Low")))
    if prev_tiers is not None:
        f = f.join(prev_tiers, "entity_id", "left")
    else:
        f = f.withColumn("prev_tier", F.lit(None).cast("string"))
    f = (f.withColumn("moved_to_high", (F.col("tier") == "High") & (F.coalesce(F.col("prev_tier"), F.lit("")) != "High"))
          .withColumn("reason_1", F.when(F.size("hits") >= 1,
                                         F.concat_ws(": ", F.col("hits")[0].rule_name, F.col("hits")[0].evidence)))
          .withColumn("reason_2", F.when(F.size("hits") >= 2,
                                         F.concat_ws(": ", F.col("hits")[1].rule_name, F.col("hits")[1].evidence)))
          .withColumn("rules_triggered", F.concat_ws(",", F.transform("hits", lambda x: x.rule_id)))
          .withColumn("window_start", F.lit(start)).withColumn("window_end", F.lit(run_s))
          .withColumn("window_complete", F.lit(behaviour_rules_on))
          .cache())

    scores = f.select(
        "entity_id", "entity_type", "n_accounts", "score", "score_percentile", "points", "tier", "prev_tier",
        "moved_to_high", "reason_1", "reason_2", "rules_triggered",
        F.round("inflow_usd", 2).alias("inflow_usd"), F.round("outflow_usd", 2).alias("outflow_usd"),
        F.round("pass_through_ratio", 3).alias("pass_through_ratio"),
        F.round("median_hours_to_forward", 1).alias("median_hours_to_forward"),
        "distinct_senders", "distinct_recipients",
        F.round("new_counterparty_share", 3).alias("new_counterparty_share"),
        F.round("spike_ratio", 2).alias("spike_ratio"),
        F.round("cash_crypto_share", 3).alias("cash_crypto_share"),
        "own_account_transfers", "high_risk_counterparties", "round_trips",
        "window_start", "window_end", "window_complete")
    scores.coalesce(1).write.mode("overwrite").parquet(f"{base}/results/customer_scores/run_date={run_s}/")

    hits = (f.select("entity_id", F.explode("hits").alias("h"))
             .select("entity_id", "h.rule_id", "h.rule_name", "h.points", "h.evidence"))
    hits.coalesce(1).write.mode("overwrite").parquet(f"{base}/results/rule_hits/run_date={run_s}/")

    tiers = {r["tier"]: r["count"] for r in scores.groupBy("tier").count().collect()}
    print(f"[score] {run_s} bank={bank} window={start}..{run_s} customers={n} tiers={tiers} "
          f"behaviour_rules_on={behaviour_rules_on}")


if __name__ == "__main__":
    main()
