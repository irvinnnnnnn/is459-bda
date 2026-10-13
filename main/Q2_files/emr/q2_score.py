"""Q2 nightly scoring on transient EMR (spec 4.4 and 4.5). All Q2 decision logic lives here.

Reads   curated/customer_txns/ (the 7 days ending on --run_date), decisions/, the previous scores/
        partition and state/cp_first_seen/.
Writes  scores/run_date=<d>/      one row per customer: score, tier, rules fired, in_queue, queue_reason
        rule_hits/run_date=<d>/   one row per (customer, rule fired) with its evidence sentence
        rule_eval/run_date=<d>/   per-rule and per-tier precision against is_laundering
        state/cp_first_seen/through=<d>/   when each customer first dealt with each counterparty (R9)

is_laundering is used for rule_eval only. It is never an input to a rule, a score or a tier.

Arguments: --bucket --our_bank --run_date [--struct_threshold_usd 14300] [--score_jump 20]
"""
import json
from datetime import date, timedelta

import boto3
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

from q2_common import s3_dates
from spark_common import get_args

WINDOW_DAYS, RECENT_DAYS = 7, 3
SMALL_TYPES = ["Country", "Individual", "Direct"]          # too few for peer percentiles
# rule_id: (name, points). Listed highest points first, which is the order hits are shown in.
RULES = [("R1", "Rapid pass-through", 25), ("R2", "Round trip", 20), ("R6", "Structuring below the threshold", 20),
         ("R3", "Collection from many senders", 15), ("R4", "Gather and scatter", 15),
         ("R7", "Activity out of line with profile", 15), ("R12", "Linked to customers needing EDD", 15),
         ("R5", "Dispersal to many recipients", 10), ("R8", "Cash or crypto concentration", 10),
         ("R9", "New-counterparty burst", 10), ("R10", "Own-account shuffling", 10), ("R11", "Currency churn", 10)]
NAME = {r: n for r, n, _ in RULES}


def usd(c):
    return F.concat(F.lit("$"), F.format_number(c, 0))


def pct(c):
    return F.format_string("%.0f%%", c * 100)


def read_or_none(spark, path):
    try:
        df = spark.read.parquet(path)
        return df if df.head(1) else None
    except AnalysisException:
        return None


def legs_of(tx, bank):
    """One row per (customer of ours, payment side). Amounts are USD."""
    common = ["txn_id", "ts", "date", F.col("payment_format").alias("fmt"),
              F.coalesce(F.col("is_laundering"), F.lit(0)).alias("laundering"),
              (F.col("from_customer_id") == F.col("to_customer_id")).alias("internal"),
              (F.col("payment_currency") != F.col("receiving_currency")).alias("converted")]
    out = tx.where(F.col("from_bank") == bank).select(
        F.col("from_customer_id").alias("customer_id"), F.col("from_entity_type").alias("etype"),
        F.col("from_account").alias("account"), F.lit("out").alias("direction"),
        F.col("to_customer_id").alias("cp"), F.col("amount_paid_usd").alias("amount"),
        F.col("payment_currency").alias("currency"), *common)
    inn = tx.where(F.col("to_bank") == bank).select(
        F.col("to_customer_id").alias("customer_id"), F.col("to_entity_type").alias("etype"),
        F.col("to_account").alias("account"), F.lit("in").alias("direction"),
        F.col("from_customer_id").alias("cp"), F.col("amount_received_usd").alias("amount"),
        F.col("receiving_currency").alias("currency"), *common)
    return out.unionByName(inn)


def latest_decisions(spark, base):
    """Current decision per customer: the latest edd_needed / cleared / reopened event (spec 4.3)."""
    ev = read_or_none(spark, f"{base}/decisions/")
    if ev is None:
        return spark.createDataFrame([], "customer_id string, decision string, cleared_until string, "
                                         "snapshot_score double, snapshot_rules string")
    w = Window.partitionBy("customer_id").orderBy(F.col("at").desc(), F.col("written_at").desc())
    return (ev.where(F.col("event").isin("edd_needed", "cleared", "reopened"))
            .withColumn("_n", F.row_number().over(w)).where("_n = 1").where(F.col("event") != "reopened")
            .select("customer_id", F.col("event").alias("decision"), "cleared_until", "snapshot_score", "snapshot_rules"))


def first_seen(spark, s3, bucket, base, tx_all, bank, run):
    """state/cp_first_seen: the earliest date each customer dealt with each counterparty."""
    snaps = [d for d in s3_dates(s3, bucket, "state/cp_first_seen/", "through") if d < run]
    new = tx_all
    old = None
    if snaps:
        old = spark.read.parquet(f"{base}/state/cp_first_seen/through={snaps[-1]}/")
        new = tx_all.where(F.col("date") > snaps[-1])
    seen = (legs_of(new, bank).where(~F.col("internal")).groupBy("customer_id", "cp").agg(F.min("date").alias("first_date")))
    if old is not None:
        seen = seen.unionByName(old).groupBy("customer_id", "cp").agg(F.min("first_date").alias("first_date"))
    seen = seen.cache()
    seen.write.mode("overwrite").parquet(f"{base}/state/cp_first_seen/through={run}/")
    for d in snaps[:-1]:                              # keep this run's snapshot and the one before
        for obj in boto3.resource("s3").Bucket(bucket).objects.filter(Prefix=f"state/cp_first_seen/through={d}/"):
            obj.delete()
    return seen


def main():
    a = get_args(["bucket", "our_bank", "run_date"], optional=["struct_threshold_usd", "score_jump"])
    bucket, base, bank, run = a["bucket"], f"s3://{a['bucket']}", str(int(a["our_bank"])), a["run_date"]
    threshold = float(a.get("struct_threshold_usd", 14300))     # S$20,000 (MAS Notice 626) in USD, Sept 2022
    band_lo, band_hi = 0.80 * threshold, 0.999 * threshold
    jump = float(a.get("score_jump", 20))
    run_d = date.fromisoformat(run)
    start = (run_d - timedelta(days=WINDOW_DAYS - 1)).isoformat()
    recent_start = (run_d - timedelta(days=RECENT_DAYS - 1)).isoformat()
    today = (run_d + timedelta(days=1)).isoformat()             # analysts work the day after the run

    spark = SparkSession.builder.appName(f"q2-score-{run}").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    s3 = boto3.client("s3")
    tx_all = (spark.read.parquet(f"{base}/curated/customer_txns/").where(F.col("date").cast("string") <= run)
              .withColumn("date", F.col("date").cast("string")).withColumn("ts", F.col("txn_ts").cast("long")))
    w = legs_of(tx_all.where(F.col("date") >= start), bank).cache()
    ext = w.where(~F.col("internal"))
    days = sorted(r[0] for r in w.select("date").distinct().collect())
    if not days:
        raise RuntimeError(f"No payments in curated/customer_txns/ for {start} to {run}")
    recent_days = max(1, sum(d >= recent_start for d in days))
    prior_days = sum(d < recent_start for d in days)
    complete = prior_days >= WINDOW_DAYS - RECENT_DAYS

    # ---- per-customer figures over the window
    is_in, recent = F.col("direction") == "in", F.col("date") >= recent_start
    cashy = F.col("fmt").isin("Cash", "Bitcoin") | (F.col("currency") == "Bitcoin")
    near = F.col("amount").between(band_lo, band_hi)
    amt = F.col("amount")
    f = ext.groupBy("customer_id").agg(
        F.first("etype", ignorenulls=True).alias("etype"),
        F.sum(F.when(is_in, amt).otherwise(0.0)).alias("inflow"), F.sum(F.when(~is_in, amt).otherwise(0.0)).alias("outflow"),
        F.sum(amt).alias("total"), F.sum(F.when(cashy, amt).otherwise(0.0)).alias("cash"),
        F.sum(F.when(recent, amt).otherwise(0.0)).alias("recent_v"), F.sum(F.when(~recent, amt).otherwise(0.0)).alias("prior_v"),
        F.sum(F.when(F.col("converted"), amt).otherwise(0.0)).alias("conv"),
        F.sum(near.cast("int")).alias("near_n"), F.sum(F.when(near, amt).otherwise(0.0)).alias("near_v"),
        F.countDistinct("txn_id").alias("payments"), F.max("laundering").alias("laundering"),
        F.countDistinct(F.when(is_in, F.col("cp"))).alias("senders"),
        F.countDistinct(F.when(~is_in, F.col("cp"))).alias("recipients"))
    accounts = w.groupBy("customer_id").agg(F.sort_array(F.collect_set("account")).alias("accounts"))

    # R1: median hours from a receipt to the next payment out
    nxt = Window.partitionBy("customer_id").orderBy("ts").rangeBetween(0, Window.unboundedFollowing)
    r1 = (ext.withColumn("next_out", F.min(F.when(~is_in, F.col("ts"))).over(nxt)).where(is_in & F.col("next_out").isNotNull())
          .groupBy("customer_id").agg(F.expr("percentile((next_out - ts) / 3600.0, 0.5)").alias("median_hours")))
    # R2: paid a counterparty >= $1k and >= 80% came back from the same counterparty within 7 days
    o = ext.where(~is_in & (amt >= 1000)).select("customer_id", "cp", "txn_id", F.col("amount").alias("a_o"), F.col("ts").alias("t_o"))
    i = ext.where(is_in).select("customer_id", "cp", F.col("amount").alias("a_i"), F.col("ts").alias("t_i"))
    r2 = (o.join(i, ["customer_id", "cp"])
          .where((F.col("t_i") > F.col("t_o")) & (F.col("t_i") - F.col("t_o") <= 7 * 86400) & (F.col("a_i") >= 0.8 * F.col("a_o")))
          .groupBy("customer_id").agg(F.countDistinct("txn_id").alias("round_trips")))
    # R9: counterparties in the recent period that the customer had never dealt with before it
    seen = first_seen(spark, s3, bucket, base, tx_all, bank, run)
    r9 = (ext.where(recent).select("customer_id", "cp").distinct().join(seen, ["customer_id", "cp"], "left")
          .groupBy("customer_id").agg(F.count("*").alias("recent_cps"),
                                      F.sum((F.col("first_date") >= recent_start).cast("int")).alias("new_cps")))
    # R10: transfers of $5k+ between the customer's own accounts
    r10 = (w.where(F.col("internal") & (F.col("direction") == "out") & (amt >= 5000))
           .groupBy("customer_id").agg(F.count("*").alias("own_transfers")))
    # R12: counterparties whose latest decision is "EDD needed" (analyst decisions, not last run's tier)
    dec = latest_decisions(spark, base).cache()
    edd = dec.where(F.col("decision") == "edd_needed").select(F.col("customer_id").alias("cp"))
    r12 = (ext.join(F.broadcast(edd), "cp", "left_semi").where(F.col("cp") != F.col("customer_id"))
           .groupBy("customer_id").agg(F.countDistinct("cp").alias("edd_links")))

    f = f.join(accounts, "customer_id", "left")
    for part in (r1, r2, r9, r10, r12):
        f = f.join(part, "customer_id", "left")
    f = (f.fillna(0, ["round_trips", "recent_cps", "new_cps", "own_transfers", "edd_links"])
         .withColumn("entity_type", F.when(F.col("etype").isin(SMALL_TYPES) | F.col("etype").isNull(), "Other").otherwise(F.col("etype")))
         .withColumn("pass_through", F.when(F.col("inflow") > 0, F.col("outflow") / F.col("inflow")))
         .withColumn("new_share", F.when(F.col("recent_cps") > 0, F.col("new_cps") / F.col("recent_cps")).otherwise(0.0))
         .withColumn("spike", F.when((F.col("prior_v") > 0) & F.lit(prior_days > 0),
                                     (F.col("recent_v") / recent_days) / (F.col("prior_v") / max(prior_days, 1))))
         .withColumn("cash_share", F.when(F.col("total") > 0, F.col("cash") / F.col("total")).otherwise(0.0))
         .withColumn("conv_share", F.when(F.col("total") > 0, F.col("conv") / F.col("total")).otherwise(0.0)))

    # peer percentiles within the entity type
    def rank(c):
        return F.percent_rank().over(Window.partitionBy("entity_type").orderBy(F.coalesce(F.col(c), F.lit(0.0))))
    comp = ["pass_through", "senders", "recipients", "new_share", "spike", "cash_share", "total"]
    for c in set(comp) | {"inflow"}:
        f = f.withColumn(c + "_pct", rank(c))
    f = f.withColumn("peer", F.round(sum(F.col(c + "_pct") for c in comp) / len(comp), 4))

    # ---- rules: (condition, evidence sentence)
    c = F.col
    fan_in = (c("senders") >= 10) | ((c("senders_pct") >= 0.99) & (c("senders") >= 5))
    spike = F.lit(complete) & (c("spike") >= 5) & (c("recent_v") >= 10000)
    s = lambda n, col: F.when(col == 1, F.lit(n)).otherwise(F.lit(n + "s"))
    checks = {
        "R1": ((c("inflow") >= 10000) & c("pass_through").between(0.9, 1.1) & (c("median_hours") <= 48),
               F.concat(pct(c("pass_through")), F.lit(" of the "), usd(c("inflow")), F.lit(" received was sent on, typically "),
                        F.format_string("%.1f", c("median_hours")), F.lit(" hours after it arrived"))),
        "R2": (c("round_trips") >= 1,
               F.concat(c("round_trips").cast("string"), F.lit(" "), s("payment", c("round_trips")),
                        F.lit(" of $1,000 or more came back from the same counterparty within 7 days"))),
        "R3": (fan_in, F.concat(F.lit("Received money from "), c("senders").cast("string"), F.lit(" different senders in 7 days"))),
        "R4": (fan_in & (c("recipients") >= 10) & (c("pass_through") >= 0.8),
               F.concat(F.lit("Collected from "), c("senders").cast("string"), F.lit(" senders and paid "),
                        c("recipients").cast("string"), F.lit(" recipients, passing on "),
                        pct(F.least(c("pass_through"), F.lit(9.99))), F.lit(" of what came in"))),
        "R5": ((c("recipients") >= 10) | ((c("recipients_pct") >= 0.99) & (c("recipients") >= 5)),
               F.concat(F.lit("Paid "), c("recipients").cast("string"), F.lit(" different recipients in 7 days"))),
        "R6": ((c("near_n") >= 3) & (c("near_v") >= threshold),
               F.concat(c("near_n").cast("string"), F.lit(f" payments of ${band_lo:,.0f} to ${band_hi:,.0f}, just under the "
                                                         f"${threshold:,.0f} threshold, totalling "), usd(c("near_v")))),
        "R7": (spike | (c("inflow_pct") >= 0.99),
               F.when(spike, F.concat(F.lit("Daily value is "), F.format_string("%.1f", c("spike")),
                                      F.lit(" times the customer's level earlier in the week ("), usd(c("recent_v")),
                                      F.lit(f" in the last {RECENT_DAYS} days)")))
                .otherwise(F.concat(F.lit("Received "), usd(c("inflow")), F.lit(" in 7 days, in the top 1% for a "),
                                    F.lower(c("entity_type"))))),
        "R8": ((c("cash_share") >= 0.5) & (c("total") >= 10000),
               F.concat(pct(c("cash_share")), F.lit(" of the "), usd(c("total")), F.lit(" moved was cash or Bitcoin"))),
        "R9": (F.lit(complete) & (c("new_cps") >= 5) & (c("new_share") >= 0.7),
               F.concat(c("new_cps").cast("string"), F.lit(" of "), c("recent_cps").cast("string"),
                        F.lit(f" counterparties in the last {RECENT_DAYS} days are new to the customer"))),
        "R10": (c("own_transfers") >= 3,
                F.concat(c("own_transfers").cast("string"), F.lit(" transfers of $5,000 or more between the customer's own accounts"))),
        "R11": ((c("conv_share") >= 0.3) & (c("total") >= 10000),
                F.concat(pct(c("conv_share")), F.lit(" of the "), usd(c("total")), F.lit(" moved was converted between currencies"))),
        "R12": (c("edd_links") >= 1,
                F.concat(F.lit("Transacted this week with "), c("edd_links").cast("string"), F.lit(" "),
                         s("customer", c("edd_links")), F.lit(" an analyst marked as needing EDD"))),
    }
    hit = [F.when(F.coalesce(checks[r][0], F.lit(False)),
                  F.struct(F.lit(r).alias("rule"), F.lit(p).alias("points"), checks[r][1].alias("evidence")))
           for r, _, p in RULES]
    f = (f.withColumn("hits", F.filter(F.array(*hit), lambda x: x.isNotNull()))
         .withColumn("rules", F.transform("hits", lambda x: x["rule"]))
         .withColumn("points", F.aggregate("hits", F.lit(0), lambda acc, x: acc + x["points"]))
         .withColumn("score", F.round(c("points") + c("peer"), 2)).cache())

    # ---- tiers: capacity-based cut-offs, computed without a global sort (spec 4.4, scalability)
    c99, c90 = f.approxQuantile("score", [0.99, 0.90], 0.0001)
    f = f.withColumn("tier", F.when((c("score") >= c99) & (c("points") > 0), "High")
                     .when((c("score") >= c90) & (c("points") > 0), "Medium").otherwise("Low"))

    # ---- queue logic (spec 4.5)
    prev_dates = [d for d in s3_dates(s3, bucket, "scores/", "run_date") if d < run]
    if prev_dates:
        prev = (spark.read.parquet(f"{base}/scores/run_date={prev_dates[-1]}/")
                .select("customer_id", c("score").alias("prev_score"), c("tier").alias("prev_tier"),
                        c("in_queue").alias("prev_in_queue"), F.from_json("rules", "array<string>").alias("prev_rules")))
    else:
        prev = spark.createDataFrame([], "customer_id string, prev_score double, prev_tier string, "
                                         "prev_in_queue boolean, prev_rules array<string>")
    f = f.join(prev, "customer_id", "left").join(
        dec.withColumn("snapshot_rules", F.from_json("snapshot_rules", "array<string>")), "customer_id", "left")
    base_q = c("tier").isin("High", "Medium")
    was = F.coalesce(c("prev_in_queue"), F.lit(False))
    reason = (F.when(~base_q, "").when(was & (c("prev_tier") == c("tier")), F.concat(F.lit("Still "), c("tier")))
              .when(was, F.concat(F.lit("Moved to "), c("tier"))).otherwise("New to the queue"))
    names = F.create_map(*[x for r, n, _ in RULES for x in (F.lit(r), F.lit(n.lower()))])
    fresh = F.array_except("rules", F.coalesce(c("snapshot_rules"), F.array().cast("array<string>")))
    cleared_live = (c("decision") == "cleared") & (F.lit(today) < c("cleared_until"))
    jumped = c("score") >= c("snapshot_score") + jump
    f = (f.withColumn("in_queue",
                      F.when(c("decision") == "edd_needed", False)
                      .when(cleared_live, (F.size(fresh) > 0) | jumped).otherwise(base_q))
         .withColumn("queue_reason",
                     F.when(c("decision") == "edd_needed", "")
                     .when(cleared_live & (F.size(fresh) > 0),
                           F.concat(F.lit("New rule since review: "), F.array_join(F.transform(fresh, lambda r: names[r]), ", ")))
                     .when(cleared_live & jumped,
                           F.concat(F.lit("Score up "), F.format_string("%.0f", c("score") - c("snapshot_score")), F.lit(" points since review")))
                     .when(cleared_live, "")
                     .when((c("decision") == "cleared") & base_q, "Clearance expired").otherwise(reason))
         .withColumn("change", F.round(c("score") - c("prev_score"), 2))
         .withColumn("new_rules", F.when(c("prev_rules").isNotNull(), F.array_except("rules", c("prev_rules")))
                     .otherwise(F.array().cast("array<string>"))))

    # ---- write
    metrics = F.to_json(F.struct(F.round("inflow", 2).alias("inflow"), F.round("outflow", 2).alias("outflow"), "payments",
                                 "senders", "recipients", F.round("pass_through", 3).alias("pass_through"),
                                 F.round("median_hours", 1).alias("median_hours")))
    scores = f.select(
        "customer_id", "entity_type", F.size("accounts").alias("n_accounts"), F.to_json("accounts").alias("accounts"),
        "points", "peer", "score", "tier", F.to_json("rules").alias("rules"), F.to_json("hits").alias("hits"),
        "in_queue", "queue_reason", "prev_score", "change", F.to_json("new_rules").alias("new_rules"),
        metrics.alias("metrics"), c("laundering").cast("int").alias("laundering"),
        F.lit(start).alias("window_start"), F.lit(bool(complete)).alias("window_complete")).cache()
    scores.coalesce(4).write.mode("overwrite").parquet(f"{base}/scores/run_date={run}/")
    (f.select("customer_id", F.explode("hits").alias("h")).select("customer_id", "h.rule", "h.points", "h.evidence")
     .coalesce(1).write.mode("overwrite").parquet(f"{base}/rule_hits/run_date={run}/"))

    per_rule = (f.select(F.explode("rules").alias("key"), "laundering").groupBy("key")
                .agg(F.count("*").alias("customers"), F.sum("laundering").alias("with_laundering"))
                .withColumn("kind", F.lit("rule")))
    per_tier = (f.groupBy(c("tier").alias("key")).agg(F.count("*").alias("customers"), F.sum("laundering").alias("with_laundering"))
                .withColumn("kind", F.lit("tier")))
    (per_rule.unionByName(per_tier).withColumn("precision", c("with_laundering") / c("customers"))
     .select("kind", "key", "customers", c("with_laundering").cast("long").alias("with_laundering"), "precision")
     .coalesce(1).write.mode("overwrite").parquet(f"{base}/rule_eval/run_date={run}/"))

    q = scores.agg(F.count("*"), F.sum((c("points") > 0).cast("int")), F.sum(c("in_queue").cast("int"))).first()
    print(json.dumps({"run_date": run, "window_start": start, "window_complete": bool(complete), "customers": q[0],
                      "with_a_rule": q[1], "in_queue": q[2], "high_cutoff": c99, "medium_cutoff": c90}))


if __name__ == "__main__":
    main()
