"""Q1 Glue feature job, historical lane (spec 3.2): historical/ -> curated/historical/.

Computes the same features as Flink for the bank's labelled case history, with Spark window
functions over each account's past 7 days. Every figure uses only payments strictly before the
payment it describes. Also writes:
  state/flink_seed/            the last --seed_days of history as JSON lines, read by Flink at first
                               start to fill its state ("Seed Flink state", spec 3.2)
  state/parity/q1_features.json  the parity check: the same sample computed by this job and by
                               q1_core (the code Flink runs), and the largest difference

Arguments: --bucket --our_bank --live_start YYYY-MM-DD [--seed_days 7]
"""
import json

import boto3
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

import q1_core as core
from spark_common import apply_clean, get_args, write_dlq

D = core.DAY
IBM_COLS = ["ts_raw", "from_bank", "from_account", "to_bank", "to_account", "amount_received",
            "receiving_currency", "amount_paid", "payment_currency", "payment_format", "is_laundering"]


def add_features(tx):
    """tx: one row per payment with txn_id, ts (epoch s), seq, src, dst, banks, amounts, currencies."""
    pair = Window.partitionBy("src", "dst").orderBy("ts", "seq")
    prev = F.lag("ts").over(pair)
    tx = (tx.withColumn("_prev", prev)
          .withColumn("_prev", F.when(F.col("ts") - F.col("_prev") <= core.PAIR_DAYS * D, F.col("_prev")))
          .withColumn("new_pair", F.col("_prev").isNull().cast("double"))
          .withColumn("fresh", (F.col("_prev").isNull() |
                                (F.col("ts") - F.col("_prev") > core.FRESH_DAYS * D)).cast("double")))

    # every payment is an "out" event for its sender and an "in" event for its receiver
    out = tx.select("txn_id", "ts", "seq", F.col("src").alias("acct"), F.col("dst").alias("cp"),
                    F.lit(0).alias("d"), F.col("amount_paid").alias("amt"), "fresh")
    inn = tx.select("txn_id", "ts", "seq", F.col("dst").alias("acct"), F.col("src").alias("cp"),
                    F.lit(1).alias("d"), F.col("amount_received").alias("amt"), "fresh")
    ev = out.unionByName(inn)
    is_in, is_out = F.col("d") == 1, F.col("d") == 0
    for w, days in core.WINDOWS:
        win = Window.partitionBy("acct").orderBy("ts").rangeBetween(-(days * D - 1), -1)
        for tag, cond in (("o", is_out), ("i", is_in)):
            ev = (ev.withColumn(f"{tag}_n_{w}", F.coalesce(F.sum(F.when(cond, 1.0)).over(win), F.lit(0.0)))
                  .withColumn(f"{tag}_amt_{w}", F.coalesce(F.sum(F.when(cond, F.col("amt"))).over(win), F.lit(0.0)))
                  .withColumn(f"{tag}_fresh_{w}", F.coalesce(F.sum(F.when(cond, F.col("fresh"))).over(win), F.lit(0.0))))
    w7 = Window.partitionBy("acct").orderBy("ts").rangeBetween(-(7 * D - 1), -1)
    past = Window.partitionBy("acct").orderBy("ts").rangeBetween(Window.unboundedPreceding, -1)
    pair_past = Window.partitionBy("acct", "cp").orderBy("ts").rangeBetween(Window.unboundedPreceding, -1)
    ev = (ev.withColumn("i_distinct", F.size(F.collect_set(F.when(is_in, F.col("cp"))).over(w7)).cast("double"))
          .withColumn("o_distinct", F.size(F.collect_set(F.when(is_out, F.col("cp"))).over(w7)).cast("double"))
          .withColumn("_lin", F.max(F.when(is_in, F.struct("ts", "seq", "amt"))).over(past))
          .withColumn("_back", F.max(F.when(is_in, F.col("ts"))).over(pair_past)))

    s = ev.where(is_out).select(
        "txn_id",
        *[F.col(f"o_{c}_{w}").alias(f"s_out_{c}_{w}") for w, _ in core.WINDOWS for c in ("n", "amt", "fresh")],
        *[F.col(f"i_{c}_{w}").alias(f"s_in_{c}_{w}") for w, _ in core.WINDOWS for c in ("n", "amt", "fresh")],
        F.col("i_distinct").alias("s_in_distinct_7D"), F.col("o_distinct").alias("s_out_distinct_7D"),
        ((F.col("ts") - F.col("_lin.ts")) / 3600.0).alias("s_hrs_since_in"),
        F.when(F.col("_lin.amt") > 0, F.col("amt") / F.col("_lin.amt")).alias("amt_match"),
        F.when(F.col("ts") - F.col("_back") <= core.PAIR_DAYS * D,
               (F.col("ts") - F.col("_back")) / 3600.0).alias("cyc2_hrs"))
    s = (s.withColumn("s_burst", F.col("s_out_n_1D") / ((F.col("s_out_n_7D") - F.col("s_out_n_1D")) / 6.0 + 1.0)))
    r = ev.where(is_in).select(
        "txn_id",
        *[F.col(f"i_{c}_{w}").alias(f"r_in_{c}_{w}") for w, _ in core.WINDOWS for c in ("n", "amt", "fresh")],
        *[F.col(f"o_{c}_{w}").alias(f"r_out_{c}_{w}") for w, _ in core.WINDOWS for c in ("n", "amt", "fresh")],
        F.col("i_distinct").alias("r_in_distinct_7D"), F.col("o_distinct").alias("r_out_distinct_7D"))
    r = r.withColumn("r_burst", F.col("r_in_n_1D") / ((F.col("r_in_n_7D") - F.col("r_in_n_1D")) / 6.0 + 1.0))

    amt = F.col("amount_paid")
    full = (tx.join(s, "txn_id").join(r, "txn_id")
            .withColumn("s_pass_1D", F.when(F.col("s_in_amt_1D") > 0,
                                           (F.col("s_out_amt_1D") + amt) / F.col("s_in_amt_1D")).otherwise(0.0))
            .withColumn("log_amt", F.log1p(amt))
            .withColumn("round_amt", (amt % 100 == 0).cast("double"))
            .withColumn("just_under_10k", ((amt >= 9000) & (amt < 10000)).cast("double"))
            .withColumn("cross_bank", (F.col("from_bank") != F.col("to_bank")).cast("double"))
            .withColumn("self_transfer", (F.col("src") == F.col("dst")).cast("double"))
            .withColumn("fx_mismatch", (F.col("payment_currency") != F.col("receiving_currency")).cast("double")))
    return full.drop("_prev", "fresh")


def parity(tx, feats, n=200, cap=2_000_000):
    """Compute the features of n recent payments again with q1_core and compare."""
    sample = [r.txn_id for r in feats.orderBy(F.col("ts").desc(), F.col("seq").desc()).limit(n).select("txn_id").collect()]
    picked = feats.where(F.col("txn_id").isin(sample)).cache()
    accts = picked.select(F.explode(F.array("src", "dst")).alias("a")).distinct()
    touch = (tx.join(F.broadcast(accts.withColumnRenamed("a", "src")), "src", "left_semi")
             .unionByName(tx.join(F.broadcast(accts.withColumnRenamed("a", "dst")), "dst", "left_semi")).distinct())
    rows = touch.orderBy("ts", "seq").limit(cap).collect()
    lf, mine = core.LocalFeatures(), {}
    for row in rows:
        f = lf.features(row.asDict())
        if row.txn_id in sample:
            mine[row.txn_id] = f
    worst, where = 0.0, None
    for row in picked.collect():
        for name in core.FEATURES:
            a, b = row[name], mine[row.txn_id][name]
            a = None if a is None or a != a else float(a)
            b = None if b != b else float(b)
            diff = 0.0 if a is None and b is None else (1e9 if a is None or b is None else
                                                        abs(a - b) / max(1.0, abs(a), abs(b)))
            if diff > worst:
                worst, where = diff, {"txn_id": row.txn_id, "feature": name, "spark": a, "flink_code": b}
    return {"payments_compared": len(mine), "features": len(core.FEATURES), "largest_relative_difference": worst,
            "at": where, "match": worst < 1e-6}


def main():
    a = get_args(["bucket", "our_bank", "live_start"], optional=["seed_days"])
    base = a["bucket"] if a["bucket"].startswith("/") else f"s3://{a['bucket']}"
    bank = str(int(a["our_bank"]))
    spark = SparkSession.builder.appName("q1-features-historical").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    # the CSV header has spaces and two "Account" columns, so columns are named by position
    schema = T.StructType([T.StructField(c, T.StringType()) for c in IBM_COLS])
    raw = (spark.read.csv(f"{base}/historical/", header=True, schema=schema, enforceSchema=True)
           .withColumn("seq", F.monotonically_increasing_id())
           .withColumn("txn_ts", F.regexp_replace(F.regexp_replace("ts_raw", "/", "-"), " ", "T"))
           .withColumn("txn_id", F.substring(F.sha1(F.concat_ws("|", *IBM_COLS, F.col("seq").cast("string"))), 1, 32))
           .drop("ts_raw"))
    good, bad = apply_clean(raw, bank)
    write_dlq(bad, base, "q1_features_historical", a["live_start"])
    tx = (good.where(F.col("date") < a["live_start"])
          .withColumn("src", F.concat_ws("_", "from_bank", "from_account"))      # as q1_core.add_keys
          .withColumn("dst", F.concat_ws("_", "to_bank", "to_account"))
          .withColumn("label", F.col("is_laundering").cast("int")).drop("is_laundering").cache())
    print(f"[q1] {tx.count()} historical payments for bank {bank} before {a['live_start']}, {bad.count()} rejected")

    feats = add_features(tx).cache()
    cols = (["txn_id", "txn_ts", "from_bank", "from_account", "to_bank", "to_account", "amount_paid",
             "payment_currency", "amount_received", "receiving_currency", "payment_format", "label"]
            + core.FEATURES + ["date"])
    feats.select(*cols).write.mode("overwrite").partitionBy("date").parquet(f"{base}/curated/historical/")

    seed_from = F.date_sub(F.to_date(F.lit(a["live_start"])), int(a.get("seed_days", 7)))
    seed = (tx.where(F.to_date("date") >= seed_from).orderBy("ts", "seq")
            .select(F.to_json(F.struct(
                "txn_id", F.date_format("txn_ts", "yyyy-MM-dd'T'HH:mm:ss").alias("txn_ts"), "from_bank",
                "from_account", "to_bank", "to_account", "amount_paid", "payment_currency", "amount_received",
                "receiving_currency", "payment_format")).alias("value")))
    seed.coalesce(1).write.mode("overwrite").text(f"{base}/state/flink_seed/")

    result = parity(tx, feats)
    print("[q1] parity with the Flink feature code:", json.dumps(result))
    if not a["bucket"].startswith("/"):
        boto3.client("s3").put_object(Bucket=a["bucket"], Key="state/parity/q1_features.json",
                                      Body=json.dumps(result, indent=2).encode())
    if not result["match"]:
        raise RuntimeError(f"Feature parity check failed: {result}")


if __name__ == "__main__":
    main()
