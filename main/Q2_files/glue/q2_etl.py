"""Q2 Glue job (spec 4.3): raw transactions + accounts + FX -> curated/customer_txns/.

Incremental: it processes the dates that are not in curated/customer_txns/ yet (the first run is
therefore the backfill) and always redoes --run_date itself.
  dates before --live_start  come from curated/historical/ (already cleaned by the Q1 feature job)
  dates from --live_start    come from raw/transactions/ (Firehose, as received) and are cleaned here
                             with the same rules as Flink; rejected rows go to dlq/ with the reason
Then it joins each payment to the customer that owns each account, as of the payment's date, and
adds the USD amounts. The run fails, and writes nothing, if a check fails:
  every account at our bank maps to a customer / every currency has a rate for the payment's own
  date / there are payments.

Arguments: --bucket --our_bank --live_start --run_date
"""
import bisect

import boto3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from q2_common import s3_dates
from spark_common import RAW_COLS, apply_clean, get_args, write_dlq

TXN = ["txn_id", "txn_ts", "from_bank", "from_account", "to_bank", "to_account", "amount_paid", "payment_currency",
       "amount_received", "receiving_currency", "payment_format", "is_laundering", "source", "date"]


def as_of(days, available):
    """For each day, the latest available date on or before it (else the earliest available).

    Used for the customer file, which applies from its snapshot date until the next snapshot.
    """
    available = sorted(available)
    return [(d, available[max(0, bisect.bisect_right(available, d) - 1)]) for d in days]


def main():
    a = get_args(["bucket", "our_bank", "live_start", "run_date"])
    bucket, base, bank, run = a["bucket"], f"s3://{a['bucket']}", str(int(a["our_bank"])), a["run_date"]
    spark = SparkSession.builder.appName("q2-etl").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    s3 = boto3.client("s3")

    done = set(s3_dates(s3, bucket, "curated/customer_txns/"))
    hist = [d for d in s3_dates(s3, bucket, "curated/historical/") if d < a["live_start"] and d <= run]
    raw = [d for d in s3_dates(s3, bucket, "raw/transactions/") if a["live_start"] <= d <= run]
    todo_hist = [d for d in hist if d not in done or d == run]
    todo_raw = [d for d in raw if d not in done or d == run]
    print(f"[q2] run {run}: new dates from history {todo_hist}, from the live feed {todo_raw}")
    if not todo_hist and not todo_raw:
        print("[q2] nothing new to process")
        return

    parts = []
    if todo_hist:
        h = (spark.read.option("basePath", f"{base}/curated/historical/")
             .parquet(*[f"{base}/curated/historical/date={d}/" for d in todo_hist]))
        parts.append(h.withColumn("is_laundering", F.col("label")).withColumn("source", F.lit("historical"))
                     .withColumn("date", F.col("date").cast("string")).select(*TXN))
    n_bad = 0
    if todo_raw:
        r = (spark.read.option("basePath", f"{base}/raw/transactions/")
             .parquet(*[f"{base}/raw/transactions/date={d}/" for d in todo_raw]).drop("date", "txn_date"))
        for c in RAW_COLS + ["is_laundering"]:
            if c not in r.columns:
                r = r.withColumn(c, F.lit(None).cast("string"))
        good, bad = apply_clean(r.select(*RAW_COLS, "is_laundering"), bank)
        n_bad = bad.count()
        if n_bad:
            write_dlq(bad, base, "q2_glue", run)
        parts.append(good.withColumn("is_laundering", F.col("is_laundering").cast("int"))
                     .withColumn("source", F.lit("live")).where(F.col("date").isin(todo_raw)).select(*TXN))
    tx = parts[0] if len(parts) == 1 else parts[0].unionByName(parts[1])
    tx = tx.where(~((F.col("from_bank") == F.col("to_bank")) & (F.col("from_account") == F.col("to_account")))).cache()
    days = [r[0] for r in tx.select("date").distinct().collect()]

    # ---- reference data: clean
    acc_schema = T.StructType([T.StructField(c, T.StringType()) for c in
                               ("bank_name", "bank_id", "account", "customer_id", "entity_name")])
    acc = (spark.read.csv(f"{base}/raw/accounts/", header=True, schema=acc_schema, enforceSchema=True)
           .where(F.col("account").isNotNull() & F.col("customer_id").isNotNull())
           .select(F.col("snapshot_date").cast("string").alias("snapshot_date"),
                   F.col("bank_id").cast("int").cast("string").alias("bank"), F.trim("account").alias("account"),
                   F.trim("customer_id").alias("customer_id"),
                   F.regexp_replace(F.trim("entity_name"), r"\s*#\d+$", "").alias("entity_type"))
           .dropDuplicates(["snapshot_date", "bank", "account"]))
    fx = (spark.read.csv(f"{base}/raw/fx/", header=True)
          .select(F.col("date").alias("fx_date"), F.trim("currency").alias("currency"),
                  F.col("usd_per_unit").cast("double").alias("rate"))
          .where(F.col("rate") > 0).dropDuplicates(["fx_date", "currency"]))
    snapshots = [r[0] for r in acc.select("snapshot_date").distinct().collect()]
    fx_dates = [r[0] for r in fx.select("fx_date").distinct().collect()]
    if not snapshots or not fx_dates:
        raise RuntimeError("Validation failed: raw/accounts/ or raw/fx/ is empty")
    no_rates = sorted(set(days) - set(fx_dates))      # a rate is for its own day: never borrow another day's
    if no_rates:
        raise RuntimeError(f"Validation failed: raw/fx/ has no rates for {no_rates[0]} to {no_rates[-1]} "
                           f"({len(no_rates)} days). Fetch them with producer.py fx and upload again.")
    cal = spark.createDataFrame([(d, s, d) for d, s in as_of(days, snapshots)],
                                "date string, snapshot_date string, fx_date string")

    # ---- join: customer as of the payment date, USD at that date's rate
    out = tx.join(F.broadcast(cal), "date")
    for side in ("from", "to"):
        owner = acc.select("snapshot_date", F.col("bank").alias(f"{side}_bank"), F.col("account").alias(f"{side}_account"),
                           F.col("customer_id").alias(f"{side}_customer_id"),
                           F.col("entity_type").alias(f"{side}_entity_type"))
        out = out.join(owner, ["snapshot_date", f"{side}_bank", f"{side}_account"], "left")
    for cur, amt in (("payment_currency", "amount_paid"), ("receiving_currency", "amount_received")):
        rate = fx.select("fx_date", F.col("currency").alias(cur), F.col("rate").alias("_rate"))
        out = (out.join(F.broadcast(rate), ["fx_date", cur], "left")
               .withColumn("_rate", F.when(F.col(cur) == "US Dollar", F.lit(1.0)).otherwise(F.col("_rate")))
               .withColumn(amt + "_usd", F.round(F.col(amt) * F.col("_rate"), 2)).drop("_rate"))
    # a counterparty at another bank is not our customer: it keeps an id so rules can still count it
    for side in ("from", "to"):
        ext = F.concat(F.lit("EXT-"), F.col(f"{side}_bank"), F.lit("-"), F.col(f"{side}_account"))
        ours = F.col(f"{side}_bank") == bank
        out = out.withColumn(f"{side}_unmapped", ours & F.col(f"{side}_customer_id").isNull()) \
                 .withColumn(f"{side}_customer_id", F.coalesce(F.col(f"{side}_customer_id"), ext))
    out = out.cache()

    # ---- validate (spec 4.3): fail the run if any check fails
    v = out.agg(F.count("*").alias("n"),
                F.sum((F.col("from_unmapped") | F.col("to_unmapped")).cast("int")).alias("unmapped"),
                F.sum((F.col("amount_paid_usd").isNull() | F.col("amount_received_usd").isNull()).cast("int")).alias("no_rate"),
                F.collect_set(F.when(F.col("amount_paid_usd").isNull(), F.col("payment_currency"))).alias("c1"),
                F.collect_set(F.when(F.col("amount_received_usd").isNull(), F.col("receiving_currency"))).alias("c2")).first()
    problems = []
    if not v["n"]:
        problems.append("no payments")
    if v["unmapped"]:
        problems.append(f"{v['unmapped']} payments have an account at our bank with no customer")
    if v["no_rate"]:
        problems.append(f"no FX rate for {sorted(set(v['c1']) | set(v['c2']))}")
    print(f"[q2] {v['n']} payments, {n_bad} rejected to dlq/, checks: {problems or 'all passed'}")
    if problems:
        raise RuntimeError("Validation failed: " + "; ".join(problems))

    (out.drop("snapshot_date", "fx_date", "from_unmapped", "to_unmapped")
     .write.mode("overwrite").partitionBy("date").parquet(f"{base}/curated/customer_txns/"))
    print(f"[q2] wrote curated/customer_txns/ for {sorted(days)}")


if __name__ == "__main__":
    main()
