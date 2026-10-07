"""
IS459 G2 - Q2 validation (EMR Spark step, runs after score.py).

A "laundering customer" = a customer with at least one laundering transaction
(in or out) at our bank inside the scoring window. Labels are used ONLY here.

Writes:
  results/validation_summary/run_date=<d>/    metric, value
  results/validation_typology/run_date=<d>/   typology, n_laundering_customers, caught_high, caught_high_or_medium
  results/validation_rules/run_date=<d>/      rule_id, rule_name, hits, laundering_hits, precision
"""
import argparse
import re
from datetime import date, timedelta

from pyspark.sql import SparkSession, functions as F


def parse():
    p = argparse.ArgumentParser()
    p.add_argument("--bucket", required=True)
    p.add_argument("--run-date", required=True)
    p.add_argument("--bank-id", required=True)
    p.add_argument("--window-days", type=int, default=7)
    return p.parse_args()


def base_path(bucket):
    return bucket if bucket.startswith("/") else f"s3://{bucket}"


def load_patterns(spark, base):
    """Parse the Kaggle *_Patterns.txt into (ts_key, from_account, to_account, typology)."""
    lines = [r[0] for r in spark.read.text(f"{base}/reference/patterns/").collect()]
    rows, typ = [], None
    for ln in lines:
        ln = ln.strip()
        m = re.match(r"BEGIN LAUNDERING ATTEMPT - ([A-Z\-]+)", ln)
        if m:
            typ = m.group(1).title()
            continue
        if ln.startswith("END LAUNDERING ATTEMPT") or not ln:
            continue
        parts = ln.split(",")
        if typ and len(parts) >= 5:
            rows.append((parts[0], parts[2], parts[4], typ))
    return spark.createDataFrame(rows, "ts_key string, from_account string, to_account string, typology string").distinct()


def main():
    a = parse()
    base, bank = base_path(a.bucket), str(int(a.bank_id))
    run = date.fromisoformat(a.run_date)
    run_s, start = run.isoformat(), (run - timedelta(days=a.window_days - 1)).isoformat()
    spark = SparkSession.builder.appName(f"aml-q2-validate-{run_s}").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")  # keep date/run_date as strings

    scores = spark.read.parquet(f"{base}/results/customer_scores/run_date={run_s}/")
    hits = spark.read.parquet(f"{base}/results/rule_hits/run_date={run_s}/")
    cur = (spark.read.parquet(f"{base}/curated/")
           .where((F.col("date") >= start) & (F.col("date") <= run_s) & (F.col("is_laundering") == 1))
           .where((F.col("from_bank") == bank) | (F.col("to_bank") == bank))
           .withColumn("ts_key", F.date_format("txn_ts", "yyyy/MM/dd HH:mm")))

    pats = load_patterns(spark, base)
    cur = cur.join(pats, ["ts_key", "from_account", "to_account"], "left") \
             .withColumn("typology", F.coalesce("typology", F.lit("Unattributed")))
    laund_legs = (cur.where(F.col("from_bank") == bank).select(F.col("from_entity_id").alias("entity_id"), "typology")
                  .unionByName(cur.where(F.col("to_bank") == bank).select(F.col("to_entity_id").alias("entity_id"), "typology")))
    laund_customers = laund_legs.select("entity_id").distinct().withColumn("is_laundering_customer", F.lit(1))

    s = (scores.join(laund_customers, "entity_id", "left").fillna(0, ["is_laundering_customer"])
               .withColumn("is_high", (F.col("tier") == "High").cast("int"))
               .withColumn("is_hm", F.col("tier").isin("High", "Medium").cast("int"))).cache()

    agg = s.agg(F.count("*").alias("n"), F.sum("is_laundering_customer").alias("pos"),
                F.sum("is_high").alias("n_high"), F.sum("is_hm").alias("n_hm"),
                F.sum(F.col("is_high") * F.col("is_laundering_customer")).alias("tp_high"),
                F.sum(F.col("is_hm") * F.col("is_laundering_customer")).alias("tp_hm")).first()
    n, pos = agg["n"], agg["pos"] or 0
    base_rate = pos / n if n else 0.0
    prec_high = agg["tp_high"] / agg["n_high"] if agg["n_high"] else 0.0
    prec_hm = agg["tp_hm"] / agg["n_hm"] if agg["n_hm"] else 0.0
    metrics = {
        "customers_scored": n,
        "laundering_customers": pos,
        "high_count": agg["n_high"],
        "medium_count": agg["n_hm"] - agg["n_high"],
        "captured_high": agg["tp_high"],
        "captured_high_or_medium": agg["tp_hm"],
        "capture_rate_high": agg["tp_high"] / pos if pos else 0.0,
        "capture_rate_high_or_medium": agg["tp_hm"] / pos if pos else 0.0,
        "precision_high": prec_high,
        "precision_high_or_medium": prec_hm,
        "base_rate": base_rate,
        "lift_high_vs_random": prec_high / base_rate if base_rate else 0.0,
        "lift_high_or_medium_vs_random": prec_hm / base_rate if base_rate else 0.0,
    }
    spark.createDataFrame([(k, float(v)) for k, v in metrics.items()], "metric string, value double") \
         .coalesce(1).write.mode("overwrite").parquet(f"{base}/results/validation_summary/run_date={run_s}/")

    typ = (laund_legs.distinct().join(s.select("entity_id", "is_high", "is_hm"), "entity_id")
           .groupBy("typology").agg(F.countDistinct("entity_id").alias("n_laundering_customers"),
                                    F.countDistinct(F.when(F.col("is_high") == 1, F.col("entity_id"))).alias("caught_high"),
                                    F.countDistinct(F.when(F.col("is_hm") == 1, F.col("entity_id"))).alias("caught_high_or_medium")))
    typ.coalesce(1).write.mode("overwrite").parquet(f"{base}/results/validation_typology/run_date={run_s}/")

    rules = (hits.join(laund_customers, "entity_id", "left").fillna(0, ["is_laundering_customer"])
             .groupBy("rule_id", "rule_name").agg(F.count("*").alias("hits"),
                                                  F.sum("is_laundering_customer").alias("laundering_hits"))
             .withColumn("precision", F.round(F.col("laundering_hits") / F.col("hits"), 4)))
    rules.coalesce(1).write.mode("overwrite").parquet(f"{base}/results/validation_rules/run_date={run_s}/")

    print(f"[validate] {run_s}: " + ", ".join(f"{k}={v:.3f}" if isinstance(v, float) else f"{k}={v}"
                                             for k, v in metrics.items()))


if __name__ == "__main__":
    main()
