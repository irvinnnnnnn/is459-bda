"""
IS459 G2 - Q2 Glue ETL job (one day at a time).
raw/date=<run_date>/ (JSON lines from Firehose)  ->  curated/date=<run_date>/ (Parquet)

Glue job parameters:  --run_date YYYY-MM-DD   --bucket <bucket-name>
Local test:           python glue_etl.py --run_date 2022-09-01 --bucket /path/to/local/lake
"""
import sys

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

SMALL_ENTITY_TYPES = ["Country", "Individual", "Direct"]   # too few for peer percentiles -> "Other"

RAW_SCHEMA = StructType([
    StructField("txn_id", StringType()),
    StructField("txn_ts", StringType()),
    StructField("txn_date", StringType()),
    StructField("from_bank", StringType()),
    StructField("from_account", StringType()),
    StructField("to_bank", StringType()),
    StructField("to_account", StringType()),
    StructField("amount_received", DoubleType()),
    StructField("receiving_currency", StringType()),
    StructField("amount_paid", DoubleType()),
    StructField("payment_currency", StringType()),
    StructField("payment_format", StringType()),
    StructField("is_laundering", IntegerType()),
])


def get_args():
    try:
        from awsglue.utils import getResolvedOptions
        return getResolvedOptions(sys.argv, ["run_date", "bucket"])
    except ImportError:
        import argparse
        p = argparse.ArgumentParser()
        p.add_argument("--run_date", required=True)
        p.add_argument("--bucket", required=True)
        return vars(p.parse_args())


def base_path(bucket):
    return bucket if bucket.startswith("/") else f"s3://{bucket}"


def load_accounts(spark, base):
    acc = spark.read.csv(f"{base}/reference/accounts/", header=True)
    etype = F.regexp_replace(F.col("Entity Name"), r"\s*#\d+$", "")
    return acc.select(
        F.col("Account Number").alias("account"),
        F.col("Entity ID").alias("entity_id"),
        F.when(etype.isin(SMALL_ENTITY_TYPES), "Other").otherwise(etype).alias("entity_type"),
    ).dropDuplicates(["account"])


def main():
    args = get_args()
    run_date, base = args["run_date"], base_path(args["bucket"])
    spark = SparkSession.builder.appName(f"aml-q2-etl-{run_date}").getOrCreate()

    raw = spark.read.schema(RAW_SCHEMA).json(f"{base}/raw/date={run_date}/")
    n_raw = raw.count()

    tx = (raw
          .dropna(subset=["txn_id", "txn_ts", "from_account", "to_account", "amount_paid", "amount_received"])
          .dropDuplicates(["txn_id"])
          .where(F.col("from_account") != F.col("to_account"))          # same-account self-transfers only
          .withColumn("txn_ts", F.to_timestamp("txn_ts"))
          .withColumn("from_bank", F.col("from_bank").cast("int").cast("string"))   # "012" -> "12"
          .withColumn("to_bank", F.col("to_bank").cast("int").cast("string")))

    fx = (spark.read.csv(f"{base}/reference/fx/", header=True)
          .select(F.col("date").alias("fx_date"), F.col("currency").alias("fx_currency"),
                  F.col("usd_per_unit").cast("double").alias("usd_per_unit")))
    fx_paid = fx.withColumnRenamed("usd_per_unit", "paid_rate")
    fx_recv = fx.withColumnRenamed("usd_per_unit", "recv_rate")

    tx = (tx
          .join(F.broadcast(fx_paid), (tx.txn_date == fx_paid.fx_date) & (tx.payment_currency == fx_paid.fx_currency), "left")
          .drop("fx_date", "fx_currency"))
    tx = (tx
          .join(F.broadcast(fx_recv), (tx.txn_date == fx_recv.fx_date) & (tx.receiving_currency == fx_recv.fx_currency), "left")
          .drop("fx_date", "fx_currency")
          .withColumn("amount_paid_usd", F.round(F.col("amount_paid") * F.col("paid_rate"), 2))
          .withColumn("amount_received_usd", F.round(F.col("amount_received") * F.col("recv_rate"), 2))
          .drop("paid_rate", "recv_rate"))

    acc = load_accounts(spark, base)
    acc_from = acc.select(F.col("account").alias("fa"), F.col("entity_id").alias("from_entity_id"),
                          F.col("entity_type").alias("from_entity_type"))
    acc_to = acc.select(F.col("account").alias("ta"), F.col("entity_id").alias("to_entity_id"),
                        F.col("entity_type").alias("to_entity_type"))
    tx = (tx.join(F.broadcast(acc_from), tx.from_account == acc_from.fa, "left").drop("fa")
            .join(F.broadcast(acc_to), tx.to_account == acc_to.ta, "left").drop("ta"))

    out = tx.select(
        "txn_id", "txn_ts", "txn_date", "from_bank", "from_account", "from_entity_id", "from_entity_type",
        "to_bank", "to_account", "to_entity_id", "to_entity_type",
        "amount_paid", "payment_currency", "amount_paid_usd",
        "amount_received", "receiving_currency", "amount_received_usd",
        "payment_format", "is_laundering")
    out.cache()
    n_out = out.count()
    n_no_fx = out.where(F.col("amount_paid_usd").isNull() | F.col("amount_received_usd").isNull()).count()
    n_no_owner = out.where(F.col("from_entity_id").isNull() | F.col("to_entity_id").isNull()).count()

    out.coalesce(4).write.mode("overwrite").parquet(f"{base}/curated/date={run_date}/")
    print(f"[etl] {run_date}: raw={n_raw} curated={n_out} missing_fx={n_no_fx} missing_owner={n_no_owner}")
    if n_no_fx:
        print("[etl] WARNING: some rows have no FX rate for their date/currency - extend fx_rates.csv")


if __name__ == "__main__":
    main()
