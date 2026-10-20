"""Spark helpers that both pipelines' batch jobs use.

apply_clean() runs aml_clean.clean() - the same function Q1's Flink job runs - as a UDF, so the
batch and streaming paths apply one set of cleaning rules (spec 4.3, "the same rules as Flink"),
and keeps one row per txn_id.
"""
import sys
import time

from pyspark.sql import functions as F
from pyspark.sql import types as T

import aml_clean

RAW_COLS = ["txn_id", "txn_ts", "from_bank", "from_account", "to_bank", "to_account", "amount_paid",
            "payment_currency", "amount_received", "receiving_currency", "payment_format"]
_CLEAN = T.StructType([T.StructField("error", T.StringType()), T.StructField("ts", T.LongType())] +
                      [T.StructField(c, T.StringType()) for c in
                       ("from_bank", "from_account", "to_bank", "to_account", "payment_currency",
                        "receiving_currency", "payment_format")] +
                      [T.StructField("amount_paid", T.DoubleType()), T.StructField("amount_received", T.DoubleType())])


def get_args(names, optional=()):
    """Job arguments on Glue (getResolvedOptions) or EMR / local (--name value)."""
    try:
        from awsglue.utils import getResolvedOptions
        args = getResolvedOptions(sys.argv, list(names))
        for k in optional:
            if f"--{k}" in sys.argv:
                args.update(getResolvedOptions(sys.argv, [k]))
        return {k: str(v).strip() for k, v in args.items()}
    except ImportError:
        import argparse
        p = argparse.ArgumentParser()
        for k in names:
            p.add_argument(f"--{k}", required=True)
        for k in optional:
            p.add_argument(f"--{k}")
        return {k: v.strip() for k, v in vars(p.parse_known_args()[0]).items() if v is not None}


def apply_clean(df, our_bank):
    """df has RAW_COLS (any types) plus other columns. Returns (our bank's clean rows, rejected rows).

    Valid payments of other banks are filtered out (aml_clean.involves_bank), not rejected.

    Clean rows gain ts (epoch seconds), txn_ts (timestamp), date and typed amounts, one row per txn_id.
    Rejected rows are (raw, error) ready for dlq/.
    """
    def fn(*vals):
        try:
            p = aml_clean.clean(dict(zip(RAW_COLS, vals)))
        except aml_clean.Reject as exc:
            return (str(exc),) + (None,) * 10
        return (None, p["ts"], p["from_bank"], p["from_account"], p["to_bank"], p["to_account"],
                p["payment_currency"], p["receiving_currency"], p["payment_format"],
                p["amount_paid"], p["amount_received"])

    udf = F.udf(fn, _CLEAN)
    other = [c for c in df.columns if c not in RAW_COLS]
    done = (df.withColumn("_raw", F.to_json(F.struct(*df.columns)))
            .withColumn("_c", udf(*[F.col(c).cast("string") for c in RAW_COLS])).cache())
    good = (done.where(F.col("_c.error").isNull())
            .where((F.col("_c.from_bank") == our_bank) | (F.col("_c.to_bank") == our_bank))
            .select("txn_id", *other, "_c.*").drop("error")
            .withColumn("txn_ts", F.col("ts").cast("timestamp"))
            .withColumn("date", F.date_format(F.col("ts").cast("timestamp"), "yyyy-MM-dd"))
            .dropDuplicates(["txn_id"]))
    bad = done.where(F.col("_c.error").isNotNull()).select(F.col("_raw").alias("raw"), F.col("_c.error").alias("error"))
    return good, bad


def write_dlq(bad, base, source, tag):
    """Rejected rows to dlq/ as JSON lines, with the reason and where they were rejected."""
    (bad.withColumn("source", F.lit(source)).withColumn("at", F.lit(aml_clean.iso(int(time.time()))))
     .write.mode("append").json(f"{base}/dlq/source={source}/run={tag}/"))
