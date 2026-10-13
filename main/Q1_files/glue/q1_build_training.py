"""Q1 training workflow, step 1 (spec 3.2): build the training set for one run.

  1. curated/live/labels/: keep the latest row per txn_id; confirmed -> 1, dismissed -> 0,
     expired -> dropped. Alerts and audit items are treated alike.
  2. Inner-join with curated/live/scored/ on txn_id. The features are the ones Flink logged when it
     scored the payment, so they are point-in-time correct. Unreviewed payments are not used.
  3. Union with curated/historical/ (already labelled).
  4. Keep the last --months months.
  5. Split by time 60 / 20 / 20: oldest -> train, next -> val, newest -> test.
  6. Write training/run=<run_id>/{train,val,test}/.
On the first run there are no live labels yet, so the model trains on history only.

Arguments: --bucket --run_id --months
"""
import json

import boto3
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

import q1_core as core
from spark_common import get_args

KEEP = ["txn_id", "txn_ts", "from_bank", "to_bank", "label"] + core.FEATURES


def read_or_none(spark, path):
    try:
        df = spark.read.parquet(path)
        return df if df.head(1) else None
    except AnalysisException:
        return None


def main():
    a = get_args(["bucket", "run_id", "months"])
    base = f"s3://{a['bucket']}"
    spark = SparkSession.builder.appName("q1-build-training").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    data = spark.read.parquet(f"{base}/curated/historical/").select(*KEEP).withColumn("source", F.lit("historical"))
    n_live = 0
    labels = read_or_none(spark, f"{base}/curated/live/labels/")
    scored = read_or_none(spark, f"{base}/curated/live/scored/")
    if labels is not None and scored is not None:
        latest = Window.partitionBy("txn_id").orderBy(F.col("exported_at").desc())
        labels = (labels.withColumn("_n", F.row_number().over(latest)).where("_n = 1")
                  .where(F.col("outcome").isin("confirmed", "dismissed"))
                  .select("txn_id", (F.col("outcome") == "confirmed").cast("int").alias("label")))
        live = (scored.dropDuplicates(["txn_id"]).join(F.broadcast(labels), "txn_id")
                .select(*KEEP).withColumn("source", F.lit("live")))
        n_live = live.count()
        data = data.unionByName(live)

    newest = data.agg(F.max("txn_ts")).first()[0]
    data = data.where(F.col("txn_ts") >= F.add_months(F.lit(newest), -int(a["months"]))).cache()
    t60, t80 = data.select(F.col("txn_ts").cast("long").alias("t")).approxQuantile("t", [0.6, 0.8], 0.0001)
    t = F.col("txn_ts").cast("long")
    data = data.withColumn("split", F.when(t < t60, "train").when(t < t80, "val").otherwise("test"))

    out = f"{base}/training/run={a['run_id']}"
    summary = {"run_id": a["run_id"], "live_labelled": n_live, "newest": str(newest), "splits": {}}
    for name in ("train", "val", "test"):
        part = data.where(F.col("split") == name).drop("split")
        part.write.mode("overwrite").parquet(f"{out}/{name}/")
        row = part.agg(F.count("*"), F.sum("label")).first()
        summary["splits"][name] = {"payments": row[0], "laundering": int(row[1] or 0)}
    print("[q1] training set:", json.dumps(summary))
    boto3.client("s3").put_object(Bucket=a["bucket"], Key=f"training/run={a['run_id']}/summary.json",
                                  Body=json.dumps(summary, indent=2).encode())


if __name__ == "__main__":
    main()
