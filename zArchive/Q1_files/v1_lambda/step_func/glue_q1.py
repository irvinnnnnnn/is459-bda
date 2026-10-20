"""
IS459 G2 - Q1 Glue job: labels + clean + filter one bank + features (+ seed DynamoDB at setup).

mode=setup    history CSV only. Splits history by time into train / valid / bench (60/20/20),
              saves the split boundaries, seeds the DynamoDB state tables for live scoring.
mode=retrain  history CSV + live raw JSON from Firehose. Live labels come ONLY from the
              alerts table (confirmed=1, dismissed=0, open=excluded, never flagged=0).
              The is_laundering field in the live stream is dropped and never used.

Writes:  s3://<bucket>/q1/features/split=<train|valid|bench|recent>/   (Parquet)
         s3://<bucket>/q1/config/split.json                            (setup only)

Glue job parameters (set defaults in Job details, --mode is passed by Step Functions):
  --mode setup|retrain   --bucket <name>   --bank_id 11   --live_start 2022-09-10
  --review_delay_hours 12   --eval_days 2   --region us-east-1
  --events_table q1-events   --state_table q1-state   --alerts_table q1-alerts
"""
import json
import sys
from datetime import timedelta
from decimal import Decimal

import numpy as np
import pandas as pd

# ------------------------------------------------------------------ features
# Same features as fraud_detect.ipynb, minus cyc3 / sg_count (bank_view always blanked them).
WINDOWS = [("1D", 1), ("5D", 5), ("7D", 7)]
PREFIXES = ["s_out", "s_in", "r_in", "r_out"]
FEATURES = ([f"{p}_{c}_{w}" for w, _ in WINDOWS for p in PREFIXES for c in ("n", "amt", "fresh")] +
            ["s_hrs_since_in", "s_pass_1D", "s_burst", "r_burst", "amt_match", "cyc2_hrs",
             "round_amt", "just_under_10k", "new_pair", "log_amt", "cross_bank", "self_transfer",
             "fx_mismatch"])
S_COLS = [f for f in FEATURES if f.startswith("s_")] + ["amt_match"]   # blanked for receiver's view of cross-bank
R_COLS = [f for f in FEATURES if f.startswith("r_")]                   # blanked for sender's view of cross-bank
SHIFT = np.int64(2 ** 31)


def build_features(d):
    """d: one bank's payments sorted by (ts, seq), with columns
    ts, src, dst, from_bank, to_bank, amt_paid, amt_recv, pay_cur, recv_cur.
    Each row only uses payments strictly before it (same logic as the notebook)."""
    n = len(d)
    ts = d["ts"].values
    codes, _ = pd.factorize(pd.concat([d["src"], d["dst"]], ignore_index=True))
    src, dst = codes[:n].astype(np.int64), codes[n:].astype(np.int64)
    amt = d["amt_paid"].values.astype(float)
    amt_in = d["amt_recv"].values.astype(float)
    out = pd.DataFrame(index=d.index)

    out["log_amt"] = np.log1p(amt)
    out["cross_bank"] = (d["from_bank"].values != d["to_bank"].values).astype(int)
    out["self_transfer"] = (src == dst).astype(int)
    out["fx_mismatch"] = (d["pay_cur"].values != d["recv_cur"].values).astype(int)
    pair = pd.DataFrame({"s": src, "d": dst, "ts": ts})
    out["new_pair"] = (~pair.duplicated(["s", "d"])).astype(int).values
    prev_pair = pair.groupby(["s", "d"])["ts"].shift()
    fresh = (prev_pair.isna() | ((pair["ts"] - prev_pair) > pd.Timedelta("5D"))).astype(float).values
    out["round_amt"] = ((amt % 100) == 0).astype(int)
    out["just_under_10k"] = ((amt >= 9000) & (amt < 10000)).astype(int)

    def window_sums(ev_key, ev_vals, q_key, days):
        ev = pd.DataFrame({"key": ev_key, "ts": ts, **ev_vals})
        cols = list(ev_vals)
        for c in cols:
            ev[c] = ev.groupby("key")[c].cumsum()
        q = pd.DataFrame({"key": q_key, "ts": ts})
        now = pd.merge_asof(q, ev, on="ts", by="key", allow_exact_matches=False)
        past = pd.merge_asof(q.assign(ts=q["ts"] - pd.Timedelta(days=days)), ev, on="ts", by="key")
        return {c: (now[c].fillna(0) - past[c].fillna(0)).values for c in cols}

    out_vals = {"n": np.ones(n), "amt": amt, "fresh": fresh}
    in_vals = {"n": np.ones(n), "amt": amt_in, "fresh": fresh}
    for w, days in WINDOWS:
        for prefix, ev_key, vals, q_key in [("s_out", src, out_vals, src), ("s_in", dst, in_vals, src),
                                            ("r_in", dst, in_vals, dst), ("r_out", src, out_vals, dst)]:
            for c, arr in window_sums(ev_key, vals, q_key, days).items():
                out[f"{prefix}_{c}_{w}"] = arr

    inflow = pd.DataFrame({"key": dst, "in_ts": ts, "in_amt": amt_in})
    li = pd.merge_asof(pd.DataFrame({"key": src, "ts": ts}), inflow, left_on="ts", right_on="in_ts",
                       by="key", allow_exact_matches=False)
    out["s_hrs_since_in"] = (ts - li["in_ts"].values) / np.timedelta64(1, "h")
    with np.errstate(divide="ignore", invalid="ignore"):
        out["amt_match"] = amt / li["in_amt"].values
    s_in1 = out["s_in_amt_1D"].values
    out["s_pass_1D"] = np.divide(out["s_out_amt_1D"].values + amt, s_in1,
                                 out=np.zeros(n), where=s_in1 > 0)

    pair_ev = pd.DataFrame({"key": src * SHIFT + dst, "ev_ts": ts})
    m = pd.merge_asof(pd.DataFrame({"key": dst * SHIFT + src, "ts": ts}), pair_ev,
                      left_on="ts", right_on="ev_ts", by="key", allow_exact_matches=False)
    out["cyc2_hrs"] = (ts - m["ev_ts"].values) / np.timedelta64(1, "h")

    out["s_burst"] = out["s_out_n_1D"] / ((out["s_out_n_7D"] - out["s_out_n_1D"]) / 6 + 1)
    out["r_burst"] = out["r_in_n_1D"] / ((out["r_in_n_7D"] - out["r_in_n_1D"]) / 6 + 1)
    return out[FEATURES].astype(float), fresh


def seed_items(d, fresh, live_start):
    """DynamoDB items that recreate the live state as of live_start (same format score_lambda writes)."""
    d = d.assign(fresh=fresh)
    iso = d["ts"].dt.strftime("%Y-%m-%dT%H:%M:%S")
    d = d.assign(iso=iso)

    events = []
    recent = d[d["ts"] >= pd.Timestamp(live_start) - pd.Timedelta(days=8)]
    for r in recent.itertuples(index=False):
        events.append({"acct": r.src, "sk": f"{r.iso}#{r.txn_id}#out", "amt": r.amt_paid, "fresh": int(r.fresh)})
        events.append({"acct": r.dst, "sk": f"{r.iso}#{r.txn_id}#in", "amt": r.amt_recv, "fresh": int(r.fresh)})

    def last_and_prev(keys):
        last = d.groupby(keys, sort=False).tail(1)
        gmax = d.groupby(keys, sort=False)["ts"].transform("max")
        prev = d[d["ts"] < gmax].groupby(keys, sort=False).tail(1)
        return last, prev

    state = {}
    last, prev = last_and_prev(["dst"])
    for r in last.itertuples(index=False):
        state[f"LASTIN#{r.dst}"] = {"pk": f"LASTIN#{r.dst}", "last_ts": r.iso, "last_amt": r.amt_recv}
    for r in prev.itertuples(index=False):
        state[f"LASTIN#{r.dst}"].update({"prev_ts": r.iso, "prev_amt": r.amt_recv})
    last, prev = last_and_prev(["src", "dst"])
    for r in last.itertuples(index=False):
        state[f"PAIR#{r.src}|{r.dst}"] = {"pk": f"PAIR#{r.src}|{r.dst}", "last_ts": r.iso}
    for r in prev.itertuples(index=False):
        state[f"PAIR#{r.src}|{r.dst}"]["prev_ts"] = r.iso
    state["META#clock"] = {"pk": "META#clock", "ts": f"{live_start}T00:00:00"}
    return events, list(state.values())


def to_ddb(item):
    return {k: (Decimal(str(v)) if isinstance(v, (float, np.floating)) else
                int(v) if isinstance(v, (np.integer,)) else v) for k, v in item.items()}


def retrain_splits(d, alerts, bench_start, clock, review_delay_hours, eval_days):
    """Live labels come only from the alerts table:
    confirmed -> 1, dismissed -> 0, open -> excluded, never flagged -> 0.
    Payments newer than (clock - review delay) are excluded (reviews may not have caught up).
    bench = fixed held-out history slice; recent = last eval_days of reviewed live data;
    the rest is split by time into train (80%) / valid (20%)."""
    d = d.copy()
    cutoff = clock - timedelta(hours=review_delay_hours)
    status = d["txn_id"].map(dict(zip(alerts["txn_id"], alerts["status"])))
    live = d["is_live"] == 1
    d.loc[live, "label"] = 0.0
    d.loc[live & (status == "confirmed"), "label"] = 1.0
    d.loc[live & (status == "open"), "label"] = np.nan
    usable = ~(live & ((d["ts"] > cutoff) | d["label"].isna()))
    bench = (~live) & (d["ts"] >= bench_start)
    recent = live & usable & (d["ts"] > cutoff - timedelta(days=eval_days))
    rest = usable & ~bench & ~recent
    d["split"] = None
    d.loc[bench, "split"] = "bench"
    d.loc[recent, "split"] = "recent"
    idx = d.index[rest]
    if len(idx):
        cut = idx[int(len(idx) * 0.8)]
        d.loc[rest & (d.index < cut), "split"] = "train"
        d.loc[rest & (d.index >= cut), "split"] = "valid"
    print(f"[q1] clock={clock} cutoff={cutoff} alerts={len(alerts)} "
          f"confirmed={int((live & (status == 'confirmed')).sum())} "
          f"dismissed={int((live & (status == 'dismissed')).sum())} "
          f"open_excluded={int((live & (status == 'open')).sum())}")
    return d


# ------------------------------------------------------------------ helpers
def get_args():
    names = ["mode", "bucket", "bank_id", "live_start", "review_delay_hours", "eval_days",
             "region", "events_table", "state_table", "alerts_table"]
    try:
        from awsglue.utils import getResolvedOptions
        return getResolvedOptions(sys.argv, names)
    except ImportError:
        import argparse
        p = argparse.ArgumentParser()
        for nme in names:
            p.add_argument(f"--{nme}", required=True)
        return vars(p.parse_args())


def write_text(s3, bucket, key, text):
    if bucket.startswith("/"):
        import os
        os.makedirs(os.path.dirname(f"{bucket}/{key}"), exist_ok=True)
        open(f"{bucket}/{key}", "w").write(text)
    else:
        s3.put_object(Bucket=bucket, Key=key, Body=text.encode())


def read_text(s3, bucket, key):
    if bucket.startswith("/"):
        return open(f"{bucket}/{key}").read()
    return s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode()


def scan_alerts(ddb, table):
    t, items, kw = ddb.Table(table), [], {"ProjectionExpression": "txn_id, #s",
                                          "ExpressionAttributeNames": {"#s": "status"}}
    while True:
        resp = t.scan(**kw)
        items += resp["Items"]
        if "LastEvaluatedKey" not in resp:
            return pd.DataFrame(items, columns=["txn_id", "status"])
        kw["ExclusiveStartKey"] = resp["LastEvaluatedKey"]


# ------------------------------------------------------------------ main
def main():
    import boto3
    from pyspark.sql import SparkSession, functions as F
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

    a = {k: (v.strip() if isinstance(v, str) else v) for k, v in get_args().items()}
    mode, bucket, bank = a["mode"], a["bucket"], str(int(a["bank_id"]))
    live_start = a["live_start"]
    base = bucket if bucket.startswith("/") else f"s3://{bucket}"     # a local folder works for testing
    spark = SparkSession.builder.appName(f"q1-features-{mode}").getOrCreate()
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    ddb = boto3.resource("dynamodb", region_name=a["region"])
    s3 = boto3.client("s3", region_name=a["region"])
    norm = lambda c: F.col(c).cast("int").cast("string")

    # ---- history: the CSV with labels, uploaded once (only dates before live_start are used)
    # columns renamed positionally: the CSV header has spaces and two "Account" columns
    hist_schema = StructType([StructField(c, StringType()) for c in
                              ["ts_raw", "from_bank_raw", "from_account", "to_bank_raw", "to_account",
                               "amount_received", "receiving_currency", "amount_paid",
                               "payment_currency", "payment_format", "is_laundering"]])
    hist = (spark.read.csv(f"{base}/q1/history/", header=True, schema=hist_schema, enforceSchema=True)
            .withColumn("seq", F.monotonically_increasing_id())
            .select(F.concat(F.lit("h"), F.col("seq").cast("string")).alias("txn_id"),
                    F.to_timestamp("ts_raw", "yyyy/MM/dd HH:mm").alias("ts"), "seq",
                    norm("from_bank_raw").alias("from_bank"), "from_account",
                    norm("to_bank_raw").alias("to_bank"), "to_account",
                    F.col("amount_paid").cast("double").alias("amt_paid"),
                    F.col("amount_received").cast("double").alias("amt_recv"),
                    F.col("payment_currency").alias("pay_cur"),
                    F.col("receiving_currency").alias("recv_cur"),
                    F.col("is_laundering").cast("int").alias("label"),
                    F.lit(0).alias("is_live")))
    hist = hist.where(F.col("ts") < F.to_timestamp(F.lit(live_start)))
    parts = [hist]

    # ---- live: raw JSON from Firehose (retrain only); is_laundering is dropped here
    if mode == "retrain":
        raw_schema = StructType([StructField(c, StringType()) for c in
                                 ["txn_id", "txn_ts", "from_bank", "from_account", "to_bank", "to_account",
                                  "receiving_currency", "payment_currency"]] +
                                [StructField("amount_received", DoubleType()),
                                 StructField("amount_paid", DoubleType())])
        live = (spark.read.schema(raw_schema).json(f"{base}/raw/")
                .dropna(subset=["txn_id", "txn_ts", "from_account", "to_account", "amount_paid"])
                .dropDuplicates(["txn_id"])
                .withColumn("seq", F.monotonically_increasing_id() + F.lit(10 ** 12))
                .select("txn_id", F.to_timestamp("txn_ts").alias("ts"), "seq",
                        norm("from_bank").alias("from_bank"), "from_account",
                        norm("to_bank").alias("to_bank"), "to_account",
                        F.col("amount_paid").alias("amt_paid"), F.col("amount_received").alias("amt_recv"),
                        F.col("payment_currency").alias("pay_cur"),
                        F.col("receiving_currency").alias("recv_cur"),
                        F.lit(None).cast("int").alias("label"), F.lit(1).alias("is_live"))
                .where(F.col("ts") >= F.to_timestamp(F.lit(live_start))))
        parts.append(live)

    df = parts[0] if len(parts) == 1 else parts[0].unionByName(parts[1])
    df = (df.dropna(subset=["ts", "from_bank", "to_bank", "amt_paid", "amt_recv"])
            .where((F.col("from_bank") == bank) | (F.col("to_bank") == bank)))      # filter to our bank
    d = df.toPandas()
    d["ts"] = pd.to_datetime(d["ts"])
    d = d.sort_values(["ts", "seq"], kind="stable").reset_index(drop=True)
    d["src"] = d["from_bank"] + "_" + d["from_account"]
    d["dst"] = d["to_bank"] + "_" + d["to_account"]
    print(f"[q1] {mode}: {len(d)} payments involve bank {bank} "
          f"({int((d['is_live'] == 1).sum())} live)")

    X, fresh = build_features(d)
    d = pd.concat([d, X], axis=1)

    # ---- labels + time split
    if mode == "setup":
        n = len(d)
        i60, i80 = int(n * 0.6), int(n * 0.8)
        bench_start = d.loc[i80, "ts"]
        d["split"] = np.where(np.arange(n) < i60, "train", "valid")
        d.loc[d["ts"] >= bench_start, "split"] = "bench"       # fixed benchmark: never trained on, by any version
        cfg = {"bank": bank, "live_start": live_start, "bench_start": bench_start.isoformat()}
        write_text(s3, bucket, "q1/config/split.json", json.dumps(cfg))
    else:
        cfg = json.loads(read_text(s3, bucket, "q1/config/split.json"))
        clock_item = ddb.Table(a["state_table"]).get_item(Key={"pk": "META#clock"}).get("Item")
        clock = pd.Timestamp(clock_item["ts"]) if clock_item else d["ts"].max()
        alerts = scan_alerts(ddb, a["alerts_table"])
        d = retrain_splits(d, alerts, pd.Timestamp(cfg["bench_start"]), clock,
                           float(a["review_delay_hours"]), float(a["eval_days"]))

    keep = d[d["split"].notna()]
    cols = ["txn_id", "ts", "from_bank", "to_bank", "label", "split"] + FEATURES
    out = keep[cols].copy()
    out["label"] = out["label"].astype(int)
    print("[q1] rows per split:", out["split"].value_counts().to_dict(),
          "positives:", out.groupby("split")["label"].sum().to_dict())
    (spark.createDataFrame(out).write.mode("overwrite")
          .partitionBy("split").parquet(f"{base}/q1/features/"))

    # ---- seed DynamoDB (setup only, so live state is never overwritten by retraining)
    if mode == "setup":
        events, state = seed_items(d, fresh, live_start)
        with ddb.Table(a["events_table"]).batch_writer() as w:
            for it in events:
                w.put_item(Item=to_ddb(it))
        with ddb.Table(a["state_table"]).batch_writer() as w:
            for it in state:
                w.put_item(Item=to_ddb(it))
        print(f"[q1] seeded {len(events)} events and {len(state)} state items")


if __name__ == "__main__":
    main()