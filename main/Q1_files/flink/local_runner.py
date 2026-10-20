#!/usr/bin/env python3
"""Fallback for the Q1 streaming job when Managed Flink is not available (for example, if the lab
account does not allow it): the same cleaning, features, scoring and outputs, in one Python process.

    python local_runner.py                 read new payments from Kinesis until Ctrl+C
    python local_runner.py --from-start    read everything still in the stream (24 hours)

It runs aml_clean, q1_core and q1_scoring, the code inside the Flink job, so scores, alerts and files are the
same. What it does not have is what Flink is for: state that survives a restart (it reloads the
7-day seed instead), parallelism, and checkpoints. Use it to demo or to test, not as the design.
"""
import argparse
import os
import sys
import time

import boto3

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path[:0] = [HERE, os.path.join(HERE, ".."), os.path.join(HERE, "..", "..", "common")]
import aml_clean  # noqa: E402
import config  # noqa: E402
import q1_core as core  # noqa: E402
import q1_scoring  # noqa: E402


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--from-start", action="store_true")
    ap.add_argument("--flush-seconds", type=int, default=30)
    ap.add_argument("--no-seed", action="store_true")
    a = ap.parse_args()
    cfg = config.load(required=("bucket", "stream"))
    bank, bucket = str(int(cfg["our_bank"])), cfg["bucket"]
    kinesis = boto3.client("kinesis", region_name=cfg["region"])
    s3 = boto3.client("s3", region_name=cfg["region"])
    scorer = q1_scoring.Scorer(bucket, bank, cfg["alerts_table"], float(cfg.get("audit_rate", 0.003)), cfg["region"])
    state = core.LocalFeatures()

    if not a.no_seed:                                   # "Seed Flink state (last 7 days, at startup)"
        n = 0
        for obj in s3.list_objects_v2(Bucket=bucket, Prefix="state/flink_seed/").get("Contents", []):
            if obj["Key"].endswith(".txt"):
                for line in s3.get_object(Bucket=bucket, Key=obj["Key"])["Body"].iter_lines():
                    try:
                        state.features(core.add_keys(aml_clean.clean_json(line.decode())))
                        n += 1
                    except aml_clean.Reject:
                        pass
        print(f"seeded state with {n} historical payments")

    kind = "TRIM_HORIZON" if a.from_start else "LATEST"
    shards = kinesis.list_shards(StreamName=cfg["stream"])["Shards"]
    its = [kinesis.get_shard_iterator(StreamName=cfg["stream"], ShardId=s["ShardId"], ShardIteratorType=kind)["ShardIterator"]
           for s in shards]
    rows, dlq, last, seen, alerts = [], [], time.time(), 0, 0
    print(f"reading {cfg['stream']} ({len(shards)} shard(s)) from {kind}. Ctrl+C to stop.")
    try:
        while True:
            got = 0
            for i, it in enumerate(its):
                r = kinesis.get_records(ShardIterator=it, Limit=1000)
                its[i] = r["NextShardIterator"]
                for rec in r["Records"]:
                    got += 1
                    raw = rec["Data"].decode(errors="replace").strip()
                    try:
                        p = aml_clean.clean_json(raw)
                    except aml_clean.Reject as exc:
                        dlq.append(q1_scoring.dlq_line(raw, str(exc)))
                        continue
                    if not aml_clean.involves_bank(p, bank):
                        continue                      # another bank's payment: not ours to score
                    core.add_keys(p)
                    arrived = int(rec["ApproximateArrivalTimestamp"].timestamp() * 1000)
                    row, item = scorer.score(p, state.features(p), arrived)
                    rows.append(row)
                    if item:
                        scorer.put_alert(item)
                        alerts += 1
            seen += got
            if time.time() - last >= a.flush_seconds and (rows or dlq):
                q1_scoring.write_scored(s3, bucket, rows, "local")
                q1_scoring.write_dlq(s3, bucket, dlq, "local")
                print(f"{time.strftime('%H:%M:%S')}  read {seen}, wrote {len(rows)} scored, {len(dlq)} rejected, {alerts} alerts so far")
                rows, dlq, last = [], [], time.time()
            if not got:
                time.sleep(0.25)
    except KeyboardInterrupt:
        if rows or dlq:
            q1_scoring.write_scored(s3, bucket, rows, "local")
            q1_scoring.write_dlq(s3, bucket, dlq, "local")
        print("stopped")


if __name__ == "__main__":
    main()
