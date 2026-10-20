#!/usr/bin/env python3
"""Load test for the Q1 streaming path: send payments at a fixed rate and measure how long the
pipeline takes to score them.

    python stress.py run --rate 20 --duration 120              20 payments a second for 2 minutes
    python stress.py run --stages 20:60,50:60,100:60           step the rate up: rate:seconds, ...
    python stress.py report --run-id <id>                      time to flag, minute by minute
    python stress.py cleanup --run-id <id>                     remove the run's alerts and files

The payments are synthetic and kept apart from the demo data, so a test never touches it:
  - accounts start with "S" and belong to our bank (some pay an account at another bank)
  - txn_id starts with "stress-<run-id>-"
  - event time starts at 2000-01-01 and moves --event-speed times faster than real time, so
    7-day windows fill up as they would in production and every file lands under date=2000-...

What "time to flag" means here (spec 3.4): arrived_at, the moment Kinesis accepted the payment,
to scored_at, the moment Flink had its score. The report reads both from curated/live/scored/.
"""
import argparse
import json
import math
import os
import random
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "common"))
import config  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
RUNS = os.path.join(HERE, ".stress_runs")
EVENT_START = datetime(2000, 1, 1, tzinfo=timezone.utc)
FORMATS = ["ACH", "Cheque", "Credit Card", "Wire", "Cash"]


def log(msg):
    print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)


def make_record(run_id, n, event_ts, bank, accounts, rng):
    src, dst = rng.randrange(accounts), rng.randrange(accounts)
    cross = rng.random() < 0.3
    amount = round(math.exp(rng.gauss(6.5, 1.8)), 2)
    return {"txn_id": f"stress-{run_id}-{n:09d}", "txn_ts": event_ts.strftime("%Y-%m-%dT%H:%M:%S"),
            "txn_date": event_ts.strftime("%Y-%m-%d"), "from_bank": bank, "from_account": f"S{src:08X}",
            "to_bank": "999999" if cross else bank, "to_account": f"S{dst:08X}",
            "amount_received": amount, "receiving_currency": "US Dollar", "amount_paid": amount,
            "payment_currency": "US Dollar", "payment_format": rng.choice(FORMATS), "is_laundering": 0}


def put(kinesis, stream, records):
    """Returns (records accepted, throttled) for one PutRecords call; throttled records are retried once."""
    entries = [{"Data": (json.dumps(r) + "\n").encode(), "PartitionKey": r["from_account"]} for r in records]
    throttled = 0
    for _ in range(2):
        resp = kinesis.put_records(StreamName=stream, Records=entries)
        if resp["FailedRecordCount"] == 0:
            return len(records), throttled
        entries = [e for e, res in zip(entries, resp["Records"]) if "ErrorCode" in res]
        throttled += len(entries)
        time.sleep(0.05)
    return len(records) - len(entries), throttled


def cmd_run(a):
    import boto3
    cfg = config.load(required=("bucket", "stream"))
    kinesis = boto3.client("kinesis", region_name=cfg["region"])
    stream = a.stream or cfg["stream"]
    stages = ([(float(r), float(s)) for r, s in (x.split(":") for x in a.stages.split(","))] if a.stages
              else [(a.rate, a.duration)])
    run_id = a.run_id or uuid.uuid4().hex[:8]
    rng = random.Random(run_id)
    bank = str(int(cfg["our_bank"]))
    log(f"run {run_id}: " + ", ".join(f"{r:g}/s for {s:g}s" for r, s in stages)
        + f" -> stream {stream}, {a.accounts} accounts")
    if max(r for r, _ in stages) > 1000 * a.shards:
        log(f"note: one Kinesis shard accepts 1,000 records a second; with {a.shards} shard(s) expect throttling")

    n = sent = throttled = 0
    t_run = time.time()
    put_ms = []
    try:
        for rate, seconds in stages:
            t0, due, last = time.monotonic(), 0.0, time.monotonic()
            stage_sent = 0
            while True:
                elapsed = time.monotonic() - t0
                if elapsed >= seconds:
                    break
                due = rate * elapsed                    # how many should have gone out by now
                batch = []
                while stage_sent + len(batch) < due and len(batch) < 500:
                    event_ts = EVENT_START + timedelta(seconds=(time.time() - t_run) * a.event_speed)
                    batch.append(make_record(run_id, n, event_ts, bank, a.accounts, rng))
                    n += 1
                if batch:
                    t = time.monotonic()
                    ok, thr = put(kinesis, stream, batch)
                    put_ms.append((time.monotonic() - t) * 1000)
                    sent, throttled, stage_sent = sent + ok, throttled + thr, stage_sent + len(batch)
                if time.monotonic() - last > 5:
                    log(f"  {rate:g}/s target, {stage_sent / max(elapsed, 1e-9):.1f}/s actual, sent {sent}, "
                        f"throttled {throttled}")
                    last = time.monotonic()
                time.sleep(max(0.0, min(0.05, 1.0 / rate)))
            log(f"stage {rate:g}/s done: {stage_sent} sent in {time.monotonic() - t0:.1f}s")
    except KeyboardInterrupt:
        log("stopped early")
    finally:
        t_end = time.time()
        os.makedirs(RUNS, exist_ok=True)
        end_event = EVENT_START + timedelta(seconds=(t_end - t_run) * a.event_speed)
        info = {"run_id": run_id, "stages": stages, "sent": sent, "throttled": throttled,
                "started": t_run, "ended": t_end, "first_date": EVENT_START.strftime("%Y-%m-%d"),
                "last_date": end_event.strftime("%Y-%m-%d")}
        with open(os.path.join(RUNS, f"{run_id}.json"), "w") as fh:
            json.dump(info, fh)
    put_ms.sort()
    if put_ms:
        log(f"sent {sent} in {t_end - t_run:.1f}s ({sent / (t_end - t_run):.1f}/s), throttled {throttled}; "
            f"PutRecords call p50 {put_ms[len(put_ms) // 2]:.0f} ms, p99 {put_ms[int(len(put_ms) * 0.99)]:.0f} ms")
    print(f"\nFlink writes scored payments to S3 every 30 seconds or so. In a minute, run:\n"
          f"  python stress.py report --run-id {run_id}")


def athena(cfg, sql):
    import boto3
    ath = boto3.client("athena", region_name=cfg["region"])
    qid = ath.start_query_execution(QueryString=sql, QueryExecutionContext={"Database": cfg["athena_db"]},
                                    WorkGroup=cfg["athena_workgroup"])["QueryExecutionId"]
    while True:
        st = ath.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]
        if st["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(0.5)
    if st["State"] != "SUCCEEDED":
        sys.exit(f"Athena query {st['State'].lower()}: {st.get('StateChangeReason', '')}")
    rows = []
    for page in ath.get_paginator("get_query_results").paginate(QueryExecutionId=qid):
        rows += [[c.get("VarCharValue") for c in r["Data"]] for r in page["ResultSet"]["Rows"]]
    return rows[1:]


def run_info(run_id):
    path = os.path.join(RUNS, f"{run_id}.json")
    if not os.path.exists(path):
        sys.exit(f"No record of run {run_id} in {RUNS}")
    with open(path) as fh:
        return json.load(fh)


def cmd_report(a):
    import boto3
    cfg = config.load()
    info = run_info(a.run_id)
    where = (f"date BETWEEN '{info['first_date']}' AND '{info['last_date']}' "
             f"AND txn_id LIKE 'stress-{a.run_id}-%'")
    lag = "date_diff('millisecond', arrived_at, scored_at)"
    rows = athena(cfg, f"""
        SELECT date_format(date_trunc('minute', arrived_at), '%H:%i') AS minute, count(DISTINCT txn_id) AS n,
               approx_percentile({lag}, 0.5), approx_percentile({lag}, 0.95), approx_percentile({lag}, 0.99),
               max({lag}), count_if(is_alert), count_if(is_audit)
        FROM live_scored WHERE {where} GROUP BY 1 ORDER BY 1""")
    total = athena(cfg, f"""
        SELECT count(DISTINCT txn_id), approx_percentile({lag}, 0.5), approx_percentile({lag}, 0.99)
        FROM live_scored WHERE {where}""")[0]
    scored = int(total[0] or 0)
    print(f"\nRun {a.run_id}: sent {info['sent']}, scored {scored}"
          + ("" if scored >= info["sent"] else f"  ({info['sent'] - scored} not in S3 yet, or still queued)"))
    print("Time to flag = Kinesis arrival -> scored, in milliseconds\n")
    print(f"{'minute':>6} {'scored':>8} {'per sec':>8} {'p50':>8} {'p95':>8} {'p99':>8} {'max':>8} {'alerts':>7} {'audit':>6}")
    for m, n, p50, p95, p99, mx, al, au in rows:
        print(f"{m:>6} {int(n):>8} {int(n) / 60:>8.1f} {int(p50):>8} {int(p95):>8} {int(p99):>8} {int(mx):>8} "
              f"{int(al):>7} {int(au):>6}")
    if scored:
        print(f"\nWhole run: p50 {int(total[1])} ms, p99 {int(total[2])} ms")

    # How far behind the stream the consumers fell (spec 3.4: Kinesis iterator age)
    cw = boto3.client("cloudwatch", region_name=cfg["region"])
    start = datetime.fromtimestamp(info["started"], timezone.utc)
    end = datetime.fromtimestamp(info["ended"], timezone.utc) + timedelta(minutes=5)
    queries = [("GetRecords.IteratorAgeMilliseconds", "AWS/Kinesis", [{"Name": "StreamName", "Value": cfg["stream"]}],
                "Kinesis iterator age, worst"),
               ("millisBehindLatest", "AWS/KinesisAnalytics",
                [{"Name": "Application", "Value": cfg.get("flink_app", "")}], "Flink behind the stream, worst")]
    for metric, ns, dims, label in queries:
        pts = cw.get_metric_statistics(Namespace=ns, MetricName=metric, Dimensions=dims, StartTime=start,
                                       EndTime=end, Period=60, Statistics=["Maximum"])["Datapoints"]
        if pts:
            print(f"{label}: {max(p['Maximum'] for p in pts) / 1000:.1f} s")
    print("\nReading it: if p99 stays flat as the rate steps up, the pipeline has room to spare. If p99 and the "
          "iterator age\nclimb together, Flink cannot keep up at that rate: raise its parallelism (flink_parallelism "
          "in terraform.tfvars).")


def cmd_cleanup(a):
    import boto3
    cfg = config.load()
    info = run_info(a.run_id)
    table = boto3.resource("dynamodb", region_name=cfg["region"]).Table(cfg["alerts_table"])
    kw, n = {"ProjectionExpression": "txn_id", "FilterExpression": "begins_with(txn_id, :p)",
             "ExpressionAttributeValues": {":p": f"stress-{a.run_id}-"}}, 0
    with table.batch_writer() as w:
        while True:
            r = table.scan(**kw)
            for it in r["Items"]:
                w.delete_item(Key={"txn_id": it["txn_id"]})
                n += 1
            if "LastEvaluatedKey" not in r:
                break
            kw["ExclusiveStartKey"] = r["LastEvaluatedKey"]
    log(f"deleted {n} alert and audit items")
    s3 = boto3.resource("s3", region_name=cfg["region"]).Bucket(cfg["bucket"])
    day, last = datetime.strptime(info["first_date"], "%Y-%m-%d"), datetime.strptime(info["last_date"], "%Y-%m-%d")
    while day <= last:                                  # stress dates hold nothing but stress payments
        for prefix in ("curated/live/scored/", "raw/transactions/"):
            s3.objects.filter(Prefix=f"{prefix}date={day:%Y-%m-%d}/").delete()
        day += timedelta(days=1)
    log(f"deleted S3 files for {info['first_date']} to {info['last_date']}")


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("run", help="send synthetic payments at a fixed rate")
    s.add_argument("--rate", type=float, default=20, help="payments per second")
    s.add_argument("--duration", type=float, default=120, help="seconds")
    s.add_argument("--stages", help="step test, e.g. 20:60,50:60,100:60 (rate:seconds). Overrides --rate/--duration")
    s.add_argument("--accounts", type=int, default=5000, help="size of the synthetic account pool")
    s.add_argument("--event-speed", type=float, default=60, help="simulated seconds per real second")
    s.add_argument("--shards", type=int, default=1, help="shards in the stream (for the throttling note only)")
    s.add_argument("--stream")
    s.add_argument("--run-id")
    s.set_defaults(func=cmd_run)
    s = sub.add_parser("report", help="time to flag for one run")
    s.add_argument("--run-id", required=True)
    s.set_defaults(func=cmd_report)
    s = sub.add_parser("cleanup", help="delete one run's alerts and S3 files")
    s.add_argument("--run-id", required=True)
    s.set_defaults(func=cmd_cleanup)
    a = p.parse_args()
    a.func(a)


if __name__ == "__main__":
    main()
