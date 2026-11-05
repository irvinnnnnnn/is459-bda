#!/usr/bin/env python3
"""What the pipeline cost over a time window, worked out from what actually ran on AWS.

    python cost.py                                    the last 24 hours
    python cost.py --since 2026-11-03T09:00           from then until now (local time)
    python cost.py --since 2026-11-03T09:00 --until 2026-11-03T12:00 --runs --save run1.json
    python cost.py --load run1.json                   the saved quantities at today's PRICES; no AWS needed

It reads usage from AWS (Glue job runs, EMR instances, SageMaker jobs, CloudWatch metrics for Flink,
Kinesis, Firehose, EC2 and DynamoDB, Athena query history) and turns it into billable quantities in
the units AWS prices by (KPU-hours, shard-hours, DPU-hours, instance-hours, GB ...). Every cost is
quantity x rate, and both are printed, so the figures can be checked or re-priced by hand. --save
keeps the quantities; --load prices them again later, e.g. after editing PRICES.

PRICES are us-east-1 on-demand rates: check them on the AWS pricing pages before quoting the result.
Not counted, because they stay within the free tier or come to cents: Lambda, Step Functions,
CloudWatch (logs, metrics, alarms), S3 requests, EBS volumes on the EMR nodes, data transfer (free inside us-east-1).
"""
import argparse
import json
import math
import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "common"))
import config  # noqa: E402

# us-east-1 on-demand list prices, USD. Pages: aws.amazon.com/<service>/pricing
PRICES = {
    "flink_kpu_hour": 0.11,               # plus 1 KPU per application for orchestration
    "flink_storage_gb_month": 0.10,       # 50 GB of running storage per processing KPU
    "kinesis_shard_hour": 0.015,
    "kinesis_put_million": 0.014,         # per million 25 KB PUT payload units
    "firehose_ingest_gb": 0.029,          # each record is billed as at least 5 KB
    "firehose_conversion_gb": 0.018,      # JSON -> Parquet
    "firehose_partitioning_gb": 0.020,    # dynamic partitioning
    "glue_dpu_hour": 0.44,                # per second, 1-minute minimum
    "ec2_hour": {"t3.small": 0.0208, "t3.micro": 0.0104, "m5.xlarge": 0.192},
    "emr_hour": {"m5.xlarge": 0.048},     # EMR's fee, on top of the EC2 price
    "public_ipv4_hour": 0.005,
    "ebs_gb_month": 0.08,                 # gp3
    "sagemaker_hour": {"ml.m5.large": 0.115, "ml.m5.xlarge": 0.23, "ml.t3.medium": 0.05},
    "dynamodb_write_million": 0.625,      # on-demand
    "dynamodb_read_million": 0.125,
    "athena_tb": 5.0,                     # each query is billed for at least 10 MB
    "s3_gb_month": 0.023,
}
# Fixed names from main/infra/*.tf (not in config.json)
GLUE_JOBS = ["q1-features-historical", "q1-build-training", "q1-label-export", "q2-etl"]
FIREHOSE = "aml-raw-landing"
EMR_PREFIX = "q2-score-"
TRAINING_PREFIX, EVAL_PREFIX = "q1-train", "q1-eval"
GSI = "status-score-index"
GLUE_WORKER_DPU = {"G.025X": 0.25, "G.1X": 1, "G.2X": 2, "G.4X": 4, "G.8X": 8}
HOUR, MONTH_HOURS, GB = 3600.0, 730.0, 1024 ** 3
NOT_DEPLOYED = "not deployed (run terraform apply)"
MISSING = ("ResourceNotFoundException", "EntityNotFoundException", "InvalidInstanceID.NotFound", "NoSuchBucket")


def rate(key):
    """PRICES["ec2_hour"]["m5.xlarge"] for "ec2_hour:m5.xlarge"; None if there is no price for it."""
    name, _, kind = key.partition(":")
    p = PRICES.get(name)
    return p.get(kind) if isinstance(p, dict) else p


class Bill:
    """Billable quantities. Each item is priced as quantity x rate(price)."""

    def __init__(self):
        self.items, self.notes, self.runs, self.facts = [], {}, [], {}

    def fact(self, name, value):
        """Not billed, but needed to scale the run later (days replayed, alerts, data sizes)."""
        self.facts[name] = value

    def add(self, service, item, quantity, price):
        self.items.append({"service": service, "item": item, "quantity": quantity, "price": price})

    def note(self, service, text):
        self.notes[service] = text

    def run(self, name, at, usage, quantity, price):
        self.runs.append({"name": name, "at": at.isoformat(), "usage": usage, "quantity": quantity, "price": price})


def cost(item):
    return item["quantity"] * (rate(item["price"]) or 0.0)


class Window:
    def __init__(self, since, until):
        self.since, self.until = since, until

    def clip(self, start, end=None):
        """Seconds of [start, end) that fall inside the window (end None = still running)."""
        start, end = max(start, self.since), min(end or self.until, self.until)
        return max(0.0, (end - start).total_seconds())

    def has(self, t):
        return t is not None and self.since <= t < self.until


def metric(cw, w, namespace, name, dims, stat="Sum", period=300):
    """Datapoints of one CloudWatch metric over the window, fetched in chunks (1,440 points a call)."""
    points, start = [], w.since
    while start < w.until:
        end = min(w.until, start + timedelta(seconds=period * 1440))
        r = cw.get_metric_statistics(Namespace=namespace, MetricName=name, StartTime=start, EndTime=end,
                                     Period=period, Statistics=[stat],
                                     Dimensions=[{"Name": k, "Value": v} for k, v in dims.items()])
        points += [p[stat] for p in r["Datapoints"]]
        start = end
    return points


def billed(seconds, minimum=60):
    return max(seconds, minimum) / HOUR if seconds > 0 else 0.0


# ------------------------------------------------------------------ services ----
def flink(s, cfg, w, b):
    app = s.client("kinesisanalyticsv2").describe_application(ApplicationName=cfg["flink_app"])["ApplicationDetail"]
    par = app["ApplicationConfigurationDescription"]["FlinkApplicationConfigurationDescription"][
        "ParallelismConfigurationDescription"]
    kpus = math.ceil(par["Parallelism"] / par["ParallelismPerKPU"])
    # the application reports uptime every 5 minutes while it runs (KPUs is only reported about once an hour)
    hours = len(metric(s.client("cloudwatch"), w, "AWS/KinesisAnalytics", "uptime",
                       {"Application": cfg["flink_app"]}, "Maximum")) * 300 / HOUR
    b.note("Managed Flink", f"{hours:.2f} h running, {kpus} KPU + 1 for orchestration")
    b.add("Managed Flink", "KPU-hours", (kpus + 1) * hours, "flink_kpu_hour")
    b.add("Managed Flink", "running storage GB-months", kpus * 50 * hours / MONTH_HOURS, "flink_storage_gb_month")


def kinesis(s, cfg, w, b):
    d = s.client("kinesis").describe_stream_summary(StreamName=cfg["stream"])["StreamDescriptionSummary"]
    hours = w.clip(d["StreamCreationTimestamp"]) / HOUR
    cw = s.client("cloudwatch")
    records = sum(metric(cw, w, "AWS/Kinesis", "IncomingRecords", {"StreamName": cfg["stream"]}))
    size = sum(metric(cw, w, "AWS/Kinesis", "IncomingBytes", {"StreamName": cfg["stream"]}))
    b.note("Kinesis Data Streams", f"{d['OpenShardCount']} shard for {hours:.1f} h, {records:,.0f} payments"
           + (f", {size / records:.0f} B each" if records else ""))
    b.add("Kinesis Data Streams", "shard-hours", d["OpenShardCount"] * hours, "kinesis_shard_hour")
    b.add("Kinesis Data Streams", "million PUT payload units", records / 1e6, "kinesis_put_million")


def firehose(s, cfg, w, b):
    cw = s.client("cloudwatch")
    dims = {"DeliveryStreamName": FIREHOSE}
    records = sum(metric(cw, w, "AWS/Firehose", "DataReadFromKinesisStream.Records", dims))
    size = sum(metric(cw, w, "AWS/Firehose", "DataReadFromKinesisStream.Bytes", dims))
    per_record = math.ceil(size / records / 5120) * 5120 if records else 0
    billed_gb = records * per_record / GB
    b.note("Data Firehose", f"{records:,.0f} records, {size / GB:.3f} GB actual")
    b.add("Data Firehose", "GB ingested (5 KB per record)", billed_gb, "firehose_ingest_gb")
    b.add("Data Firehose", "GB converted to Parquet", billed_gb, "firehose_conversion_gb")
    b.add("Data Firehose", "GB dynamically partitioned", size / GB, "firehose_partitioning_gb")


def glue(s, cfg, w, b):
    client = s.client("glue")
    for job in GLUE_JOBS:
        service = f"Glue {job}"
        try:
            pages = list(client.get_paginator("get_job_runs").paginate(JobName=job))
        except client.exceptions.EntityNotFoundException:
            b.note(service, NOT_DEPLOYED)
            continue
        dpu_hours, n, secs = 0.0, 0, 0
        for page in pages:
            for r in page["JobRuns"]:
                if not w.has(r.get("StartedOn")):
                    continue
                dpu = r.get("MaxCapacity") or r.get("NumberOfWorkers", 0) * GLUE_WORKER_DPU.get(r.get("WorkerType"), 1)
                t = r.get("ExecutionTime", 0)
                q = dpu * billed(t)
                dpu_hours, n, secs = dpu_hours + q, n + 1, secs + t
                b.run(service, r["StartedOn"], f"{t} s x {dpu:g} DPU, {r['JobRunState']}", q, "glue_dpu_hour")
        b.note(service, f"{n} run{'s' * (n != 1)}, {secs:,} s in all")
        b.add(service, "DPU-hours (1-minute minimum per run)", dpu_hours, "glue_dpu_hour")


def emr(s, cfg, w, b):
    client, hours, n, secs = s.client("emr"), {}, 0, 0.0
    for page in client.get_paginator("list_clusters").paginate(CreatedAfter=w.since - timedelta(days=1)):
        for c in page["Clusters"]:
            if not c["Name"].startswith(EMR_PREFIX):
                continue
            up, mine = 0.0, {}
            for ipage in client.get_paginator("list_instances").paginate(ClusterId=c["Id"]):
                for i in ipage["Instances"]:
                    tl = i["Status"]["Timeline"]
                    t = w.clip(tl["CreationDateTime"], tl.get("EndDateTime")) if "CreationDateTime" in tl else 0
                    mine[i["InstanceType"]] = mine.get(i["InstanceType"], 0.0) + billed(t)
                    up = max(up, t)
            if up:
                n, secs = n + 1, secs + up
                for kind, h in mine.items():
                    hours[kind] = hours.get(kind, 0.0) + h
                    b.run("EMR " + c["Name"], c["Status"]["Timeline"]["CreationDateTime"],
                          f"{up:.0f} s up, {h:.3f} {kind} instance-hours, {c['Status']['State']}", h, f"ec2_hour:{kind}")
    b.note("EMR (Q2 scoring clusters)", f"{n} cluster{'s' * (n != 1)}, {secs:,.0f} s in all" if n
           else "no Q2 clusters in this window (created per Q2 run)")
    for kind, h in hours.items():
        b.add("EMR (Q2 scoring clusters)", f"{kind} instance-hours: EC2", h, f"ec2_hour:{kind}")
        b.add("EMR (Q2 scoring clusters)", f"{kind} instance-hours: EMR fee", h, f"emr_hour:{kind}")


def sagemaker(s, cfg, w, b):
    sm, hours, n = s.client("sagemaker"), {}, 0
    for page in sm.get_paginator("list_training_jobs").paginate(NameContains=TRAINING_PREFIX,
                                                                 CreationTimeAfter=w.since, CreationTimeBefore=w.until):
        for j in page["TrainingJobSummaries"]:
            d = sm.describe_training_job(TrainingJobName=j["TrainingJobName"])
            rc, t = d["ResourceConfig"], d.get("BillableTimeInSeconds", 0)
            h = t / HOUR * rc["InstanceCount"]
            hours[rc["InstanceType"]], n = hours.get(rc["InstanceType"], 0.0) + h, n + 1
            b.run("SageMaker " + j["TrainingJobName"], j["CreationTime"], f"{t} billable s on {rc['InstanceType']}",
                  h, f"sagemaker_hour:{rc['InstanceType']}")
    for page in sm.get_paginator("list_processing_jobs").paginate(NameContains=EVAL_PREFIX,
                                                                   CreationTimeAfter=w.since, CreationTimeBefore=w.until):
        for j in page["ProcessingJobSummaries"]:
            d = sm.describe_processing_job(ProcessingJobName=j["ProcessingJobName"])
            cc = d["ProcessingResources"]["ClusterConfig"]
            t = (d["ProcessingEndTime"] - d["ProcessingStartTime"]).total_seconds() if d.get("ProcessingEndTime") else 0
            h = t / HOUR * cc["InstanceCount"]
            hours[cc["InstanceType"]], n = hours.get(cc["InstanceType"], 0.0) + h, n + 1
            b.run("SageMaker " + j["ProcessingJobName"], j["CreationTime"], f"{t:.0f} s on {cc['InstanceType']}",
                  h, f"sagemaker_hour:{cc['InstanceType']}")
    b.note("SageMaker (training + evaluation)", f"{n} job{'s' * (n != 1)}" if n
           else "no jobs in this window (created by setup / retrain)")
    for kind, h in hours.items():
        b.add("SageMaker (training + evaluation)", f"{kind} instance-hours", h, f"sagemaker_hour:{kind}")


def ec2_apps(s, cfg, w, b):
    iid = cfg.get("apps_instance_id")
    if not iid:
        return
    ec2 = s.client("ec2")
    inst = ec2.describe_instances(InstanceIds=[iid])["Reservations"][0]["Instances"][0]
    # basic monitoring sends CPU every 5 minutes while the instance runs, so this survives stops and starts
    hours = len(metric(s.client("cloudwatch"), w, "AWS/EC2", "CPUUtilization", {"InstanceId": iid}, "Average")) * 300 / HOUR
    disk = sum(v["Size"] for v in ec2.describe_volumes(
        Filters=[{"Name": "attachment.instance-id", "Values": [iid]}])["Volumes"])
    b.note("EC2 apps server", f"{inst['InstanceType']} running {hours:.1f} h, {disk} GB disk")
    b.add("EC2 apps server", f"{inst['InstanceType']} instance-hours", hours, f"ec2_hour:{inst['InstanceType']}")
    b.add("EC2 apps server", "public IPv4 address-hours", hours, "public_ipv4_hour")
    b.add("EC2 apps server", "EBS GB-months", disk * w.clip(inst["LaunchTime"]) / HOUR / MONTH_HOURS, "ebs_gb_month")


def dynamodb(s, cfg, w, b):
    cw, t = s.client("cloudwatch"), cfg["alerts_table"]
    units = {}
    for kind in ("Write", "Read"):
        units[kind] = sum(sum(metric(cw, w, "AWS/DynamoDB", f"Consumed{kind}CapacityUnits", dims))
                          for dims in ({"TableName": t}, {"TableName": t, "GlobalSecondaryIndexName": GSI}))
    b.note("DynamoDB alerts table", f"{units['Write']:,.0f} write units, {units['Read']:,.0f} read units")
    b.add("DynamoDB alerts table", "million write request units", units["Write"] / 1e6, "dynamodb_write_million")
    b.add("DynamoDB alerts table", "million read request units", units["Read"] / 1e6, "dynamodb_read_million")


def athena(s, cfg, w, b):
    a, scanned, n, token = s.client("athena"), 0, 0, None
    while True:
        r = a.list_query_executions(WorkGroup=cfg["athena_workgroup"], **({"NextToken": token} if token else {}))
        ids, older = r["QueryExecutionIds"], False
        for k in range(0, len(ids), 50):
            for q in a.batch_get_query_execution(QueryExecutionIds=ids[k:k + 50])["QueryExecutions"]:
                at = q["Status"].get("SubmissionDateTime")
                if at and at < w.since:
                    older = True
                if w.has(at):
                    n += 1
                    scanned += max(10 * 1024 ** 2, q.get("Statistics", {}).get("DataScannedInBytes", 0))
        token = r.get("NextToken")
        if not token or older:
            break
    b.note("Athena", f"{n:,} queries")
    b.add("Athena", "TB scanned (10 MB minimum per query)", scanned / 1024 ** 4, "athena_tb")


def s3(s, cfg, w, b):
    # AWS reports the bucket size once a day, so add up the objects instead (by top-level prefix)
    by_prefix = {}
    for page in s.client("s3").get_paginator("list_objects_v2").paginate(Bucket=cfg["bucket"]):
        for o in page.get("Contents", []):
            top = o["Key"].split("/", 2)
            top = "/".join(top[:2]) if top[0] in ("raw", "curated") and len(top) > 2 else top[0]
            by_prefix[top] = by_prefix.get(top, 0) + o["Size"]
    b.fact("s3_bytes_by_prefix", dict(sorted(by_prefix.items())))
    gb = sum(by_prefix.values()) / GB
    b.note("S3 storage", f"{gb:.2f} GB stored")
    b.add("S3 storage", "GB-months", gb * (w.until - w.since).total_seconds() / HOUR / MONTH_HOURS, "s3_gb_month")


def q2_queue(s, cfg, w, b):
    """Customers in the Q2 queue after each nightly run (in_queue in scores/), for estimate.py's reviews."""
    import io
    import pyarrow.parquet as pq
    c, queue = s.client("s3"), {}
    for page in c.get_paginator("list_objects_v2").paginate(Bucket=cfg["bucket"], Prefix="scores/run_date="):
        for o in page.get("Contents", []):
            if o["Key"].endswith(".parquet"):
                run = o["Key"].split("run_date=", 1)[1].split("/", 1)[0]
                body = c.get_object(Bucket=cfg["bucket"], Key=o["Key"])["Body"].read()
                flags = pq.read_table(io.BytesIO(body), columns=["in_queue"]).column("in_queue").to_pylist()
                queue[run] = queue.get(run, 0) + sum(1 for v in flags if v)
    b.fact("q2_queue", dict(sorted(queue.items())))


def run_facts(s, cfg, w, b):
    """What the run covered, to scale its cost to more days or a bigger dataset."""
    clock = json.loads(s.client("s3").get_object(Bucket=cfg["bucket"], Key="state/sim_clock.json")["Body"].read())
    through = datetime.fromtimestamp(clock["sent_through"], timezone.utc)
    start = datetime.fromisoformat(cfg["live_start"]).replace(tzinfo=timezone.utc)
    b.fact("live_start", cfg["live_start"])
    b.fact("replayed_through", through.isoformat())
    b.fact("live_days_replayed", round((through - start).total_seconds() / 86400))
    b.fact("replay_speed", clock["speed"])
    b.fact("our_bank", cfg["our_bank"])
    q2 = s.client("stepfunctions").list_executions(stateMachineArn=cfg["q2_state_machine_arn"])["executions"]
    b.fact("q2_runs", {e["name"]: e["status"] for e in q2 if w.has(e["startDate"])})
    db, n, key = s.client("dynamodb"), 0, None
    while True:
        r = db.scan(TableName=cfg["alerts_table"], Select="COUNT", **({"ExclusiveStartKey": key} if key else {}))
        n, key = n + r["Count"], r.get("LastEvaluatedKey")
        if not key:
            break
    b.fact("alerts_in_table", n)


COLLECTORS = [("Managed Flink", flink), ("Kinesis Data Streams", kinesis), ("Data Firehose", firehose),
              ("Glue", glue), ("EMR (Q2 scoring clusters)", emr), ("SageMaker (training + evaluation)", sagemaker),
              ("EC2 apps server", ec2_apps), ("DynamoDB alerts table", dynamodb), ("Athena", athena), ("S3 storage", s3),
              ("Q2 queue", q2_queue), ("Run facts", run_facts)]


def collect(cfg, w):
    import boto3
    s = boto3.Session(region_name=cfg["region"])
    try:
        s.client("sts").get_caller_identity()
    except Exception as exc:
        sys.exit(f"Cannot reach AWS ({type(exc).__name__}). If the lab session ended, paste the new credentials "
                 "(README step 2) and run this again.")
    b, failed = Bill(), []
    for name, fn in COLLECTORS:
        try:
            fn(s, cfg, w, b)
        except Exception as exc:                    # one service the lab will not let us read must not stop the rest
            code = getattr(exc, "response", {}).get("Error", {}).get("Code", type(exc).__name__)
            if code in MISSING or (name == "Athena" and "WorkGroup is not found" in str(exc)):
                b.note(name, NOT_DEPLOYED)
            else:
                failed.append(f"{name}: {code}: {str(exc)[:120]}")
    return b, failed


# --------------------------------------------------------------------- report ----
def fmt(q):
    return f"{q:,.6f}".rstrip("0").rstrip(".") if q else "0"


def report(b, since, until, show_runs):
    local = lambda t: t.astimezone().strftime("%Y-%m-%d %H:%M")     # noqa: E731
    print(f"\nCost from {local(since)} to {local(until)} ({(until - since).total_seconds() / HOUR:.1f} h)\n")
    by_service = {svc: 0.0 for svc in b.notes}
    for it in b.items:
        by_service[it["service"]] = by_service.get(it["service"], 0.0) + cost(it)
    total = sum(by_service.values())
    width = max([len(svc) for svc in by_service] + [10])
    for svc, c in sorted(by_service.items(), key=lambda kv: -kv[1]):
        share = f"{c / total:5.1%}" if total else "     "
        print(f"  {svc:<{width}}  ${c:8.2f}  {share}  {b.notes.get(svc, '')}")
    print(f"  {'TOTAL':<{width}}  ${total:8.2f}")

    print("\nBillable quantities (cost = quantity x rate; rates are PRICES in main/scripts/cost.py):")
    unpriced = []
    for it in b.items:
        r = rate(it["price"])
        if r is None:
            unpriced.append(it["price"])
        print(f"  {it['service']:<{width}}  {it['item']:<40}  {fmt(it['quantity']):>14}  x {('$' + fmt(r)) if r is not None else 'NO PRICE':>10}"
              f"  = ${cost(it):.4f}")
    if unpriced:
        print(f"\n  No price for {sorted(set(unpriced))}: add them to PRICES, then run again with --load.")
    if show_runs and b.runs:
        print("\nEach run (quantity is DPU-hours for Glue, instance-hours for EMR and SageMaker):")
        for r in sorted(b.runs, key=lambda r: r["at"]):
            c = r["quantity"] * (rate(r["price"]) or 0.0)
            if r["price"].startswith("ec2_hour:"):                 # an EMR node also pays EMR's fee
                c += r["quantity"] * (rate("emr_hour:" + r["price"].split(":", 1)[1]) or 0.0)
            print(f"  {local(datetime.fromisoformat(r['at']))}  {r['name']:<42}  {fmt(r['quantity']):>10}  ${c:6.3f}  {r['usage']}")


def parse_time(s):
    return datetime.fromisoformat(s).astimezone(timezone.utc)      # no zone given = this computer's local time


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--since", help="start, e.g. 2026-11-03T09:00 (local time; default: 24 hours ago)")
    p.add_argument("--until", help="end (default: now)")
    p.add_argument("--runs", action="store_true", help="also list each Glue, EMR and SageMaker run")
    p.add_argument("--save", help="write the billable quantities to this JSON file")
    p.add_argument("--load", help="price the quantities in this JSON file (from --save) instead of reading AWS")
    a = p.parse_args()
    failed = []
    if a.load:
        with open(a.load) as fh:
            saved = json.load(fh)
        b = Bill()
        b.items, b.notes, b.runs, b.facts = saved["items"], saved["notes"], saved.get("runs", []), saved.get("facts", {})
        b.items = [it for it in b.items if not it["service"].startswith("CloudWatch")]   # runs saved before it was dropped
        b.notes = {k: v for k, v in b.notes.items() if not k.startswith("CloudWatch")}
        since, until = datetime.fromisoformat(saved["since"]), datetime.fromisoformat(saved["until"])
    else:
        cfg = config.load()
        until = parse_time(a.until) if a.until else datetime.now(timezone.utc)
        since = parse_time(a.since) if a.since else until - timedelta(hours=24)
        b, failed = collect(cfg, Window(since, until))
    report(b, since, until, a.runs)
    if b.facts:
        print("\nRun facts (not billed; to scale the cost to more days or a bigger dataset):")
        for k, v in b.facts.items():
            print(f"  {k:<22} {json.dumps(v)}")
    if failed:
        print("\nCould not read (not counted above):")
        for f in failed:
            print(f"  {f}")
    if a.save:
        with open(a.save, "w") as fh:
            json.dump({"since": since.isoformat(), "until": until.isoformat(), "items": b.items, "notes": b.notes,
                       "runs": b.runs, "facts": b.facts, "prices_used": {it["price"]: rate(it["price"]) for it in b.items}}, fh, indent=1)
        print(f"\nSaved the quantities to {a.save}. Price them again later with: python main/scripts/cost.py --load {a.save}")
    print("\nNot counted (free tier or cents): Lambda, Step Functions, CloudWatch (logs, metrics, alarms), S3 requests\n"
          "(billed per request, not per GB), EMR node disks, data transfer (free inside us-east-1).")
    print("Rates are PRICES at the top of main/scripts/cost.py: check them against the AWS pricing pages.")


if __name__ == "__main__":
    main()
