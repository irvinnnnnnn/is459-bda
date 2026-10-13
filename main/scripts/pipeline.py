#!/usr/bin/env python3
"""Run the parts of the pipeline that Terraform creates but does not start.

    python pipeline.py setup          once: history features + Flink seed, then train the first model
    python pipeline.py start          start the Flink application (about 3 minutes)
    python pipeline.py stop           stop it. Flink bills by the hour while it runs, so stop after a demo
    python pipeline.py status
    python pipeline.py retrain        run the Q1 train-and-promote workflow
    python pipeline.py labels         run the Q1 label export now
    python pipeline.py q2 --run-date 2022-09-08     run the Q2 nightly workflow for one day
    python pipeline.py reset          empty the alerts table and the live outputs, to replay from the start
    python pipeline.py apps           where the two Streamlit apps on EC2 are (the address changes on restart)
    python pipeline.py apps stop      stop that instance (apps start | apps stop)
"""
import argparse
import json
import os
import sys
import time
from datetime import datetime

import boto3

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "common"))
import config  # noqa: E402


def log(msg):
    print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)


def run_glue(cfg, job, args=None):
    glue = boto3.client("glue", region_name=cfg["region"])
    run_id = glue.start_job_run(JobName=job, Arguments=args or {})["JobRunId"]
    log(f"Glue job {job} started")
    while True:
        run = glue.get_job_run(JobName=job, RunId=run_id)["JobRun"]
        if run["JobRunState"] not in ("STARTING", "RUNNING", "STOPPING", "WAITING"):
            break
        time.sleep(20)
    log(f"Glue job {job}: {run['JobRunState']}")
    if run["JobRunState"] != "SUCCEEDED":
        sys.exit(run.get("ErrorMessage", "The job failed. See its CloudWatch log."))


def run_sfn(cfg, arn, payload):
    sfn = boto3.client("stepfunctions", region_name=cfg["region"])
    ex = sfn.start_execution(stateMachineArn=arn, input=json.dumps(payload))["executionArn"]
    log(f"workflow {arn.rsplit(':', 1)[1]} started")
    seen = 0
    while True:
        d = sfn.describe_execution(executionArn=ex)
        events = sfn.get_execution_history(executionArn=ex, maxResults=1000)["events"]
        for e in events[seen:]:
            if e["type"].endswith("StateEntered"):
                log(f"  {e['stateEnteredEventDetails']['name']}")
        seen = len(events)
        if d["status"] != "RUNNING":
            break
        time.sleep(20)
    log(f"workflow {d['status']}")
    if d["status"] != "SUCCEEDED":
        sys.exit(f"{d.get('error', '')} {d.get('cause', '')}"[:1500] or "See the execution in the Step Functions console.")
    return json.loads(d.get("output") or "{}")


def flink_status(cfg):
    if not cfg.get("flink_app"):
        sys.exit("No Flink application in main/config.json. If your account has no Managed Flink, run "
                 "main/Q1_files/flink/local_runner.py instead.")
    ka = boto3.client("kinesisanalyticsv2", region_name=cfg["region"])
    return ka, ka.describe_application(ApplicationName=cfg["flink_app"])["ApplicationDetail"]["ApplicationStatus"]


def wait_flink(cfg, want):
    while True:
        ka, st = flink_status(cfg)
        if st == want:
            return log(f"Flink application is {st}")
        if st not in ("STARTING", "STOPPING", "UPDATING", "AUTOSCALING"):
            sys.exit(f"Flink application is {st}; expected {want}. See /aws/managed-flink/{cfg['flink_app']} in CloudWatch Logs.")
        time.sleep(15)


def cmd_setup(cfg, a):
    run_glue(cfg, cfg["features_job"])
    s3 = boto3.client("s3", region_name=cfg["region"])
    parity = json.loads(s3.get_object(Bucket=cfg["bucket"], Key="state/parity/q1_features.json")["Body"].read())
    log(f"feature parity, Spark vs the Flink code: {parity['payments_compared']} payments, largest relative "
        f"difference {parity['largest_relative_difference']:.1e}")
    out = run_sfn(cfg, cfg["q1_state_machine_arn"], {})
    log(f"first model: {json.dumps(out.get('evaluation', {}).get('result', {}).get('challenger', {}))}")
    print("Next: python pipeline.py start")


def cmd_start(cfg, a):
    ka, st = flink_status(cfg)
    if st == "READY":
        ka.start_application(ApplicationName=cfg["flink_app"])
        log("starting Flink (about 3 minutes) ...")
    wait_flink(cfg, "RUNNING")
    print("Give it a minute to load the model and the 7-day seed, then start the producer.")


def cmd_stop(cfg, a):
    ka, st = flink_status(cfg)
    if st == "RUNNING":
        ka.stop_application(ApplicationName=cfg["flink_app"], Force=a.force)
        log("stopping Flink (it takes a snapshot of its state first) ...")
    wait_flink(cfg, "READY")


def cmd_status(cfg, a):
    _, st = flink_status(cfg)
    print(f"Flink application: {st}")
    s3 = boto3.client("s3", region_name=cfg["region"])
    try:
        m = json.loads(s3.get_object(Bucket=cfg["bucket"], Key="models/best/manifest.json")["Body"].read())
        print(f"Approved model:    {m['version']}  threshold {m['threshold']:.6f}  approved {m.get('approved_at')}")
    except s3.exceptions.NoSuchKey:
        print("Approved model:    none yet (run: python pipeline.py setup)")
    n = boto3.client("dynamodb", region_name=cfg["region"]).describe_table(TableName=cfg["alerts_table"])["Table"]["ItemCount"]
    print(f"Alerts table:      about {n} items (DynamoDB updates this count every few hours)")


def cmd_reset(cfg, a):
    if not a.yes:
        sys.exit("This deletes every alert, the scored and raw live payments, labels, Q2 outputs and decisions, "
                 "and the simulated clock. The history, the models and Terraform's resources are kept. "
                 "Add --yes to go ahead.")
    _, st = flink_status(cfg)
    if st != "READY":
        sys.exit("Stop Flink first (python pipeline.py stop). Its state would otherwise still hold the old replay.")
    table = boto3.resource("dynamodb", region_name=cfg["region"]).Table(cfg["alerts_table"])
    kw, n = {"ProjectionExpression": "txn_id"}, 0
    with table.batch_writer() as w:
        while True:
            r = table.scan(**kw)
            for it in r["Items"]:
                w.delete_item(Key=it)
                n += 1
            if "LastEvaluatedKey" not in r:
                break
            kw["ExclusiveStartKey"] = r["LastEvaluatedKey"]
    log(f"deleted {n} alert items")
    bucket = boto3.resource("s3", region_name=cfg["region"]).Bucket(cfg["bucket"])
    for prefix in ("raw/transactions/", "curated/live/", "curated/customer_txns/", "decisions/", "scores/",
                   "rule_hits/", "rule_eval/", "state/cp_first_seen/", "state/sim_clock.json"):
        bucket.objects.filter(Prefix=prefix).delete()
        log(f"emptied s3://{cfg['bucket']}/{prefix}")
    print("Flink's saved state still holds the old replay. Start it clean with:\n"
          "  python pipeline.py start --fresh")


def cmd_apps(cfg, a):
    if not cfg.get("apps_instance_id"):
        sys.exit("No apps instance in main/config.json (apps_on_ec2 is off, or terraform apply has not run).")
    ec2 = boto3.client("ec2", region_name=cfg["region"])
    iid = cfg["apps_instance_id"]
    if a.action == "stop":
        ec2.stop_instances(InstanceIds=[iid])
        ec2.get_waiter("instance_stopped").wait(InstanceIds=[iid])
    elif a.action == "start":
        ec2.start_instances(InstanceIds=[iid])
        ec2.get_waiter("instance_running").wait(InstanceIds=[iid])
    inst = ec2.describe_instances(InstanceIds=[iid])["Reservations"][0]["Instances"][0]
    state, ip = inst["State"]["Name"], inst.get("PublicIpAddress")
    print(f"Apps instance {iid}: {state}")
    if state == "running" and ip:
        print(f"  Q1 investigators  http://{ip}:8501")
        print(f"  Q2 KYC analysts   http://{ip}:8502")
        print("Use http, not https. The apps take about 3 minutes to come up after the instance starts.")


def cmd_start_fresh(cfg):
    ka, st = flink_status(cfg)
    if st == "READY":
        ka.start_application(ApplicationName=cfg["flink_app"], RunConfiguration={
            "ApplicationRestoreConfiguration": {"ApplicationRestoreType": "SKIP_RESTORE_FROM_SNAPSHOT"}})
        log("starting Flink with empty state; it will reload the 7-day seed ...")
    wait_flink(cfg, "RUNNING")


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    for name in ("setup", "status", "retrain", "labels"):
        sub.add_parser(name)
    s = sub.add_parser("start")
    s.add_argument("--fresh", action="store_true", help="ignore the saved state and reload the 7-day seed")
    s = sub.add_parser("stop")
    s.add_argument("--force", action="store_true", help="stop without taking a snapshot")
    s = sub.add_parser("q2")
    s.add_argument("--run-date", help="YYYY-MM-DD (default: yesterday by the simulated clock)")
    s = sub.add_parser("apps")
    s.add_argument("action", nargs="?", choices=["show", "start", "stop"], default="show")
    s = sub.add_parser("reset")
    s.add_argument("--yes", action="store_true")
    a = p.parse_args()
    cfg = config.load()
    if a.cmd == "setup":
        cmd_setup(cfg, a)
    elif a.cmd == "start":
        cmd_start_fresh(cfg) if a.fresh else cmd_start(cfg, a)
    elif a.cmd == "stop":
        cmd_stop(cfg, a)
    elif a.cmd == "status":
        cmd_status(cfg, a)
    elif a.cmd == "retrain":
        out = run_sfn(cfg, cfg["q1_state_machine_arn"], {})
        print(json.dumps(out.get("evaluation", {}).get("result", {}), indent=2))
    elif a.cmd == "labels":
        run_glue(cfg, cfg["label_export_job"])
    elif a.cmd == "q2":
        run_sfn(cfg, cfg["q2_state_machine_arn"], {"run_date": a.run_date} if a.run_date else {})
    elif a.cmd == "reset":
        cmd_reset(cfg, a)
    elif a.cmd == "apps":
        cmd_apps(cfg, a)


if __name__ == "__main__":
    main()
