"""First step of the Q2 nightly workflow: decide which day the run is for, once, so the Glue job
and the EMR job cannot disagree.

  input {"run_date": "2022-09-08"}   use it (the producer passes the simulated day that just ended)
  input {}                           yesterday, by the simulated clock if one is running, else the real clock
"""
import json
import os
import time
from datetime import datetime, timedelta, timezone

import boto3

s3 = boto3.client("s3")


def handler(event, context):
    if event.get("run_date"):
        return {"run_date": event["run_date"]}
    now = time.time()
    try:
        st = json.loads(s3.get_object(Bucket=os.environ["BUCKET"], Key="state/sim_clock.json")["Body"].read())
        elapsed = (now - float(st["wall_anchor"])) * float(st["speed"]) if st.get("running") else 0.0
        now = float(st["sim_anchor"]) + elapsed
    except s3.exceptions.NoSuchKey:
        pass
    return {"run_date": (datetime.fromtimestamp(now, timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")}
