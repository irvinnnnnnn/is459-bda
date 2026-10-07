"""
IS459 G2 - Q1 promote Lambda (called by the Step Function as the last step).

setup:    the first model goes straight to models/best/.
retrain:  reads q1/evaluation/<version>/result.json. The new model is promoted only if it
          catches at least as much laundering as the current best (same data, same 1% alert rate)
          on every evaluation set that has enough laundering cases to judge.
Promotion = copy models/archive/<version>/ -> models/best/ (meta.json last, which the
scoring Lambda watches). Every version stays in archive/, so rolling back is a copy too.

Event: {"mode": "setup"|"retrain", "version": "<execution name>", "bucket": "<bucket>"}
"""
import json

import boto3

MIN_POSITIVES = 3      # don't judge on a set with fewer laundering cases than this
MARGIN = 0.0           # new recall must be >= best recall + MARGIN
s3 = boto3.client("s3")


def decide(result):
    checks, notes = [], []
    for name in ["bench", "recent"]:
        r = result.get(name, {})
        if r.get("positives", 0) < MIN_POSITIVES:
            notes.append(f"{name}: skipped ({r.get('positives', 0)} laundering cases)")
            continue
        new, best = r["new"]["recall"], r["best"]["recall"]
        checks.append(new >= best + MARGIN)
        notes.append(f"{name}: new recall {new:.3f} vs best {best:.3f}")
    return (bool(checks) and all(checks)), "; ".join(notes)


def handler(event, context):
    bucket, version, mode = event["bucket"], event["version"], event["mode"]
    if mode == "setup":
        promote, reason = True, "initial model"
    else:
        key = f"q1/evaluation/{version}/result.json"
        result = json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
        promote, reason = decide(result)

    if promote:
        src = f"q1/models/archive/{version}/"
        for name in ["model.txt", "model.json", "parity.json", "meta.json"]:
            s3.copy_object(Bucket=bucket, Key=f"q1/models/best/{name}",
                           CopySource={"Bucket": bucket, "Key": src + name})
    print(f"[promote] version={version} promoted={promote} ({reason})")
    return {"version": version, "promoted": promote, "reason": reason}
