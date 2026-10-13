"""Q1 training workflow, step 3 (spec 3.2): SageMaker Processing job, champion vs challenger.

Scores the new model (challenger) and the model currently in models/best/manifest.json (champion)
on the SAME test split, which neither training job has seen, with the same alert budget.
Metrics: PR-AUC, and recall when each model alerts on its top 1% of the test payments.

Writes to models/artifacts/run=<run_id>/:
  evaluation.json   both models' figures and "promote": true/false   (the "better?" choice reads this)
  manifest.json     the pointer that becomes models/best/manifest.json if the model is approved

Arguments: --bucket --run_id --bank
"""
import argparse
import json
import os
import subprocess
import sys
import tempfile
from datetime import datetime, timezone

if not os.environ.get("SKIP_PIP"):
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "lightgbm==4.3.0", "pyarrow"])

import boto3  # noqa: E402
import lightgbm as lgb  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from sklearn.metrics import average_precision_score  # noqa: E402

MANIFEST = "models/best/manifest.json"


def load_model(s3, bucket, prefix):
    meta = json.loads(s3.get_object(Bucket=bucket, Key=prefix + "meta.json")["Body"].read())
    with tempfile.NamedTemporaryFile(suffix=".txt", delete=False) as fh:
        s3.download_fileobj(bucket, prefix + "model.txt", fh)
    return lgb.Booster(model_file=fh.name), meta


def payment_scores(booster, meta, df):
    """One score per payment: the higher of our bank's views, the other bank's side blanked."""
    bank, feats = meta["bank"], meta["features"]
    cross = df["from_bank"] != df["to_bank"]
    parts = []
    for side, blank in (("from_bank", meta["r_cols"]), ("to_bank", meta["s_cols"])):
        sub = df[df[side] == bank]
        if len(sub):
            x = sub[feats].astype(np.float32)
            x.loc[cross[sub.index].values, blank] = np.nan
            parts.append(pd.Series(booster.predict(x), index=sub.index))
    return pd.concat(parts).groupby(level=0).max().reindex(df.index).fillna(0.0).values


def evaluate(booster, meta, df):
    score, y = payment_scores(booster, meta, df), df["label"].values
    k = max(1, int(round(len(df) * meta.get("alert_rate", 0.01))))
    caught = int(y[np.argsort(-score)[:k]].sum())
    return {"version": meta["version"], "alerts": k, "caught": caught,
            "recall_at_budget": caught / int(y.sum()) if y.sum() else 0.0,
            "precision_at_budget": caught / k,
            "pr_auc": float(average_precision_score(y, score)) if y.sum() else 0.0}


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--bucket", required=True)
    p.add_argument("--run_id", required=True)
    p.add_argument("--bank", required=True)
    a = p.parse_args()
    s3 = boto3.client("s3")
    df = pd.read_parquet(os.environ.get("TEST_DIR", "/opt/ml/processing/test")).reset_index(drop=True)
    df["from_bank"], df["to_bank"] = df["from_bank"].astype(str), df["to_bank"].astype(str)
    df["label"] = df["label"].astype(int)

    prefix = f"models/artifacts/run={a.run_id}/"
    new, new_meta = load_model(s3, a.bucket, prefix)
    result = {"run_id": a.run_id, "test_payments": int(len(df)), "test_laundering": int(df.label.sum()),
              "challenger": evaluate(new, new_meta, df), "champion": None}
    try:
        current = json.loads(s3.get_object(Bucket=a.bucket, Key=MANIFEST)["Body"].read())
    except s3.exceptions.NoSuchKey:
        current = None
    if current is None:
        result.update(promote=True, reason="No approved model yet: the first model is promoted.")
    else:
        old, old_meta = load_model(s3, a.bucket, current["artifact"])
        result["champion"] = evaluate(old, old_meta, df)
        c, o = result["challenger"], result["champion"]
        better = (c["recall_at_budget"], c["pr_auc"]) > (o["recall_at_budget"], o["pr_auc"])
        result.update(promote=bool(better), reason=(
            f"Challenger recall {c['recall_at_budget']:.3f} / PR-AUC {c['pr_auc']:.4f} vs champion "
            f"{o['recall_at_budget']:.3f} / {o['pr_auc']:.4f} at the same alert budget on the same test split."))
    print(json.dumps(result, indent=2))

    manifest = {"version": new_meta["version"], "artifact": prefix, "threshold": new_meta["threshold"],
                "training_data": f"training/run={a.run_id}/",
                "metrics": result["challenger"], "approved_at": datetime.now(timezone.utc).isoformat(timespec="seconds")}
    for name, obj in (("evaluation.json", result), ("manifest.json", manifest)):
        s3.put_object(Bucket=a.bucket, Key=prefix + name, Body=json.dumps(obj, indent=2).encode(),
                      ContentType="application/json")


if __name__ == "__main__":
    main()
