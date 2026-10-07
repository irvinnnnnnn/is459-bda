"""
IS459 G2 - Q1 SageMaker Processing job: compare the new model with the current best.

Both models are scored on the SAME data at the SAME alert rate (top 1% of payments):
  bench   fixed held-out slice of history, identical for every model version
  recent  most recent reviewed live days (labels from investigator decisions)

Inputs (mounted by Step Functions):
  /opt/ml/processing/code/       this script
  /opt/ml/processing/features/   s3://<bucket>/q1/features/
  /opt/ml/processing/models/     s3://<bucket>/q1/models/   (best/ and archive/<version>/)
Output:
  /opt/ml/processing/output/result.json  ->  s3://<bucket>/q1/evaluation/<version>/
Args: --version <new model version>
"""
import argparse
import json
import os
import subprocess
import sys

if not os.environ.get("SKIP_PIP"):
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "lightgbm==4.3.0", "pyarrow==14.0.2"])

import lightgbm as lgb  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

BASE = os.environ.get("PROCESSING_BASE", "/opt/ml/processing")


def bank_views(df, bank, feats, s_cols, r_cols):
    send, recv = df[df["from_bank"] == bank], df[df["to_bank"] == bank]
    x0 = send[feats].astype(np.float32)
    x0.loc[(send["from_bank"] != send["to_bank"]).values, r_cols] = np.nan
    x1 = recv[feats].astype(np.float32)
    x1.loc[(recv["from_bank"] != recv["to_bank"]).values, s_cols] = np.nan
    return [(x0, send.index), (x1, recv.index)]


def load(folder):
    meta = json.load(open(os.path.join(folder, "meta.json")))
    return lgb.Booster(model_file=os.path.join(folder, "model.txt")), meta


def evaluate(booster, meta, df):
    """Recall and precision when this model alerts on its top 1% of the payments in df."""
    parts = [pd.Series(booster.predict(x[meta["features"]]), index=idx)
             for x, idx in bank_views(df, meta["bank"], meta["features"], meta["s_cols"], meta["r_cols"])
             if len(x)]
    score = pd.concat(parts).groupby(level=0).max().reindex(df.index).values
    y = df["label"].values
    k = max(1, int(round(len(df) * meta["alert_rate"])))
    top = np.argsort(-score)[:k]
    caught = int(y[top].sum())
    return {"alerts": k, "caught": caught,
            "recall": caught / int(y.sum()) if y.sum() else None,
            "precision": caught / k}


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--version", required=True)
    a = p.parse_args()
    df = pd.read_parquet(f"{BASE}/features")
    df["split"] = df["split"].astype(str)
    df["from_bank"], df["to_bank"] = df["from_bank"].astype(str), df["to_bank"].astype(str)

    best, best_meta = load(f"{BASE}/models/best")
    new, new_meta = load(f"{BASE}/models/archive/{a.version}")
    result = {"new_version": a.version, "best_version": best_meta["version"]}
    for name in ["bench", "recent"]:
        part = df[df["split"] == name].reset_index(drop=True)
        if part.empty:
            result[name] = {"payments": 0, "positives": 0}
            continue
        result[name] = {"payments": int(len(part)), "positives": int(part["label"].sum()),
                        "new": evaluate(new, new_meta, part), "best": evaluate(best, best_meta, part)}
    print(json.dumps(result, indent=2))
    os.makedirs(f"{BASE}/output", exist_ok=True)
    json.dump(result, open(f"{BASE}/output/result.json", "w"), indent=2)


if __name__ == "__main__":
    main()
