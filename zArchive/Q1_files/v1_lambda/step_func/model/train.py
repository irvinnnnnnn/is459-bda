"""
IS459 G2 - Q1 SageMaker training (script mode, scikit-learn container + lightgbm).

Reads  /opt/ml/input/data/train/  (= s3://<bucket>/q1/features/, uses split=train and split=valid)
Trains one LightGBM model for our bank (same settings as fraud_detect.ipynb), sets the alert
threshold so ~1% of validation payments are alerted, and saves to
s3://<bucket>/q1/models/archive/<version>/  (model.txt, model.json, meta.json, parity.json)

Hyperparameters (passed by Step Functions): --bucket --bank --version
"""
import argparse
import json
import os
from datetime import datetime, timezone

import boto3
import lightgbm as lgb
import numpy as np
import pandas as pd

ALERT_RATE, NEG_RATE, POS_WEIGHT, SEED = 0.01, 0.2, 5, 0


def bank_views(df, bank, feats, s_cols, r_cols):
    """Sender's-bank view and receiver's-bank view; the other bank's side is blanked for cross-bank."""
    send, recv = df[df["from_bank"] == bank], df[df["to_bank"] == bank]
    x0 = send[feats].astype(np.float32)
    x0.loc[(send["from_bank"] != send["to_bank"]).values, r_cols] = np.nan
    x1 = recv[feats].astype(np.float32)
    x1.loc[(recv["from_bank"] != recv["to_bank"]).values, s_cols] = np.nan
    return [(x0, send.index), (x1, recv.index)]


def payment_scores(model, df, bank, feats, s_cols, r_cols):
    """One score per payment = max over the views our bank has of it."""
    parts = [pd.Series(model.predict(x), index=idx) for x, idx in bank_views(df, bank, feats, s_cols, r_cols)
             if len(x)]
    return pd.concat(parts).groupby(level=0).max().reindex(df.index)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--bucket", required=True)
    p.add_argument("--bank", required=True)
    p.add_argument("--version", required=True)
    a, _ = p.parse_known_args()
    bank = str(int(a.bank))
    data_dir = os.environ.get("SM_CHANNEL_TRAIN", "/opt/ml/input/data/train")

    df = pd.read_parquet(data_dir)
    df["split"] = df["split"].astype(str)
    df["from_bank"], df["to_bank"] = df["from_bank"].astype(str), df["to_bank"].astype(str)
    meta_cols = {"txn_id", "ts", "from_bank", "to_bank", "label", "split"}
    feats = [c for c in df.columns if c not in meta_cols]
    s_cols = [f for f in feats if f.startswith("s_")] + ["amt_match"]
    r_cols = [f for f in feats if f.startswith("r_")]
    train = df[df["split"] == "train"].reset_index(drop=True)
    valid = df[df["split"] == "valid"].reset_index(drop=True)
    print(f"[train] train={len(train)} (pos {int(train['label'].sum())})  "
          f"valid={len(valid)} (pos {int(valid['label'].sum())})")

    # training rows: both views, all laundering + a 20% sample of normal payments (weighted back up)
    rng = np.random.default_rng(SEED)
    keep = (train["label"] == 1) | (rng.random(len(train)) < NEG_RATE)
    sub = train[keep]
    xs, ys, ws = [], [], []
    for x, idx in bank_views(sub, bank, feats, s_cols, r_cols):
        y = sub.loc[idx, "label"].values
        xs.append(x), ys.append(y), ws.append(np.where(y == 1, POS_WEIGHT, 1.0 / NEG_RATE))
    X, y, w = pd.concat(xs), np.concatenate(ys), np.concatenate(ws)
    model = lgb.LGBMClassifier(n_estimators=400, learning_rate=0.05, num_leaves=31, min_child_samples=50,
                               subsample=0.8, subsample_freq=1, colsample_bytree=0.8,
                               random_state=SEED, verbose=-1).fit(X, y, sample_weight=w)
    booster = model.booster_

    # threshold: top 1% of validation payments become alerts
    pv = payment_scores(booster, valid, bank, feats, s_cols, r_cols) if len(valid) else \
        payment_scores(booster, train, bank, feats, s_cols, r_cols)
    thr = float(np.quantile(pv.values, 1 - ALERT_RATE))
    yv = (valid if len(valid) else train)["label"].values
    flag = pv.values >= thr
    precision = float(yv[flag].mean()) if flag.any() else 0.0
    recall = float(yv[flag].sum() / yv.sum()) if yv.sum() else 0.0
    print(f"[train] threshold={thr:.4f} valid precision={precision:.3f} recall={recall:.3f}")

    # parity samples so the Lambda's pure-Python scorer can check itself against LightGBM
    sample = X.sample(min(50, len(X)), random_state=SEED)
    parity = {"x": [[None if v != v else float(v) for v in row] for row in sample.values.tolist()],
              "prob": booster.predict(sample).tolist()}

    meta = {"version": a.version, "bank": bank, "threshold": thr, "alert_rate": ALERT_RATE,
            "features": feats, "s_cols": s_cols, "r_cols": r_cols,
            "trained_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "n_train": int(len(train)), "n_train_pos": int(train["label"].sum()),
            "valid_precision": precision, "valid_recall": recall}

    out_dir = os.environ.get("SM_MODEL_DIR", "/opt/ml/model")
    os.makedirs(out_dir, exist_ok=True)
    booster.save_model(os.path.join(out_dir, "model.txt"))
    files = {"model.json": booster.dump_model(), "meta.json": meta, "parity.json": parity}
    for name, obj in files.items():
        with open(os.path.join(out_dir, name), "w") as f:
            json.dump(obj, f)
    s3 = boto3.client("s3")
    for name in ["model.txt", "model.json", "parity.json", "meta.json"]:
        s3.upload_file(os.path.join(out_dir, name), a.bucket, f"q1/models/archive/{a.version}/{name}")
    print(f"[train] saved to s3://{a.bucket}/q1/models/archive/{a.version}/")


if __name__ == "__main__":
    main()
