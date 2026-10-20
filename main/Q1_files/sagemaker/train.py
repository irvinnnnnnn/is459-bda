"""Q1 training workflow, step 2 (spec 3.2): SageMaker training job (script mode, scikit-learn
container + lightgbm from requirements.txt).

Reads the train/ and val/ channels (training/run=<run_id>/train/ and val/). val is used for early
stopping and to set the alert threshold; the test split never reaches this job.
Writes to models/artifacts/run=<run_id>/:
  model.txt     LightGBM model
  model.json    the same model as a JSON dump, which Flink scores in pure Python
  meta.json     version, threshold, feature lists, validation figures
  parity.json   sample inputs with LightGBM's own probabilities and SHAP values, so Flink can check
                its scorer against LightGBM each time it loads the model

Hyperparameters: --bucket --bank --run_id
"""
import argparse
import json
import os
from datetime import datetime, timezone

import boto3
import lightgbm as lgb
import numpy as np
import pandas as pd

ALERT_RATE = 0.01                       # alert budget: the top 1% of payments
NEG_RATE, POS_WEIGHT, SEED = 0.2, 5, 0
META_COLS = {"txn_id", "txn_ts", "from_bank", "to_bank", "label", "source"}
PARAMS = {"objective": "binary", "metric": "average_precision", "learning_rate": 0.05, "num_leaves": 31,
          "min_child_samples": 50, "subsample": 0.8, "subsample_freq": 1, "colsample_bytree": 0.8,
          "seed": SEED, "verbose": -1}


def bank_views(df, bank, feats, s_cols, r_cols):
    """Our bank sees a payment as the sender's bank, the receiver's bank, or both. For a cross-bank
    payment the other bank's features are blanked: we cannot see that account's other payments."""
    cross = df["from_bank"] != df["to_bank"]
    send, recv = df[df["from_bank"] == bank], df[df["to_bank"] == bank]
    x0 = send[feats].astype(np.float32)
    x0.loc[cross[send.index].values, r_cols] = np.nan
    x1 = recv[feats].astype(np.float32)
    x1.loc[cross[recv.index].values, s_cols] = np.nan
    return [(x0, send.index), (x1, recv.index)]


def payment_scores(booster, df, bank, feats, s_cols, r_cols):
    """One score per payment: the higher of the views our bank has of it."""
    parts = [pd.Series(booster.predict(x), index=idx) for x, idx in bank_views(df, bank, feats, s_cols, r_cols)
             if len(x)]
    return pd.concat(parts).groupby(level=0).max().reindex(df.index).fillna(0.0)


def stacked(df, bank, feats, s_cols, r_cols, weights=False):
    xs, ys, ws = [], [], []
    for x, idx in bank_views(df, bank, feats, s_cols, r_cols):
        y = df.loc[idx, "label"].values
        xs.append(x), ys.append(y), ws.append(np.where(y == 1, POS_WEIGHT, 1.0 / NEG_RATE))
    return pd.concat(xs), np.concatenate(ys), (np.concatenate(ws) if weights else None)


def load(folder):
    df = pd.read_parquet(folder).reset_index(drop=True)
    df["from_bank"], df["to_bank"] = df["from_bank"].astype(str), df["to_bank"].astype(str)
    df["label"] = df["label"].astype(int)
    return df


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--bucket", required=True)
    p.add_argument("--bank", required=True)
    p.add_argument("--run_id", required=True)
    a, _ = p.parse_known_args()
    bank = str(int(a.bank))
    train = load(os.environ.get("SM_CHANNEL_TRAIN", "/opt/ml/input/data/train"))
    val = load(os.environ.get("SM_CHANNEL_VAL", "/opt/ml/input/data/val"))
    feats = [c for c in train.columns if c not in META_COLS]
    s_cols = [f for f in feats if f.startswith("s_")] + ["amt_match"]
    r_cols = [f for f in feats if f.startswith("r_")]
    print(f"[train] train={len(train)} ({int(train.label.sum())} laundering)  "
          f"val={len(val)} ({int(val.label.sum())} laundering)  features={len(feats)}")

    # all laundering payments + a 20% sample of the rest, weighted back up
    rng = np.random.default_rng(SEED)
    sub = train[(train.label == 1) | (rng.random(len(train)) < NEG_RATE)]
    X, y, w = stacked(sub, bank, feats, s_cols, r_cols, weights=True)
    dtrain = lgb.Dataset(X, y, weight=w)
    if val.label.sum() > 0:
        Xv, yv, _ = stacked(val, bank, feats, s_cols, r_cols)
        booster = lgb.train(PARAMS, dtrain, num_boost_round=600, valid_sets=[lgb.Dataset(Xv, yv, reference=dtrain)],
                            callbacks=[lgb.early_stopping(50, verbose=False)])
    else:                                   # no laundering in val: nothing to stop on
        booster = lgb.train(PARAMS, dtrain, num_boost_round=300)
    rounds = booster.best_iteration or booster.current_iteration()
    booster = lgb.Booster(model_str=booster.model_to_string(num_iteration=rounds))

    ref = val if len(val) else train
    pv = payment_scores(booster, ref, bank, feats, s_cols, r_cols).values
    thr = float(np.quantile(pv, 1 - ALERT_RATE))
    flag, yv = pv >= thr, ref.label.values
    meta = {"version": a.run_id, "bank": bank, "threshold": thr, "alert_rate": ALERT_RATE,
            "features": feats, "s_cols": s_cols, "r_cols": r_cols, "rounds": int(rounds),
            "trained_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "n_train": int(len(train)), "n_train_pos": int(train.label.sum()),
            "n_live_labelled": int((train.get("source") == "live").sum()) if "source" in train else 0,
            "valid_precision": float(yv[flag].mean()) if flag.any() else 0.0,
            "valid_recall": float(yv[flag].sum() / yv.sum()) if yv.sum() else 0.0}
    print(f"[train] rounds={rounds} threshold={thr:.6f} val precision={meta['valid_precision']:.3f} "
          f"recall={meta['valid_recall']:.3f}")

    # parity samples: the highest-scoring rows (deep tree paths) plus a random handful
    Xs = pd.concat([X.iloc[np.argsort(-booster.predict(X))[:25]], X.sample(min(25, len(X)), random_state=SEED)])
    parity = {"x": [[None if v != v else float(v) for v in row] for row in Xs.values.tolist()],
              "prob": booster.predict(Xs).tolist(),
              "shap": booster.predict(Xs, pred_contrib=True)[:, :-1].tolist()}

    out_dir = os.environ.get("SM_MODEL_DIR", "/opt/ml/model")
    os.makedirs(out_dir, exist_ok=True)
    booster.save_model(os.path.join(out_dir, "model.txt"))
    for name, obj in {"model.json": booster.dump_model(), "meta.json": meta, "parity.json": parity}.items():
        with open(os.path.join(out_dir, name), "w") as fh:
            json.dump(obj, fh)
    s3 = boto3.client("s3")
    prefix = f"models/artifacts/run={a.run_id}/"
    for name in ("model.txt", "model.json", "meta.json", "parity.json"):
        s3.upload_file(os.path.join(out_dir, name), a.bucket, prefix + name)
    print(f"[train] saved to s3://{a.bucket}/{prefix}")


if __name__ == "__main__":
    main()
