"""Local check of the shared scoring code against LightGBM itself. No AWS needed.

    pip install lightgbm pandas numpy
    python main/producer/producer.py prepare --csv <HI-Small_Trans.csv>     (once)
    python main/tests/test_core.py

It replays one bank's payments through q1_core (the code Flink runs), trains a LightGBM model on
the features, and then checks that q1_core.Model gives the same probabilities and the same SHAP
values as LightGBM's predict() and predict(pred_contrib=True).
"""
import argparse
import csv
import glob
import os
import sys
import time

import lightgbm as lgb
import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path[:0] = [os.path.join(HERE, "..", "common"), os.path.join(HERE, "..", "Q1_files")]
import aml_clean  # noqa: E402
import q1_core as core  # noqa: E402


DATA = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "producer", "data")
TX_COLS = ["timestamp", "from_bank", "from_account", "to_bank", "to_account", "amount_received",
           "receiving_currency", "amount_paid", "payment_currency", "payment_format", "is_laundering"]


def load(data_dir, bank):
    """Our bank's payments as prepared by producer.py: the history file, then the live day files."""
    files = [os.path.join(data_dir, "historical", "history.csv")] + sorted(glob.glob(os.path.join(data_dir, "live", "date=*.csv")))
    if not os.path.exists(files[0]):
        sys.exit(f"No prepared data in {data_dir}. Run: python main/producer/producer.py prepare --csv <HI-Small_Trans.csv>")
    rows, bad = [], 0
    for path in files:
        with open(path, newline="") as fh:
            reader = csv.reader(fh)
            next(reader)                               # both files have the same columns, in this order
            for n, vals in enumerate(reader):
                r = dict(zip(TX_COLS, vals))
                r["txn_id"] = f"{os.path.basename(path)[:15]}-{n}"
                r["txn_ts"] = r["timestamp"].replace("/", "-").replace(" ", "T") + ":00"
                try:
                    p = aml_clean.clean(r)
                except aml_clean.Reject:
                    bad += 1
                    continue
                if not aml_clean.involves_bank(p, bank):
                    continue
                core.add_keys(p)
                p["label"] = int(r["is_laundering"])
                rows.append(p)
    rows.sort(key=lambda p: p["ts"])
    return rows, bad


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", default=DATA)
    ap.add_argument("--bank", default="11")
    a = ap.parse_args()
    rows, bad = load(a.data_dir, str(int(a.bank)))
    print(f"{len(rows)} payments for bank {a.bank}, {bad} rejected by clean()")

    t0 = time.perf_counter()
    lf = core.LocalFeatures()
    X = pd.DataFrame([lf.features(p) for p in rows])[core.FEATURES]
    print(f"features: {(time.perf_counter() - t0) / len(rows) * 1e6:.0f} microseconds per payment")
    assert list(X.columns) == core.FEATURES and len(core.FEATURES) == 53
    y = np.array([p["label"] for p in rows])

    params = {"objective": "binary", "learning_rate": 0.05, "num_leaves": 31, "min_child_samples": 50,
              "subsample": 0.8, "subsample_freq": 1, "colsample_bytree": 0.8, "seed": 0, "verbose": -1}
    booster = lgb.train(params, lgb.Dataset(X.astype(np.float32), y), num_boost_round=400)
    meta = {"version": "test", "threshold": 0.5, "s_cols": core.S_COLS, "r_cols": core.R_COLS}
    model = core.Model(booster.dump_model(), meta)

    # rows with missing values and the highest scores exercise the most tree paths
    probs = booster.predict(X.astype(np.float32))
    pick = list(np.argsort(-probs)[:40]) + list(np.random.default_rng(0).choice(len(X), 60, replace=False))
    sample = X.iloc[pick].astype(np.float32)
    blanked = sample.copy()
    blanked[core.R_COLS] = np.nan
    sample = pd.concat([sample, blanked])
    ref_p = booster.predict(sample)
    ref_s = booster.predict(sample, pred_contrib=True)[:, :-1]

    t0 = time.perf_counter()
    mine_p = [model.predict(r) for r in sample.values.tolist()]
    t_pred = (time.perf_counter() - t0) / len(sample)
    t0 = time.perf_counter()
    mine_s = [model.shap(r) for r in sample.values.tolist()]
    t_shap = (time.perf_counter() - t0) / len(sample)
    dp = float(np.max(np.abs(np.array(mine_p) - ref_p)))
    ds = float(np.max(np.abs(np.array(mine_s) - ref_s)))
    print(f"probability: largest difference from LightGBM {dp:.2e}   ({t_pred * 1e3:.2f} ms per payment)")
    print(f"SHAP:        largest difference from LightGBM {ds:.2e}   ({t_shap * 1e3:.0f} ms per explained payment)")
    assert dp < 1e-9, "probabilities differ from LightGBM"
    assert ds < 1e-6, "SHAP values differ from LightGBM"

    parity = {"x": [[None if v != v else float(v) for v in r] for r in sample.values.tolist()[:20]],
              "prob": ref_p[:20].tolist(), "shap": ref_s[:20].tolist()}
    assert max(model.parity(parity)) < 1e-6
    p = rows[pick[0]]
    prob, x, blank = model.score(X.iloc[pick[0]].to_dict(), p, str(int(a.bank)))
    print("top payment:", round(prob, 4), model.reasons(x, blank, p["from_bank"] != p["to_bank"]))
    print("OK")


if __name__ == "__main__":
    main()
