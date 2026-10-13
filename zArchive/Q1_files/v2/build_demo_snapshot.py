"""Build demo_snapshot.json for the investigator app from real data and the real scoring code.

    pip install lightgbm pandas
    python build_demo_snapshot.py [--prepared-dir <folder of date=*.csv>] [--bank 11]

What it does, so that everything the app shows is something the pipeline produces:
  1. Replays our bank's HI-Small payments in time order through score_lambda.build_features
     (the same feature code that scores live payments), plus the distinct sender / receiver
     counts that PIPELINE_SPEC.md section 3.2 adds.
  2. Trains LightGBM with train.py's settings on the days before VALID_DAY and sets the alert
     threshold so the top 1% of VALID_DAY's payments are alerts.
  3. Scores every payment from LIVE_START with score_lambda's scorer. Payments at or above the
     threshold become alerts; a random AUDIT_RATE of the rest become audit items.
  4. Saves the alert items (the fields Flink writes to DynamoDB) and each of our customers'
     payments (what the app would query from curated/live/).

is_laundering is used only as the training label for the historical days. It is not written
to the snapshot.
"""
import argparse
import glob
import hashlib
import json
import os
import sys
from datetime import datetime, timedelta, timezone

import lightgbm as lgb
import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))


def find(name):
    """Locate a file or folder by name anywhere in the repo, so folder renames don't break this."""
    top = HERE
    while not os.path.exists(os.path.join(top, "PIPELINE_SPEC.md")) and os.path.dirname(top) != top:
        top = os.path.dirname(top)
    for folder, dirs, files in os.walk(top):
        dirs[:] = [d for d in dirs if d not in (".venv", "__pycache__", ".git", "site-packages")]
        if name in files or name in dirs:
            return os.path.join(folder, name)
    sys.exit(f"Could not find {name} under {top}")


sys.path.insert(0, os.path.dirname(find("score_lambda.py")))
import score_lambda as sl  # noqa: E402

TX_COLS = ["timestamp", "from_bank", "from_account", "to_bank", "to_account", "amount_received",
           "receiving_currency", "amount_paid", "payment_currency", "payment_format", "is_laundering"]
VALID_DAY, LIVE_START = "2022-09-07", "2022-09-08"
ALERT_RATE, NEG_RATE, POS_WEIGHT, SEED = 0.01, 0.2, 5, 0        # same as train.py
# Priority bands: share of validation-day payments scoring at least this high. Alerts are the
# top 1%; within them, High is the top 0.2% and Medium the top 0.5%. The cut-offs are fixed at
# training time and saved with the model, so the scorer only compares a score against them.
HIGH_RATE, MEDIUM_RATE = 0.002, 0.005
AUDIT_RATE = 0.003
MAX_ACTIVITY_ROWS = 1500
def describe(name, value, p):
    """A risk factor is the feature's name and value; the app turns it into a sentence."""
    return {"feature": name, "value": None if value != value else float(value)}


sl.describe = describe


def score(model, feats, p, bank):
    """score_lambda.score, with risk factors limited to behaviour an investigator can check.

    For a cross-bank payment the other bank's features are blanked. The model still learns from the
    blanks (and from the cross_bank flag): together they mean "the other party is at another bank".
    That is context about the payment, shown on the payment itself, and not a reason for the alert,
    so those features are never picked as risk factors. The score is unaffected.
    """
    names, cross = model.names, p["from_bank"] != p["to_bank"]
    views = []
    if p["from_bank"] == bank:
        views.append(set(model.meta["r_cols"]) if cross else set())
    if p["to_bank"] == bank:
        views.append(set(model.meta["s_cols"]) if cross else set())
    best = None
    for blank in views:
        x = [float("nan") if n in blank else feats[n] for n in names]
        prob, contrib = model.predict(x)
        if best is None or prob > best[0]:
            best = (prob, contrib, x, blank)
    prob, contrib, x, blank = best
    grouped = (blank | {"cross_bank"}) if cross else set()
    ranked = [(c, describe(names[i], x[i], p)) for i, c in contrib.items() if names[i] not in grouped]
    ranked.sort(key=lambda r: r[0], reverse=True)
    return prob, [r for c, r in ranked if c > 0][:3]


class Store(sl.MemoryStore):
    """MemoryStore that drops events older than 7 days as it goes (payments arrive in time order)."""

    def events(self, acct, t):
        evs, lo, i = self.ev.get(acct, []), t - timedelta(days=7), 0
        while i < len(evs) and evs[i]["ts"] <= lo:
            i += 1
        if i:
            del evs[:i]
        return [e for e in evs if e["ts"] < t]


class Counterparties:
    """Per account: who it received from and who it paid in the last 7 days (spec 3.2 step 3)."""

    def __init__(self):
        self.seen = {}

    def counts(self, acct, t):
        rows, lo, i = self.seen.get(acct, []), t - timedelta(days=7), 0
        while i < len(rows) and rows[i][0] <= lo:
            i += 1
        if i:
            del rows[:i]
        live = [r for r in rows if r[0] < t]
        return (float(len({cp for _, d, cp in live if d == "in"})),
                float(len({cp for _, d, cp in live if d == "out"})))

    def add(self, p):
        self.seen.setdefault(p["src"], []).append((p["ts"], "out", p["dst"]))
        self.seen.setdefault(p["dst"], []).append((p["ts"], "in", p["src"]))


def load(prepared_dir, bank):
    frames = [pd.read_csv(f, dtype=str) for f in sorted(glob.glob(os.path.join(prepared_dir, "date=*.csv")))]
    if not frames:
        sys.exit(f"No date=*.csv files in {prepared_dir}. Run producer.py prepare first.")
    df = pd.concat(frames, ignore_index=True)[TX_COLS]
    fb, tb = df.from_bank.astype(int).astype(str), df.to_bank.astype(int).astype(str)
    df = df[(fb == bank) | (tb == bank)].sort_values("timestamp", kind="stable").reset_index(drop=True)
    payments = []
    for r in df.to_dict("records"):
        ts = datetime.strptime(r["timestamp"], "%Y/%m/%d %H:%M")
        fb, tb = str(int(r["from_bank"])), str(int(r["to_bank"]))
        payments.append({
            "txn_id": hashlib.sha1("|".join(str(r[c]) for c in TX_COLS).encode()).hexdigest(),  # as producer.py
            "ts": ts, "from_bank": fb, "to_bank": tb, "from_account": r["from_account"],
            "to_account": r["to_account"], "src": f"{fb}_{r['from_account']}", "dst": f"{tb}_{r['to_account']}",
            "amt_paid": float(r["amount_paid"]), "amt_recv": float(r["amount_received"]),
            "pay_cur": r["payment_currency"], "recv_cur": r["receiving_currency"],
            "payment_format": r["payment_format"], "_label": int(r["is_laundering"])})
    return payments


def features(payments):
    store, cps, rows = Store(), Counterparties(), []
    for n, p in enumerate(payments):
        ev_src = store.events(p["src"], p["ts"])
        ev_dst = ev_src if p["dst"] == p["src"] else store.events(p["dst"], p["ts"])
        state = store.get_state([f"LASTIN#{p['src']}", f"PAIR#{p['src']}|{p['dst']}",
                                 f"PAIR#{p['dst']}|{p['src']}", f"LASTIN#{p['dst']}"])
        f, fresh = sl.build_features(p, ev_src, ev_dst, state)
        f["s_in_distinct_7D"], f["s_out_distinct_7D"] = cps.counts(p["src"], p["ts"])
        f["r_in_distinct_7D"], f["r_out_distinct_7D"] = cps.counts(p["dst"], p["ts"])
        rows.append(f)
        store.write(*sl.state_updates(p, state, fresh))
        cps.add(p)
        if n % 10000 == 0:
            print(f"  features {n}/{len(payments)}")
    return pd.DataFrame(rows)


def bank_views(df, meta, bank, feats, s_cols, r_cols):
    """train.py: sender's-bank view and receiver's-bank view, the other bank's side blanked."""
    send, recv = df[meta.from_bank == bank], df[meta.to_bank == bank]
    x0 = send[feats].astype(np.float32)
    x0.loc[(meta.from_bank != meta.to_bank)[send.index].values, r_cols] = np.nan
    x1 = recv[feats].astype(np.float32)
    x1.loc[(meta.from_bank != meta.to_bank)[recv.index].values, s_cols] = np.nan
    return [(x0, send.index), (x1, recv.index)]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--prepared-dir", default=None, help="folder of date=*.csv files (default: find prep_all)")
    ap.add_argument("--bank", default="11")
    ap.add_argument("--out", default=os.path.join(HERE, "demo_snapshot.json"))
    a = ap.parse_args()
    bank = str(int(a.bank))

    payments = load(a.prepared_dir or find("prep_all"), bank)
    print(f"bank {bank}: {len(payments)} payments")
    X = features(payments)
    feats = list(X.columns)
    s_cols = [f for f in feats if f.startswith("s_")] + ["amt_match"]
    r_cols = [f for f in feats if f.startswith("r_")]
    meta = pd.DataFrame({"day": [p["ts"].strftime("%Y-%m-%d") for p in payments],
                         "from_bank": [p["from_bank"] for p in payments],
                         "to_bank": [p["to_bank"] for p in payments],
                         "label": [p["_label"] for p in payments]})
    is_train, is_valid, is_live = meta.day < VALID_DAY, meta.day == VALID_DAY, meta.day >= LIVE_START

    # ---- train (train.py settings)
    rng = np.random.default_rng(SEED)
    keep = is_train & ((meta.label == 1) | (rng.random(len(meta)) < NEG_RATE))
    xs, ys, ws = [], [], []
    for x, idx in bank_views(X[keep], meta[keep], bank, feats, s_cols, r_cols):
        y = meta.label[idx].values
        xs.append(x), ys.append(y), ws.append(np.where(y == 1, POS_WEIGHT, 1.0 / NEG_RATE))
    booster = lgb.LGBMClassifier(
        n_estimators=400, learning_rate=0.05, num_leaves=31, min_child_samples=50, subsample=0.8,
        subsample_freq=1, colsample_bytree=0.8, random_state=SEED, verbose=-1).fit(
        pd.concat(xs), np.concatenate(ys), sample_weight=np.concatenate(ws)).booster_
    parts = [pd.Series(booster.predict(x), index=idx)
             for x, idx in bank_views(X[is_valid], meta[is_valid], bank, feats, s_cols, r_cols) if len(x)]
    pv = pd.concat(parts).groupby(level=0).max()
    thr = float(np.quantile(pv.values, 1 - ALERT_RATE))
    bands = {"high": float(np.quantile(pv.values, 1 - HIGH_RATE)),
             "medium": float(np.quantile(pv.values, 1 - MEDIUM_RATE))}
    yv = meta.label[pv.index].values
    flag = pv.values >= thr
    model_meta = {"version": f"hi-small-{VALID_DAY}", "bank": bank, "threshold": thr, "alert_rate": ALERT_RATE,
                  "bands": bands, "band_rates": {"high": HIGH_RATE, "medium": MEDIUM_RATE},
                  "features": feats, "s_cols": s_cols, "r_cols": r_cols,
                  "n_train": int(is_train.sum()), "n_train_pos": int(meta.label[is_train].sum()),
                  "valid_precision": float(yv[flag].mean()) if flag.any() else 0.0,
                  "valid_recall": float(yv[flag].sum() / yv.sum()) if yv.sum() else 0.0}
    print(f"trained on {model_meta['n_train']} payments ({model_meta['n_train_pos']} laundering); "
          f"threshold {thr:.4f}; validation precision {model_meta['valid_precision']:.2f}, "
          f"recall {model_meta['valid_recall']:.2f}")

    # ---- score the live days with the Lambda's own scorer
    model = sl.Model(booster.dump_model(), model_meta)
    summary = ["s_in_n_7D", "s_in_amt_7D", "s_out_n_7D", "s_out_amt_7D", "s_in_distinct_7D", "s_out_distinct_7D",
               "r_in_n_7D", "r_in_amt_7D", "r_out_n_7D", "r_out_amt_7D", "r_in_distinct_7D", "r_out_distinct_7D"]
    items, n_alert_pos, n_live_pos, audit_pos, traffic = [], 0, 0, 0, {}
    for i in np.flatnonzero(is_live.values):
        p, f = payments[i], X.iloc[i].to_dict()
        prob, reasons = score(model, f, p, bank)
        alert = prob >= thr
        hour = p["ts"].replace(minute=0, tzinfo=timezone.utc).timestamp()
        traffic.setdefault(hour, [0, 0])
        traffic[hour][0] += 1
        traffic[hour][1] += int(alert)
        n_live_pos += p["_label"]
        n_alert_pos += p["_label"] if alert else 0
        if not alert and rng.random() >= AUDIT_RATE:
            continue
        audit_pos += 0 if alert else p["_label"]
        items.append({
            "txn_id": p["txn_id"], "is_audit": not alert,
            "txn_ts": p["ts"].replace(tzinfo=timezone.utc).timestamp(),
            "from_bank": p["from_bank"], "from_account": p["from_account"],
            "to_bank": p["to_bank"], "to_account": p["to_account"],
            "amount": p["amt_paid"], "currency": p["pay_cur"],
            "amount_received": p["amt_recv"], "receiving_currency": p["recv_cur"],
            "payment_format": p["payment_format"], "score": round(prob, 6), "threshold": round(thr, 6),
            "model_version": model.version, "reasons": reasons,
            "priority": None if not alert else ("High" if prob >= bands["high"] else
                                                "Medium" if prob >= bands["medium"] else "Low"),
            "features": {k: (None if f[k] != f[k] else f[k]) for k in summary}})
    alerts = [it for it in items if not it["is_audit"]]
    print(f"live from {LIVE_START}: {int(is_live.sum())} payments, {len(alerts)} alerts "
          f"({n_alert_pos} of the {n_live_pos} laundering payments), {len(items) - len(alerts)} audit items "
          f"({audit_pos} laundering)")

    # ---- our customers' payments, as the app would read them from curated/live/
    want = {f"{it[s + '_bank']}_{it[s + '_account']}" for it in items for s in ("from", "to")
            if it[s + "_bank"] == bank}
    activity = {k: [] for k in want}
    for p in payments:
        ts = p["ts"].replace(tzinfo=timezone.utc).timestamp()
        if p["src"] in activity:
            activity[p["src"]].append([ts, "out", p["dst"], p["amt_paid"], p["pay_cur"], p["payment_format"]])
        if p["dst"] in activity:
            activity[p["dst"]].append([ts, "in", p["src"], p["amt_recv"], p["recv_cur"], p["payment_format"]])
    for k, last in {k: max(it["txn_ts"] for it in items if k in (f"{it['from_bank']}_{it['from_account']}",
                                                                 f"{it['to_bank']}_{it['to_account']}"))
                    for k in want}.items():
        activity[k] = [r for r in activity[k] if r[0] <= last][-MAX_ACTIVITY_ROWS:]

    keep_meta = {k: model_meta[k] for k in ("version", "bank", "threshold", "alert_rate", "bands", "band_rates", "n_train",
                                           "n_train_pos", "valid_precision", "valid_recall")}
    with open(a.out, "w", encoding="utf-8") as fh:
        json.dump({"model": keep_meta, "live_start": LIVE_START, "audit_rate": AUDIT_RATE,
                   "items": items, "activity": activity,
                   "traffic": [[h, n, a] for h, (n, a) in sorted(traffic.items())]}, fh)
    print(f"wrote {a.out} ({os.path.getsize(a.out) / 1e6:.1f} MB)")


if __name__ == "__main__":
    main()
