"""Build demo_snapshot.json for the Q2 KYC app from the real HI-Small data.

    pip install pandas
    python build_demo_snapshot.py [--bank 11]

This is a small, local stand-in for the nightly batch in PIPELINE_SPEC.md section 4, so that everything
the app shows is something that pipeline produces:
  1. Glue job (4.3): map each account to its customer, convert amounts to USD with the day's FX rate,
     and fail if an account has no customer or a currency has no rate.
  2. EMR (4.4): for each run date, take the 7 days ending that day, combine all of a customer's
     accounts, and apply rules R1 to R11. Each rule that fires carries an evidence sentence.
  3. Saves, per run date, each customer's points, peer composite, rules fired and figures, plus the
     customer's accounts and transactions for the drill-down.

Left to the app's demo backend, because they depend on analyst decisions made in the app:
  R12 (linked to customers needing EDD), tiers, in_queue and queue_reason (4.4 and 4.5).

is_laundering is kept only as one yes/no per customer per run, for the rule precision figures (4.4).
It is never a scoring input and never shown against a customer.
"""
import argparse
import glob
import hashlib
import json
import os
import sys
from datetime import timedelta

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
TX_COLS = ["timestamp", "from_bank", "from_account", "to_bank", "to_account", "amount_received",
           "receiving_currency", "amount_paid", "payment_currency", "payment_format", "is_laundering"]
RUN_DATES = ["2022-09-07", "2022-09-08", "2022-09-09", "2022-09-10"]
WINDOW_DAYS, RECENT_DAYS = 7, 3
# R6: MAS Notice 626 threshold of S$20,000, about US$14,300 at the September 2022 rate. The band is
# 80% to 99.9% of it. Both are parameters (spec 6, open decisions).
STRUCT_THRESHOLD_USD = 14300.0
STRUCT_BAND = (0.80 * STRUCT_THRESHOLD_USD, 0.999 * STRUCT_THRESHOLD_USD)
SMALL_TYPES = {"Country", "Individual", "Direct"}       # too few for peer percentiles
RULES = {   # rule_id: (name, points, what it looks for, MAS grounding)
    "R1": ("Rapid pass-through", 25, "Money received is sent on again within two days",
           "Assets withdrawn immediately after deposit without business reason"),
    "R2": ("Round trip", 20, "Money paid to a counterparty comes back from the same counterparty",
           "U-turn transactions"),
    "R3": ("Collection from many senders", 15, "Unusually many different senders pay the customer",
           "Payments to a single account from many different people without explanation"),
    "R4": ("Gather and scatter", 15, "Money collected from many senders is passed on to many recipients",
           "Scam money-mule pattern"),
    "R5": ("Dispersal to many recipients", 10, "The customer pays unusually many different recipients",
           "Unusual destination of funds (layering)"),
    "R6": ("Structuring below the threshold", 20, "Several payments sit just under the reporting threshold",
           "Splitting to avoid reporting or CDD thresholds"),
    "R7": ("Activity out of line with profile", 15, "Value moved is far above the customer's own recent level, or its peers'",
           "Activity inconsistent with the customer's profile"),
    "R8": ("Cash or crypto concentration", 10, "Most of the value moved is cash or Bitcoin",
           "Cash-heavy activity; digital-asset risk"),
    "R9": ("New-counterparty burst", 10, "Most recent counterparties are new to the customer",
           "Proxy for mule onboarding or newly active accounts"),
    "R10": ("Own-account shuffling", 10, "Repeated large transfers between the customer's own accounts",
            "Movement across multiple accounts without clear purpose"),
    "R11": ("Currency churn", 10, "A large share of value is converted between currencies",
            "Layering through conversion"),
    "R12": ("Linked to customers needing EDD", 15, "Transacted with a customer an analyst marked as needing EDD",
            "Dealings with parties already assessed as higher risk"),
}


def find(name):
    """Locate a file or folder by name anywhere in the repo, so folder renames don't break this."""
    top = os.path.abspath(os.path.join(HERE, "..", "..", ".."))
    for folder, dirs, files in os.walk(top):
        dirs[:] = [d for d in dirs if d not in (".venv", "__pycache__", ".git", "site-packages")]
        if name in files or name in dirs:
            return os.path.join(folder, name)
    sys.exit(f"Could not find {name} under {top}")


def pct_rank(s):
    """Spark percent_rank: (rank - 1) / (n - 1), ties share the lowest rank."""
    n = len(s)
    return (s.rank(method="min") - 1) / (n - 1) if n > 1 else pd.Series(0.0, index=s.index)


# ------------------------------------------------------------------ Glue job ----
def load_legs(bank):
    """curated/customer_txns for our bank, as one row per (customer, transaction side)."""
    frames = [pd.read_csv(f, dtype=str) for f in sorted(glob.glob(os.path.join(find("prep_all"), "date=*.csv")))]
    tx = pd.concat(frames, ignore_index=True)[TX_COLS]
    tx["from_bank"] = tx.from_bank.astype(int).astype(str)
    tx["to_bank"] = tx.to_bank.astype(int).astype(str)
    tx = tx[(tx.from_bank == bank) | (tx.to_bank == bank)]
    tx = tx[~((tx.from_bank == tx.to_bank) & (tx.from_account == tx.to_account))].copy()   # same-account moves
    tx["txn_id"] = [hashlib.sha1("|".join(r).encode()).hexdigest()[:16] for r in tx[TX_COLS].itertuples(index=False)]
    tx = tx.drop_duplicates("txn_id")
    tx["ts"] = pd.to_datetime(tx.timestamp, format="%Y/%m/%d %H:%M")
    tx["date"] = tx.ts.dt.strftime("%Y-%m-%d")
    for c in ("amount_paid", "amount_received"):
        tx[c] = tx[c].astype(float)

    acc = pd.read_csv(os.path.join(os.path.dirname(find("HI-Small_accounts.csv")), "HI-Small_accounts.csv"), dtype=str)
    acc["bank"] = acc["Bank ID"].astype(int).astype(str)
    etype = acc["Entity Name"].str.replace(r"\s*#\d+$", "", regex=True)
    acc["entity_type"] = np.where(etype.isin(SMALL_TYPES), "Other", etype)
    acc = acc.drop_duplicates(["bank", "Account Number"])
    owner = acc.set_index(["bank", "Account Number"])["Entity ID"]
    for side in ("from", "to"):
        tx[f"{side}_entity"] = pd.MultiIndex.from_frame(tx[[f"{side}_bank", f"{side}_account"]]).map(owner)

    fx = pd.read_csv(os.path.join(os.path.dirname(find("HI-Small_accounts.csv")), "fx_rates.csv"))
    rate = fx.set_index(["date", "currency"]).usd_per_unit
    for cur, amt in (("payment_currency", "amount_paid"), ("receiving_currency", "amount_received")):
        r = pd.MultiIndex.from_frame(tx[["date", cur]]).map(rate)
        r = np.where(tx[cur] == "US Dollar", 1.0, r)
        tx[amt + "_usd"] = (tx[amt] * r).round(2)

    # validation (spec 4.3): fail the run if any check fails
    problems = []
    if tx[["from_entity", "to_entity"]].isna().any().any():
        problems.append(f"{int(tx[['from_entity', 'to_entity']].isna().any(axis=1).sum())} payments have an account with no customer")
    if tx[["amount_paid_usd", "amount_received_usd"]].isna().any().any():
        missing = sorted(set(tx.loc[tx.amount_paid_usd.isna(), "payment_currency"]) |
                         set(tx.loc[tx.amount_received_usd.isna(), "receiving_currency"]))
        problems.append(f"no FX rate for {missing}")
    if not len(tx):
        problems.append("no payments")
    if problems:
        sys.exit("Validation failed: " + "; ".join(problems))

    common = dict(txn_id=tx.txn_id, ts=tx.ts, date=tx.date, fmt=tx.payment_format,
                  laundering=tx.is_laundering.astype(int), internal=tx.from_entity == tx.to_entity,
                  converted=tx.payment_currency != tx.receiving_currency)
    out = pd.DataFrame({"entity_id": tx.from_entity, "account": tx.from_account, "direction": "out",
                        "cp_entity": tx.to_entity, "cp_bank": tx.to_bank, "cp_account": tx.to_account,
                        "amount": tx.amount_paid_usd, "orig_amount": tx.amount_paid,
                        "currency": tx.payment_currency, **common})[tx.from_bank == bank]
    inn = pd.DataFrame({"entity_id": tx.to_entity, "account": tx.to_account, "direction": "in",
                        "cp_entity": tx.from_entity, "cp_bank": tx.from_bank, "cp_account": tx.from_account,
                        "amount": tx.amount_received_usd, "orig_amount": tx.amount_received,
                        "currency": tx.receiving_currency, **common})[tx.to_bank == bank]
    legs = pd.concat([out, inn], ignore_index=True)
    ours = acc[acc.bank == bank]
    customers = ours.groupby("Entity ID").agg(entity_type=("entity_type", "first"),
                                              accounts=("Account Number", lambda s: sorted(set(s))))
    customers.index.name = "entity_id"
    print(f"bank {bank}: {len(tx)} payments, {len(legs)} customer-side rows, {len(customers)} customers on file")
    return legs, customers


# ----------------------------------------------------------------------- EMR ----
def score_run(legs, customers, run, first_seen):
    run_d = pd.Timestamp(run)
    start = (run_d - timedelta(days=WINDOW_DAYS - 1)).strftime("%Y-%m-%d")
    recent_start = (run_d - timedelta(days=RECENT_DAYS - 1)).strftime("%Y-%m-%d")
    w = legs[(legs.date >= start) & (legs.date <= run)]
    ext = w[~w.internal]
    days = set(w.date)
    recent_days = max(1, sum(d >= recent_start for d in days))
    prior_days = sum(d < recent_start for d in days)
    complete = prior_days >= WINDOW_DAYS - RECENT_DAYS

    is_in, recent = ext.direction == "in", ext.date >= recent_start
    cashy = ext.fmt.isin(["Cash", "Bitcoin"]) | (ext.currency == "Bitcoin")
    near = ext.amount.between(*STRUCT_BAND)
    e = ext.assign(inflow=ext.amount.where(is_in, 0), outflow=ext.amount.where(~is_in, 0),
                   cash=ext.amount.where(cashy, 0), recent_v=ext.amount.where(recent, 0),
                   prior_v=ext.amount.where(~recent, 0), conv=ext.amount.where(ext.converted, 0),
                   near_n=near.astype(int), near_v=ext.amount.where(near, 0))
    f = e.groupby("entity_id").agg(
        inflow=("inflow", "sum"), outflow=("outflow", "sum"), total=("amount", "sum"), cash=("cash", "sum"),
        recent_v=("recent_v", "sum"), prior_v=("prior_v", "sum"), conv=("conv", "sum"),
        near_n=("near_n", "sum"), near_v=("near_v", "sum"), n_txn=("txn_id", "nunique"),
        laundering=("laundering", "max"))
    f["senders"] = ext[is_in].groupby("entity_id").cp_entity.nunique()
    f["recipients"] = ext[~is_in].groupby("entity_id").cp_entity.nunique()

    # R1: median hours from a receipt to the next payment out
    ins = ext[is_in][["entity_id", "ts"]].sort_values("ts")
    outs = ext[~is_in][["entity_id", "ts"]].rename(columns={"ts": "out_ts"}).sort_values("out_ts")
    nxt = pd.merge_asof(ins, outs, left_on="ts", right_on="out_ts", by="entity_id", direction="forward")
    nxt = nxt.dropna(subset=["out_ts"])
    f["median_hours"] = ((nxt.out_ts - nxt.ts).dt.total_seconds() / 3600).groupby(nxt.entity_id).median()

    # R2: paid a counterparty >= $1k and >= 80% came back from the same counterparty within 7 days
    o = ext[(~is_in) & (ext.amount >= 1000)][["entity_id", "cp_entity", "txn_id", "amount", "ts"]]
    i = ext[is_in][["entity_id", "cp_entity", "amount", "ts"]]
    rt = o.merge(i, on=["entity_id", "cp_entity"], suffixes=("_o", "_i"))
    rt = rt[(rt.ts_i > rt.ts_o) & (rt.ts_i - rt.ts_o <= pd.Timedelta(days=7)) & (rt.amount_i >= 0.8 * rt.amount_o)]
    f["round_trips"] = rt.groupby("entity_id").txn_id.nunique()

    # R9: counterparties first seen in the recent period (state/cp_first_seen/)
    for (ent, cp), d in ext.groupby(["entity_id", "cp_entity"]).date.min().items():
        if (ent, cp) not in first_seen or d < first_seen[(ent, cp)]:
            first_seen[(ent, cp)] = d
    rc = ext[recent][["entity_id", "cp_entity"]].drop_duplicates()
    rc["new"] = [first_seen[k] >= recent_start for k in zip(rc.entity_id, rc.cp_entity)]
    f["recent_cps"] = rc.groupby("entity_id").size()
    f["new_cps"] = rc.groupby("entity_id").new.sum()

    # R10: transfers of $5k+ between the customer's own accounts
    own = w[w.internal & (w.direction == "out") & (w.amount >= 5000)]
    f["own_transfers"] = own.groupby("entity_id").size()

    f = f.join(customers[["entity_type"]], how="inner")
    f[["senders", "recipients", "round_trips", "recent_cps", "new_cps", "own_transfers"]] = \
        f[["senders", "recipients", "round_trips", "recent_cps", "new_cps", "own_transfers"]].fillna(0).astype(int)
    f["pass_through"] = (f.outflow / f.inflow).where(f.inflow > 0)
    f["new_share"] = (f.new_cps / f.recent_cps).where(f.recent_cps > 0, 0)
    f["spike"] = ((f.recent_v / recent_days) / (f.prior_v / max(prior_days, 1))).where((f.prior_v > 0) & (prior_days > 0))
    f["cash_share"] = (f.cash / f.total).where(f.total > 0, 0)
    f["conv_share"] = (f.conv / f.total).where(f.total > 0, 0)

    by_type = f.groupby("entity_type")
    for c in ("senders", "recipients", "inflow"):
        f[c + "_pct"] = by_type[c].transform(pct_rank)
    comp = ["pass_through", "senders", "recipients", "new_share", "spike", "cash_share", "total"]
    f["peer"] = sum(f[c].fillna(0).groupby(f.entity_type).transform(pct_rank) for c in comp) / len(comp)

    fan_in = (f.senders >= 10) | ((f.senders_pct >= 0.99) & (f.senders >= 5))
    spike = complete & (f.spike >= 5) & (f.recent_v >= 10000)
    peer_high = f.inflow_pct >= 0.99
    usd = lambda v: f"${v:,.0f}"
    checks = {
        "R1": ((f.inflow >= 10000) & f.pass_through.between(0.9, 1.1) & (f.median_hours <= 48),
               lambda r: f"{r.pass_through:.0%} of the {usd(r.inflow)} received was sent on, "
                         f"typically {r.median_hours:.1f} hours after it arrived"),
        "R2": (f.round_trips >= 1,
               lambda r: f"{r.round_trips} payment{'s' if r.round_trips != 1 else ''} of $1,000 or more came back "
                         "from the same counterparty within 7 days"),
        "R3": (fan_in, lambda r: f"Received money from {r.senders} different senders in 7 days"),
        "R4": (fan_in & (f.recipients >= 10) & (f.pass_through >= 0.8),
               lambda r: f"Collected from {r.senders} senders and paid {r.recipients} recipients, "
                         f"passing on {min(r.pass_through, 9.99):.0%} of what came in"),
        "R5": ((f.recipients >= 10) | ((f.recipients_pct >= 0.99) & (f.recipients >= 5)),
               lambda r: f"Paid {r.recipients} different recipients in 7 days"),
        "R6": ((f.near_n >= 3) & (f.near_v >= STRUCT_THRESHOLD_USD),
               lambda r: f"{r.near_n} payments of {usd(STRUCT_BAND[0])} to {usd(STRUCT_BAND[1])}, just under the "
                         f"{usd(STRUCT_THRESHOLD_USD)} threshold, totalling {usd(r.near_v)}"),
        "R7": (spike | peer_high,
               lambda r: (f"Daily value is {r.spike:.1f} times the customer's level earlier in the week "
                          f"({usd(r.recent_v)} in the last {RECENT_DAYS} days)"
                          if complete and r.spike >= 5 and r.recent_v >= 10000 else
                          f"Received {usd(r.inflow)} in 7 days, in the top 1% for a {r.entity_type.lower()}")),
        "R8": ((f.cash_share >= 0.5) & (f.total >= 10000),
               lambda r: f"{r.cash_share:.0%} of the {usd(r.total)} moved was cash or Bitcoin"),
        "R9": (complete & (f.new_cps >= 5) & (f.new_share >= 0.7),
               lambda r: f"{r.new_cps} of {r.recent_cps} counterparties in the last {RECENT_DAYS} days are new to the customer"),
        "R10": (f.own_transfers >= 3,
                lambda r: f"{r.own_transfers} transfers of $5,000 or more between the customer's own accounts"),
        "R11": ((f.conv_share >= 0.3) & (f.total >= 10000),
                lambda r: f"{r.conv_share:.0%} of the {usd(r.total)} moved was converted between currencies"),
    }
    hits = {ent: [] for ent in f.index}
    for rid, (cond, evidence) in checks.items():
        for r in f[cond.fillna(False)].itertuples():
            hits[r.Index].append([rid, evidence(r)])
    cps = {ent: sorted(set(g) & set(customers.index) - {ent}) for ent, g in ext.groupby("entity_id").cp_entity}
    rows = []
    for r in f.itertuples():
        h = sorted(hits[r.Index], key=lambda x: -RULES[x[0]][1])
        rows.append({
            "id": r.Index, "type": r.entity_type, "points": sum(RULES[x[0]][1] for x in h),
            "peer": round(float(r.peer), 4), "hits": h, "cps": cps.get(r.Index, []), "l": int(r.laundering),
            "m": {"inflow": round(r.inflow, 2), "outflow": round(r.outflow, 2), "payments": int(r.n_txn),
                  "senders": int(r.senders), "recipients": int(r.recipients),
                  "pass_through": None if pd.isna(r.pass_through) else round(float(r.pass_through), 3),
                  "median_hours": None if pd.isna(r.median_hours) else round(float(r.median_hours), 1)}})
    fired = sum(1 for x in rows if x["points"])
    print(f"  {run}: window {start} to {run}, {len(rows)} active customers, {fired} with a rule fired, "
          f"window complete: {complete}")
    return {"window_start": start, "window_complete": bool(complete), "customers": rows}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bank", default="11")
    ap.add_argument("--out", default=os.path.join(HERE, "demo_snapshot.json"))
    a = ap.parse_args()
    bank = str(int(a.bank))
    legs, customers = load_legs(bank)
    first_seen, runs = {}, {}
    # state/cp_first_seen must be built in date order, starting before the first run shown
    for d in sorted(set(legs.date)):
        if d < RUN_DATES[0]:
            ext = legs[(legs.date == d) & ~legs.internal]
            for k in zip(ext.entity_id, ext.cp_entity):
                first_seen.setdefault(k, d)
    for run in RUN_DATES:
        runs[run] = score_run(legs, customers, run, first_seen)

    seen = {c["id"] for r in runs.values() for c in r["customers"]}
    lg = legs[legs.entity_id.isin(seen) & (legs.date <= RUN_DATES[-1])].sort_values("ts")
    txns = {}
    for r in lg.itertuples():
        txns.setdefault(r.entity_id, []).append([
            int(r.ts.timestamp()), r.account, r.direction, r.cp_entity, r.cp_bank, r.cp_account,
            r.amount, r.orig_amount, r.currency, r.fmt, int(r.internal)])
    with open(a.out, "w", encoding="utf-8") as fh:
        json.dump({"bank": bank, "runs": runs, "rules": RULES,
                   "accounts": {e: customers.accounts[e] for e in seen},
                   "structuring": {"threshold_usd": STRUCT_THRESHOLD_USD, "band": STRUCT_BAND},
                   "txns": txns}, fh)
    print(f"wrote {a.out} ({os.path.getsize(a.out) / 1e6:.1f} MB)")


if __name__ == "__main__":
    main()
