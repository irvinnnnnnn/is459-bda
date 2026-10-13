#!/usr/bin/env python3
"""
IS459 G2 - Q2 producer.
Replays the IBM AML transactions CSV into a Kinesis Data Stream, one simulated
day at a time, keeping only transactions that involve "our" bank.

Modes:
  list-banks  rank banks to help choose "our bank"
  prepare     filter to one bank and split into per-day files
  replay      send day files to Kinesis, optionally trigger Step Functions per day
"""
import argparse
import csv
import glob
import hashlib
import json
import os
import random
import signal
import sys
import time
from collections import defaultdict
from datetime import datetime

import pandas as pd

TX_COLS = ["timestamp", "from_bank", "from_account", "to_bank", "to_account",
           "amount_received", "receiving_currency", "amount_paid",
           "payment_currency", "payment_format", "is_laundering"]
CHUNK = 500_000
CHECKPOINT = ".producer_checkpoint.json"


def log(msg):
    print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)


def norm_bank(b):
    """Bank IDs are zero-padded in the transactions file but not in accounts.csv."""
    return str(int(b))


def read_chunks(path):
    # header=0 + names= handles the duplicated "Account" column; dtype=str keeps leading zeros
    return pd.read_csv(path, header=0, names=TX_COLS, dtype=str, chunksize=CHUNK)


# ---------------------------------------------------------------- list-banks
def cmd_list_banks(a):
    txns = defaultdict(int)
    accts = defaultdict(set)
    l_txns = defaultdict(int)
    l_accts = defaultdict(set)
    for ch in read_chunks(a.csv):
        for bcol, acol in (("from_bank", "from_account"), ("to_bank", "to_account")):
            banks = ch[bcol].map(norm_bank)
            for b, acc, lab in zip(banks, ch[acol], ch["is_laundering"]):
                txns[b] += 1
                accts[b].add(acc)
                if lab == "1":
                    l_txns[b] += 1
                    l_accts[b].add(acc)
    rows = [(b, txns[b], len(accts[b]), l_txns[b], len(l_accts[b])) for b in txns]
    rows.sort(key=lambda r: (r[4], r[1]), reverse=True)
    print(f"{'bank':>8} {'txn_sides':>10} {'accounts':>9} {'laund_txn':>10} {'laund_accts':>12}")
    for r in rows[:a.top]:
        print(f"{r[0]:>8} {r[1]:>10} {r[2]:>9} {r[3]:>10} {r[4]:>12}")


# ---------------------------------------------------------------- prepare
def cmd_prepare(a):
    bank = norm_bank(a.bank_id)
    os.makedirs(a.out_dir, exist_ok=True)
    for f in glob.glob(os.path.join(a.out_dir, "date=*.csv")):
        os.remove(f)
    counts = defaultdict(int)
    for ch in read_chunks(a.csv):
        mask = (ch["from_bank"].map(norm_bank) == bank) | (ch["to_bank"].map(norm_bank) == bank)
        sub = ch[mask].copy()
        if sub.empty:
            continue
        sub["_date"] = sub["timestamp"].str[:10].str.replace("/", "-")
        for d, grp in sub.groupby("_date"):
            path = os.path.join(a.out_dir, f"date={d}.csv")
            grp.drop(columns="_date").to_csv(path, mode="a", index=False,
                                             header=not os.path.exists(path))
            counts[d] += len(grp)
    for path in glob.glob(os.path.join(a.out_dir, "date=*.csv")):
        df = pd.read_csv(path, dtype=str)
        df.sort_values("timestamp", kind="stable").to_csv(path, index=False)
    log(f"Bank {bank}: prepared {sum(counts.values())} transactions")
    for d in sorted(counts):
        print(f"  {d}: {counts[d]}")


# ---------------------------------------------------------------- replay
def to_record(r):
    raw = "|".join(str(r[c]) for c in TX_COLS)
    ts = datetime.strptime(r["timestamp"], "%Y/%m/%d %H:%M")
    return {
        "txn_id": hashlib.sha1(raw.encode()).hexdigest(),
        "txn_ts": ts.isoformat(),
        "txn_date": ts.strftime("%Y-%m-%d"),
        "from_bank": r["from_bank"],
        "from_account": r["from_account"],
        "to_bank": r["to_bank"],
        "to_account": r["to_account"],
        "amount_received": float(r["amount_received"]),
        "receiving_currency": r["receiving_currency"],
        "amount_paid": float(r["amount_paid"]),
        "payment_currency": r["payment_currency"],
        "payment_format": r["payment_format"],
        "is_laundering": int(r["is_laundering"]),
    }


class RateLimiter:
    def __init__(self, rate):
        self.rate = rate
        self.start = time.monotonic()
        self.sent = 0

    def wait(self, n):
        self.sent += n
        ahead = self.sent / self.rate - (time.monotonic() - self.start)
        if ahead > 0:
            time.sleep(ahead)


def send_batch(kinesis, stream, records, max_retries=8):
    """put_records with retry of failed records only. Returns number permanently failed."""
    entries = [{"Data": (json.dumps(r) + "\n").encode(), "PartitionKey": r["from_account"]}
               for r in records]
    for attempt in range(max_retries + 1):
        resp = kinesis.put_records(StreamName=stream, Records=entries)
        if resp["FailedRecordCount"] == 0:
            return 0
        entries = [e for e, res in zip(entries, resp["Records"]) if "ErrorCode" in res]
        time.sleep(min(10, 0.2 * 2 ** attempt) + random.random() * 0.1)
    return len(entries)


def batches(records, max_n=500, max_bytes=4_500_000):
    batch, size = [], 0
    for r in records:
        s = len(json.dumps(r)) + 64
        if batch and (len(batch) >= max_n or size + s > max_bytes):
            yield batch
            batch, size = [], 0
        batch.append(r)
        size += s
    if batch:
        yield batch


def load_checkpoint():
    if os.path.exists(CHECKPOINT):
        with open(CHECKPOINT) as f:
            return json.load(f).get("last_completed_date")
    return None


def save_checkpoint(d):
    with open(CHECKPOINT, "w") as f:
        json.dump({"last_completed_date": d}, f)


def wait_for_sfn(sfn, arn):
    while True:
        st = sfn.describe_execution(executionArn=arn)
        if st["status"] != "RUNNING":
            return st
        time.sleep(15)


def cmd_replay(a):
    files = sorted(glob.glob(os.path.join(a.prepared_dir, "date=*.csv")))
    days = [(os.path.basename(f)[5:15], f) for f in files]
    if a.start_date:
        days = [d for d in days if d[0] >= a.start_date]
    if a.end_date:
        days = [d for d in days if d[0] <= a.end_date]
    last = None if a.ignore_checkpoint else load_checkpoint()
    if last:
        log(f"Checkpoint found: resuming after {last}")
        days = [d for d in days if d[0] > last]
    if a.max_days:
        days = days[:a.max_days]
    if not days:
        log("Nothing to replay.")
        return

    if a.dry_run:
        for d, f in days:
            df = pd.read_csv(f, dtype=str)
            log(f"[dry-run] {d}: {len(df)} records")
        sample = pd.read_csv(days[0][1], dtype=str).head(3)
        for _, r in sample.iterrows():
            print(json.dumps(to_record(r), indent=2))
        return

    import boto3
    kinesis = boto3.client("kinesis", region_name=a.region)
    sfn = boto3.client("stepfunctions", region_name=a.region) if a.sfn_arn else None
    limiter = RateLimiter(a.rate)
    current = {"date": last}

    def on_interrupt(sig, frame):
        log("Interrupted. Checkpoint left at last completed day: %s" % current["date"])
        sys.exit(1)
    signal.signal(signal.SIGINT, on_interrupt)

    total_sent = total_failed = 0
    for d, f in days:
        t0 = time.monotonic()
        df = pd.read_csv(f, dtype=str)
        recs = [to_record(r) for r in df.to_dict("records")]
        sent = failed = 0
        for b in batches(recs):
            limiter.wait(len(b))
            failed += send_batch(kinesis, a.stream, b)
            sent += len(b)
        total_sent += sent
        total_failed += failed
        log(f"{d}: sent {sent}, failed {failed}, {time.monotonic() - t0:.1f}s")

        log(f"Waiting {a.flush_wait}s for Firehose to flush...")
        time.sleep(a.flush_wait)

        if sfn:
            name = f"run-{d}-{int(time.time())}"
            ex = sfn.start_execution(stateMachineArn=a.sfn_arn, name=name,
                                     input=json.dumps({"run_date": d}))
            log(f"Started Step Functions execution {name}")
            if a.wait_for_sfn:
                st = wait_for_sfn(sfn, ex["executionArn"])
                log(f"Execution {name} finished: {st['status']}")
                if st["status"] != "SUCCEEDED":
                    log(f"Stopping. Error: {st.get('error')} {st.get('cause', '')[:500]}")
                    sys.exit(2)

        save_checkpoint(d)
        current["date"] = d

    log(f"Done. Days: {len(days)}, sent: {total_sent}, failed: {total_failed}")


# ---------------------------------------------------------------- main
def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("list-banks")
    s.add_argument("--csv", required=True)
    s.add_argument("--top", type=int, default=20)
    s.set_defaults(func=cmd_list_banks)

    s = sub.add_parser("prepare")
    s.add_argument("--csv", required=True)
    s.add_argument("--bank-id", required=True)
    s.add_argument("--out-dir", required=True)
    s.set_defaults(func=cmd_prepare)

    s = sub.add_parser("replay")
    s.add_argument("--prepared-dir", required=True)
    s.add_argument("--stream", default="aml-transactions")
    s.add_argument("--region", default="us-east-1")
    s.add_argument("--start-date")
    s.add_argument("--end-date")
    s.add_argument("--max-days", type=int)
    s.add_argument("--rate", type=float, default=900, help="records/second")
    s.add_argument("--flush-wait", type=int, default=90, help="seconds to wait after each day")
    s.add_argument("--sfn-arn")
    s.add_argument("--wait-for-sfn", action="store_true")
    s.add_argument("--ignore-checkpoint", action="store_true")
    s.add_argument("--dry-run", action="store_true")
    s.set_defaults(func=cmd_replay)

    a = p.parse_args()
    a.func(a)


if __name__ == "__main__":
    main()
