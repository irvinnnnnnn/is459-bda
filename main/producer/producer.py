#!/usr/bin/env python3
"""Stand-in for the payment feed (PIPELINE_SPEC.md 2.4): replays the IBM AML transactions of every
bank into Kinesis on a simulated clock. The pipelines keep the payments that involve our bank,
which is set in common/settings.json.

    python producer.py dates   --csv HI-Small_Trans.csv            what dates the dataset covers; writes nothing
    python producer.py prepare --csv HI-Small_Trans.csv            split into history + live day files
    python producer.py fx                                          fetch FX rates for the dataset's date range
    python producer.py fx --check                                  check the FX file covers every payment
    python producer.py upload  --accounts HI-Small_accounts.csv --fx fx_rates.csv
    python producer.py replay                                      speed 60: 1 real second = 1 simulated minute
    python producer.py replay  --speed 1440                        1 real minute = 1 simulated day
    python producer.py replay  --day-minutes 5                     the same thing said the other way round
    python producer.py replay  --resume                            carry on where the last run stopped
    python producer.py clock   show | pause | resume | clear

How the replay keeps time. The producer starts a simulated clock at --start (default: the first
live day, 00:00) and saves it to S3 (common/sim_clock.py).

--speed N means the simulated world runs N times faster than the real one:
    1 real second = N simulated seconds        speed 60:   1 real second = 1 simulated minute
    1 simulated day = 1440 / N real minutes    speed 60:   a day takes 24 real minutes
--day-minutes M is the same setting asked the other way: "make one simulated day last M real minutes"
(it is just speed = 1440 / M).
 Each payment is sent at the moment the simulated clock reaches the payment's
own timestamp. Both Streamlit apps read the same clock, so "now", "expires in" and "today's queue"
on screen are the producer's now. Stopping the producer (Ctrl+C) pauses the clock; --resume
restarts both from the same simulated moment.

As each simulated day starts, the producer sends that day's FX rates to raw/fx/date=<day>/ (from --fx).

At each simulated midnight the producer can start the daily jobs for the day that just ended:
    --labels          start the Q1 label export (Glue)
    --q2 async        start the Q2 nightly workflow and keep going
    --q2 wait         pause the clock, run the Q2 workflow, then resume (the queue is ready at 00:00)
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
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "common"))
import config  # noqa: E402
from sim_clock import SimClock  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(HERE, "data")                                         # what prepare writes
DATASETS = os.path.normpath(os.path.join(HERE, "..", "..", "data"))        # where the downloaded files live
TX_COLS = ["timestamp", "from_bank", "from_account", "to_bank", "to_account", "amount_received",
           "receiving_currency", "amount_paid", "payment_currency", "payment_format", "is_laundering"]
IBM_HEADER = ["Timestamp", "From Bank", "Account", "To Bank", "Account", "Amount Received", "Receiving Currency",
              "Amount Paid", "Payment Currency", "Payment Format", "Is Laundering"]


def log(msg):
    print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)


def epoch(s):
    """'2022-09-08' or '2022-09-08T13:30' (UTC) -> epoch seconds."""
    d = datetime.fromisoformat(s.replace("/", "-"))
    return d.replace(tzinfo=timezone.utc).timestamp()


def pace(speed):
    """A speed in plain words."""
    def span(seconds):
        for size, unit in ((86400, "day"), (3600, "hour"), (60, "minute"), (1, "second")):
            if seconds >= size:
                n = seconds / size
                return f"{n:g} {unit}{'' if n == 1 else 's'}"
        return f"{seconds:g} seconds"
    return (f"speed {speed:g}: 1 real second = {span(speed)} simulated, "
            f"1 simulated day = {span(round(86400 / speed, 1))} real")


def stamp(ts):
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d %H:%M")


# ------------------------------------------------------------------ prepare ----
SUMMARY = "summary.json"
# IBM currency names -> ISO codes, as served by the Frankfurter API (spec 4.3, reference data). Its v2
# rates come from central banks and official sources; Bitcoin from the Swiss tax administration.
FRANKFURTER = "https://api.frankfurter.dev/v2/rates"
FX_CODES = {"Euro": "EUR", "UK Pound": "GBP", "Yen": "JPY", "Yuan": "CNY", "Rupee": "INR", "Ruble": "RUB",
            "Australian Dollar": "AUD", "Canadian Dollar": "CAD", "Mexican Peso": "MXN", "Brazil Real": "BRL",
            "Swiss Franc": "CHF", "Shekel": "ILS", "Saudi Riyal": "SAR", "Bitcoin": "BTC"}


def suggest_live_start(counts):
    """A reasonable first live day: the last 30% of the dataset's busy days are replayed live.

    The IBM sets end with a thin tail of a few payments a day; days below a tenth of the typical
    day are not counted as busy.
    """
    sizes = sorted(counts.values())
    typical = sizes[len(sizes) // 2]
    busy = sorted(d for d, n in counts.items() if n >= typical / 10)
    return busy[-max(1, round(len(busy) * 0.3))], busy


def report_dates(counts, live_start):
    """Print what the dataset covers and whether live_start suits it. Returns False if it cannot work."""
    days = sorted(counts)
    suggestion, busy = suggest_live_start(counts)
    hist = sum(n for d, n in counts.items() if d < live_start)
    live = sum(n for d, n in counts.items() if d >= live_start)
    print(f"The dataset covers {days[0]} to {days[-1]}: {sum(counts.values()):,} payments over {len(days)} days.")
    print(f"Busy days: {busy[0]} to {busy[-1]} ({len(busy)} days, about {counts[busy[len(busy) // 2]]:,} payments a day). "
          f"After that, a tail of {len([d for d in days if d > busy[-1]])} days with few payments.")
    print(f"live_start is {live_start} (main/common/settings.json): {hist:,} payments of history before it, "
          f"{live:,} to replay from it.")
    ok = hist > 0 and live > 0
    if not ok:
        print(f"*** live_start {live_start} does not fit this dataset: "
              + ("there is no history before it to train on." if not hist else "there is nothing after it to replay."))
    if not ok or suggestion != live_start:
        n = len([d for d in busy if d >= suggestion])
        print(f"Suggested live_start for this dataset: {suggestion} "
              f"(the last {n} of its {len(busy)} busy days are replayed, the rest train the model).")
        print("To use it: set live_start in main/common/settings.json, then run prepare again.")
    return ok


def cmd_dates(a):
    """Read the CSV once and report its date range. Writes nothing."""
    counts = defaultdict(int)
    with open(a.csv, newline="") as fh:
        reader = csv.reader(fh)
        next(reader)
        for row in reader:
            counts[row[0][:10].replace("/", "-")] += 1
    report_dates(counts, config.load(required=())["live_start"])


def cmd_prepare(a):
    """Every bank's payments, split at live_start: history (labelled, for training) and live day files.

    Nothing is filtered here. The pipelines keep our bank's payments (common/settings.json).
    The CSV is read row by row and written straight out, so the large sets do not need to fit in
    memory; only one live day at a time is sorted.
    """
    live_start = config.load(required=())["live_start"]
    live_dir, hist_dir = os.path.join(a.out_dir, "live"), os.path.join(a.out_dir, "historical")
    for d in (live_dir, hist_dir):
        os.makedirs(d, exist_ok=True)
    for f in glob.glob(os.path.join(live_dir, "date=*.csv")):
        os.remove(f)
    counts, files, writers = defaultdict(int), {}, {}
    with open(a.csv, newline="") as src, open(os.path.join(hist_dir, "history.csv"), "w", newline="") as hist:
        reader, hw = csv.reader(src), csv.writer(hist)
        next(reader)                                   # the header has two "Account" columns; go by position
        hw.writerow(IBM_HEADER)
        for n, row in enumerate(reader, 1):
            day = row[0][:10].replace("/", "-")
            counts[day] += 1
            if day < live_start:
                hw.writerow(row)                       # the Glue job orders history itself
                continue
            if day not in writers:
                files[day] = open(os.path.join(live_dir, f"date={day}.csv"), "w", newline="")
                writers[day] = csv.writer(files[day])
            writers[day].writerow(row)
            if n % 5_000_000 == 0:
                log(f"  {n:,} rows read")
    for fh in files.values():
        fh.close()
    for day in files:                                  # the replay needs each day in time order
        path = os.path.join(live_dir, f"date={day}.csv")
        with open(path, newline="") as fh:
            rows = list(csv.reader(fh))
        rows.sort(key=lambda r: r[0])                  # stable: keeps the file's order within a minute
        with open(path, "w", newline="") as fh:
            w = csv.writer(fh)
            w.writerow(TX_COLS)
            w.writerows(rows)
    with open(os.path.join(a.out_dir, SUMMARY), "w") as fh:
        json.dump({"csv": os.path.basename(a.csv), "first_date": min(counts), "last_date": max(counts),
                   "live_start": live_start, "days": dict(sorted(counts.items()))}, fh, indent=1)
    ok = report_dates(counts, live_start)
    print("Live days to replay:")
    for day in sorted(files):
        print(f"  {day}: {counts[day]:,}")
    if not ok:
        sys.exit(1)
    print(f"Written to {os.path.abspath(a.out_dir)}. Next: python producer.py upload --accounts ... --fx ...")


def data_summary(data_dir):
    path = os.path.join(data_dir, SUMMARY)
    if not os.path.exists(path):
        sys.exit(f"No {SUMMARY} in {data_dir}. Run: python producer.py prepare --csv <transactions CSV>")
    with open(path) as fh:
        return json.load(fh)


def fx_gaps(path, first, last):
    """Dates between first and last (inclusive) for which the FX file has no rates."""
    with open(path, newline="") as fh:
        have = {r["date"] for r in csv.DictReader(fh)}
    day, end, missing = datetime.fromisoformat(first), datetime.fromisoformat(last), []
    while day <= end:
        if day.strftime("%Y-%m-%d") not in have:
            missing.append(day.strftime("%Y-%m-%d"))
        day += timedelta(days=1)
    return missing


# ----------------------------------------------------------------------- fx ----
def fx_missing(fx_path, tx_csv):
    """Every (date, currency) a payment in the transactions CSV uses, paid or received, that the FX
    file has no rate for. The Q2 Glue job fails its run on any of these, so they are found here first."""
    have = defaultdict(set)
    with open(fx_path, newline="") as fh:
        for r in csv.DictReader(fh):
            have[r["date"]].add(r["currency"])
    need = defaultdict(set)
    with open(tx_csv, newline="") as fh:
        reader = csv.reader(fh)
        next(reader)
        for row in reader:
            need[row[0][:10].replace("/", "-")].update((row[6], row[8]))
    return [(d, c) for d in sorted(need) for c in sorted(need[d]) if c not in have[d]]


def check_fx(fx_path, tx_csv):
    for path, fix in ((fx_path, "python producer.py fx"), (tx_csv, "pass the transactions CSV with --csv")):
        if not os.path.exists(path):
            sys.exit(f"No file at {path}. To fix: {fix}")
    log(f"checking {fx_path} against every payment in {tx_csv}")
    missing = fx_missing(fx_path, tx_csv)
    if missing:
        for d, c in missing[:20]:
            print(f"  no rate: {d} {c}")
        sys.exit(f"{len(missing)} date/currency pairs used by payments have no rate"
                 f"{' (first 20 above)' if len(missing) > 20 else ''}. The Q2 runs for those dates would fail.")
    log("FX check passed: every payment's date and currencies have a rate")


def cmd_fx(a):
    """Daily FX rates for the prepared dataset's whole date range, from the Frankfurter API.

    Afterwards the file is checked against the transactions CSV; --check only does the check.
    """
    import urllib.request
    info = data_summary(a.data_dir)
    tx_csv = a.csv or os.path.join(DATASETS, info["csv"])
    if a.check:
        return check_fx(a.out, tx_csv)
    first, last = datetime.fromisoformat(info["first_date"]), datetime.fromisoformat(info["last_date"])
    days = [(first + timedelta(days=k)).strftime("%Y-%m-%d") for k in range((last - first).days + 1)]
    # a week either side, so weekends and holidays at the edges can take the nearest published day
    url = (f"{FRANKFURTER}?from={first - timedelta(days=7):%Y-%m-%d}&to={last + timedelta(days=7):%Y-%m-%d}"
           f"&base=USD&quotes={','.join(FX_CODES.values())}")
    try:
        with urllib.request.urlopen(url, timeout=30) as resp:
            published = json.load(resp)                    # [{date, base, quote, rate: units per USD}]
    except Exception as exc:
        sys.exit(f"Frankfurter API failed ({exc}). Try again later; nothing was written.")
    rows, problems = [(d, "US Dollar", 1.0, "base") for d in days], []
    for name, code in FX_CODES.items():
        quoted = {r["date"]: 1 / r["rate"] for r in published if r["quote"] == code and r.get("rate")}
        if not quoted:
            problems.append(name)
            continue
        known = sorted(quoted)
        for d in days:                    # not published that day: the last one before it, else the first after
            before = [k for k in known if k <= d]
            k = before[-1] if before else known[0]
            rows.append((d, name, quoted[k], "frankfurter" if k == d else "frankfurter-carried"))
    if problems:
        sys.exit(f"Frankfurter returned no rates for {problems}. Try again later; nothing was written.")
    with open(a.out, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["date", "currency", "usd_per_unit", "source"])
        w.writerows(sorted(rows))
    log(f"wrote {a.out}: {1 + len(FX_CODES)} currencies, {days[0]} to {days[-1]}")
    check_fx(a.out, tx_csv)
    print(f"Next: python producer.py upload --accounts <accounts CSV> --fx {a.out}")


def fx_by_day(path):
    """The FX file as {date: CSV text of that day's rates}, the form raw/fx/date=<day>/fx.csv takes."""
    days = defaultdict(list)
    with open(path, newline="") as fh:
        for r in csv.DictReader(fh):
            days[r["date"]].append(f"{r['currency']},{r['usd_per_unit']},{r.get('source', '')}")
    return {d: "currency,usd_per_unit,source\n" + "\n".join(rows) + "\n" for d, rows in days.items()}


def put_fx_day(s3, bucket, day, text):
    s3.put_object(Bucket=bucket, Key=f"raw/fx/date={day}/fx.csv", Body=text.encode())


# ------------------------------------------------------------------- upload ----
def cmd_upload(a):
    """One-off uploads: the labelled history, the customer master export and the history days' FX rates.

    The live days' FX rates are not uploaded here: replay sends each one as its simulated day starts,
    the way a bank receives the day's rates.
    """
    import boto3
    cfg = config.load()
    info = data_summary(a.data_dir)
    if a.fx:
        missing = fx_gaps(a.fx, info["first_date"], info["last_date"])
        if missing:
            sys.exit(f"{a.fx} has no rates for {len(missing)} of the dataset's days ({missing[0]} to {missing[-1]}). "
                     f"The dataset covers {info['first_date']} to {info['last_date']}. Fetch rates for the whole "
                     f"range with: python producer.py fx   (then upload --fx data/fx_rates.csv)")
    s3 = boto3.client("s3", region_name=cfg["region"])
    snapshot = a.snapshot_date or info["first_date"]
    todo = [(os.path.join(a.data_dir, "historical", "history.csv"), "historical/history.csv")]
    if a.accounts:
        todo.append((a.accounts, f"raw/accounts/snapshot_date={snapshot}/accounts.csv"))
    for path, key in todo:
        s3.upload_file(path, cfg["bucket"], key)
        log(f"uploaded {path} -> s3://{cfg['bucket']}/{key}")
    if a.fx:
        hist = {d: text for d, text in fx_by_day(a.fx).items() if d < cfg["live_start"]}
        for day, text in sorted(hist.items()):
            put_fx_day(s3, cfg["bucket"], day, text)
        log(f"uploaded FX rates for the {len(hist)} history days -> s3://{cfg['bucket']}/raw/fx/date=.../fx.csv "
            "(replay sends each live day's)")


# ------------------------------------------------------------------- replay ----
def load_day(path, day):
    """One day file -> [(due, record)], where due is the simulated second at which to send it.

    Timestamps are to the minute, so payments in the same minute are spread across it in file order.
    """
    out, minute = [], defaultdict(list)
    with open(path, newline="") as fh:
        for n, r in enumerate(csv.DictReader(fh)):
            ts = datetime.strptime(r["timestamp"], "%Y/%m/%d %H:%M").replace(tzinfo=timezone.utc)
            raw = "|".join(r[c] for c in TX_COLS)
            rec = {"txn_id": hashlib.sha1(f"{raw}|{day}|{n}".encode()).hexdigest()[:32],
                   "txn_ts": ts.strftime("%Y-%m-%dT%H:%M:%S"), "txn_date": ts.strftime("%Y-%m-%d"),
                   "from_bank": r["from_bank"], "from_account": r["from_account"],
                   "to_bank": r["to_bank"], "to_account": r["to_account"],
                   "amount_received": float(r["amount_received"]), "receiving_currency": r["receiving_currency"],
                   "amount_paid": float(r["amount_paid"]), "payment_currency": r["payment_currency"],
                   "payment_format": r["payment_format"], "is_laundering": int(r["is_laundering"])}
            minute[ts.timestamp()].append(rec)
    for ts in sorted(minute):
        recs = minute[ts]
        out += [(ts + 60.0 * i / len(recs), rec) for i, rec in enumerate(recs)]
    return out


class Sender:
    def __init__(self, kinesis, stream):
        self.kinesis, self.stream, self.sent, self.failed = kinesis, stream, 0, 0

    def send(self, records, max_retries=8):
        """PutRecords in batches of 500, retrying only the records that failed."""
        for i in range(0, len(records), 500):
            entries = [{"Data": (json.dumps(r) + "\n").encode(), "PartitionKey": r["from_account"]}
                       for r in records[i:i + 500]]
            n = len(entries)
            for attempt in range(max_retries + 1):
                resp = self.kinesis.put_records(StreamName=self.stream, Records=entries)
                if resp["FailedRecordCount"] == 0:
                    entries = []
                    break
                entries = [e for e, res in zip(entries, resp["Records"]) if "ErrorCode" in res]
                time.sleep(min(5, 0.1 * 2 ** attempt) + random.random() * 0.05)
            self.sent += n - len(entries)
            self.failed += len(entries)


class DayEnd:
    """Starts the daily jobs for a simulated day that has just ended."""

    def __init__(self, a, cfg, clock):
        import boto3
        self.a, self.cfg, self.clock = a, cfg, clock
        self.glue = boto3.client("glue", region_name=cfg["region"])
        self.sfn = boto3.client("stepfunctions", region_name=cfg["region"])

    def run(self, day):
        a, cfg = self.a, self.cfg
        if a.labels:
            try:
                self.glue.start_job_run(JobName=cfg["label_export_job"])
                log(f"day {day} ended: started the Q1 label export")
            except Exception as exc:
                log(f"label export not started: {exc}")
        if a.q2 == "off":
            return
        wait = a.q2 == "wait"
        if wait:
            self.clock.pause()
            log(f"day {day} ended: clock paused at {stamp(self.clock.now())} for the Q2 nightly run")
        log(f"waiting {a.flush_wait}s for Firehose to land the last payments in raw/transactions/")
        time.sleep(a.flush_wait)
        name = f"run-{day}-{int(time.time())}"
        arn = self.sfn.start_execution(stateMachineArn=cfg["q2_state_machine_arn"], name=name,
                                       input=json.dumps({"run_date": day}))["executionArn"]
        log(f"started Q2 nightly workflow {name}")
        if wait:
            while True:
                st = self.sfn.describe_execution(executionArn=arn)["status"]
                if st != "RUNNING":
                    break
                time.sleep(15)
            log(f"Q2 nightly workflow {st}")
            self.clock.resume()
            log("clock resumed")


def cmd_replay(a):
    files = sorted(glob.glob(os.path.join(a.data_dir, "live", "date=*.csv")))
    days = [(os.path.basename(f)[5:15], f) for f in files]
    if not days:
        sys.exit(f"No day files in {os.path.join(a.data_dir, 'live')}. Run: python producer.py prepare --csv ...")
    if a.end_date:
        days = [d for d in days if d[0] <= a.end_date]

    if getattr(a, "day_minutes", None):
        a.speed = 1440 / a.day_minutes
    if a.dry_run:
        start = epoch(a.start or days[0][0])
        for day, f in days:
            recs = [r for r in load_day(f, day) if r[0] >= start]
            if recs:
                real = (recs[-1][0] - recs[0][0]) / a.speed
                log(f"[dry-run] {day}: {len(recs)} payments, about {len(recs) / max(real, 1):.1f} per real second")
        log(f"[dry-run] {pace(a.speed)}")
        print(json.dumps(load_day(days[0][1], days[0][0])[0][1], indent=2))
        return

    import boto3
    cfg = config.load(required=("bucket", "stream"))
    clock = SimClock(cfg["bucket"], cfg["region"], cache_seconds=3600)      # we are the only writer
    sender = Sender(boto3.client("kinesis", region_name=cfg["region"]), a.stream or cfg["stream"])
    s3 = boto3.client("s3", region_name=cfg["region"])
    fx_path = getattr(a, "fx", None)
    fx = fx_by_day(fx_path) if fx_path and os.path.exists(fx_path) else {}
    if not fx:
        log(f"no FX file at {fx_path}: the live days' rates will not be sent, so the Q2 runs will fail validation")
    saved = clock.state(fresh=True)
    if a.resume and saved:
        start, sent_through = saved["sim_anchor"] if not saved["running"] else clock.now(), saved.get("sent_through", 0)
        log(f"resuming at simulated {stamp(start)}")
    else:
        start = epoch(a.start or days[0][0])
        sent_through = start - 1e-6
    days = [d for d in days if not (epoch(d[0]) + 86400 <= start and sent_through >= epoch(d[0]) + 86400 - 60)]
    loaded = {days[0][0]: load_day(days[0][1], days[0][0])} if days else {}    # read before the clock starts
    clock.start(start, a.speed, sent_through=sent_through)
    log(f"simulated clock started at {stamp(start)}. {pace(a.speed)}")

    stopping = {"now": False}

    def on_interrupt(sig, frame):
        stopping["now"] = True
    signal.signal(signal.SIGINT, on_interrupt)
    day_end = DayEnd(a, cfg, clock)
    last_note = last_log = time.time()
    try:
        for day, f in days:
            midnight = epoch(day) + 86400
            queue = [r for r in (loaded.pop(day, None) or load_day(f, day)) if r[0] > sent_through]
            log(f"{day}: {len(queue)} payments to send")
            if day in fx:                                # the day's rates arrive as the day starts
                put_fx_day(s3, cfg["bucket"], day, fx[day])
                log(f"{day}: sent the day's FX rates to raw/fx/date={day}/")
            elif fx:
                log(f"{day}: the FX file has no rates for this day; its Q2 run will fail validation")
            i = 0
            while not stopping["now"]:
                now = clock.now()
                j = i
                while j < len(queue) and queue[j][0] <= now:
                    j += 1
                if j > i:
                    sender.send([r for _, r in queue[i:j]])
                    sent_through, i = queue[j - 1][0], j
                if time.time() - last_note > 15:
                    clock.note(sent_through=sent_through)
                    last_note = time.time()
                if time.time() - last_log > 10:
                    lag = max(0.0, now - queue[i][0]) / a.speed if i < len(queue) else 0.0
                    log(f"  simulated {stamp(now)}  sent {sender.sent}  failed {sender.failed}"
                        + (f"  behind by {lag:.1f}s" if lag > 1 else ""))
                    last_log = time.time()
                if i >= len(queue) and now >= midnight:
                    break
                time.sleep(a.tick)
            if stopping["now"]:
                break
            sent_through = max(sent_through, midnight - 1e-6)
            clock.note(sent_through=sent_through)
            day_end.run(day)
    finally:
        clock.pause(sent_through=sent_through)
        log(f"clock paused at simulated {stamp(clock.now())}. Sent {sender.sent}, failed {sender.failed}. "
            "Continue with: python producer.py replay --resume")


# -------------------------------------------------------------------- clock ----
def cmd_clock(a):
    cfg = config.load()
    clock = SimClock(cfg["bucket"], cfg["region"], cache_seconds=0)
    if a.action == "pause":
        clock.pause()
    elif a.action == "resume":
        clock.resume(a.speed)
    elif a.action == "clear":
        clock.clear()
    st = clock.state(fresh=True)
    if not st:
        print("No simulated clock: the apps and jobs use the real time.")
        return
    print(f"simulated time {stamp(clock.now())}  speed {st['speed']:g}  "
          f"{'running' if st['running'] else 'paused'}")


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("prepare", help="split the transactions CSV into history and live day files")
    s.add_argument("--csv", required=True)
    s.add_argument("--out-dir", default=DATA)
    s.set_defaults(func=cmd_prepare)

    s = sub.add_parser("dates", help="report the dataset's date range and a suitable live_start")
    s.add_argument("--csv", required=True)
    s.set_defaults(func=cmd_dates)

    s = sub.add_parser("fx", help="fetch daily FX rates for the prepared dataset's date range (Frankfurter API)")
    s.add_argument("--data-dir", default=DATA)
    s.add_argument("--out", default=os.path.join(DATASETS, "fx_rates.csv"))
    s.add_argument("--check", action="store_true", help="only check --out against the transactions; fetch nothing")
    s.add_argument("--csv", help="transactions CSV to check against (default: the one prepare was run on)")
    s.set_defaults(func=cmd_fx)

    s = sub.add_parser("upload", help="upload the history, accounts and FX files to S3")
    s.add_argument("--data-dir", default=DATA)
    s.add_argument("--accounts", help="customer master export (HI-Small_accounts.csv)")
    s.add_argument("--fx", help="FX rates CSV: date,currency,usd_per_unit,source (only the history days are sent)")
    s.add_argument("--snapshot-date", help="date the accounts export applies from (default: the dataset's first day)")
    s.set_defaults(func=cmd_upload)

    s = sub.add_parser("replay", help="send the live days to Kinesis on the simulated clock")
    s.add_argument("--data-dir", default=DATA)
    pace_args = s.add_mutually_exclusive_group()
    pace_args.add_argument("--speed", type=float, default=60,
                           help="how many times faster than real time (default 60: 1 real second = 1 simulated minute)")
    pace_args.add_argument("--day-minutes", type=float,
                           help="instead of --speed: how many real minutes one simulated day should take")
    s.add_argument("--start", help="simulated start, YYYY-MM-DD or YYYY-MM-DDTHH:MM (default: first live day)")
    s.add_argument("--end-date", help="last day to send, YYYY-MM-DD")
    s.add_argument("--fx", default=os.path.join(DATASETS, "fx_rates.csv"),
                   help="FX rates CSV; each live day's rates are sent to raw/fx/ as the day starts")
    s.add_argument("--resume", action="store_true", help="continue from the saved clock")
    s.add_argument("--stream", help="Kinesis stream (default: from config.json)")
    s.add_argument("--labels", action="store_true", help="start the Q1 label export at each simulated midnight")
    s.add_argument("--q2", choices=["off", "async", "wait"], default="off",
                   help="start the Q2 nightly workflow at each simulated midnight")
    s.add_argument("--flush-wait", type=int, default=90, help="seconds to let Firehose flush before Q2 starts")
    s.add_argument("--tick", type=float, default=0.1, help="seconds between checks of the clock")
    s.add_argument("--dry-run", action="store_true", help="show the rates and a sample record; send nothing")
    s.set_defaults(func=cmd_replay)

    s = sub.add_parser("clock", help="show or control the simulated clock")
    s.add_argument("action", choices=["show", "pause", "resume", "clear"])
    s.add_argument("--speed", type=float, help="new speed when resuming")
    s.set_defaults(func=cmd_clock)

    a = p.parse_args()
    a.func(a)


if __name__ == "__main__":
    main()
