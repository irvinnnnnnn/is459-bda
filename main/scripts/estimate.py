#!/usr/bin/env python3
"""Project a measured run (cost.py --save) onto bigger datasets: HI-Small, HI-Medium, HI-Large.

    python estimate.py --run run1.json                     the live days (last 30% of busy days), in real time
    python estimate.py --run run1.json --mode full         every busy day streamed, in real time
    python estimate.py --run run1.json --mode month        30 days, running 24/7
    python estimate.py --run run1.json --mode demo --speed 60

Every billed line of the run is scaled by what drives it:
  time      billed by the hour, and the hours do not depend on data (Flink, Kinesis shards, the apps server):
            scaled by how long the pipeline runs
  size      billed by data (Kinesis PUTs, Firehose GB, DynamoDB writes, S3): scaled by payments
  job       billed by the hour, but a bigger input makes the job run longer (Glue, EMR, SageMaker):
            run time = fixed startup + seconds per payment x payments, then priced like the run was
  use       Athena queries and DynamoDB reads from people reviewing in the two apps (assumed use, APP_*):
            an Athena query reads every row of the days it covers (only its columns), and a DynamoDB scan
            reads the whole alerts table, so both grow with the payments a day

The fixed startup and seconds per payment of Q2 come from the run itself (Q2 days of different sizes).
Jobs that ran once (training) have one point, so their fixed startup is an assumption, in JOB_FIXED_S.
Dataset sizes in DATASETS were counted from the downloaded files (python main/producer/producer.py dates
--csv <file>). The history days are trained on in batch; only the live days go through Kinesis and Flink.
"""
import argparse
import json
import math
import os
import sys
from datetime import datetime

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from cost import HOUR, MONTH_HOURS, GB, rate  # noqa: E402

SUMMARY = os.path.join(HERE, "..", "producer", "data", "summary.json")   # the measured run's dataset (prepare)

# Counted from the Kaggle files ("IBM Transactions for Anti Money Laundering"): history payments (before the
# suggested live_start), live payments (live_start to the last busy day; the thin tail after it is left out),
# busy days. Busy days are one unbroken block in all three: 09-01..09-10, 09-01..09-16, 08-01..11-05 (2022).
DATASETS = {
    "HI-Small": (3_731_672, 1_345_565, 10),        # live from 2022-09-08, 3 days
    "HI-Medium": (21_149_143, 10_742_108, 16),     # live from 2022-09-12, 5 days
    "HI-Large": (127_613_986, 52_047_524, 97),     # live from 2022-10-08, 29 days
}
LIVE_SHARE = 0.3                  # the last 30% of busy days are replayed live (producer.suggest_live_start)
OUR_BANK_SHARE = 0.0526           # payments involving bank 11, measured on the HI-Small sample's live days
SHARD_RECORDS_PER_S = 1000        # Kinesis limit per shard
FLINK_OUR_BANK_PER_KPU = 100      # bank-11 payments a second one KPU scored in the stress test (SHAP included)
TRAINING_GB_LIMIT = 4.0           # ml.m5.large has 8 GB of memory; above this the instance must grow
# Fixed startup seconds of the jobs that ran only once (one point cannot separate fixed from per-payment time)
JOB_FIXED_S = {"q1-features-historical": 60, "q1-build-training": 30, "q1-train": 60, "q1-eval": 60,
               "q1-label-export": None}          # None: no relation to data size (14-16 s on any day)
# People reviewing in the apps. Use is assumed (the measured run had no one using the apps); what each
# action reads and writes comes from the code. The Q1 Metrics page (monitoring) is left out.
Q1_REVIEWS_PER_DAY = 200          # alerts + audit items reviewed a day, or all of them when fewer are raised
Q2_REVIEWS_PER_DAY = 15           # customers reviewed a day, or the whole queue when it is shorter
APP_HOURS_PER_DAY = 8             # one working shift per investigator; Q1 has as many as its reviews need
MINUTES_PER_LOOKUP = 5            # on the review page; the rest of the shift the Q1 queue page is open
Q1_SIDES_PER_ALERT = 1.02         # activity queries per alert, one per side at bank 11 (1.6% have both, HI-Small)
Q1_QUEUE_SCANS_PER_HOUR = 1200    # the Q1 queue re-reads the whole alerts table every 3 s (REFRESH_SECONDS)
Q1_SCANS_PER_LOOKUP = 10          # 5 reruns (open, claim, decision, reason, comment) x 2 table scans each
Q1_WRITES_PER_LOOKUP = 2          # claim + decision, each an update of the whole item
Q2_DECISION_READS_PER_LOOKUP = 3  # the decisions log is re-read before the claim and the decision, plus a reload
REVIEW_DAYS = 7                   # an alert stays in the table until the label export after its review window
ATHENA_MIN_BYTES = 10 * 1024 ** 2
DDB_READ_BYTES = 4096             # a scan costs 0.5 read units per 4 KB read (eventually consistent)
# (days the query covers, share of the table's bytes in the columns it reads). Shares were measured on Parquet
# files with the real schemas and real HI-Small rows: the scored rows are mostly feature columns, never read.
APP_QUERIES = {"q1-account": (30, 0.08), "q2-customer": (7, 0.60)}
GROUPS = {"time": "Billed by time (hours running)", "size": "Billed by size (payments, GB)",
          "job": "Billed by time, run time grows with data", "use": "From people using the apps"}


# ------------------------------------------------------------------ the measured run ----
class Base:
    def __init__(self, run, summary):
        self.run, f = run, run["facts"]
        days, start = summary["days"], summary["live_start"]
        self.live_days = f["live_days_replayed"]
        live = sorted(d for d in days if d >= start)[:self.live_days]
        self.day_payments = {d: days[d] for d in live}
        self.history = sum(n for d, n in days.items() if d < start)
        self.live = sum(self.day_payments.values())
        self.hours = (datetime.fromisoformat(run["until"]) - datetime.fromisoformat(run["since"])).total_seconds() / HOUR
        # Flink starts only after setup and training, so it runs for less than the whole window:
        # its hours are the KPU-hours over the KPUs billed ("1 KPU + 1 for orchestration")
        note = run.get("notes", {}).get("Managed Flink", "")
        kpu_hours = next((i["quantity"] for i in run["items"] if (i["service"], i["item"]) == ("Managed Flink", "KPU-hours")), None)
        self.flink_hours = (kpu_hours / (int(note.split(", ")[1].split()[0]) + 1)
                            if kpu_hours and note.endswith("orchestration") else self.hours)
        self.speed = f["replay_speed"]
        self.s3 = f["s3_bytes_by_prefix"]
        self.items = {(i["service"], i["item"]): i for i in run["items"]}
        self.jobs = self.fit_jobs()

    def runs(self, prefix):
        return sorted((r for r in self.run["runs"] if r["name"].startswith(prefix)), key=lambda r: r["at"])

    @staticmethod
    def seconds(r):
        return float(r["usage"].split()[0])           # "128 s x 2 DPU", "468 s up", "119 billable s ..."

    def fit_jobs(self):
        """For each job: fixed seconds, seconds per payment, units per second (DPU or instances), price."""
        jobs = {}
        # Q2: one run per live day, in date order. The first run also backfills, so it is left out of the fit.
        for name, prefix in (("q2-etl", "Glue q2-etl"), ("q2-emr", "EMR q2-score-")):
            rs = self.runs(prefix)
            pts = [(n, self.seconds(r)) for n, r in zip(self.day_payments.values(), rs)][1:]
            fixed, per = fit(pts)
            r = rs[-1]
            jobs[name] = dict(fixed=fixed, per=per, units=r["quantity"] * HOUR / self.seconds(r), price=r["price"],
                              driver="day", note=f"fitted on {len(pts)} Q2 days")
        for name, prefix in (("q1-features-historical", "Glue q1-features-historical"),
                             ("q1-build-training", "Glue q1-build-training"),
                             ("q1-train", "SageMaker q1-train"), ("q1-eval", "SageMaker q1-eval"),
                             ("q1-label-export", "Glue q1-label-export")):
            rs = self.runs(prefix)
            if not rs:
                continue
            s = sum(self.seconds(r) for r in rs) / len(rs)
            fixed = JOB_FIXED_S[name]
            jobs[name] = dict(fixed=s if fixed is None else min(fixed, s), per=0 if fixed is None else
                              (s - min(fixed, s)) / self.history, units=rs[-1]["quantity"] * HOUR / self.seconds(rs[-1])
                              if "Glue" not in prefix else glue_dpu(rs[-1]), price=rs[-1]["price"],
                              driver="day" if name == "q1-label-export" else "history",
                              note="no relation to size" if fixed is None else f"assumed {fixed} s fixed")
        return jobs


def filled_days(window, days):
    """Days of data a window holds on average over `days` days, when the data starts empty and fills it."""
    return sum(min(d, window) for d in range(1, days + 1)) / days


def glue_dpu(r):
    return float(r["usage"].split(" x ")[1].split()[0])


def fit(pts):
    """Least-squares line through (payments, seconds): fixed seconds and seconds per payment."""
    if len(pts) < 2:
        return pts[0][1] if pts else 0.0, 0.0
    n = len(pts)
    mx, my = sum(p[0] for p in pts) / n, sum(p[1] for p in pts) / n
    var = sum((p[0] - mx) ** 2 for p in pts)
    per = max(0.0, sum((p[0] - mx) * (p[1] - my) for p in pts) / var) if var else 0.0
    return max(0.0, my - per * mx), per


# ------------------------------------------------------------------ one target ----
class Target:
    def __init__(self, name, history, live, busy_days, base, mode, speed):
        self.name, self.base, self.mode, self.speed = name, base, mode, speed
        live_days = max(1, round(busy_days * LIVE_SHARE))
        self.history = history
        self.q2_days = {"month": 30, "full": busy_days}.get(mode, live_days)
        # live and demo stream the live days as they are; full and month spread every busy day evenly
        self.per_day = live / live_days if mode in ("live", "demo") else (history + live) / busy_days
        self.live = self.per_day * self.q2_days
        self.lines, self.durations, self.warnings = [], {}, []
        self.build()

    def job_seconds(self, name, size):
        j = self.base.jobs[name]
        return j["fixed"] + j["per"] * size

    def add(self, group, service, item, quantity, price, detail=""):
        self.lines.append(dict(group=group, service=service, item=item, quantity=quantity, price=price, detail=detail))

    def build(self):
        b = self.base
        # job: run time from data size, then priced per second like the measured run
        setup_s = 0.0
        for name, j in b.jobs.items():
            size = self.per_day if j["driver"] == "day" else self.history
            s = self.job_seconds(name, size)
            runs = self.q2_days if j["driver"] == "day" else 1
            billed = max(s, 60) if j["price"] == "glue_dpu_hour" else s
            self.durations[name] = s
            unit = "DPU-hours" if j["price"] == "glue_dpu_hour" else "instance-hours"
            detail = f"{runs} run(s) x {s:,.0f} s x {j['units']:.3g}"
            self.add("job", name, unit, runs * billed * j["units"] / HOUR, j["price"], detail)
            if name == "q2-emr":                                   # EMR's own fee, on top of the EC2 price
                self.add("job", name, "EMR fee instance-hours", runs * billed * j["units"] / HOUR,
                         j["price"].replace("ec2_hour", "emr_hour"), detail)
            if j["driver"] == "history":
                setup_s += s
        q2_s = self.durations.get("q2-etl", 0) + self.durations.get("q2-emr", 0)

        # how long the always-on part runs, and how much of it the payment rate needs
        if self.mode == "demo":
            replay_h = self.q2_days * 24 / self.speed
            base_setup = sum(self.base.jobs[n]["fixed"] + self.base.jobs[n]["per"] * b.history
                             for n, j in b.jobs.items() if j["driver"] == "history")
            base_q2 = sum(self.base.jobs[n]["fixed"] + self.base.jobs[n]["per"] * list(b.day_payments.values())[-1]
                          for n in ("q2-etl", "q2-emr"))
            other_h = b.hours - b.live_days * 24 / b.speed
            hours = other_h + (setup_s - base_setup) / HOUR + replay_h + (q2_s - base_q2) / HOUR
            flink_hours = hours - (b.hours - b.flink_hours) - (setup_s - base_setup) / HOUR
            rate_s = self.per_day * self.speed / 86400
            if q2_s > 86400 / self.speed:
                self.warnings.append(f"a Q2 day takes {q2_s / 60:.0f} min but a replayed day only "
                                     f"{86400 / self.speed / 60:.0f} min: Q2 runs overlap (lab vCPU limit)")
            if hours > 4:
                self.warnings.append(f"{hours:.1f} h is longer than a 4-hour lab session")
        else:
            hours = MONTH_HOURS if self.mode == "month" else self.q2_days * 24
            flink_hours = hours                                    # in real time Flink runs the whole period
            rate_s = self.per_day / 86400
        self.hours, self.rate = hours, rate_s
        shards = max(1, math.ceil(rate_s / SHARD_RECORDS_PER_S))
        kpus = max(1, math.ceil(rate_s * OUR_BANK_SHARE / FLINK_OUR_BANK_PER_KPU))
        self.shards, self.kpus = shards, kpus

        # time: the measured quantity x (hours now / hours then) x (capacity now / capacity then)
        capacity = {"Managed Flink": (kpus + 1) / 2, "Kinesis Data Streams": shards}
        for (service, item), i in b.items.items():
            if service in ("Managed Flink", "EC2 apps server") or "shard" in item:
                grow = capacity.get(service, 1.0) if "storage" not in item else kpus
                h, h_then = (flink_hours, b.flink_hours) if service == "Managed Flink" else (hours, b.hours)
                detail = {"Managed Flink": f"{kpus} KPU + 1", "Kinesis Data Streams": f"{shards} shard"}.get(service, "")
                self.add("time", service, item, i["quantity"] * h / h_then * grow, i["price"],
                         f"{h:,.1f} h" + (f", {detail}" if detail else ""))

        # size: the measured quantity x (payments now / payments then)
        r_live = self.live / b.live
        for (service, item), i in b.items.items():
            if service in ("Kinesis Data Streams", "Data Firehose") and "shard" not in item:
                self.add("size", service, item, i["quantity"] * r_live, i["price"])
            elif (service, item) == ("DynamoDB alerts table", "million write request units"):
                self.add("size", service, "million write request units (alerts)", i["quantity"] * r_live, i["price"])
        # the alerts table: each alert is written once, read by every scan of the table, and deleted by the daily
        # label export once its review window has closed. The item size comes from the run's write units per alert.
        writes = b.items.get(("DynamoDB alerts table", "million write request units"), {}).get("quantity", 0) * 1e6
        alerts = b.run["facts"].get("alerts_in_table") or 1
        item_wru = writes / alerts                                # write units per item: about 2, items of 1-2 KB
        alerts_per_day = alerts / b.live * self.per_day
        held = filled_days(REVIEW_DAYS, self.q2_days)            # days of alerts in the table, on average
        scan_ru = max(0.5, alerts_per_day * held * item_wru * 1024 / DDB_READ_BYTES * 0.5)
        deleted = alerts_per_day * max(0, self.q2_days - REVIEW_DAYS)
        self.add("size", "DynamoDB alerts table", "million read request units (label export)",
                 self.q2_days * scan_ru / 1e6, "dynamodb_read_million", f"{self.q2_days} daily scans")
        self.add("size", "DynamoDB alerts table", "million write request units (label export)",
                 deleted * item_wru / 1e6, "dynamodb_write_million", f"{deleted:,.0f} alerts deleted")
        fixed = b.s3.get("code", 0)
        data_gb = (sum(b.s3.values()) - fixed) / GB * (self.history + self.live) / (b.history + b.live) + fixed / GB
        self.stored_gb = data_gb
        self.add("size", "S3 storage", "GB-months", data_gb * hours / MONTH_HOURS, "s3_gb_month",
                 f"{data_gb:,.1f} GB for {hours:,.0f} h")

        # use: people reviewing in the apps, each day the smaller of the daily cap and what was flagged that day.
        # Q2's queue comes from the run's scores (in_queue), grown with the payments a day.
        q1_day = min(Q1_REVIEWS_PER_DAY, alerts_per_day)
        queue = b.run["facts"].get("q2_queue")
        if queue:
            q2_day = min(Q2_REVIEWS_PER_DAY, sum(queue.values()) / len(queue) * self.per_day / (b.live / b.live_days))
        else:
            q2_day = Q2_REVIEWS_PER_DAY
            self.warnings.append("the run has no Q2 queue size (save it again with the current cost.py): "
                                 f"assumed at least {Q2_REVIEWS_PER_DAY} customers a day")
        q1_lookups, q2_lookups = q1_day * self.q2_days, q2_day * self.q2_days
        people = max(1, math.ceil(q1_day * MINUTES_PER_LOOKUP / 60 / APP_HOURS_PER_DAY))
        app_h = min(hours, self.q2_days * APP_HOURS_PER_DAY) * people
        queue_h = max(0.0, app_h - q1_lookups * MINUTES_PER_LOOKUP / 60)
        # curated/live holds the streamed days only, so a Q1 look-up's 30 days fill up as the period goes on.
        # The first Q2 run backfills history into customer_txns, so its 7 days are full from the first day.
        per_payment = {"q1": b.s3.get("curated/live", 0) / b.live,
                       "q2": b.s3.get("curated/customer_txns", 0) / (b.history + b.live)}
        days_read = {"q1": filled_days(APP_QUERIES["q1-account"][0], self.q2_days), "q2": APP_QUERIES["q2-customer"][0]}

        def scan(query):
            share = APP_QUERIES[query][1]
            return max(self.per_day * days_read[query[:2]] * per_payment[query[:2]] * share, ATHENA_MIN_BYTES)

        # Q1: the account's activity, per side at bank 11. Q2: the customer's payments, the decisions log
        # (small, so billed at the minimum), and each night's scores (SELECT *) once a day.
        q1 = q1_lookups * Q1_SIDES_PER_ALERT * scan("q1-account")
        scores = max(self.per_day * b.s3.get("scores", 0) / b.live, ATHENA_MIN_BYTES)
        q2 = (q2_lookups * (scan("q2-customer") + Q2_DECISION_READS_PER_LOOKUP * ATHENA_MIN_BYTES)
              + self.q2_days * scores)
        self.add("use", "Athena: Q1 investigator app", "TB scanned", q1 / GB / 1024, "athena_tb",
                 f"{q1_lookups:,.0f} alerts reviewed")
        self.add("use", "Athena: Q2 KYC app", "TB scanned", q2 / GB / 1024, "athena_tb",
                 f"{q2_lookups:,.0f} customers reviewed")
        scans = queue_h * Q1_QUEUE_SCANS_PER_HOUR + q1_lookups * Q1_SCANS_PER_LOOKUP
        self.add("use", "DynamoDB: Q1 investigator app", "million read request units", scans * scan_ru / 1e6,
                 "dynamodb_read_million", f"{people} people, {scans:,.0f} table scans")
        self.add("use", "DynamoDB: Q1 investigator app", "million write request units",
                 q1_lookups * Q1_WRITES_PER_LOOKUP * item_wru / 1e6, "dynamodb_write_million", "claims + decisions")

        train_gb = b.s3.get("training", 0) / GB * self.history / b.history
        if train_gb > TRAINING_GB_LIMIT:
            self.warnings.append(f"training data about {train_gb:.1f} GB: ml.m5.large (8 GB) is too small")

    def total(self, group=None):
        return sum(cost(l) for l in self.lines if group in (None, l["group"]))


def cost(line):
    return line["quantity"] * (rate(line["price"]) or 0.0)


# ------------------------------------------------------------------ report ----
def report(targets, base, mode, speed):
    what = {"full": "every busy day of each dataset, in real time",
            "live": "each dataset's live days, in real time", "month": "30 days, running 24/7",
            "demo": f"a lab demo replayed at speed {speed:g}"}[mode]
    print(f"Projection of {base.run.get('since', '')[:10]} run onto {what}\n")
    w = 16
    head = f"  {'':<38}" + "".join(f"{t.name:>{w}}" for t in targets)
    rows = [("payments per busy day", lambda t: f"{t.per_day:,.0f}"),
            ("history payments (training)", lambda t: f"{t.history / 1e6:,.2f} M"),
            ("Q2 days", lambda t: f"{t.q2_days}"),
            ("payments streamed", lambda t: f"{t.live / 1e6:,.2f} M"),
            ("hours running", lambda t: f"{t.hours:,.1f}"),
            ("payments a second (average)", lambda t: f"{t.rate:,.1f}"),
            ("Kinesis shards / Flink KPUs", lambda t: f"{t.shards} / {t.kpus}")]
    print(head)
    for label, fn in rows:
        print(f"  {label:<38}" + "".join(f"{fn(t):>{w}}" for t in targets))

    print("\nRun time of each job, from data size (seconds):")
    for name, j in base.jobs.items():
        size = "per payment that day" if j["driver"] == "day" else "per history payment"
        label = f"{name} ({j['fixed']:.0f} s + {j['per'] * 1e3:.3f} ms {size})"
        print(f"  {label:<70}" + "".join(f"{t.durations[name]:>{w},.0f}" for t in targets))
    for name, j in base.jobs.items():
        print(f"    {name}: {j['note']}")

    def money(x):
        return f"{'$' + format(x, ',.2f'):>{w}}"

    print("\nCost:")
    for group, title in GROUPS.items():
        print(f"  {title}")
        for l in (l for l in targets[0].lines if l["group"] == group):
            cells = [next(m for m in t.lines if (m["service"], m["item"]) == (l["service"], l["item"])) for t in targets]
            print(f"    {l['service'] + ': ' + l['item']:<66}" + "".join(money(cost(c)) for c in cells))
        print(f"    {'subtotal':<66}" + "".join(money(t.total(group)) for t in targets))
    print(f"  {'TOTAL':<68}" + "".join(money(t.total()) for t in targets))

    print("\nQuantities (cost = quantity x rate, rates from PRICES in cost.py):")
    for t in targets:
        print(f"  {t.name}")
        for l in t.lines:
            print(f"    {l['service'] + ': ' + l['item']:<58} {l['detail']:<26} {l['quantity']:>12,.4f}"
                  f" x ${rate(l['price']):<7g} = ${cost(l):,.4f}")
    for t in targets:
        for msg in t.warnings:
            print(f"! {t.name}: {msg}")
    print("\nAssumptions: payments spread evenly over the days streamed; Q2 time grows in a straight line with the"
          "\nday's payments; one training run per projection (no retraining); the apps server and Q2 cluster sizes"
          f"\nstay the same; each day up to {Q1_REVIEWS_PER_DAY} Q1 alerts and {Q2_REVIEWS_PER_DAY} Q2 customers are"
          f"\nreviewed, {MINUTES_PER_LOOKUP} min each, in {APP_HOURS_PER_DAY} h shifts; Metrics page not opened"
          "\n(APP_*, Q1_*, Q2_* constants).")


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--run", required=True, help="the file cost.py --save wrote")
    p.add_argument("--summary", default=SUMMARY, help="the measured run's dataset summary (producer prepare)")
    p.add_argument("--mode", choices=["live", "full", "month", "demo"], default="live")
    p.add_argument("--speed", type=float, default=60, help="replay speed for --mode demo")
    a = p.parse_args()
    with open(a.run) as fh:
        run = json.load(fh)
    with open(a.summary) as fh:
        summary = json.load(fh)
    if "facts" not in run:
        sys.exit(f"{a.run} has no run facts: save it again with the current cost.py.")
    base = Base(run, summary)
    # the measured dataset itself comes first, so the projection can be checked against the run
    sizes = sorted(summary["days"].values())
    busy = len([n for n in sizes if n >= sizes[len(sizes) // 2] / 10])
    targets = [Target("measured set", base.history, base.live, busy, base, a.mode, a.speed)]
    targets += [Target(name, h, n, days, base, a.mode, a.speed) for name, (h, n, days) in DATASETS.items()]
    report(targets, base, a.mode, a.speed)


if __name__ == "__main__":
    main()
