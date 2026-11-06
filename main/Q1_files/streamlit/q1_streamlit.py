"""Q1 investigator app (PIPELINE_SPEC.md section 3.2) - single-file Streamlit app.

Demo (no AWS needed). Run from this folder so the dark theme in .streamlit/config.toml applies:
    cd Q1_files/v2/streamlit
    streamlit run q1_streamlit.py

The demo replays demo_snapshot.json, which build_demo_snapshot.py makes from the real HI-Small
payments with the pipeline's own feature and scoring code. Nothing shown is invented: scores,
thresholds, reasons and account payments are what the model and pipeline produce. The demo clock
runs at one simulated minute per real second, starting 12 hours into the first live day.

Against AWS (after terraform apply, which writes main/config.json with the resource names):
    set Q1_MODE=aws            (Mac/Linux: export Q1_MODE=aws)
    streamlit run q1_streamlit.py

Each name in config.json can be overridden by an env var: AWS_REGION, S3_BUCKET, BANK_ID, ALERTS_TABLE,
ATHENA_DB, ATHENA_WORKGROUP, LIVE_TABLE (live_scored), REFRESH_SECONDS (3).

Time. Against AWS the app's "now" is the producer's simulated clock (state/sim_clock.json in the
bucket; see main/common/sim_clock.py), so the 7-day review window, "expires in" and the decision
times move with the replay. With no clock file the app uses the real time.

Where each thing on screen comes from (spec: "apps read DynamoDB; pipelines read S3"):
  Worklist, payment, score, risk factors, 7-day figures      the alert item Flink writes to DynamoDB
  Due in                                                     the item's expires_at (7 days after it was raised)
  Other alerts on the customer                               the same DynamoDB table
  Decision, reason code, comment                             conditional write to that item
  Customer's payments                                        Athena query on curated/live/scored/
"""
import html
import json
import os
import re
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import altair as alt
import numpy as np
import pandas as pd
import streamlit as st

DEMO = os.getenv("Q1_MODE", "demo").strip().lower() != "aws"
_CFG_FILE = Path(__file__).resolve().parents[2] / "config.json"     # written by terraform apply
_CFG = json.loads(_CFG_FILE.read_text()) if _CFG_FILE.exists() else {}
REGION = os.getenv("AWS_REGION", _CFG.get("region", "us-east-1"))
S3_BUCKET = os.getenv("S3_BUCKET", _CFG.get("bucket", ""))
BANK_ID = str(int(os.getenv("BANK_ID", _CFG.get("our_bank", "11"))))
ALERTS_TABLE = os.getenv("ALERTS_TABLE", _CFG.get("alerts_table", "aml-q1-alerts"))
ATHENA_DB = os.getenv("ATHENA_DB", _CFG.get("athena_db", "aml"))
ATHENA_WG = os.getenv("ATHENA_WORKGROUP", _CFG.get("athena_workgroup", "primary"))
LIVE_TABLE = os.getenv("LIVE_TABLE", "live_scored")       # the table over curated/live/scored/
LABELS_TABLE = os.getenv("LABELS_TABLE", "live_labels")   # the table over curated/live/labels/
REFRESH = int(os.getenv("REFRESH_SECONDS", "3"))

DAY = 86400
TTL = 7 * DAY                       # alerts and audit items both live 7 days
PAGE = 10                           # queue rows shown before "Show more"
OUTCOME = {"open": "Open", "confirmed": "Marked as suspicious", "dismissed": "Closed, not suspicious"}
KIND = {"Alerts": False, "Audit": True}        # view name -> is_audit
SORTS = {"Alerts": ["Highest risk", "Oldest first", "Newest first"], "Audit": ["Oldest first", "Newest first"]}
# Disposition reason codes, recorded with each decision.
REASONS = {
    "confirmed": ["Money passed straight through the account", "Many senders paying into one account",
                  "One account paying out to many receivers", "Money returned to where it came from",
                  "Activity out of line with the customer's history", "Other (explain in the comment)"],
    "dismissed": ["Consistent with the customer's usual activity", "Established counterparty",
                  "Routine business payments (payroll, suppliers, sales)",
                  "Transfer between the customer's own accounts", "Other (explain in the comment)"]}
SNAPSHOT = Path(__file__).parent / "demo_snapshot.json"
SPEED = 60                          # demo: simulated seconds per real second 1 second = 1 min, 1 min =1 hour in the demo
DEMO_HEAD_START = 12 * 3600         # demo starts this far into the first live day


# ============================================================== demo backend ====
class _Demo:
    """Plays demo_snapshot.json forward in time. Decisions are kept in memory."""

    def __init__(self):
        snap = json.loads(SNAPSHOT.read_text(encoding="utf-8"))
        self.model, self.activity = snap["model"], snap["activity"]
        self.traffic = snap.get("traffic", [])
        self.source = sorted(snap["items"], key=lambda it: it["txn_ts"])
        day0 = datetime.strptime(snap["live_start"], "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()
        self.sim0 = day0 + DEMO_HEAD_START
        self.lock = threading.Lock()
        self.reset()

    def reset(self):
        self.t0, self.items, self.next, self.expired = time.time(), {}, 0, 0

    def now(self):
        return self.sim0 + (time.time() - self.t0) * SPEED

    def tick(self):
        now = self.now()
        while self.next < len(self.source) and self.source[self.next]["txn_ts"] <= now:
            src = self.source[self.next]
            self.items[src["txn_id"]] = {**src, "status": "open", "reviewer": None, "note": "",
                                         "reason": "", "claimed_by": None, "created_at": src["txn_ts"], "decided_at": None,
                                         "expires_at": src["txn_ts"] + TTL}
            self.next += 1
        for tid in [k for k, it in self.items.items() if it["expires_at"] <= now]:
            self.expired += self.items[tid]["status"] == "open"
            del self.items[tid]

    def decide(self, tid, status, reviewer, note, expect, reason=""):
        it = self.items.get(tid)
        if it is None:
            return False, "This item has expired and can no longer be changed."
        if it["status"] != expect:
            return False, f"Already {OUTCOME[it['status']].lower()} by {it.get('reviewer') or 'someone else'}."
        if expect == "open" and it.get("claimed_by") not in (None, reviewer):
            return False, f"{it['claimed_by']} is reviewing this one. Only they can record the decision."
        it["claimed_by"] = None
        it.update(status=status, note=note, reason=reason, reviewer=None if status == "open" else reviewer,
                  decided_at=None if status == "open" else self.now())
        return True, ""


    def claim(self, tid, reviewer, take=True):
        it = self.items.get(tid)
        if it is None or it["status"] != "open":
            return False, "This item is no longer open."
        if it.get("claimed_by") not in (None, reviewer):
            return False, f"{it['claimed_by']} is already reviewing this one."
        it["claimed_by"] = reviewer if take else None
        return True, ""


@st.cache_resource(max_entries=1)
def _demo_for(version):
    return _Demo()


def _demo():
    return _demo_for((SNAPSHOT.stat().st_mtime, Path(__file__).stat().st_mtime))


# =============================================================== AWS backend ====
# Attribute and column names are this app's contract with the Flink job (q1_core.alert_item and
# q1_scoring.scored_columns); they match the item layout in demo_snapshot.json.
@st.cache_resource
def _aws():
    import boto3
    s = boto3.Session(region_name=REGION)
    return {"alerts": s.resource("dynamodb").Table(ALERTS_TABLE), "athena": s.client("athena"),
            "s3": s.client("s3"), "cloudwatch": s.client("cloudwatch"), "sfn": s.client("stepfunctions")}


_CLOCK = {"read_at": 0.0, "state": None}


def _sim_now():
    """The producer's simulated clock, or the real time when no replay is running."""
    if S3_BUCKET and time.time() - _CLOCK["read_at"] > 5:
        try:
            body = _aws()["s3"].get_object(Bucket=S3_BUCKET, Key="state/sim_clock.json")["Body"].read()
            _CLOCK["state"] = json.loads(body)
        except Exception:
            _CLOCK["state"] = None
        _CLOCK["read_at"] = time.time()
    c = _CLOCK["state"]
    if not c:
        return time.time()
    if not c.get("running"):
        return float(c["sim_anchor"])
    return float(c["sim_anchor"]) + (time.time() - float(c["wall_anchor"])) * float(c["speed"])


def _day(ts):
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d")


def _athena(sql):
    ath = _aws()["athena"]
    kw = {"QueryString": sql, "QueryExecutionContext": {"Database": ATHENA_DB}, "WorkGroup": ATHENA_WG}
    if S3_BUCKET:
        kw["ResultConfiguration"] = {"OutputLocation": f"s3://{S3_BUCKET}/athena-results/"}
    qid = ath.start_query_execution(**kw)["QueryExecutionId"]
    while True:
        s = ath.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]
        if s["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(0.4)
    if s["State"] != "SUCCEEDED":
        raise RuntimeError(f"Athena query {s['State'].lower()}: {s.get('StateChangeReason', '')}")
    rows = []
    for page in ath.get_paginator("get_query_results").paginate(QueryExecutionId=qid):
        rows += [[c.get("VarCharValue") for c in r["Data"]] for r in page["ResultSet"]["Rows"]]
    # dtype=object keeps Athena's empty values as None; pandas 3's string dtype would turn them into NaN, which is truthy
    return pd.DataFrame(rows[1:], columns=rows[0], dtype=object) if rows else pd.DataFrame()


def _plain(v):
    if hasattr(v, "is_finite"):                 # Decimal
        return float(v)
    if isinstance(v, dict):
        return {k: _plain(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [_plain(x) for x in v]
    return v


def _norm(raw):
    """DynamoDB item -> the dict shape the UI uses."""
    it = _plain(raw)
    it["reasons"] = list(it.get("reasons") or [])
    it["is_audit"] = bool(it.get("is_audit"))
    it.setdefault("status", "open")
    it.setdefault("created_at", time.time())
    it.setdefault("expires_at", it["created_at"] + TTL)
    for k in ("reviewer", "note", "reason", "decided_at", "from_bank", "to_bank", "model_version", "features",
              "claimed_by"):
        it.setdefault(k, None)
    return it


def _aws_items():
    table, items, kw = _aws()["alerts"], [], {}
    while True:
        r = table.scan(**kw)
        items += r["Items"]
        if "LastEvaluatedKey" not in r:
            break
        kw["ExclusiveStartKey"] = r["LastEvaluatedKey"]
    now = _sim_now()                       # past its 7-day window: hidden here, removed by the label export
    return [it for it in map(_norm, items) if it["expires_at"] > now]


def _aws_decide(tid, status, reviewer, note, expect, reason=""):
    from botocore.exceptions import ClientError
    try:
        mine = " AND (attribute_not_exists(claimed_by) OR claimed_by = :r)" if expect == "open" else ""
        t = int(_sim_now())                # the outcome is final once the 7-day window has closed
        _aws()["alerts"].update_item(
            Key={"txn_id": tid}, ConditionExpression="#s = :f AND expires_at > :t" + mine,
            UpdateExpression="SET #s = :s, reviewer = :r, decided_at = :t, note = :n, reason = :q REMOVE claimed_by",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":s": status, ":f": expect, ":r": reviewer, ":n": note, ":q": reason, ":t": t})
        return True, ""
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return False, ("Someone else already changed this item, or its review window has closed. "
                           "The queue has the latest state.")
        raise


def _aws_claim(tid, reviewer, take=True):
    from botocore.exceptions import ClientError
    try:
        _aws()["alerts"].update_item(
            Key={"txn_id": tid}, ExpressionAttributeNames={"#s": "status"},
            ConditionExpression="#s = :o AND (attribute_not_exists(claimed_by) OR claimed_by = :r)",
            UpdateExpression="SET claimed_by = :r" if take else "REMOVE claimed_by",
            ExpressionAttributeValues={":o": "open", ":r": reviewer})
        return True, ""
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return False, "Someone else is already reviewing this one, or it is no longer open."
        raise


@st.cache_data(ttl=300, show_spinner=False)
def _aws_activity(bank, account, until_ts):
    if not re.fullmatch(r"[0-9A-Za-z]+", account) or not bank.isdigit():
        raise ValueError("Unexpected account id")
    mine_to = f"(to_account = '{account}' AND CAST(to_bank AS integer) = {int(bank)})"
    mine_from = f"(from_account = '{account}' AND CAST(from_bank AS integer) = {int(bank)})"
    df = _athena(f"""
        SELECT to_unixtime(txn_ts) AS ts, 'in' AS direction,
               concat(cast(from_bank AS varchar), '_', from_account) AS counterparty,
               amount_received AS amount, receiving_currency AS currency, payment_format AS format
        FROM {LIVE_TABLE} WHERE {mine_to} AND date BETWEEN '{_day(until_ts - 30 * DAY)}' AND '{_day(until_ts)}'
          AND txn_ts BETWEEN from_unixtime({until_ts - 30 * DAY}) AND from_unixtime({until_ts})
        UNION ALL
        SELECT to_unixtime(txn_ts), 'out', concat(cast(to_bank AS varchar), '_', to_account),
               amount_paid, payment_currency, payment_format
        FROM {LIVE_TABLE} WHERE {mine_from} AND date BETWEEN '{_day(until_ts - 30 * DAY)}' AND '{_day(until_ts)}'
          AND txn_ts BETWEEN from_unixtime({until_ts - 30 * DAY}) AND from_unixtime({until_ts})""")
    return df.astype({"ts": float, "amount": float}) if len(df) else df


@st.cache_data(ttl=60, show_spinner=False)
def _aws_traffic(first_day):
    df = _athena(f"""
        SELECT to_unixtime(date_trunc('hour', txn_ts)) AS hour, count(DISTINCT txn_id) AS payments,
               count(DISTINCT CASE WHEN is_alert THEN txn_id END) AS alerts
        FROM {LIVE_TABLE} WHERE date >= '{first_day}' GROUP BY 1 ORDER BY 1""")
    return df.astype(float) if len(df) else pd.DataFrame(columns=["hour", "payments", "alerts"])


@st.cache_data(ttl=300, show_spinner=False)
def _aws_expired(first_day):
    df = _athena(f"SELECT count(DISTINCT txn_id) FROM {LABELS_TABLE} "
                 f"WHERE export_date >= '{first_day}' AND outcome = 'expired'")
    return int(df.iloc[0, 0] or 0) if len(df) else 0


@st.cache_data(ttl=60, show_spinner=False)
def _aws_pipeline(day):
    """Spec 3.4 metrics, in the order of _PIPELINE. Anything that cannot be read shows as a dash."""
    from datetime import timedelta
    aws, end = _aws(), datetime.now(timezone.utc)
    stream = _CFG.get("stream", "aml-payments")

    def cw(namespace, metric, dims, stat):
        pts = aws["cloudwatch"].get_metric_statistics(
            Namespace=namespace, MetricName=metric, Dimensions=[{"Name": k, "Value": v} for k, v in dims.items()],
            StartTime=end - timedelta(minutes=10), EndTime=end, Period=60, Statistics=[stat])["Datapoints"]
        return [p[stat] for p in sorted(pts, key=lambda p: p["Timestamp"])]

    def lag():
        v = cw("AWS/Kinesis", "GetRecords.IteratorAgeMilliseconds", {"StreamName": stream}, "Maximum")
        return f"{max(v) / 1000:.1f} s behind (worst of the last 10 minutes)" if v else "No reads in the last 10 minutes"

    def throughput():
        v = cw("AWS/Kinesis", "IncomingRecords", {"StreamName": stream}, "Sum")
        return f"{v[-1] / 60:.1f} payments a second (latest minute)" if v else "Nothing arriving"

    def checkpoint():
        v = cw("AWS/KinesisAnalytics", "lastCheckpointDuration", {"Application": _CFG.get("flink_app", "")}, "Maximum")
        return f"{v[-1] / 1000:.1f} s" if v else "-"

    def time_to_flag():
        lag_ms = "date_diff('millisecond', arrived_at, scored_at)"
        df = _athena(f"SELECT approx_percentile({lag_ms}, 0.5), approx_percentile({lag_ms}, 0.99) "
                     f"FROM {LIVE_TABLE} WHERE date = '{day}'")
        if not len(df) or df.iloc[0, 0] is None:
            return "No payments scored for this day yet"
        return (f"median {float(df.iloc[0, 0]) / 1000:.2f} s, slowest 1% {float(df.iloc[0, 1]) / 1000:.2f} s "
                f"(payments dated {day})")

    def retrain():
        runs = aws["sfn"].list_executions(stateMachineArn=_CFG["q1_state_machine_arn"], statusFilter="SUCCEEDED",
                                          maxResults=1)["executions"]
        return f"{(runs[0]['stopDate'] - runs[0]['startDate']).total_seconds() / 60:.0f} min" if runs else "No run yet"

    out = []
    for fn in (lag, throughput, lambda: "See the Flink dashboard (Managed Flink console)", checkpoint,
               time_to_flag, retrain):
        try:
            out.append(fn())
        except Exception:
            out.append("-")
    return out


# =========================================================== backend facade ====
def now():
    return _demo().now() if DEMO else _sim_now()


def load_items():
    """Everything currently in the alerts table: open and decided, alerts and audit items."""
    if not DEMO:
        return _aws_items()
    d = _demo()
    with d.lock:
        d.tick()
        return list(d.items.values())


def get_item(tid):
    if DEMO:
        return _demo().items.get(tid)
    return next((it for it in load_items() if it["txn_id"] == tid), None)


def decide(tid, status, reviewer, note="", expect="open", reason=""):
    """Conditional write: only succeeds if the item is still in the state the reviewer saw."""
    if not DEMO:
        return _aws_decide(tid, status, reviewer, note, expect, reason)
    d = _demo()
    with d.lock:
        d.tick()
        return d.decide(tid, status, reviewer, note, expect, reason)


def claim(tid, reviewer, take=True):
    """Mark an open item as being reviewed by one investigator (or release it). Conditional write."""
    if not DEMO:
        return _aws_claim(tid, reviewer, take)
    d = _demo()
    with d.lock:
        return d.claim(tid, reviewer, take)


def traffic():
    """Payments scored and alerts raised per hour since the live stream started."""
    if DEMO:
        return pd.DataFrame(_demo().traffic, columns=["hour", "payments", "alerts"])
    return _aws_traffic(_day(now() - 8 * DAY))


def expired_count():
    """Alerts and audit items that reached 7 days with no decision."""
    return _demo().expired if DEMO else _aws_expired(_day(now() - 30 * DAY))


_ACT_COLS = ["ts", "direction", "counterparty", "amount", "currency", "format"]


def account_activity(item, side):
    """One of our customers' payments in and out, up to the payment under review."""
    bank, account = str(item[f"{side}_bank"]), item[f"{side}_account"]
    if not DEMO:
        return _aws_activity(bank, account, int(item["txn_ts"]))
    df = pd.DataFrame(_demo().activity.get(f"{bank}_{account}", []), columns=_ACT_COLS)
    return df[(df.ts <= item["txn_ts"]) & (df.ts > item["txn_ts"] - 30 * DAY)]


# ======================================================================= UI ====
BG, CARD, INK, MUTED, LINE = "#0E131C", "#161D2A", "#E4E9F2", "#93A0B5", "#273247"
RED, AMBER = "#E5544B", "#E0A030"
CSS = f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600&display=swap');
html, body, .stApp, .stApp button, .stApp input, .stApp textarea, .stApp label, .stApp p {{
  font-family: 'IBM Plex Sans', 'Segoe UI', system-ui, sans-serif; }}
.block-container {{ padding-top: 3.2rem; padding-bottom: 3rem; max-width: 1400px; }}
h1, h2, h3, h4 {{ color: {INK}; font-weight: 600; letter-spacing: -0.01em; }}
.head h1 {{ font-size: 1.6rem; margin: 0; padding: 0; }}
.head p {{ color: {MUTED}; margin: 2px 0 10px; max-width: 70ch; }}
.side b {{ display: block; font-size: 1.5rem; font-weight: 600; color: {INK}; line-height: 1.2; }}
.side span {{ color: {MUTED}; font-size: .86rem; }}
.side div {{ margin-bottom: 12px; }}

[class*="st-key-qrow_"], [class*="st-key-qsel_"], [class*="st-key-card_"] {{ background: {CARD}; }}
[class*="st-key-qrow_"], [class*="st-key-qsel_"] {{ padding: 10px 12px 10px 14px; border-radius: 8px;
  border: 1px solid {LINE}; border-left: 4px solid transparent; }}
[class*="st-key-qsel_"] {{ border-color: #8FB4FF; border-left-color: #8FB4FF; background: #1B2538; }}
.qr {{ display: flex; gap: 12px; align-items: stretch; }}
.qr-score {{ flex: 0 0 68px; border-radius: 6px; display: flex; flex-direction: column; align-items: center;
  justify-content: center; font-variant-numeric: tabular-nums; }}
.qr-score b {{ font-size: 1rem; font-weight: 600; color: {INK}; line-height: 1.15; }}
.qr-score span {{ font-size: .72rem; color: {MUTED}; }}
.qr-main {{ flex: 1; min-width: 0; }}
.qr-top {{ font-weight: 600; color: {INK}; font-variant-numeric: tabular-nums; white-space: nowrap;
  overflow: hidden; text-overflow: ellipsis; }}
.qr-top .to {{ color: {MUTED}; font-weight: 400; padding: 0 5px; }}
.qr-why {{ color: {MUTED}; font-size: .88rem; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; margin: 1px 0 7px; }}
.qr-why b {{ color: {INK}; font-weight: 500; font-variant-numeric: tabular-nums; margin-right: 6px; }}
.ttl {{ display: flex; align-items: center; gap: 10px; font-size: .8rem; color: {MUTED}; font-variant-numeric: tabular-nums; }}
.ttl .bar {{ flex: 1; height: 4px; background: #232D40; border-radius: 2px; overflow: hidden; }}
.ttl .bar i {{ display: block; height: 100%; background: #6F7F99; border-radius: 2px; }}
.ttl.soon .bar i {{ background: {AMBER}; }} .ttl.soon em {{ color: {AMBER}; font-weight: 600; }}
.ttl.last .bar i {{ background: {RED}; }} .ttl.last em {{ color: #FF8A82; font-weight: 600; }}
.ttl em {{ font-style: normal; white-space: nowrap; }}
.newtag {{ background: #8FB4FF; color: #0E131C; border-radius: 3px; font-size: .7rem; font-weight: 600; padding: 1px 6px; margin-left: 8px; }}

.flow {{ display: grid; grid-template-columns: 1fr auto 1fr; gap: 14px; align-items: center; }}
.party {{ border: 1px solid {LINE}; border-radius: 8px; padding: 10px 14px; background: #111826; }}
.party small {{ color: {MUTED}; display: block; font-size: .82rem; }}
.party b {{ font-size: 1.12rem; font-weight: 600; font-variant-numeric: tabular-nums; }}
.party .ours {{ color: #8FB4FF; font-weight: 600; }}
.hop {{ text-align: center; }}
.hop b {{ display: block; font-size: 1.3rem; font-weight: 600; font-variant-numeric: tabular-nums; white-space: nowrap; }}
.hop span {{ color: {MUTED}; font-size: .85rem; }}
.hop .line {{ height: 2px; background: {MUTED}; margin: 6px 0; position: relative; }}
.hop .line::after {{ content: ''; position: absolute; right: -1px; top: -4px; border-left: 8px solid {MUTED};
  border-top: 5px solid transparent; border-bottom: 5px solid transparent; }}
@media (max-width: 760px) {{ .flow {{ grid-template-columns: 1fr; }} }}
.facts {{ display: flex; flex-wrap: wrap; gap: 6px 28px; margin-top: 12px; font-size: .9rem; color: {MUTED}; }}
.facts b {{ color: {INK}; font-weight: 500; }}

.score {{ display: flex; align-items: baseline; gap: 16px; flex-wrap: wrap; }}
.score b {{ font-size: 1.9rem; font-weight: 600; font-variant-numeric: tabular-nums; }}
.score b.hit {{ color: #FF8A82; }}
.score span, .sub {{ color: {MUTED}; font-size: .9rem; }}
.why {{ padding: 7px 0 7px 12px; border-left: 3px solid #3A4967; margin: 6px 0; }}
table.seven {{ width: 100%; border-collapse: collapse; font-variant-numeric: tabular-nums; margin-top: 6px; }}
table.seven th {{ color: {MUTED}; font-weight: 400; font-size: .82rem; text-align: right; padding: 4px 8px; border: 0; }}
table.seven td {{ text-align: right; padding: 6px 8px; border: 0; border-top: 1px solid {LINE}; font-weight: 500; }}
table.seven th:first-child, table.seven td:first-child {{ text-align: left; padding-left: 0; }}
table.seven tr {{ border: 0; }}

.st-key-btn_confirm button {{ background: #C8392F; border-color: #C8392F; color: #fff; }}
.st-key-btn_confirm button:hover {{ background: #E5544B; border-color: #E5544B; color: #fff; }}
.st-key-undo {{ background: #1B2538; border-radius: 8px; padding: 8px 14px; }}
/* sidebar: navigation at the top, investigator name and clock pinned to the bottom */
[data-testid="stSidebarContent"] {{ position: relative; }}
[data-testid="stSidebarUserContent"], [data-testid="stSidebarUserContent"] div:has(> .st-key-side_bottom),
[data-testid="stSidebarUserContent"] > div {{ position: static; }}
.st-key-side_bottom {{ position: absolute; bottom: 1.25rem; left: 1.5rem; right: 1.5rem; width: auto; }}
.stApp button:focus-visible {{ outline: 2px solid #8FB4FF; outline-offset: 2px; }}
.pill {{ display: inline-block; padding: 2px 10px; border-radius: 4px; font-weight: 600; font-size: .82rem;
  margin: 0 6px 6px 0; font-variant-numeric: tabular-nums; }}
.p-risk {{ background: #C8392F; color: #fff; }}
.p-audit {{ background: #2A3550; color: {INK}; }}
.p-plain {{ background: #1F2838; color: {INK}; border: 1px solid {LINE}; font-weight: 500; }}
.p-urgent {{ background: {AMBER}; color: #1A1203; }}
.p-new {{ background: #8FB4FF; color: #0E131C; }}
.pay {{ font-size: 1.08rem; margin: 2px 0 4px; font-variant-numeric: tabular-nums; }}
.pay b {{ font-weight: 600; }}
.pay .to {{ color: {MUTED}; padding: 0 7px; }}
.pay .amt {{ font-weight: 600; margin: 0 8px 0 14px; }}
.pay span {{ color: {MUTED}; }}
.pay .amt {{ color: {INK}; }}
[class*="st-key-c_"] button {{ background: #C8392F; border-color: #C8392F; color: #fff; }}
[class*="st-key-c_"] button:hover {{ background: #E5544B; border-color: #E5544B; color: #fff; }}
.chip {{ display: inline-flex; align-items: center; gap: 7px; padding: 3px 11px 3px 9px; border-radius: 999px;
  font-weight: 500; font-size: .84rem; white-space: nowrap; }}
.chip i {{ width: 6px; height: 6px; border-radius: 50%; background: currentColor; }}
.c-high {{ background: rgba(229,84,75,.16); color: #FF8A82; }}
.c-medium {{ background: rgba(224,160,48,.16); color: #F0BC5E; }}
.c-low {{ background: rgba(147,160,181,.14); color: #B7C2D4; }}
.c-none {{ background: rgba(147,160,181,.10); color: {MUTED}; }}
.st-key-wl_table {{ background: {CARD}; border-radius: 12px; gap: 0; }}
.st-key-wl_table > div:first-child {{ margin-bottom: 10px; }}
.wl {{ display: grid; grid-template-columns: minmax(96px, 1fr) minmax(96px, 1fr) minmax(130px, 1.2fr) 110px 90px 100px;
  gap: 16px; align-items: center; font-variant-numeric: tabular-nums; }}
.wl > span {{ min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }}
.wl .r {{ text-align: right; }}
.wl .dim {{ color: {MUTED}; }}
.wl .ours {{ color: #8FB4FF; }}
.wl .risk {{ font-weight: 600; }}
.wl-head {{ color: {MUTED}; font-size: .74rem; letter-spacing: .08em; text-transform: uppercase; }}
.st-key-wl_table [data-testid="stHorizontalBlock"]:has(.wl) {{ border-top: 1px solid #1F283A; padding: 0 4px;
  min-height: 40px; align-items: center; }}
.st-key-wl_table [data-testid="stHorizontalBlock"]:has(.wl-head) {{ border-top: 1px solid {LINE};
  border-bottom: 1px solid {LINE}; }}
.st-key-wl_table [data-testid="stHorizontalBlock"]:has(.wl) button {{ min-height: 0; padding: 0;
  font-family: 'IBM Plex Mono', ui-monospace, Consolas, monospace; font-size: .9rem; color: {INK}; }}
/* whole row is the click target: the ID button's ::after is stretched over the row */
.st-key-wl_table [data-testid="stHorizontalBlock"]:has(.wl):not(:has(.wl-head)) {{ position: relative; cursor: pointer; }}
.st-key-wl_table [data-testid="stHorizontalBlock"]:has(.wl):not(:has(.wl-head)):hover {{ background: #1B2538; }}
.st-key-wl_table [data-testid="stHorizontalBlock"]:has(.wl):not(:has(.wl-head)) :is(div, button, span, p) {{ position: static !important; }}
.st-key-wl_table [data-testid="stHorizontalBlock"]:has(.wl):not(:has(.wl-head)) button::after {{ content: '';
  position: absolute; inset: 0; }}
.st-key-wl_table [data-testid="stHorizontalBlock"]:has(.wl):not(:has(.wl-head)):hover button {{ color: #8FB4FF; }}
.due {{ font-weight: 500; }} .due.soon {{ color: {AMBER}; font-weight: 600; }} .due.last {{ color: #FF8A82; font-weight: 600; }}
.stApp [data-testid="stBaseButton-primary"] {{ color: #0E131C; font-weight: 600; }}
.st-key-reviewer_bar {{ background: #1B2538; border-radius: 8px; padding: 8px 14px; margin-bottom: 4px; }}
.ahead {{ margin: 6px 0 4px; }}
.ahead > div:first-child {{ display: flex; align-items: center; gap: 14px; }}
.ahead h2 {{ margin: 0; padding: 0; font-size: 1.6rem; }}
.ahead .facts {{ margin-top: 6px; }}
</style>
"""


def esc(x):
    return html.escape(str(x if x is not None else ""))


def fmt_ts(ts):
    if ts is None or pd.isna(ts):
        return "-"
    return datetime.fromtimestamp(float(ts), timezone.utc).strftime("%d %b %Y, %H:%M")


def fmt_left(secs):
    secs = max(0, int(secs))
    d, h, m = secs // DAY, secs % DAY // 3600, secs % 3600 // 60
    return f"{d}d {h}h" if d else (f"{h}h {m:02d}m" if h else f"{m}m")


def fmt_score(s, compact=False):
    """Model probabilities are mostly very small, so show two significant figures."""
    s = float(s)
    if compact and s < 0.000001:
        return "~0"
    if s >= 0.1:
        return f"{s:.3f}" if s >= 0.995 else f"{s:.2f}"
    if s < 0.000001:
        return "under 0.000001"
    return np.format_float_positional(s, precision=2, unique=False, fractional=False, trim="-")


def fmt_risk(s):
    """The model's probability that the payment is laundering, as a percentage."""
    p = min(float(s) * 100, 99.9)
    if p >= 99:
        return f"{p:.1f}%"
    if p >= 10:
        return f"{p:.0f}%"
    if p >= 1:
        return f"{p:.1f}%"
    if p < 0.01:
        return "under 0.01%"
    return np.format_float_positional(p, precision=2, unique=False, fractional=False, trim="-") + "%"


def money(x, cur=""):
    return f"{float(x):,.2f} {cur}".strip()


def is_ours(bank):
    return bank is not None and str(bank).lstrip("0") == BANK_ID


def reviewer_name():
    return (st.session_state.get("reviewer") or "").strip()


# ------------------------------------------------------------ risk factors ----
# The feature list is fixed by the feature code, so each feature has one sentence. Retraining only
# changes which features are picked for a payment, never this wording.
_WHO = {"s": "sender", "r": "receiver"}
_FLAGS = {"new_pair": ("This is the first payment between these two accounts",
                       "These two accounts have paid each other before"),
          "round_amt": ("The amount is a round number", "The amount is not a round number"),
          "just_under_10k": ("The amount is just under 10,000", "The amount is not just under 10,000"),
          "cross_bank": ("The payment crosses between our bank and another bank",
                         "Both accounts are held at our bank"),
          "self_transfer": ("The sender and receiver are the same account",
                            "The sender and receiver are different accounts"),
          "fx_mismatch": ("The payment converts from one currency to another",
                          "The payment stays in one currency")}


def _ago(hours):
    if hours < 1:
        return f"{max(1, round(hours * 60))} minutes"
    if hours < 48:
        return f"{hours:.0f} hour{'s' if round(hours) != 1 else ''}"
    return f"{hours / 24:.0f} days"


def _window(w):
    days = int(w[:-1])
    return "the 24 hours" if days == 1 else f"the {days} days"


def phrase(reason, it):
    """One risk factor as a sentence. Accepts {feature, value}, or an already-written string."""
    if not isinstance(reason, dict):
        return str(reason)
    name, v = reason.get("feature", ""), reason.get("value")
    if name == "log_amt":
        return f"The size of the payment: {money(it['amount'], it['currency'])}"
    if name == "other_bank":
        return ("The payment comes from an account at another bank" if is_ours(it["to_bank"]) else
                "The payment goes to an account at another bank")
    if name in _FLAGS:
        return _FLAGS[name][0 if v == 1 else 1]
    if name == "s_hrs_since_in":
        return ("The sender had not received any money before this payment" if v is None else
                f"The sender received money {_ago(v)} before making this payment")
    if name == "amt_match":
        return ("The sender had not received any money before this payment" if v is None else
                f"This payment is {v:.0%} of the last amount the sender received" if v < 2 else
                f"This payment is {v:,.0f} times the last amount the sender received" if v < 1000 else
                "This payment is far larger than the last amount the sender received")
    if name == "s_pass_1D":
        return ("The sender is paying out without having received money in the last 24 hours" if not v else
                f"Counting this payment, the sender has paid out {v:.0%} of what it received in the last 24 hours"
                if v < 2 else
                f"Counting this payment, the sender has paid out {v:,.0f} times what it received in the last 24 hours"
                if v < 1000 else "The sender has paid out far more than it received in the last 24 hours")
    if name == "cyc2_hrs":
        return ("The receiver has not paid the sender before" if v is None else
                f"The receiver paid the sender {_ago(v)} earlier, so money is moving back the other way")
    parts = name.split("_")
    who = _WHO.get(parts[0], "account")
    if v is None:
        return f"The {who} is at another bank, so we cannot see its other payments"
    if name in ("s_burst", "r_burst"):
        act = "made" if who == "sender" else "received"
        return (f"The {who} {act} {v:.1f} times its usual daily number of payments in the last 24 hours"
                if v >= 1.05 else f"The {who} {act} no more payments than usual in the last 24 hours")
    if len(parts) == 4:
        _, direction, stat, w = parts
        verb, when = ("made" if direction == "out" else "received"), f"in {_window(w)} before this one"
        if stat == "n":
            return (f"The {who} {verb} no payments {when}" if v == 0 else
                    f"The {who} {verb} {v:,.0f} payment{'s' if v != 1 else ''} {when}")
        if stat == "amt":
            return (f"The {who} {'paid out' if direction == 'out' else 'received'} nothing {when}" if v == 0 else
                    f"The {who} {'paid out' if direction == 'out' else 'received'} {v:,.0f} in total {when}")
        if stat == "distinct":
            link = "paid" if direction == "out" else "was paid by"
            return (f"The {who} {link} no other accounts {when}" if v == 0 else
                    f"The {who} {link} {v:,.0f} different account{'s' if v != 1 else ''} {when}")
        if stat == "fresh":
            other = "accounts it had not paid recently" if direction == "out" else "accounts that had not paid it recently"
            return (f"None of the {who}'s payments {when} {'went to' if direction == 'out' else 'came from'} new accounts"
                    if v == 0 else
                    f"{v:,.0f} of the payments the {who} {verb} {when} {'went to' if direction == 'out' else 'came from'} {other}")
    return f"{name} = {v}"


def risk_factors(it):
    out = []
    for r in it.get("reasons") or []:
        text = phrase(r, it)
        if text not in out:
            out.append(text)
    return out


# ------------------------------------------------------------- worklist ----
def alert_id(it):
    return ("AUD-" if it["is_audit"] else "ALT-") + it["txn_id"][:8].upper()


def customer(it):
    """The account our bank holds, the other party, and the direction seen from our customer."""
    f_ours, t_ours = is_ours(it["from_bank"]), is_ours(it["to_bank"])
    if f_ours and t_ours:
        side, direction = "from", "Internal"
    elif t_ours:
        side, direction = "to", "Incoming"
    else:
        side, direction = "from", "Outgoing"
    other = "to" if side == "from" else "from"
    return {"side": side, "account": it[f"{side}_account"], "direction": direction,
            "cp_account": it[f"{other}_account"], "cp_bank": it[f"{other}_bank"],
            "cp_ours": is_ours(it[f"{other}_bank"])}


def _due(it, t):
    left = it["expires_at"] - t
    cls = "last" if left < 6 * 3600 else "soon" if left < DAY else ""
    return f"<span class='due {cls}'>{fmt_left(left)}</span>"


def _open_sorted(kind, items):
    rows = [it for it in items if it["status"] == "open" and it["is_audit"] == KIND[kind]]
    q = (st.session_state.get(f"find_{kind}") or "").strip().upper()
    if q:
        rows = [it for it in rows if q in it["from_account"].upper() or q in it["to_account"].upper()
                or q in alert_id(it)]
    order = st.session_state.get(f"sort_{kind}") or SORTS[kind][0]
    key = {"Highest risk": lambda it: -it["score"],
           "Oldest first": lambda it: it["created_at"], "Newest first": lambda it: -it["created_at"]}[order]
    return sorted(rows, key=key)


_HEAD = ("<div class='wl wl-head'><span>Sender</span><span>Receiver</span><span class='r'>Amount</span>"
         "<span>Flagged</span><span>Expires in</span><span class='r'>Model risk</span></div>")


def _acct(it, side):
    cls = " class='ours'" if is_ours(it[f"{side}_bank"]) else ""
    return f"<span{cls}>{esc(it[side + '_account'])}</span>"


def _row_html(it, t):
    ts = fmt_ts(it["created_at"])
    return (f"<div class='wl'>{_acct(it, 'from')}{_acct(it, 'to')}"
            f"<span class='r'>{money(it['amount'], esc(it['currency']))}</span>"
            + (f"<span class='ours'>In review: {esc(it['claimed_by'])}</span>" if it.get("claimed_by") else
               f"<span class='dim'>{ts[:6]}, {ts[-5:]}</span>") + f"<span>{_due(it, t)}</span>"
            f"<span class='r risk'>{fmt_risk(it['score'])}</span></div>")


@st.fragment(run_every=f"{REFRESH}s")
def worklist(kind):
    is_audit = KIND[kind]
    items, t = load_items(), now()
    mine = [it for it in items if it["is_audit"] == is_audit]
    open_ = [it for it in mine if it["status"] == "open"]
    c = st.columns(4)
    c[0].metric("Open", len(open_))
    if is_audit:
        c[1].metric("Expiring within 24 hours", sum(it["expires_at"] - t < DAY for it in open_))
        c[2].metric("Found suspicious", sum(it["status"] == "confirmed" for it in mine))
        c[3].metric("Found not suspicious", sum(it["status"] == "dismissed" for it in mine))
    else:
        c[1].metric("New in the last hour", sum(t - it["created_at"] < 3600 for it in open_))
        c[2].metric("Expiring within 24 hours", sum(it["expires_at"] - t < DAY for it in open_))
        c[3].metric("Reviewed, last 7 days", sum(it["status"] != "open" for it in mine))
    f = st.session_state.pop("flash", None)
    if f:
        getattr(st, f[0])(f[1])

    with st.container(border=True, key="wl_table"):
        f = st.columns([4.2, 2.4, 2.2], vertical_alignment="center")
        f[0].markdown(f"**Needs review** :gray[{len(open_)}]")
        f[1].selectbox("Sort by", SORTS[kind], key=f"sort_{kind}", label_visibility="collapsed",
                       format_func=lambda s: f"Sort: {s.lower()}")
        f[2].text_input("Search", key=f"find_{kind}", placeholder="Search account or alert ID",
                        label_visibility="collapsed")
        rows = _open_sorted(kind, items)
        st.session_state[f"order_{kind}"] = [it["txn_id"] for it in rows]
        if not rows:
            if len(open_):
                st.info("No open items match these filters.")
            elif is_audit:
                st.info("No audit items waiting. New ones are drawn at random from payments the model did not flag.")
            else:
                st.info("No open alerts. New ones appear here as payments are scored.")
            return
        shown = st.session_state.get(f"shown_{kind}", 15)
        a, b = st.columns([1.5, 8.5], vertical_alignment="center")
        a.html("<div class='wl-head wl-first'>" + ("Audit item" if is_audit else "Alert") + "</div>")
        b.html(_HEAD)
        for it in rows[:shown]:
            a, b = st.columns([1.5, 8.5], vertical_alignment="center")
            b.html(_row_html(it, t))
            if a.button(alert_id(it), type="tertiary", key=f"open_{it['txn_id']}"):
                st.session_state[f"open_{kind}"] = it["txn_id"]
                st.rerun()
        if len(rows) > shown:
            if st.button(f"Show more ({len(rows) - shown} left)", key=f"more_{kind}", type="tertiary"):
                st.session_state[f"shown_{kind}"] = shown + 15
                st.rerun(scope="fragment")
    st.caption("Model risk is the model's estimate of the chance the payment is laundering. "
               + ("The top 1% of payments by risk become alerts. " if not is_audit else "")
               + "Accounts in blue are our customers. Click a row to open it.")


# ----------------------------------------------------------- alert page ----
def _our_sides(it):
    sides = [s for s in ("from", "to") if is_ours(it[f"{s}_bank"])]
    same = it["from_account"] == it["to_account"] and it["from_bank"] == it["to_bank"]
    return sides[:1] if same else sides


def _party(label, bank, account):
    ours = is_ours(bank)
    where = "Our customer" if ours else (f"Bank {esc(bank)}" if bank is not None else "")
    return (f"<div class='party'><small>{label}</small><b>{esc(account)}</b>"
            f"<small class='{'ours' if ours else ''}'>{where}</small></div>")


def _transaction(it):
    fx = ""
    if it.get("receiving_currency") and it["receiving_currency"] != it["currency"]:
        fx = f"<span>Receiver gets <b>{money(it['amount_received'], esc(it['receiving_currency']))}</b></span>"
    st.html(f"""<div class="flow">{_party('Sender', it['from_bank'], it['from_account'])}
<div class="hop"><b>{money(it['amount'])}</b><div class="line"></div>
<span>{esc(it['currency'])} by {esc(it['payment_format'])}</span></div>
{_party('Receiver', it['to_bank'], it['to_account'])}</div>
<div class="facts"><span>Paid <b>{fmt_ts(it['txn_ts'])}</b></span>{fx}
<span>Payment reference <b>{esc(it['txn_id'][:16])}</b></span></div>""")


def _risk_factors(it):
    reasons = "".join(f"<div class='why'>{esc(r)}</div>" for r in risk_factors(it))
    if not reasons:
        reasons = "<div class='sub'>No single factor raised this payment's risk.</div>"
    lead = ("What counted most towards this payment's risk, even though it was not flagged."
            if it["is_audit"] else "The three things that raised this payment's risk the most.")
    st.html(f"<div class='sub' style='margin:-6px 0 8px'>{lead}</div>{reasons}")


def _activity(it):
    """The customer's 7-day figures (the model's features) and their payments (curated/live/)."""
    sides = _our_sides(it)
    labels = {"from": f"Sender {it['from_account']}", "to": f"Receiver {it['to_account']}"}
    side = sides[0]
    if len(sides) > 1:
        side = st.segmented_control("Account", sides, default=sides[0], format_func=labels.get,
                                    key=f"acct_{it['txn_id']}", label_visibility="collapsed") or sides[0]
    feats, p = it.get("features") or {}, "s" if side == "from" else "r"
    if feats.get(f"{p}_in_n_7D") is not None:
        rows = "".join(
            f"<tr><td>{name}</td>" + "".join(
                f"<td>{(feats.get(f'{p}_{d}_{k}_7D') or 0):,.0f}</td>" for k in ("n", "amt", "distinct"))
            + "</tr>" for d, name in (("in", "Received"), ("out", "Paid out")))
        st.html(f"<div class='sub'>Account {esc(it[side + '_account'])} in the 7 days before this payment</div>"
                f"<table class='seven'><tr><th></th><th>Payments</th><th>Total amount</th>"
                f"<th>Different accounts</th></tr>{rows}</table>")
    if not DEMO and not st.toggle("Load this account's payments", key=f"show_{it['txn_id']}"):
        return
    try:
        df = account_activity(it, side)
    except Exception as exc:
        st.error(f"Payments could not be loaded: {exc}")
        return
    if df.empty:
        st.caption("No payments found for this account in the last 30 days.")
        return
    pv = df.sort_values("ts", ascending=False)
    other = f"{it['to_bank']}_{it['to_account']}" if side == "from" else f"{it['from_bank']}_{it['from_account']}"
    this = (pv.ts == it["txn_ts"]) & (pv.counterparty == other) & (
        pv.direction == ("out" if side == "from" else "in"))
    st.dataframe(pd.DataFrame({
        "": np.where(this, "This one", ""), "Time": pv.ts.map(fmt_ts),
        "Direction": pv.direction.map({"in": "In", "out": "Out"}), "Counterparty": pv.counterparty,
        "Amount": pv.amount, "Currency": pv.currency, "Format": pv["format"]}),
        hide_index=True, width="stretch", height=min(330, 40 + 35 * len(pv)),
        alt="This account's payments up to the alerted payment, newest first",
        column_config={"Amount": st.column_config.NumberColumn(format="accounting")})
    st.caption(f"{len(pv)} payments up to this one, newest first. Counterparties are shown as bank_account.")


def _flow_nodes(df, keep):
    """Per counterparty: payments, total and currency. The 8 largest, always including `keep`."""
    g = df.groupby("counterparty").agg(n=("amount", "size"), amt=("amount", "sum"), curs=("currency", "nunique"),
                                       cur=("currency", "first")).sort_values("amt", ascending=False)
    top = g.head(8)
    if keep in g.index and keep not in top.index:
        top = pd.concat([g.head(7), g.loc[[keep]]])
    return top, len(g)


def _flow_label(n, amt, curs, cur, noun="payment"):
    return f"{n:,} {noun}{'' if n == 1 else 's'} · " + (money(amt, cur) if curs == 1 else "mixed currencies")


_FLOW_SPANS = {"Last 48 hours": 2, "Last 72 hours": 3, "Last 7 days": 7}


def _flow(it):
    """Who paid this account and who it paid, in the days up to the alerted payment (same data as above)."""
    tid, sides = it["txn_id"], _our_sides(it)
    side = st.session_state.get(f"acct_{tid}")
    side = side if side in sides else sides[0]
    if not DEMO and not st.session_state.get(f"show_{tid}"):
        st.caption("Turn on \"Load this account's payments\" above to see the flow.")
        return
    try:
        df = account_activity(it, side)
    except Exception as exc:
        st.error(f"Payments could not be loaded: {exc}")
        return
    span = st.columns([1, 3])[0].selectbox("Period", list(_FLOW_SPANS), key=f"span_{tid}",
                                           label_visibility="collapsed")
    df = df[df.ts >= it["txn_ts"] - _FLOW_SPANS[span] * DAY]
    if df.empty:
        st.caption(f"No payments on this account in the {span.lower()} before this payment.")
        return
    me = f"{it[side + '_bank']}_{it[side + '_account']}"
    other = f"{it['to_bank']}_{it['to_account']}" if side == "from" else f"{it['from_bank']}_{it['from_account']}"
    hot = "out" if side == "from" else "in"
    loops = df[(df.counterparty == me) & (df.direction == "out")]   # self-transfers are in the data twice
    df = df[df.counterparty != me]
    cols = {d: _flow_nodes(df[df.direction == d], other if d == hot else None) for d in ("in", "out")}
    both = set(cols["in"][0].index) | set(cols["out"][0].index)
    both &= set(df[df.direction == "in"].counterparty) & set(df[df.direction == "out"].counterparty)

    top = 30 + (46 if len(loops) else 0)
    rows = max(len(nodes) + (total > len(nodes)) for nodes, total in cols.values())
    body = max(rows * 46, 52)
    cy, height = top + body / 2, top + body + 6
    biggest = max([nodes.amt.max() for nodes, _ in cols.values() if len(nodes)] + [0])
    text = "font-family='Segoe UI, system-ui, sans-serif' font-size='11'"
    edges, boxes = [], []
    for d, x in (("in", 0), ("out", 650)):
        nodes, total = cols[d]
        boxes.append(f"<text x='{x}' y='14' {text} fill='{MUTED}' letter-spacing='.08em'>"
                     f"{'PAID IN' if d == 'in' else 'PAID OUT'}</text>")
        for i, (cp, r) in enumerate(nodes.iterrows()):
            y = top + i * 46 + 23
            alerted = d == hot and cp == other
            width = 1.5 + (8.5 * np.log1p(r.amt) / np.log1p(biggest) if biggest > 0 else 0)
            path = f"M250,{y} C300,{y} 300,{cy} 350,{cy}" if d == "in" else f"M550,{cy} C600,{cy} 600,{y} 650,{y}"
            edges.append(f"<path d='{path}' fill='none' stroke='{RED if alerted else '#4A5A78'}' "
                         f"stroke-width='{width:.1f}' stroke-opacity='{1 if alerted else .8}' stroke-linecap='round'/>")
            edge = RED if alerted else AMBER if cp in both else LINE
            dash = " stroke-dasharray='4 3'" if cp in both and not alerted else ""
            name = cp if len(cp) <= 24 else cp[:23] + "…"
            boxes.append(
                f"<rect x='{x}' y='{y - 18}' width='250' height='36' rx='6' fill='#111826' stroke='{edge}'{dash}/>"
                f"<text x='{x + 10}' y='{y - 3}' font-family='Segoe UI, system-ui, sans-serif' font-size='13' font-weight='600' "
                f"fill='{'#8FB4FF' if is_ours(cp.split('_')[0]) else INK}'>{esc(name)}</text>"
                f"<text x='{x + 10}' y='{y + 12}' {text} fill='{MUTED}'>"
                f"{esc(_flow_label(r.n, r.amt, r.curs, r.cur))}</text>")
            if alerted:
                boxes.append(f"<text x='{x + 240}' y='{y - 3}' text-anchor='end' {text} fill='#FF8A82'>"
                             f"This payment</text>")
            if cp in both:
                boxes.append(f"<text x='{x + 240}' y='{y + (12 if alerted else -3)}' text-anchor='end' {text} "
                             f"fill='{AMBER}'>Both sides</text>")
        if total > len(nodes):
            boxes.append(f"<text x='{x + 10}' y='{top + len(nodes) * 46 + 27}' {text} fill='{MUTED}'>"
                         f"+ {total - len(nodes)} more accounts with smaller totals</text>")
    if len(loops):
        ry = cy - 26
        edges.append(f"<path d='M415,{ry} C395,{ry - 42} 505,{ry - 42} 485,{ry}' fill='none' "
                     f"stroke='{RED if other == me else '#4A5A78'}' stroke-width='2' stroke-linecap='round'/>")
        boxes.append(f"<text x='450' y='{ry - 38}' text-anchor='middle' {text} fill='{MUTED}'>" + esc(_flow_label(
            len(loops), loops.amount.sum(), loops.currency.nunique(), loops.currency.iloc[0], "transfer")
            .replace(" · ", " to itself · ")) + "</text>")
    boxes.append(
        f"<rect x='350' y='{cy - 26}' width='200' height='52' rx='6' fill='#1B2538' stroke='#8FB4FF'/>"
        f"<text x='450' y='{cy - 3}' text-anchor='middle' font-family='Segoe UI, system-ui, sans-serif' font-size='13' font-weight='600' "
        f"fill='{INK}'>{esc(me)}</text>"
        f"<text x='450' y='{cy + 14}' text-anchor='middle' {text} fill='#8FB4FF'>Customer account</text>")
    # st.html strips inline SVG, so the drawing goes through st.image
    st.image(f"<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 900 {height}' width='900' height='{height}'>"
             + "".join(edges + boxes) + "</svg>", width="stretch")


def _related(it, items):
    mine = {f"{it[s + '_bank']}_{it[s + '_account']}" for s in _our_sides(it)}
    rel = [o for o in items if o["txn_id"] != it["txn_id"] and mine & {
        f"{o['from_bank']}_{o['from_account']}", f"{o['to_bank']}_{o['to_account']}"}]
    if not rel:
        st.html("<div class='sub'>No other alerts or audit items on this account in the last 7 days.</div>")
        return
    rel.sort(key=lambda o: -o["created_at"])
    st.dataframe(pd.DataFrame({
        "Alert": [alert_id(o) for o in rel], "Model risk": [fmt_risk(o["score"]) for o in rel],
        "Flagged": [fmt_ts(o["created_at"]) for o in rel],
        "From": [o["from_account"] for o in rel], "To": [o["to_account"] for o in rel],
        "Amount": [o["amount"] for o in rel], "Currency": [o["currency"] for o in rel],
        "Status": [OUTCOME[o["status"]] for o in rel]}),
        hide_index=True, width="stretch", height=min(260, 40 + 35 * len(rel)),
        alt="Other alerts and audit items on this customer",
        column_config={"Amount": st.column_config.NumberColumn(format="accounting")})


def _on_submit(kind, tid):
    who = reviewer_name()
    outcome = st.session_state.get(f"out_{tid}")
    reason = st.session_state.get(f"why_{tid}_{outcome}")
    note = (st.session_state.get(f"note_{tid}") or "").strip()
    problem = ("Enter your name in the sidebar first. It is recorded with every decision." if not who else
               "Choose a decision before submitting." if not outcome else
               "Choose a reason for the decision." if not reason else
               "Add a comment to explain the reason." if reason.startswith("Other") and not note else "")
    if problem:
        st.session_state["flash"] = ("warning", problem)
        return
    try:
        ok, msg = decide(tid, outcome, who, note, "open", reason)
    except Exception as exc:
        ok, msg = False, f"The decision was not saved: {exc}"
    if not ok:
        st.session_state["flash"] = ("error", msg)
        return
    order = st.session_state.get(f"order_{kind}", [])
    nxt = (order[order.index(tid) + 1:] + order[:order.index(tid)]) if tid in order else []
    st.session_state[f"order_{kind}"] = nxt
    st.session_state[f"open_{kind}"] = nxt[0] if nxt else None
    st.session_state["flash"] = ("success", f"{OUTCOME[outcome]}: {tid[:8].upper()}. "
                                 + ("Opened the next one in the queue." if nxt else "The queue is clear."))


def _go(kind, tid):
    st.session_state[f"open_{kind}"] = tid


def _decision(kind, it):
    tid, is_audit = it["txn_id"], it["is_audit"]
    if it.get("claimed_by") not in (None, reviewer_name()):
        st.info(f"{it['claimed_by']} is reviewing this one. Only they can record the decision.")
        return
    labels = {"confirmed": "Mark as suspicious", "dismissed": "Close as not suspicious"}
    outcome = st.radio("Decision", list(labels), format_func=labels.get, index=None, key=f"out_{tid}",
                       label_visibility="collapsed")
    if outcome:
        st.selectbox("Reason", REASONS[outcome], index=None, placeholder="Choose a reason",
                     key=f"why_{tid}_{outcome}")
    st.text_area("Comment", key=f"note_{tid}", height=90,
                 placeholder="What you checked and why you decided this")
    st.button("Submit and open next", key="btn_submit", type="primary", width="stretch",
              on_click=_on_submit, args=(kind, tid))


def _on_claim(tid, take):
    who = reviewer_name()
    if not who:
        st.session_state["flash"] = ("warning", "Enter your name in the sidebar first.")
        return
    try:
        ok, msg = claim(tid, who, take)
    except Exception as exc:
        ok, msg = False, f"Could not update who is reviewing: {exc}"
    if not ok:
        st.session_state["flash"] = ("error", msg)


def _reviewer_bar(it):
    """Who is working on this item, so two investigators don't review the same one."""
    by, tid = it.get("claimed_by"), it["txn_id"]
    with st.container(key="reviewer_bar"):
        a, b = st.columns([6, 1.6], vertical_alignment="center")
        if by and by == reviewer_name():
            a.markdown("**You are reviewing this one.** Other investigators see it as taken.")
            b.button("Stop reviewing", key="btn_release", on_click=_on_claim, args=(tid, False), width="stretch")
        elif by:
            a.markdown(f"**{by} is reviewing this one.** You can look, but only they can record the decision.")
        else:
            a.markdown("**No one is reviewing this one yet.** Take it so other investigators don't pick it up too.")
            b.button("Start review", key="btn_claim", on_click=_on_claim, args=(tid, True), width="stretch")


def alert_page(kind, it):
    tid, t, items = it["txn_id"], now(), load_items()
    order = st.session_state.get(f"order_{kind}", [])
    pos = order.index(tid) if tid in order else None
    n = st.columns([2, 5.4, 1.1, 1], vertical_alignment="center")
    n[0].button("Back to queue", icon=":material/arrow_back:", key="btn_back", on_click=_go, args=(kind, None),
                width="stretch")
    if pos is not None:
        n[1].caption(f"{pos + 1} of {len(order)} in the queue")
        n[2].button("Previous", key="btn_prev", disabled=pos == 0, width="stretch",
                    on_click=_go, args=(kind, order[pos - 1] if pos else None))
        n[3].button("Next", key="btn_next", disabled=pos >= len(order) - 1, width="stretch",
                    on_click=_go, args=(kind, order[pos + 1] if pos < len(order) - 1 else None))
    f = st.session_state.pop("flash", None)
    if f:
        getattr(st, f[0])(f[1])
    c = customer(it)
    st.html(f"""<div class="ahead"><div><h2>{alert_id(it)}</h2><span class="chip c-low">Model risk {fmt_risk(it['score'])}</span></div>
<div class="facts"><span>{c['direction']} payment on customer account <b>{esc(c['account'])}</b></span>
<span>Flagged <b>{fmt_ts(it['created_at'])}</b></span><span>Expires in <b>{_due(it, t)}</b></span></div></div>""")
    _reviewer_bar(it)
    left, right = st.columns([7, 3.4], gap="medium")
    with left:
        with st.container(border=True, key=f"card_txn_{tid}"):
            st.markdown("##### Alerted payment")
            _transaction(it)
        with st.container(border=True, key=f"card_why_{tid}"):
            st.markdown("##### Risk factors" if not it["is_audit"] else "##### What the model saw")
            _risk_factors(it)
        with st.container(border=True, key=f"card_act_{tid}"):
            st.markdown("##### Account activity")
            _activity(it)
        with st.container(border=True, key=f"card_flow_{tid}"):
            st.markdown("##### Money flow around this payment")
            _flow(it)
        with st.container(border=True, key=f"card_rel_{tid}"):
            st.markdown("##### Other alerts on this account")
            _related(it, items)
    with right:
        with st.container(border=True, key=f"card_dec_{tid}"):
            st.markdown("##### Decision")
            _decision(kind, it)


def board(kind):
    tid = st.session_state.get(f"open_{kind}")
    it = get_item(tid) if tid else None
    if it is not None and it["status"] == "open":
        alert_page(kind, it)
        return
    st.session_state[f"open_{kind}"] = None
    st.title("Alert queue" if kind == "Alerts" else "Audit sample")
    if kind == "Audit":
        st.caption("A random sample of payments the model did not flag. Reviewing them catches what the "
                   "model missed and measures how often it misses.")
    worklist(kind)


# -------------------------------------------------------------- reviewed ----
def _on_change(tid, status, expect):
    who = reviewer_name()
    if not who:
        st.session_state["flash"] = ("warning", "Enter your name in the sidebar first.")
        return
    ok, msg = decide(tid, status, who, "", expect, "Reopened" if status == "open" else "Decision changed")
    if not ok:
        st.session_state["flash"] = ("error", msg)
    elif status == "open":          # reopening takes you straight to that item's page
        it = get_item(tid)
        kind = "Audit" if it and it["is_audit"] else "Alerts"
        st.session_state[f"open_{kind}"] = tid
        st.session_state["goto"] = kind


def reviewed():
    st.title("Reviewed")
    f = st.session_state.pop("flash", None)
    if f:
        getattr(st, f[0])(f[1])
    done = sorted((it for it in load_items() if it["status"] != "open"),
                  key=lambda it: -(it["decided_at"] or 0))
    if not done:
        st.info("No decisions yet. Review an alert from the queue to record one.")
        return
    c = st.columns(4)
    n_alert = [it for it in done if not it["is_audit"]]
    n_esc = sum(it["status"] == "confirmed" for it in n_alert)
    c[0].metric("Reviewed, last 7 days", len(done))
    c[1].metric("Alerts marked suspicious", n_esc)
    c[2].metric("Alerts closed", len(n_alert) - n_esc)
    c[3].metric("Audit items found suspicious", sum(it["status"] == "confirmed" and it["is_audit"] for it in done))
    ev = st.dataframe(pd.DataFrame({
        "Alert": [alert_id(it) for it in done], "Decision": [OUTCOME[it["status"]] for it in done],
        "Reason": [it.get("reason") or "" for it in done], "Decided": [fmt_ts(it["decided_at"]) for it in done],
        "By": [it["reviewer"] or "-" for it in done],
        "Model risk": [fmt_risk(it["score"]) for it in done],
        "From": [it["from_account"] for it in done], "To": [it["to_account"] for it in done],
        "Amount": [it["amount"] for it in done], "Currency": [it["currency"] for it in done],
        "Comment": [it["note"] or "" for it in done]}),
        hide_index=True, width="stretch", on_select="rerun", selection_mode="single-row", key="reviewed_table",
        alt="Decisions recorded in the last 7 days",
        column_config={"Amount": st.column_config.NumberColumn(format="accounting")})
    st.caption("Decisions from the last 7 days. Select a row to change one.")
    if ev.selection.rows and ev.selection.rows[0] < len(done):
        it = done[ev.selection.rows[0]]
        other = "dismissed" if it["status"] == "confirmed" else "confirmed"
        b = st.columns([2, 1.6, 4])
        b[0].button(f"Change to: {OUTCOME[other].lower()}", key="re_flip", width="stretch",
                    on_click=_on_change, args=(it["txn_id"], other, it["status"]))
        b[1].button("Reopen for review", key="re_open", width="stretch",
                    on_click=_on_change, args=(it["txn_id"], "open", it["status"]))


# --------------------------------------------------------------- metrics ----
_PIPELINE = [
    ("Stream lag", "How far Flink is behind the newest payment in Kinesis (iterator age)", "CloudWatch, Kinesis"),
    ("Throughput", "Payments arriving per second (incoming records)", "CloudWatch, Kinesis"),
    ("Backpressure", "Share of time Flink operators are blocked waiting on a slower step", "Flink dashboard"),
    ("Checkpoint duration", "How long Flink takes to save its state", "CloudWatch, Managed Flink"),
    ("Time to flag", "Median and slowest 1% from a payment arriving to its score being written",
     "Athena on curated/live/scored/"),
    ("Retrain duration", "How long the last training run took", "Step Functions run history")]


def _hour_chart(df, column, title, colour, day):
    """One day, midnight to midnight: each bar spans its hour, and the axis ends at 00:00 the next day."""
    start, end = _day_label(day), _day_label(day + DAY)
    label = (f"datum.value == 0 ? '{start} 00:00' : datum.value == 24 ? '{end} 00:00' : "
             "(datum.value < 10 ? '0' : '') + datum.value + ':00'")
    return alt.Chart(df).mark_bar(color=colour, cornerRadiusEnd=3).encode(
        x=alt.X("k:Q", title=None, scale=alt.Scale(domain=[0, 24], nice=False),
                axis=alt.Axis(values=list(range(0, 25, 3)), labelExpr=label, labelColor=MUTED, grid=False,
                              domain=False, ticks=False, labelPadding=8, labelFlush=True)),
        x2="k2:Q",
        y=alt.Y(f"{column}:Q", title=title, scale=alt.Scale(zero=True),
                axis=alt.Axis(gridColor="#1F283A", labelColor=MUTED, titleColor=MUTED, domain=False, ticks=False,
                              tickMinStep=1)),
        y2=alt.datum(0),
        tooltip=[alt.Tooltip("span:N", title="Hour"), alt.Tooltip(f"{column}:Q", title=title, format=",")]
    ).properties(height=220).configure(font="IBM Plex Sans", background="transparent").configure_view(strokeWidth=0)


def _day_label(day):
    return datetime.fromtimestamp(day, timezone.utc).strftime("%d %b")


@st.fragment(run_every="20s")
def metrics():
    t, items = now(), load_items()
    try:
        df = traffic()
    except Exception as exc:
        st.error(f"Traffic could not be loaded: {exc}")
        df = pd.DataFrame(columns=["hour", "payments", "alerts"])
    df = df[df.hour <= t]
    today = t - t % DAY
    days = sorted({h - h % DAY for h in df.hour} | {today})
    day = st.segmented_control("Day", days, default=today, format_func=_day_label, key="metrics_day") or today
    if day not in days:
        day = today
    # 24 slots, 00:00 to 23:00; hours that have not happened yet stay empty
    grid = pd.DataFrame({"hour": [day + 3600 * k for k in range(24)]}).merge(df, on="hour", how="left")
    grid["label"] = [f"{k:02d}" for k in range(24)]
    grid["k"], grid["k2"] = [k + 0.08 for k in range(24)], [k + 0.92 for k in range(24)]
    grid["span"] = [f"{k:02d}:00 to {k:02d}:59" for k in range(24)]
    seen = grid.hour <= t
    grid.loc[seen, ["payments", "alerts"]] = grid.loc[seen, ["payments", "alerts"]].fillna(0)
    n_pay, n_alert = int(grid.payments.sum()), int(grid.alerts.sum())
    busiest = grid.loc[grid.payments.idxmax()] if n_pay else None
    c = st.columns(4)
    c[0].metric("Payments scored", f"{n_pay:,}")
    c[1].metric("Busiest hour", f"{busiest.label}:00" if busiest is not None else "-",
                help=f"{int(busiest.payments):,} payments" if busiest is not None else None)
    c[2].metric("Alerts raised", f"{n_alert:,}")
    c[3].metric("Share of payments flagged", f"{n_alert / n_pay:.1%}" if n_pay else "-")
    st.markdown("##### Payments scored each hour")
    st.altair_chart(_hour_chart(grid, "payments", "Payments", "#3987e5", day), width="stretch")
    st.markdown("##### Alerts raised each hour")
    st.altair_chart(_hour_chart(grid, "alerts", "Alerts", "#d95926", day), width="stretch")
    st.caption(("Bars fill in as the day goes on. " if day == today else "")
               + "The model is set to flag about 1% of payments.")

    st.markdown("##### Review work")
    done = [it for it in items if it["status"] != "open"]
    alerts_done = [it for it in done if not it["is_audit"]]
    c = st.columns(4)
    c[0].metric("Reviewed, last 7 days", len(done))
    c[1].metric("Alerts marked suspicious", sum(it["status"] == "confirmed" for it in alerts_done))
    c[2].metric("Audit items found suspicious", sum(it["status"] == "confirmed" and it["is_audit"] for it in done))
    c[3].metric("Expired unreviewed", expired_count(),
                help="Alerts and audit items that reached 7 days with no decision.")

    st.markdown("##### Stream and model pipeline")
    st.dataframe(pd.DataFrame({
        "Metric": [m for m, _, _ in _PIPELINE], "What it tells you": [w for _, w, _ in _PIPELINE],
        "Source": [s for _, _, s in _PIPELINE],
        "Now": ["Needs the live pipeline"] * len(_PIPELINE) if DEMO else _aws_pipeline(_day(t))}),
        hide_index=True, width="stretch", alt="Pipeline health metrics and where each one comes from")
    if DEMO:
        st.caption("The demo replays scored payments from a file, so there is no Kinesis stream or Flink job "
                   "to measure. These fill in when the app runs against the AWS pipeline.")


def page_metrics():
    st.title("Metrics")
    _safe(metrics)
    if DEMO:
        st.markdown("##### Demo")
        a, b = st.columns([3, 1], vertical_alignment="center")
        a.caption("The demo replays real HI-Small payments for bank 11. One real second is one simulated "
                  "minute, starting at 12:00 on the first live day.")
        if b.button("Restart the demo", width="stretch"):
            with _demo().lock:
                _demo().reset()
            for k in [k for k in st.session_state if k.startswith(("open_", "order_"))]:
                del st.session_state[k]
            st.rerun()


# ----------------------------------------------------------------- main ----
def _safe(body, *args):
    try:
        body(*args)
    except Exception as exc:
        if DEMO:
            raise
        st.error(f"Could not reach AWS: {exc}")


def page_alerts():
    _safe(board, "Alerts")


def page_audit():
    _safe(board, "Audit")


def page_reviewed():
    _safe(reviewed)


@st.fragment(run_every="20s")
def side_clock():
    t = now()
    simulated = DEMO or bool(_CLOCK["state"])
    st.caption(("Simulated time " if simulated else "") + (fmt_ts(t - t % 3600) if DEMO else fmt_ts(t))
               + ("" if simulated else " UTC"))


def main():
    st.set_page_config(page_title="Laundering alert review", page_icon=":material/account_balance:", layout="wide")
    st.html(CSS)
    if DEMO and not SNAPSHOT.exists():
        st.error("demo_snapshot.json is missing. Build it with: python build_demo_snapshot.py")
        return
    pages = {"Alerts": st.Page(page_alerts, title="Alert queue", icon=":material/notifications_active:",
                               url_path="alerts", default=True),
             "Audit": st.Page(page_audit, title="Audit sample", icon=":material/fact_check:", url_path="audit"),
             "Reviewed": st.Page(page_reviewed, title="Reviewed", icon=":material/task_alt:", url_path="reviewed"),
             "Metrics": st.Page(page_metrics, title="Metrics", icon=":material/monitoring:", url_path="metrics")}
    page = st.navigation(list(pages.values()))
    goto = st.session_state.pop("goto", None)
    if goto:
        st.switch_page(pages[goto])
    with st.sidebar:
        with st.container(key="side_bottom"):
            st.text_input("Investigator", key="reviewer", value="Demo investigator" if DEMO else "",
                          help="Recorded with every decision you make.")
            side_clock()
    page.run()


def _in_streamlit():
    try:
        from streamlit.runtime.scriptrunner import get_script_run_ctx
        return get_script_run_ctx() is not None
    except Exception:
        return False


if __name__ == "__main__":
    if _in_streamlit():
        main()
    else:
        print("This is a Streamlit app. Start it with:\n  streamlit run q1_streamlit.py")
