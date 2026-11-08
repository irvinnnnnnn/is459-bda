"""Q2 KYC app (PIPELINE_SPEC.md section 4): which customers to review first for enhanced due diligence.

Run from this folder so the dark theme in .streamlit/config.toml applies:
    cd Q2_files/v2/streamlit
    streamlit run q2_streamlit.py

Three views, as in the spec (4.3):
  Priority queue    customers the nightly run put in the queue, ranked, with why each is there
  Customer page     rules fired with evidence, the customer's accounts combined, score history,
                    transactions, and the decision (claim, EDD needed, cleared until a date)
  Overview          workload, which rules fire most, score distribution, rule precision

Demo data: demo_snapshot.json, built by build_demo_snapshot.py from the real HI-Small payments with
the spec's rules R1 to R11. The demo backend below stands in for the parts of the pipeline that
depend on analyst decisions: the DynamoDB decisions table, and the end of the nightly EMR job
(R12, tiers, in_queue, queue_reason). "Run the next night" on the Overview page runs that job for
the next date using the decisions made in the app.

Against AWS (after terraform apply, which writes main/config.json with the resource names):
    set Q2_MODE=aws            (Mac/Linux: export Q2_MODE=aws)
    streamlit run q2_streamlit.py

Where each thing comes from against AWS (spec 4.3: the app reads through Athena and writes decisions
straight to S3):
  queue, scores, rules fired, rule precision   Athena on scores/ (written by the nightly EMR job)
  customer transactions                        Athena on curated/customer_txns/, on demand
  claims and decisions                         events in decisions/, one small Parquet file each, written
                                               by this app and read back through Athena
Decision times come from the producer's simulated clock (state/sim_clock.json) when a replay is
running, so they line up with the payments on screen.
"""
import html
import io
import json
import math
import os
import re
import threading
import time
import uuid
from collections.abc import Mapping
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

DEMO = os.getenv("Q2_MODE", "demo").strip().lower() != "aws"
_CFG_FILE = Path(__file__).resolve().parents[2] / "config.json"     # written by terraform apply
_CFG = json.loads(_CFG_FILE.read_text()) if _CFG_FILE.exists() else {}
REGION = os.getenv("AWS_REGION", _CFG.get("region", "us-east-1"))
S3_BUCKET = os.getenv("S3_BUCKET", _CFG.get("bucket", ""))
BANK_ID = str(int(os.getenv("BANK_ID", _CFG.get("our_bank", "11"))))
ATHENA_DB = os.getenv("ATHENA_DB", _CFG.get("athena_db", "aml"))
ATHENA_WG = os.getenv("ATHENA_WORKGROUP", _CFG.get("athena_workgroup", "primary"))
SNAPSHOT = Path(__file__).parent / "demo_snapshot.json"
HIGH_PCT, MEDIUM_PCT = 0.01, 0.10            # tiers: top 1% High, next 9% Medium (spec 4.4)
SCORE_JUMP = 20                              # a cleared customer returns if the score rises this much (4.5)
CLEAR_DAYS = [7, 30, 90]                     # clearance periods an analyst can choose (4.5)
DECISION = {"claimed": "In review", "edd_needed": "EDD needed", "cleared": "Cleared"}


def day_after(d, n=1):
    return (date.fromisoformat(d) + timedelta(days=n)).isoformat()


# ============================================================== demo backend ====
class _Demo:
    """Stands in for DynamoDB decisions, decisions/ in S3, and the decision-dependent end of EMR."""

    def __init__(self):
        self.snap = json.loads(SNAPSHOT.read_text(encoding="utf-8"))
        self.rules = self.snap["rules"]
        self.runs = sorted(self.snap["runs"])
        self.lock = threading.Lock()
        self.reset()

    def reset(self):
        self.decisions, self.events, self.out, self.i = {}, [], {}, 1
        self._score(self.runs[0])
        self._score(self.runs[1])

    @property
    def run(self):
        return self.runs[self.i]

    @property
    def today(self):
        return day_after(self.run)          # analysts work the day after the nightly run

    def next_night(self):
        if self.i < len(self.runs) - 1:
            self.i += 1
            self._score(self.run)
            return True
        return False

    def _score(self, run):
        """EMR, decision-dependent part: R12, score, tiers, in_queue and queue_reason for one run."""
        today = day_after(run)
        edd = {e for e, d in self.decisions.items() if d["decision"] == "edd_needed"}
        prev = self.out.get(self.runs[self.runs.index(run) - 1]) if self.runs.index(run) else None
        rows = []
        for c in self.snap["runs"][run]["customers"]:
            hits = [list(h) for h in c["hits"]]
            linked = [cp for cp in c["cps"] if cp in edd]
            if linked:                       # R12 uses analyst decisions, not last run's tier
                hits.append(["R12", f"Transacted this week with {len(linked)} customer"
                                    f"{'s' if len(linked) != 1 else ''} an analyst marked as needing EDD"])
            hits.sort(key=lambda h: -self.rules[h[0]][1])
            points = sum(self.rules[h[0]][1] for h in hits)
            rows.append({"id": c["id"], "type": c["type"], "n_accounts": len(self.snap["accounts"][c["id"]]),
                         "points": points, "peer": c["peer"], "score": round(points + c["peer"], 2),
                         "hits": hits, "rules": [h[0] for h in hits], "m": c["m"], "l": c["l"]})
        rows.sort(key=lambda r: (-r["score"], r["id"]))
        n = len(rows)
        n_high, n_med = max(1, math.ceil(HIGH_PCT * n)), max(1, math.ceil(MEDIUM_PCT * n))
        for rank, r in enumerate(rows, 1):
            r["tier"] = ("High" if rank <= n_high and r["points"] else
                         "Medium" if rank <= n_med and r["points"] else "Low")
            p = prev.get(r["id"]) if prev else None
            r["prev_score"] = p["score"] if p else None
            r["change"] = round(r["score"] - p["score"], 2) if p else None
            r["new_rules"] = [x for x in r["rules"] if p is not None and x not in p["rules"]]
            base = r["tier"] in ("High", "Medium")
            was_queued = bool(p and p["in_queue"])
            reason = ("" if not base else f"Still {r['tier']}" if was_queued and p["tier"] == r["tier"]
                      else f"Moved to {r['tier']}" if was_queued else "New to the queue")
            d = self.decisions.get(r["id"])
            if d is None or d["decision"] == "claimed":
                r["in_queue"], r["queue_reason"] = base, reason
            elif d["decision"] == "edd_needed":          # moved to the EDD process; still scored
                r["in_queue"], r["queue_reason"] = False, ""
            elif today < d["cleared_until"]:
                fresh = [x for x in r["rules"] if x not in d["snapshot"]["rules"]]
                if fresh:
                    r["in_queue"] = True
                    r["queue_reason"] = "New rule since review: " + ", ".join(self.rules[x][0].lower() for x in fresh)
                elif r["score"] >= d["snapshot"]["score"] + SCORE_JUMP:
                    r["in_queue"] = True
                    r["queue_reason"] = f"Score up {r['score'] - d['snapshot']['score']:.0f} points since review"
                else:
                    r["in_queue"], r["queue_reason"] = False, ""
            else:
                r["in_queue"], r["queue_reason"] = base, "Clearance expired" if base else ""
        self.out[run] = {r["id"]: r for r in rows}

    # ---- DynamoDB decisions (one item per customer, conditional writes)
    def _event(self, cid, event, analyst, **extra):
        self.events.append({"at": self.today, "customer": cid, "event": event, "analyst": analyst, **extra})

    def claim(self, cid, analyst):
        d = self.decisions.get(cid)
        if d and d["decision"] == "claimed" and d["analyst"] != analyst:
            return False, f"Already in review by {d['analyst']}."
        self.decisions[cid] = {"decision": "claimed", "analyst": analyst, "at": self.today, "run": self.run,
                               "previous": d if d and d["decision"] != "claimed" else (d or {}).get("previous")}
        self._event(cid, "claimed", analyst)
        return True, ""

    def release(self, cid, analyst):
        d = self.decisions.get(cid)
        if d and d["decision"] == "claimed" and d["analyst"] == analyst:
            if d.get("previous"):
                self.decisions[cid] = d["previous"]
            else:
                del self.decisions[cid]
            self._event(cid, "released", analyst)

    def decide(self, cid, decision, analyst, note, days=None):
        d = self.decisions.get(cid)
        if d and d["decision"] == "claimed" and d["analyst"] != analyst:
            return False, f"In review by {d['analyst']}. Only they can record the decision."
        row = self.out[self.run][cid]
        self.decisions[cid] = {
            "decision": decision, "analyst": analyst, "at": self.today, "run": self.run, "note": note,
            "cleared_until": day_after(self.today, days) if decision == "cleared" else None,
            "snapshot": {"score": row["score"], "rules": list(row["rules"])}}
        self._event(cid, decision, analyst, cleared_until=self.decisions[cid]["cleared_until"], note=note)
        return True, ""

    def reopen(self, cid, analyst):
        if cid in self.decisions:
            del self.decisions[cid]
            self._event(cid, "reopened", analyst)


@st.cache_resource(max_entries=1)
def _demo_for(version):
    return _Demo()


# =============================================================== AWS backend ====
RULES = {   # rule_id: (name, points, what it looks for, MAS grounding). The EMR job owns the logic.
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


def _athena(sql):
    ath = _aws().athena
    qid = ath.start_query_execution(QueryString=sql, QueryExecutionContext={"Database": ATHENA_DB},
                                    WorkGroup=ATHENA_WG)["QueryExecutionId"]
    while True:
        q = ath.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]
        if q["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(0.3)
    if q["State"] != "SUCCEEDED":
        raise RuntimeError(f"Athena query {q['State'].lower()}: {q.get('StateChangeReason', '')}")
    rows = []
    for page in ath.get_paginator("get_query_results").paginate(QueryExecutionId=qid):
        rows += [[c.get("VarCharValue") for c in r["Data"]] for r in page["ResultSet"]["Rows"]]
    # dtype=object keeps Athena's empty values as None; pandas 3's string dtype would turn them into NaN, which is truthy
    return pd.DataFrame(rows[1:], columns=rows[0], dtype=object) if rows else pd.DataFrame()


def _num(v):
    return None if v is None or v == "" else float(v)


class _Scores(dict):
    """scores/ by run date, loaded through Athena the first time a run is asked for."""

    def __init__(self, backend):
        super().__init__()
        self.backend = backend

    def __missing__(self, run):
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", run):
            raise KeyError(run)
        df = _athena(f"SELECT * FROM scores WHERE run_date = '{run}'")
        rows = {}
        for r in df.to_dict("records"):
            rows[r["customer_id"]] = {
                "id": r["customer_id"], "type": r["entity_type"], "n_accounts": int(r["n_accounts"] or 0),
                "accounts": json.loads(r["accounts"] or "[]"), "points": int(r["points"] or 0),
                "peer": float(r["peer"] or 0), "score": float(r["score"] or 0), "tier": r["tier"],
                "hits": [[h["rule"], h["evidence"]] for h in json.loads(r["hits"] or "[]")],
                "rules": json.loads(r["rules"] or "[]"), "m": json.loads(r["metrics"] or "{}"),
                "l": int(r["laundering"] or 0), "in_queue": r["in_queue"] == "true",
                "queue_reason": r["queue_reason"] or "", "prev_score": _num(r["prev_score"]),
                "change": _num(r["change"]), "new_rules": json.loads(r["new_rules"] or "[]"),
                "window_start": r["window_start"]}
        self[run] = rows
        return rows


class _Accounts(Mapping):
    """Customer -> the accounts it used in the latest window. Its keys are "our customers"."""

    def __init__(self, backend):
        self.backend = backend

    def _rows(self):
        return self.backend.out[self.backend.run]

    def __getitem__(self, cid):
        return self._rows()[cid]["accounts"]

    def __iter__(self):
        return iter(self._rows())

    def __len__(self):
        return len(self._rows())


class _Windows(Mapping):
    def __init__(self, backend):
        self.backend = backend

    def __getitem__(self, run):
        rows = self.backend.out[run]
        first = next(iter(rows.values()), None)
        return {"window_start": first["window_start"] if first else day_after(run, -6)}

    def __iter__(self):
        return iter(self.backend.runs)

    def __len__(self):
        return len(self.backend.runs)


class _Aws:
    """Same interface as _Demo, backed by the pipeline's stores."""

    def __init__(self):
        import boto3
        session = boto3.Session(region_name=REGION)
        self.s3, self.athena = session.client("s3"), session.client("athena")
        self.rules = {k: list(v) for k, v in RULES.items()}
        self.lock = threading.Lock()
        self.out = _Scores(self)
        self.snap = {"accounts": _Accounts(self), "runs": _Windows(self)}
        self._runs, self._runs_at, self._events, self._events_at, self._clock = [], 0.0, None, 0.0, (0.0, None)

    # ---- nightly runs: the run_date folders the EMR job has written
    @property
    def runs(self):
        if time.time() - self._runs_at > 60:
            found, kw = [], {"Bucket": S3_BUCKET, "Prefix": "scores/run_date=", "Delimiter": "/"}
            while True:
                r = self.s3.list_objects_v2(**kw)
                found += [p["Prefix"].rstrip("/").split("=", 1)[1] for p in r.get("CommonPrefixes", [])]
                if not r.get("IsTruncated"):
                    break
                kw["ContinuationToken"] = r["NextContinuationToken"]
            self._runs, self._runs_at = sorted(found), time.time()
        return self._runs

    @property
    def i(self):
        return len(self.runs) - 1

    @property
    def run(self):
        return self.runs[-1]

    @property
    def today(self):
        return day_after(self.run)          # analysts work the day after the nightly run

    def now(self):
        """The producer's simulated clock, or the real time when no replay is running."""
        read_at, state = self._clock
        if time.time() - read_at > 5:
            try:
                state = json.loads(self.s3.get_object(Bucket=S3_BUCKET, Key="state/sim_clock.json")["Body"].read())
            except Exception:
                state = None
            self._clock = (time.time(), state)
        if not state:
            return time.time()
        if not state.get("running"):
            return float(state["sim_anchor"])
        return float(state["sim_anchor"]) + (time.time() - float(state["wall_anchor"])) * float(state["speed"])

    # ---- decisions/: an append-only event log; current state = the latest events per customer
    def _load_events(self, fresh=False):
        if fresh or self._events is None or time.time() - self._events_at > 30:
            first = day_after(self.runs[0], -120) if self.runs else "2000-01-01"
            df = _athena(f"""SELECT customer_id, event, analyst, to_unixtime(at) AS at, cleared_until, note,
                                    snapshot_score, snapshot_rules, run_date, to_unixtime(written_at) AS written_at
                             FROM decisions WHERE date >= '{first}'""")
            self._events, self._events_at = df.to_dict("records"), time.time()
        return self._events

    @property
    def decisions(self):
        decided, claimed = {}, {}
        for e in sorted(self._load_events(), key=lambda e: (float(e["at"]), float(e["written_at"] or 0))):
            cid = e["customer_id"]
            base = {"analyst": e["analyst"], "run": e["run_date"],
                    "at": datetime.fromtimestamp(float(e["at"]), timezone.utc).strftime("%Y-%m-%d")}
            if e["event"] in ("edd_needed", "cleared"):
                decided[cid] = {**base, "decision": e["event"], "note": e["note"] or "",
                                "cleared_until": e["cleared_until"] or None,
                                "snapshot": {"score": float(e["snapshot_score"] or 0),
                                             "rules": json.loads(e["snapshot_rules"] or "[]")}}
                claimed.pop(cid, None)
            elif e["event"] == "reopened":
                decided.pop(cid, None)
                claimed.pop(cid, None)
            elif e["event"] == "claimed":
                claimed[cid] = base
            elif e["event"] == "released":
                claimed.pop(cid, None)
        state = dict(decided)
        for cid, c in claimed.items():
            state[cid] = {**c, "decision": "claimed", "previous": decided.get(cid)}
        return state

    def _write(self, cid, event, analyst, cleared_until=None, note="", snapshot=None):
        import pyarrow as pa
        import pyarrow.parquet as pq
        now, wall = self.now(), time.time()
        row = {"customer_id": cid, "event": event, "analyst": analyst, "at": int(now * 1000),
               "cleared_until": cleared_until, "note": note or "",
               "snapshot_score": snapshot["score"] if snapshot else None,
               "snapshot_rules": json.dumps(snapshot["rules"]) if snapshot else None,
               "run_date": self.run, "written_at": int(wall * 1000)}
        schema = pa.schema([("customer_id", pa.string()), ("event", pa.string()), ("analyst", pa.string()),
                            ("at", pa.timestamp("ms")), ("cleared_until", pa.string()), ("note", pa.string()),
                            ("snapshot_score", pa.float64()), ("snapshot_rules", pa.string()),
                            ("run_date", pa.string()), ("written_at", pa.timestamp("ms"))])
        buf = io.BytesIO()
        pq.write_table(pa.Table.from_pylist([row], schema=schema), buf)
        day = datetime.fromtimestamp(now, timezone.utc).strftime("%Y-%m-%d")
        self.s3.put_object(Bucket=S3_BUCKET, Body=buf.getvalue(),
                           Key=f"decisions/date={day}/{int(wall * 1000)}-{uuid.uuid4().hex[:8]}.parquet")
        if self._events is not None:        # show our own write at once; others' arrive with the next read
            self._events.append({**row, "at": now, "written_at": wall})

    def _held_by_other(self, cid, analyst):
        """Re-read the log before a write; refuse if another analyst holds an open claim (spec 4.3)."""
        self._load_events(fresh=True)
        d = self.decisions.get(cid)
        return d["analyst"] if d and d["decision"] == "claimed" and d["analyst"] != analyst else None

    def claim(self, cid, analyst):
        other = self._held_by_other(cid, analyst)
        if other:
            return False, f"Already in review by {other}."
        self._write(cid, "claimed", analyst)
        return True, ""

    def release(self, cid, analyst):
        d = self.decisions.get(cid)
        if d and d["decision"] == "claimed" and d["analyst"] == analyst:
            self._write(cid, "released", analyst)

    def decide(self, cid, decision, analyst, note, days=None):
        other = self._held_by_other(cid, analyst)
        if other:
            return False, f"In review by {other}. Only they can record the decision."
        row = self.out[self.run][cid]
        self._write(cid, decision, analyst, note=note,
                    cleared_until=day_after(self.today, days) if decision == "cleared" else None,
                    snapshot={"score": row["score"], "rules": list(row["rules"])})
        return True, ""

    def reopen(self, cid, analyst):
        self._write(cid, "reopened", analyst)

    def next_night(self):                   # the nightly run is the Step Functions workflow, not a button
        return False

    def reset(self):
        pass

    def txns(self, cid, start, run):
        """Athena, on demand: one customer's payments from curated/customer_txns/, both sides."""
        if not re.fullmatch(r"[0-9A-Za-z_-]+", cid):
            raise ValueError("Unexpected customer id")
        side = """SELECT to_unixtime(txn_ts) AS ts, {me}_account AS account, '{d}' AS direction,
                         {cp}_customer_id AS cp_entity, {cp}_bank AS cp_bank, {cp}_account AS cp_account,
                         {amt}_usd AS amount, {amt} AS orig_amount, {cur} AS currency, payment_format AS format,
                         CASE WHEN from_customer_id = to_customer_id THEN 1 ELSE 0 END AS internal
                  FROM customer_txns
                  WHERE date BETWEEN '{start}' AND '{run}' AND {me}_customer_id = '{cid}' AND {me}_bank = '{bank}'"""
        df = _athena(side.format(me="from", cp="to", d="out", amt="amount_paid", cur="payment_currency",
                                 start=start, run=run, cid=cid, bank=BANK_ID)
                     + " UNION ALL "
                     + side.format(me="to", cp="from", d="in", amt="amount_received", cur="receiving_currency",
                                   start=start, run=run, cid=cid, bank=BANK_ID))
        if not len(df):
            return df
        return df.astype({"ts": float, "amount": float, "orig_amount": float, "internal": int})


@st.cache_resource
def _aws():
    return _Aws()


def demo():
    """The backend: the snapshot player in demo mode, the pipeline's stores against AWS."""
    if not DEMO:
        return _aws()
    return _demo_for((SNAPSHOT.stat().st_mtime, Path(__file__).stat().st_mtime))


# ---- what the app reads. In AWS mode each of these becomes the query named in its docstring.
def run_date():
    return demo().run


def scores(run=None):
    """Athena: scores/run_date=<run>/, joined to the previous partition for the change columns."""
    return list(demo().out[run or demo().run].values())


def decisions():
    """DynamoDB decisions: the current item for every customer that has one."""
    return dict(demo().decisions)


def handled_today(d):
    """A decision made since the last run. The customer stays in tonight's data but is done for now."""
    return bool(d) and d["decision"] in ("edd_needed", "cleared") and d["run"] == demo().run


def queue():
    """Athena: WHERE run_date = <today> AND in_queue, combined with live status from DynamoDB."""
    ds = decisions()
    return [r for r in scores() if r["in_queue"] and not handled_today(ds.get(r["id"]))]


def customer_txns(cid, run=None):
    """Athena, on demand: curated/customer_txns/ for one customer, the 7 days ending on the run date."""
    d = demo()
    run = run or d.run
    start = d.snap["runs"][run]["window_start"]
    cols = ["ts", "account", "direction", "cp_entity", "cp_bank", "cp_account", "amount", "orig_amount",
            "currency", "format", "internal"]
    if not DEMO:
        df = _aws_txns(cid, start, run)
        return df if len(df) else pd.DataFrame(columns=cols)
    df = pd.DataFrame(d.snap["txns"].get(cid, []), columns=cols)
    day = pd.to_datetime(df.ts, unit="s").dt.strftime("%Y-%m-%d")
    return df[(day >= start) & (day <= run)]


@st.cache_data(ttl=600, show_spinner="Loading the customer's transactions")
def _aws_txns(cid, start, run):
    return _aws().txns(cid, start, run)


def history(cid):
    """Athena: this customer's row in every scores/ partition so far."""
    d = demo()
    return [(run, d.out[run][cid]) for run in d.runs[:d.i + 1] if cid in d.out[run]]


def rule_eval():
    """Athena: rule_eval/run_date=<run>/. Uses is_laundering for evaluation only."""
    d, rows = demo(), scores()
    per_rule = []
    for rid, (name, points, _, _) in d.rules.items():
        fired = [r for r in rows if rid in r["rules"]]
        per_rule.append({"rule": rid, "name": name, "points": points, "customers": len(fired),
                         "precision": sum(r["l"] for r in fired) / len(fired) if fired else None})
    per_tier = []
    for tier in ("High", "Medium", "Low"):
        t = [r for r in rows if r["tier"] == tier]
        per_tier.append({"tier": tier, "customers": len(t),
                         "precision": sum(r["l"] for r in t) / len(t) if t else None})
    return per_rule, per_tier


# ======================================================================= UI ====
BG, CARD, INK, MUTED, LINE = "#0E131C", "#161D2A", "#E4E9F2", "#93A0B5", "#273247"
RED, AMBER, BLUE = "#E5544B", "#E0A030", "#8FB4FF"
CSS = f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600&display=swap');
html, body, .stApp, .stApp button, .stApp input, .stApp textarea, .stApp label, .stApp p {{
  font-family: 'IBM Plex Sans', 'Segoe UI', system-ui, sans-serif; }}
.block-container {{ padding-top: 3.2rem; padding-bottom: 3rem; max-width: 1400px; }}
h1, h2, h3, h4, h5 {{ color: {INK}; font-weight: 600; letter-spacing: -0.01em; }}
[class*="st-key-card_"] {{ background: {CARD}; }}
.sub {{ color: {MUTED}; font-size: .9rem; }}
.facts {{ display: flex; flex-wrap: wrap; gap: 6px 28px; margin-top: 6px; font-size: .92rem; color: {MUTED}; }}
.facts b {{ color: {INK}; font-weight: 500; }}
.chip {{ display: inline-flex; align-items: center; gap: 7px; padding: 3px 11px 3px 9px; border-radius: 999px;
  font-weight: 500; font-size: .84rem; white-space: nowrap; }}
.chip i {{ width: 6px; height: 6px; border-radius: 50%; background: currentColor; }}
.c-high {{ background: rgba(229,84,75,.16); color: #FF8A82; }}
.c-medium {{ background: rgba(224,160,48,.16); color: #F0BC5E; }}
.c-low {{ background: rgba(147,160,181,.14); color: #B7C2D4; }}
.tag {{ background: rgba(143,180,255,.16); color: {BLUE}; border-radius: 4px; font-size: .74rem; font-weight: 600;
  padding: 1px 7px; margin-left: 8px; white-space: nowrap; }}
.up {{ color: #FF8A82; }} .down {{ color: #7FD1A6; }}

/* queue table: single-line rows, the whole row opens the customer */
.st-key-wl_table {{ background: {CARD}; border-radius: 12px; gap: 0; }}
.st-key-wl_table > div:first-child {{ margin-bottom: 10px; }}
.wl {{ display: grid; grid-template-columns: 150px 78px minmax(210px, 2.4fr) minmax(130px, 1.2fr) 56px 64px 92px;
  gap: 16px; align-items: center; font-variant-numeric: tabular-nums; }}
.wl > span {{ min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }}
.wl .r {{ text-align: right; }}
.wl .dim {{ color: {MUTED}; }}
.wl .score {{ font-weight: 600; }}
.wl-head {{ color: {MUTED}; font-size: .74rem; letter-spacing: .08em; text-transform: uppercase; }}
@media (max-width: 1560px) {{ .wl {{ grid-template-columns: 78px minmax(210px, 2.4fr) minmax(130px, 1.2fr) 56px 64px 92px; }}
  .wl .type {{ display: none; }} }}
.st-key-wl_table [data-testid="stHorizontalBlock"]:has(.wl) {{ border-top: 1px solid #1F283A; padding: 0 4px;
  min-height: 40px; align-items: center; }}
.st-key-wl_table [data-testid="stHorizontalBlock"]:has(.wl-head) {{ border-top: 1px solid {LINE};
  border-bottom: 1px solid {LINE}; }}
.st-key-wl_table [data-testid="stHorizontalBlock"]:has(.wl) button {{ min-height: 0; padding: 0;
  font-family: 'IBM Plex Mono', ui-monospace, Consolas, monospace; font-size: .9rem; color: {INK}; }}
.st-key-wl_table [data-testid="stHorizontalBlock"]:has(.wl):not(:has(.wl-head)) {{ position: relative; cursor: pointer; }}
.st-key-wl_table [data-testid="stHorizontalBlock"]:has(.wl):not(:has(.wl-head)):hover {{ background: #1B2538; }}
.st-key-wl_table [data-testid="stHorizontalBlock"]:has(.wl):not(:has(.wl-head)) :is(div, button, span, p) {{ position: static !important; }}
.st-key-wl_table [data-testid="stHorizontalBlock"]:has(.wl):not(:has(.wl-head)) button::after {{ content: '';
  position: absolute; inset: 0; }}
.st-key-wl_table [data-testid="stHorizontalBlock"]:has(.wl):not(:has(.wl-head)):hover button {{ color: {BLUE}; }}

.ahead > div:first-child {{ display: flex; align-items: center; gap: 14px; flex-wrap: wrap; }}
.ahead h2 {{ margin: 0; padding: 0; font-size: 1.6rem; }}
.ahead .big {{ font-size: 1.05rem; color: {INK}; }}
.picture {{ font-size: 1.02rem; line-height: 1.55; margin: -4px 0 14px; max-width: 75ch; }}
.flow3 {{ display: grid; grid-template-columns: 1fr 170px 1fr; gap: 14px; align-items: start; }}
.fl-head {{ color: {MUTED}; font-size: .86rem; margin-bottom: 8px; white-space: nowrap; }}
.fl-head b {{ color: {INK}; font-size: 1.15rem; font-weight: 600; margin-left: 6px; font-variant-numeric: tabular-nums; }}
.fl {{ margin-bottom: 10px; font-variant-numeric: tabular-nums; }}
.fl-top {{ display: flex; justify-content: space-between; gap: 10px; font-size: .92rem; }}
.fl b {{ font-weight: 500; }} .fl small, .me small {{ color: {MUTED}; font-size: .78rem; }}
.fl-bar {{ height: 8px; background: #232D40; border-radius: 4px; overflow: hidden; margin: 3px 0 1px; }}
.fl-bar i {{ display: block; height: 100%; border-radius: 4px; }}
.fl-bar.in i {{ background: #3987e5; }} .fl-bar.out i {{ background: #d95926; }}
.ds {{ display: grid; grid-template-columns: repeat(7, minmax(0, 1fr)); gap: 8px; border-bottom: 1px solid {LINE}; }}
.ds-day {{ text-align: center; }}
.ds-bars {{ display: flex; justify-content: center; align-items: flex-end; gap: 4px; height: 82px; }}
.ds-bar {{ display: flex; flex-direction: column; align-items: center; justify-content: flex-end; width: 44%; max-width: 34px; }}
.ds-bar span {{ font-size: .72rem; color: {MUTED}; font-variant-numeric: tabular-nums; white-space: nowrap; margin-bottom: 2px; }}
.ds-bar i {{ display: block; width: 100%; border-radius: 3px 3px 0 0; }}
.ds-bar.in i {{ background: #3987e5; }} .ds-bar.out i {{ background: #d95926; }}
.ds-day small {{ display: block; color: {MUTED}; font-size: .78rem; padding: 5px 0 6px; }}
.ds-key {{ color: {MUTED}; font-size: .8rem; margin-top: 6px; display: flex; align-items: center; gap: 6px; }}
.ds-key span {{ width: 10px; height: 10px; border-radius: 2px; display: inline-block; }}
.ds-key span.in {{ background: #3987e5; }} .ds-key span.out {{ background: #d95926; margin-left: 12px; }}
.fl-mid {{ display: flex; align-items: center; gap: 6px; margin-top: 30px; }}
.fl-mid .arrow {{ flex: 1; height: 2px; background: {MUTED}; position: relative; }}
.fl-mid .arrow::after {{ content: ''; position: absolute; right: -1px; top: -4px; border-left: 7px solid {MUTED};
  border-top: 5px solid transparent; border-bottom: 5px solid transparent; }}
.me {{ border: 1px solid {BLUE}; border-radius: 8px; padding: 8px 10px; text-align: center; background: #111826; }}
.me b {{ display: block; font-weight: 600; font-variant-numeric: tabular-nums; }} .me small {{ display: block; }}
@media (max-width: 900px) {{ .flow3 {{ grid-template-columns: 1fr; }} .fl-mid .arrow {{ display: none; }} }}
.rule {{ display: grid; grid-template-columns: 1fr auto; gap: 4px 16px; padding: 10px 0 10px 12px;
  border-left: 3px solid #3A4967; margin: 8px 0; }}
.rule b {{ font-weight: 600; }}
.rule .pts {{ font-weight: 600; font-variant-numeric: tabular-nums; color: #F0BC5E; white-space: nowrap; }}
.rule .ev {{ grid-column: 1 / -1; color: {INK}; }}
table.t {{ width: 100%; border-collapse: collapse; font-variant-numeric: tabular-nums; margin-top: 4px; }}
table.t th {{ color: {MUTED}; font-weight: 400; font-size: .82rem; text-align: right; padding: 4px 8px; border: 0; }}
table.t td {{ text-align: right; padding: 6px 8px; border: 0; border-top: 1px solid {LINE}; }}
table.t th:first-child, table.t td:first-child {{ text-align: left; padding-left: 0; white-space: nowrap; }}
table.t tr {{ border: 0; }} table.t tr.total td {{ font-weight: 600; }}
.stApp [data-testid="stBaseButton-primary"] {{ color: #0E131C; font-weight: 600; }}
.st-key-reviewer_bar {{ background: #1B2538; border-radius: 8px; padding: 8px 14px; margin-bottom: 4px; }}
.note {{ background: #1B2538; border-radius: 8px; padding: 10px 14px; margin-bottom: 10px; }}

/* sidebar: navigation at the top, analyst name and run date pinned to the bottom */
[data-testid="stSidebarContent"] {{ position: relative; }}
[data-testid="stSidebarUserContent"], [data-testid="stSidebarUserContent"] div:has(> .st-key-side_bottom),
[data-testid="stSidebarUserContent"] > div {{ position: static; }}
.st-key-side_bottom {{ position: absolute; bottom: 1.25rem; left: 1.5rem; right: 1.5rem; width: auto; }}
.stApp button:focus-visible {{ outline: 2px solid {BLUE}; outline-offset: 2px; }}
</style>
"""


def esc(x):
    return html.escape(str(x if x is not None else ""))


def fmt_day(d):
    return date.fromisoformat(d).strftime("%d %b %Y") if d else "-"


def fmt_ts(ts):
    return datetime.fromtimestamp(float(ts), timezone.utc).strftime("%d %b, %H:%M")


def usd(x):
    return f"${float(x):,.0f}"


def analyst():
    return (st.session_state.get("analyst") or "").strip()


def chip(tier):
    return f"<span class='chip c-{tier.lower()}'><i></i>{tier}</span>"


def change_html(r):
    c = r.get("change")
    if c is None:
        return "<span class='dim'>new</span>"
    if abs(c) < 0.5:
        return "<span class='dim'>0</span>"
    return f"<span class='{'up' if c > 0 else 'down'}'>{c:+.0f}</span>"


def rule_name(rid):
    return demo().rules[rid][0]


def flash():
    f = st.session_state.pop("flash", None)
    if f:
        getattr(st, f[0])(f[1])


# ---------------------------------------------------------------- queue ----
SORTS = ["Highest score", "Biggest rise", "Most rules"]


def _sorted_queue():
    rows = queue()
    q = (st.session_state.get("find") or "").strip().upper()
    if q:
        rows = [r for r in rows if q in r["id"].upper()]
    key = {"Highest score": lambda r: -r["score"],
           "Biggest rise": lambda r: -(r["change"] if r["change"] is not None else r["score"]),
           "Most rules": lambda r: (-len(r["rules"]), -r["score"])}[st.session_state.get("sort") or SORTS[0]]
    return sorted(rows, key=key)


_HEAD = ("<div class='wl wl-head'><span class='type'>Type</span><span class='r'>Accounts</span><span>Rules fired</span>"
         "<span>Why in the queue</span><span class='r'>Score</span><span class='r'>Change</span><span>Tier</span></div>")


def _row_html(r, d):
    names = [rule_name(x) for x in r["rules"]]
    rules = names[0] + (f" <span class='dim'>+{len(names) - 1} more</span>" if len(names) > 1 else "") if names else "-"
    why = esc(r["queue_reason"])
    if d and d["decision"] == "claimed":
        why = f"<span class='dim'>In review by {esc(d['analyst'])}</span>"
    return (f"<div class='wl'><span class='type dim'>{esc(r['type'])}</span><span class='r'>{r['n_accounts']}</span>"
            f"<span>{rules}</span><span>{why}</span><span class='r score'>{r['score']:.0f}</span>"
            f"<span class='r'>{change_html(r)}</span><span>{chip(r['tier'])}</span></div>")


def page_queue():
    cid = st.session_state.get("open")
    if cid and cid in demo().out[run_date()]:
        customer_page(cid)
        return
    st.session_state["open"] = None
    st.title("Priority queue")
    st.caption("Customers to review for enhanced due diligence, highest score first. Every night each customer's "
               "accounts are combined and checked against 12 rules; each rule that fires adds points to the score.")
    rows, ds = queue(), decisions()
    c = st.columns(4)
    c[0].metric("To review", len(rows))
    c[1].metric("High tier", sum(r["tier"] == "High" for r in rows))
    c[2].metric("New since the last run", sum(r["queue_reason"].startswith(("New", "Moved")) for r in rows))
    c[3].metric("In review now", sum(1 for r in rows if (ds.get(r["id"]) or {}).get("decision") == "claimed"))
    flash()
    with st.container(border=True, key="wl_table"):
        f = st.columns([4.2, 2.4, 2.2], vertical_alignment="center")
        f[0].markdown(f"**Needs review** :gray[{len(rows)}]")
        f[1].selectbox("Sort by", SORTS, key="sort", label_visibility="collapsed",
                       format_func=lambda s: f"Sort: {s.lower()}")
        f[2].text_input("Search", key="find", placeholder="Search customer ID", label_visibility="collapsed")
        rows = _sorted_queue()
        st.session_state["order"] = [r["id"] for r in rows]
        if not rows:
            st.info("No customers match." if st.session_state.get("find") else
                    "The queue is clear. Tonight's run will refill it.")
            return
        shown = st.session_state.get("shown", 20)
        a, b = st.columns([1.5, 8.5], vertical_alignment="center")
        a.html("<div class='wl-head'>Customer</div>")
        b.html(_HEAD)
        for r in rows[:shown]:
            a, b = st.columns([1.5, 8.5], vertical_alignment="center")
            b.html(_row_html(r, ds.get(r["id"])))
            if a.button(r["id"], type="tertiary", key=f"open_{r['id']}"):
                st.session_state["open"] = r["id"]
                st.rerun()
        if len(rows) > shown:
            if st.button(f"Show more ({len(rows) - shown} left)", key="more", type="tertiary"):
                st.session_state["shown"] = shown + 20
                st.rerun()
    st.caption("Tiers rank customers against each other: the top 1% by score are High and the next 9% Medium. "
               "A tier is a priority for review, not a verdict. Click a row to open the customer.")


# -------------------------------------------------------- customer page ----
def _go(cid):
    st.session_state["open"] = cid


def _on_claim(cid):
    if not analyst():
        st.session_state["flash"] = ("warning", "Enter your name in the sidebar first.")
        return
    with demo().lock:
        ok, msg = demo().claim(cid, analyst())
    if not ok:
        st.session_state["flash"] = ("error", msg)


def _on_release(cid):
    with demo().lock:
        demo().release(cid, analyst())


def _on_submit(cid):
    choice = st.session_state.get(f"out_{cid}")
    note = (st.session_state.get(f"note_{cid}") or "").strip()
    days = st.session_state.get(f"days_{cid}") or CLEAR_DAYS[0]
    problem = ("Enter your name in the sidebar first. It is recorded with every decision." if not analyst() else
               "Choose a decision before submitting." if not choice else
               "Add a comment explaining why EDD is needed." if choice == "edd_needed" and not note else "")
    if problem:
        st.session_state["flash"] = ("warning", problem)
        return
    with demo().lock:
        ok, msg = demo().decide(cid, choice, analyst(), note, days if choice == "cleared" else None)
    if not ok:
        st.session_state["flash"] = ("error", msg)
        return
    order = st.session_state.get("order", [])
    nxt = (order[order.index(cid) + 1:] + order[:order.index(cid)]) if cid in order else []
    st.session_state["order"] = nxt
    st.session_state["open"] = nxt[0] if nxt else None
    done = "EDD needed" if choice == "edd_needed" else f"Cleared for {days} days"
    st.session_state["flash"] = ("success", f"{done}: {cid}. " + ("Opened the next customer." if nxt else "The queue is clear."))


def _flow_side(ext, direction, scale):
    """Top counterparties on one side of the customer, as labelled bars."""
    side = ext[ext.direction == direction]
    if side.empty:
        return "<div class='sub'>Nothing in this window.</div>"
    ours = set(demo().snap["accounts"])
    g = side.groupby("cp_entity").agg(amount=("amount", "sum"), n=("amount", "size"), bank=("cp_bank", "first"))
    g = g.sort_values("amount", ascending=False)
    rows = [(cp, "Our customer" if cp in ours else f"Bank {x.bank}", x.amount, x.n) for cp, x in g.head(4).iterrows()]
    if len(g) > 4:
        rest = g.iloc[4:]
        rows.append((f"{len(rest)} others", "", rest.amount.sum(), int(rest.n.sum())))
    out = ""
    for name, where, amount, n in rows:
        out += (f"<div class='fl'><div class='fl-top'><b>{esc(name)}</b><span>{usd(amount)}</span></div>"
                f"<div class='fl-bar {direction}'><i style='width:{max(2, 100 * amount / scale):.0f}%'></i></div>"
                f"<small>{esc(where) + ', ' if where else ''}{n} payment{'s' if n != 1 else ''}</small></div>")
    return out


def short(x):
    return f"${x / 1e6:.1f}M" if x >= 1e6 else f"${x / 1e3:.0f}k" if x >= 1e3 else f"${x:.0f}"


def _days_strip(ext):
    """Money in and out for each of the 7 days: steady activity or a sudden burst."""
    start = date.fromisoformat(demo().snap["runs"][run_date()]["window_start"])
    day = pd.to_datetime(ext.ts, unit="s").dt.strftime("%Y-%m-%d")
    by = ext.groupby([day, "direction"]).amount.sum()
    top = max(by.max() if len(by) else 0, 1)
    cols = ""
    for k in range(7):
        d = (start + timedelta(days=k)).isoformat()
        bars = ""
        for direction in ("in", "out"):
            v = float(by.get((d, direction), 0))
            bars += (f"<div class='ds-bar {direction}' title='{'In' if direction == 'in' else 'Out'} {usd(v)}'>"
                     f"<span>{short(v) if v else ''}</span><i style='height:{(4 + 56 * v / top) if v else 0:.0f}px'></i></div>")
        cols += f"<div class='ds-day'><div class='ds-bars'>{bars}</div><small>{(start + timedelta(days=k)).strftime('%d %b')}</small></div>"
    return (f"<div class='ds'>{cols}</div><div class='ds-key'><span class='in'></span>Money in"
            f"<span class='out'></span>Money out</div>")


def _picture_card(r, tx):
    """Who the customer is and how money moves through them: the first thing an analyst needs."""
    ext, m = tx[tx.internal == 0], r["m"]
    ins, outs = ext[ext.direction == "in"], ext[ext.direction == "out"]
    t_in, t_out = ins.amount.sum(), outs.amount.sum()
    s = (f"A {esc(r['type'].lower())} with {r['n_accounts']} account{'s' if r['n_accounts'] != 1 else ''} at our bank. "
         f"In the 7 days to {fmt_day(run_date())} it ")
    s += (f"received <b>{usd(t_in)}</b> in {len(ins)} payment{'s' if len(ins) != 1 else ''} from {m['senders']} "
          f"sender{'s' if m['senders'] != 1 else ''}" if len(ins) else "received nothing")
    s += " and " + (f"paid out <b>{usd(t_out)}</b> in {len(outs)} payment{'s' if len(outs) != 1 else ''} to "
                    f"{m['recipients']} recipient{'s' if m['recipients'] != 1 else ''}." if len(outs) else "paid out nothing.")
    if t_in > 0 and t_out > 0:
        s += f" That is {min(t_out / t_in, 99):.0%} of what came in going back out."
    if len(ext):
        by = (ext.groupby("format").amount.sum() / ext.amount.sum()).sort_values(ascending=False)
        s += " Paid by " + ", ".join(f"{k.lower() if k != 'ACH' else k} ({v:.0%})" for k, v in by.head(3).items()
                                     if v >= 0.05) + "."
    own = tx[(tx.internal == 1) & (tx.direction == "out")]
    middle = (f"<small>{len(own)} transfer{'s' if len(own) != 1 else ''} between its own accounts, "
              f"{usd(own.amount.sum())}</small>" if len(own) else "")
    scale = max(t_in, t_out, 1)
    st.html(f"""<div class="picture">{s}</div>
<div class="flow3">
<div><div class="fl-head">Money in <b>{usd(t_in)}</b></div>{_flow_side(ext, 'in', scale)}</div>
<div class="fl-mid"><span class="arrow"></span><div class="me"><small>Customer</small><b>{esc(r['id'])}</b>
<small>{r['n_accounts']} account{'s' if r['n_accounts'] != 1 else ''}</small>{middle}</div><span class="arrow"></span></div>
<div><div class="fl-head">Money out <b>{usd(t_out)}</b></div>{_flow_side(ext, 'out', scale)}</div>
</div>
<div class="fl-head" style="margin-top:14px">Day by day</div>{_days_strip(ext)}""")


def _rules_card(r, d):
    rules, since = demo().rules, (d or {}).get("snapshot", {}).get("rules")
    out = ""
    for rid, evidence in r["hits"]:
        tag = ("<span class='tag'>New since your review</span>" if since is not None and rid not in since else
               "<span class='tag'>New since the last run</span>" if rid in r["new_rules"] else "")
        out += (f"<div class='rule'><b>{esc(rules[rid][0])}{tag}</b><span class='pts'>+{rules[rid][1]} points</span>"
                f"<span class='ev'>{esc(evidence)}</span>"
                f"<span class='sub' style='grid-column:1/-1'>{esc(rules[rid][2])}.</span></div>")
    if not out:
        out = "<div class='sub'>No rule fired for this customer in this run.</div>"
    st.html(f"<div class='sub' style='margin:-6px 0 4px'>Each rule that fired adds its points. The score is the "
            f"total, {r['points']} points, plus a tie-breaker of {r['peer']:.2f} for how the customer compares "
            f"with others of its type.</div>{out}")


def _accounts_card(r, tx):
    ext = tx[tx.internal == 0]
    rows, tot = "", [0, 0.0, 0, 0.0]
    for acct in demo().snap["accounts"][r["id"]]:
        a = ext[ext.account == acct]
        i, o = a[a.direction == "in"], a[a.direction == "out"]
        vals = [len(i), i.amount.sum(), len(o), o.amount.sum()]
        tot = [x + y for x, y in zip(tot, vals)]
        rows += (f"<tr><td>{esc(acct)}</td><td>{vals[0]}</td><td>{usd(vals[1])}</td><td>{vals[2]}</td>"
                 f"<td>{usd(vals[3])}</td></tr>")
    if r["n_accounts"] > 1:
        rows += (f"<tr class='total'><td>All {r['n_accounts']} accounts</td><td>{tot[0]}</td><td>{usd(tot[1])}</td>"
                 f"<td>{tot[2]}</td><td>{usd(tot[3])}</td></tr>")
    own = int(((tx.internal == 1) & (tx.direction == "out")).sum())
    m = r["m"]
    st.html(f"<div class='sub' style='margin:-6px 0 4px'>The rules look at all of this customer's accounts together. "
            f"An account can look normal on its own while the combined picture does not.</div>"
            f"<table class='t'><tr><th>Account</th><th>Payments in</th><th>Received</th><th>Payments out</th>"
            f"<th>Paid out</th></tr>{rows}</table>"
            f"<div class='facts'><span>Different senders <b>{m['senders']}</b></span>"
            f"<span>Different recipients <b>{m['recipients']}</b></span>"
            f"<span>Transfers between own accounts <b>{own}</b></span></div>")


def _history_card(cid):
    rows = ""
    for run, r in reversed(history(cid)):
        names = ", ".join(rule_name(x) for x in r["rules"]) or "None"
        rows += (f"<tr><td>{fmt_day(run)}</td><td>{r['score']:.0f}</td><td>{chip(r['tier'])}</td>"
                 f"<td style='text-align:left'>{esc(names)}</td></tr>")
    st.html(f"<table class='t'><tr><th>Nightly run</th><th>Score</th><th>Tier</th>"
            f"<th style='text-align:left'>Rules fired</th></tr>{rows}</table>")


def _txns_card(cid, tx):
    if tx.empty:
        st.caption("No transactions in this window.")
        return
    pv = tx.sort_values("ts", ascending=False)
    ours = set(demo().snap["accounts"])
    st.dataframe(pd.DataFrame({
        "Time": pv.ts.map(fmt_ts), "Account": pv.account, "Direction": pv.direction.map({"in": "In", "out": "Out"}),
        "Counterparty": [("Own account " + a) if i else c for c, a, i in zip(pv.cp_entity, pv.cp_account, pv.internal)],
        "Counterparty is": ["Same customer" if i else "Our customer" if c in ours else f"Bank {b}"
                            for c, b, i in zip(pv.cp_entity, pv.cp_bank, pv.internal)],
        "Amount (USD)": pv.amount, "Paid in": [f"{a:,.2f} {c}" for a, c in zip(pv.orig_amount, pv.currency)],
        "Format": pv["format"]}), hide_index=True, width="stretch", height=min(360, 40 + 35 * len(pv)),
        column_config={"Amount (USD)": st.column_config.NumberColumn(format="dollar")},
        alt="This customer's transactions across all accounts in the 7-day window")
    st.caption(f"{len(pv)} transactions across all of the customer's accounts, newest first.")


def _decision_card(cid, d):
    me, claimed_by = analyst(), d["analyst"] if d and d["decision"] == "claimed" else None
    if claimed_by and claimed_by != me:
        st.info(f"In review by {claimed_by}. Only they can record the decision.")
        return
    labels = {"edd_needed": "EDD needed", "cleared": "Clear for now"}
    choice = st.radio("Decision", list(labels), format_func=labels.get, index=None, key=f"out_{cid}",
                      label_visibility="collapsed")
    if choice == "cleared":
        st.segmented_control("Clear for", CLEAR_DAYS, default=CLEAR_DAYS[0], key=f"days_{cid}",
                             format_func=lambda n: f"{n} days")
        st.caption("7 days covers exactly the activity you reviewed. Choose longer only for a pattern you have "
                   "confirmed as stable and legitimate. A new rule or a jump in score brings the customer back early.")
    elif choice == "edd_needed":
        st.caption("Moves the customer to the EDD process and out of this queue. They are still scored every night.")
    st.text_area("Comment", key=f"note_{cid}", height=90, placeholder="What you checked and why you decided this")
    st.button("Submit and open next", key="btn_submit", type="primary", width="stretch",
              on_click=_on_submit, args=(cid,))


def _reviewer_bar(cid, d):
    """Who is working on this customer, so two analysts don't review the same one."""
    by = d["analyst"] if d and d["decision"] == "claimed" else None
    with st.container(key="reviewer_bar"):
        a, b = st.columns([6, 1.6], vertical_alignment="center")
        if by and by == analyst():
            a.markdown("**You are reviewing this customer.** Other analysts see it as taken.")
            b.button("Stop reviewing", key="btn_release", on_click=_on_release, args=(cid,), width="stretch")
        elif by:
            a.markdown(f"**{by} is reviewing this customer.** You can look, but only they can record the decision.")
        else:
            a.markdown("**No one is reviewing this customer yet.** Take it so other analysts don't pick it up too.")
            b.button("Start review", key="btn_claim", on_click=_on_claim, args=(cid,), width="stretch")


def customer_page(cid):
    r, d = demo().out[run_date()][cid], decisions().get(cid)
    order = st.session_state.get("order", [])
    pos = order.index(cid) if cid in order else None
    n = st.columns([2, 5.4, 1.1, 1], vertical_alignment="center")
    n[0].button("Back to queue", icon=":material/arrow_back:", key="btn_back", on_click=_go, args=(None,),
                width="stretch")
    if pos is not None:
        n[1].caption(f"{pos + 1} of {len(order)} in the queue")
        n[2].button("Previous", key="btn_prev", disabled=pos == 0, width="stretch",
                    on_click=_go, args=(order[pos - 1] if pos else None,))
        n[3].button("Next", key="btn_next", disabled=pos >= len(order) - 1, width="stretch",
                    on_click=_go, args=(order[pos + 1] if pos < len(order) - 1 else None,))
    flash()
    change = "" if r["change"] is None or abs(r["change"]) < 0.5 else \
        f", <span class='{'up' if r['change'] > 0 else 'down'}'>{r['change']:+.0f} since the last run</span>"
    window = demo().snap["runs"][run_date()]["window_start"]
    st.html(f"""<div class="ahead"><div><h2>Customer {esc(cid)}</h2>{chip(r['tier'])}
<span class="big">Score {r['score']:.0f}{change}</span></div>
<div class="facts"><span>{esc(r['type'])}</span><span>Accounts with us <b>{r['n_accounts']}</b></span>
<span>Activity from <b>{fmt_day(window)}</b> to <b>{fmt_day(run_date())}</b></span>
{f"<span>In the queue because <b>{esc(r['queue_reason'][0].lower() + r['queue_reason'][1:])}</b></span>" if r['queue_reason'] else ''}</div></div>""")
    prev = d if d and d["decision"] == "cleared" else (d or {}).get("previous")
    if prev and prev.get("decision") == "cleared":
        st.html(f"<div class='note'>Cleared by {esc(prev['analyst'])} on {fmt_day(prev['at'])} until "
                f"{fmt_day(prev['cleared_until'])}, at a score of {prev['snapshot']['score']:.0f}."
                + (f" Comment: {esc(prev['note'])}" if prev.get("note") else "") + "</div>")
    _reviewer_bar(cid, d)
    tx = customer_txns(cid)
    left, right = st.columns([7, 3.4], gap="medium")
    with left:
        with st.container(border=True, key=f"card_pic_{cid}"):
            st.markdown("##### This customer's money")
            _picture_card(r, tx)
        with st.container(border=True, key=f"card_rules_{cid}"):
            st.markdown("##### Why it scored")
            _rules_card(r, prev)
        with st.container(border=True, key=f"card_acct_{cid}"):
            st.markdown("##### Accounts combined")
            _accounts_card(r, tx)
        with st.container(border=True, key=f"card_hist_{cid}"):
            st.markdown("##### Score history")
            _history_card(cid)
        with st.container(border=True, key=f"card_tx_{cid}"):
            st.markdown("##### Transactions in the window")
            _txns_card(cid, tx)
    with right:
        with st.container(border=True, key=f"card_dec_{cid}"):
            st.markdown("##### Decision")
            _decision_card(cid, d)


# -------------------------------------------------------------- reviewed ----
def _on_reopen(cid):
    with demo().lock:
        demo().reopen(cid, analyst() or "unknown")
    if cid in demo().out[run_date()] and demo().out[run_date()][cid]["in_queue"]:
        st.session_state["open"] = cid
        st.session_state["goto"] = "Queue"
    else:
        st.session_state["flash"] = ("info", f"{cid} is reopened. It returns to the queue if tonight's run puts it there.")


def page_reviewed():
    st.title("Reviewed")
    st.caption("The current decision for each customer. Tonight's run reads these to decide who is in tomorrow's queue.")
    flash()
    ds = {c: d for c, d in decisions().items() if d["decision"] != "claimed"}
    if not ds:
        st.info("No decisions yet. Open a customer from the queue to record one.")
        return
    items = sorted(ds.items(), key=lambda kv: kv[1]["at"], reverse=True)
    c = st.columns(4)
    c[0].metric("Decisions", len(items))
    c[1].metric("EDD needed", sum(d["decision"] == "edd_needed" for _, d in items))
    c[2].metric("Cleared", sum(d["decision"] == "cleared" for _, d in items))
    ev = st.dataframe(pd.DataFrame({
        "Customer": [c for c, _ in items], "Decision": [DECISION[d["decision"]] for _, d in items],
        "Cleared until": [fmt_day(d["cleared_until"]) if d["cleared_until"] else "" for _, d in items],
        "Decided": [fmt_day(d["at"]) for _, d in items], "By": [d["analyst"] for _, d in items],
        "Score at review": [round(d["snapshot"]["score"]) for _, d in items],
        "Rules at review": [", ".join(rule_name(x) for x in d["snapshot"]["rules"]) for _, d in items],
        "Comment": [d.get("note") or "" for _, d in items]}),
        hide_index=True, width="stretch", on_select="rerun", selection_mode="single-row", key="reviewed_table",
        alt="Current decision for each reviewed customer")
    st.caption("Select a row to reopen it.")
    if ev.selection.rows and ev.selection.rows[0] < len(items):
        cid = items[ev.selection.rows[0]][0]
        st.button(f"Reopen {cid} for review", key="re_open", on_click=_on_reopen, args=(cid,))


# -------------------------------------------------------------- overview ----
def _bars(df, x, y, title, colour):
    return alt.Chart(df).mark_bar(color=colour, cornerRadiusEnd=3).encode(
        y=alt.Y(f"{y}:N", sort=None, title=None, axis=alt.Axis(labelColor=INK, labelLimit=260, domain=False, ticks=False)),
        x=alt.X(f"{x}:Q", title=title, axis=alt.Axis(gridColor="#1F283A", labelColor=MUTED, titleColor=MUTED,
                                                    domain=False, ticks=False, tickMinStep=1)),
        tooltip=[alt.Tooltip(f"{y}:N", title="Rule"), alt.Tooltip(f"{x}:Q", title=title)]
    ).properties(height=max(120, 30 * len(df))).configure(font="IBM Plex Sans", background="transparent"
                                                           ).configure_view(strokeWidth=0)


def _on_next_night():
    with demo().lock:
        moved = demo().next_night()
    st.session_state["open"] = None
    st.session_state["flash"] = ("success", f"Ran the night of {fmt_day(demo().run)}. The queue is rebuilt from the new "
                                 "scores and the decisions recorded so far.") if moved else \
        ("info", "That was the last night in the demo data.")


def _on_restart():
    with demo().lock:
        demo().reset()
    for k in [k for k in st.session_state if k.startswith(("open", "order", "shown"))]:
        del st.session_state[k]


def page_overview():
    st.title("Overview")
    st.caption(f"The nightly run for {fmt_day(run_date())}: how big the review workload is, and whether the rules "
               "are pulling in the right customers.")
    flash()
    rows = scores()
    fired = [r for r in rows if r["points"]]
    c = st.columns(4)
    c[0].metric("Customers scored", f"{len(rows):,}")
    c[1].metric("With a rule fired", len(fired))
    c[2].metric("In the queue", sum(r["in_queue"] for r in rows))
    c[3].metric("High tier", sum(r["tier"] == "High" for r in rows))

    per_rule, per_tier = rule_eval()
    left, right = st.columns(2, gap="large")
    with left:
        st.markdown("##### Which rules fire most")
        df = pd.DataFrame([{"Rule": p["name"], "Customers": p["customers"]} for p in per_rule if p["customers"]])
        df = df.sort_values("Customers", ascending=False)
        st.altair_chart(_bars(df, "Customers", "Rule", "Customers", "#3987e5"), width="stretch")
    with right:
        st.markdown("##### Scores of customers with a rule fired")
        bins = pd.cut([r["score"] for r in fired], bins=range(0, int(max(r["score"] for r in fired)) + 11, 10),
                      right=False)
        hist = bins.value_counts().sort_index()
        hd = pd.DataFrame({"Score": [f"{int(i.left)} to {int(i.right) - 1}" for i in hist.index],
                           "Customers": hist.values})
        st.altair_chart(_bars(hd, "Customers", "Score", "Customers", "#d95926"), width="stretch")
        st.caption(f"The other {len(rows) - len(fired):,} customers fired no rule and score under 1.")

    st.markdown("##### Are the rules too noisy?")
    st.caption("For each rule: how many customers it fired on, and what share of them had a transaction labelled as "
               "laundering in the dataset. The labels are used only for this check, never for scoring.")
    st.dataframe(pd.DataFrame({
        "Rule": [p["name"] for p in per_rule], "Points": [p["points"] for p in per_rule],
        "Customers fired on": [p["customers"] for p in per_rule],
        "Share involved in laundering": [None if p["precision"] is None else p["precision"] * 100 for p in per_rule],
        "What it looks for": [demo().rules[p["rule"]][2] for p in per_rule],
        "MAS red flag": [demo().rules[p["rule"]][3] for p in per_rule]}),
        hide_index=True, width="stretch", alt="Each rule, how often it fired and how precise it was",
        column_config={"Share involved in laundering": st.column_config.NumberColumn(format="%.0f%%")})
    t = st.columns(3)
    for col, p in zip(t, per_tier):
        col.metric(f"{p['tier']} tier involved in laundering",
                   "-" if p["precision"] is None else f"{p['precision']:.0%}", help=f"{p['customers']:,} customers")

    if not DEMO:
        st.caption("The next nightly run rebuilds the queue from new scores and the decisions recorded so far.")
        return
    st.markdown("##### Demo")
    a, b, c2 = st.columns([3, 1.3, 1.3], vertical_alignment="center")
    last = demo().i >= len(demo().runs) - 1
    a.caption(f"Scores are from the night of {fmt_day(run_date())}. Running the next night re-scores every customer "
              "and rebuilds the queue using the decisions recorded so far: cleared customers drop out unless a "
              "new rule fires, and customers marked EDD needed raise the score of those who transact with them.")
    b.button("Run the next night", key="btn_night", type="primary", width="stretch", disabled=last,
             on_click=_on_next_night, help="No more nights in the demo data." if last else None)
    c2.button("Restart the demo", key="btn_restart", width="stretch", on_click=_on_restart)


# ----------------------------------------------------------------- main ----
def main():
    st.set_page_config(page_title="KYC review queue", page_icon=":material/verified_user:", layout="wide")
    st.html(CSS)
    if DEMO and not SNAPSHOT.exists():
        st.error("demo_snapshot.json is missing. Build it with: python build_demo_snapshot.py")
        return
    if not DEMO:
        try:
            ready = bool(demo().runs)
        except Exception as exc:
            st.error(f"Could not reach AWS: {exc}")
            return
        if not ready:
            st.info("No nightly run has finished yet. Start one with: python main/scripts/pipeline.py q2 "
                    "(or run the producer with --q2 async).")
            return
    pages = {"Queue": st.Page(page_queue, title="Priority queue", icon=":material/format_list_numbered:",
                              url_path="queue", default=True),
             "Reviewed": st.Page(page_reviewed, title="Reviewed", icon=":material/task_alt:", url_path="reviewed"),
             "Overview": st.Page(page_overview, title="Overview", icon=":material/monitoring:", url_path="overview")}
    page = st.navigation(list(pages.values()))
    goto = st.session_state.pop("goto", None)
    if goto:
        st.switch_page(pages[goto])
    with st.sidebar:
        with st.container(key="side_bottom"):
            st.text_input("Analyst", key="analyst", value="Demo analyst" if DEMO else "",
                          help="Recorded with every claim and decision you make.")
            st.caption(f"Scores from the night of {fmt_day(run_date())}")
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
        print("This is a Streamlit app. Start it with:\n  streamlit run q2_streamlit.py")
