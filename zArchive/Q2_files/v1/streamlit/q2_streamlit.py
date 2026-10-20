"""Q2 KYC customer risk dashboard - single-file Streamlit app.

Run (same as the Week 5 lab, on EC2 or your laptop):
    pip install streamlit boto3 pandas
    export S3_BUCKET=<your-bucket> BANK_ID=12          # Windows: set S3_BUCKET=... / set BANK_ID=12
    streamlit run q2_streamlit.py --server.address 0.0.0.0 --server.port 8501
Demo data (no AWS needed):
    DEMO_MODE=1 streamlit run q2_streamlit.py

Optional env vars: ATHENA_DATABASE (aml_q2), ATHENA_WORKGROUP (primary), AWS_REGION (us-east-1),
WINDOW_DAYS (7, must match score.py), BANK_NAME.

Data loaders return pandas DataFrames and are cached for a few minutes. With DEMO_MODE on,
the same loaders return generated data in the pipeline's exact schemas.

Ground-truth labels (is_laundering, validation_*) are only used on the Rule scorecard tab.

Dates: run dates are SIMULATED (2022). Reviewer suppressions are measured in simulated days
from the run they were made on, never against today's real date.

streamlit run q2_streamlit.py --server.address 0.0.0.0 --server.port 8501
"""
import hashlib
import json
import os
import re
import time
from datetime import date, datetime, timedelta, timezone
from functools import lru_cache
from pathlib import Path

import numpy as np
import pandas as pd
import streamlit as st


def _flag(name):
    return os.getenv(name, "").strip().lower() in {"1", "true", "yes", "on"}


DEMO_MODE = _flag("DEMO_MODE")
DATABASE = os.getenv("ATHENA_DATABASE", "aml_q2")
WORKGROUP = os.getenv("ATHENA_WORKGROUP", "primary")
S3_BUCKET = os.getenv("S3_BUCKET", "")
REGION = os.getenv("AWS_REGION", "us-east-1")
BANK_ID = os.getenv("BANK_ID", "12").strip()
BANK_NAME = os.getenv("BANK_NAME", "").strip()

OVERRIDES_PREFIX = "state/overrides/"
WINDOW_DAYS = int(os.getenv("WINDOW_DAYS", "7"))      # must match score.py --window-days
DISMISS_DAYS = 30                                     # simulated days
TTL = 300            # seconds; Athena results
TTL_OVERRIDES = 30   # seconds; reviewer decisions should show up quickly

TIERS = ["High", "Medium", "Low"]
ENTITY_TYPES = ["Corporation", "Partnership", "Sole Proprietorship", "Other"]   # score.py merges small types into Other
TYPOLOGIES = ["FAN-IN", "FAN-OUT", "GATHER-SCATTER", "SCATTER-GATHER",
              "CYCLE", "STACK", "BIPARTITE", "RANDOM"]

# Rule catalogue - names and points match score.py exactly, highest points first.
# `plain` is the reviewer-facing wording. `targets` is the typologies each rule was
# designed to catch (design intent, not measured).
RULES = {
    "R1": dict(name="Rapid pass-through", points=25,
               plain="Sends on almost all incoming money within two days",
               targets=["STACK", "CYCLE", "GATHER-SCATTER", "SCATTER-GATHER"]),
    "R2": dict(name="Round-trip (U-turn)", points=20,
               plain="Money sent out comes back from the same counterparty",
               targets=["CYCLE"]),
    "R3": dict(name="Cash structuring", points=20,
               plain="Several cash payments just under the $10k reporting threshold",
               targets=[]),
    "R4": dict(name="Fan-in", points=15,
               plain="Collects money from an unusually large number of senders",
               targets=["FAN-IN", "GATHER-SCATTER", "SCATTER-GATHER"]),
    "R5": dict(name="Fan-out", points=15,
               plain="Pays out to an unusually large number of recipients",
               targets=["FAN-OUT", "GATHER-SCATTER", "SCATTER-GATHER"]),
    "R6": dict(name="Linked to High-risk customers", points=15,
               plain="Deals with customers rated High in the last run",
               targets=["BIPARTITE", "STACK"]),
    "R7": dict(name="New-counterparty burst", points=10,
               plain="Most recent counterparties have never been seen before",
               targets=["FAN-IN", "FAN-OUT", "BIPARTITE"]),
    "R8": dict(name="Activity spike", points=10,
               plain="Activity jumped sharply against the customer's own history",
               targets=["FAN-IN", "FAN-OUT", "RANDOM"]),
    "R9": dict(name="Cash/crypto concentration", points=10,
               plain="Most value moves in cash or Bitcoin",
               targets=["RANDOM"]),
    "R13": dict(name="Own-account shuffling", points=10,
                plain="Moves money back and forth between own accounts",
                targets=["STACK"]),
}
_RULE_BY_NAME = {v["name"]: k for k, v in RULES.items()}

SCORE_COLS = ["run_date", "entity_id", "entity_type", "n_accounts", "score", "score_percentile",
              "tier", "prev_tier", "moved_to_high", "reason_1", "reason_2", "inflow_usd",
              "outflow_usd", "pass_through_ratio", "median_hours_to_forward", "distinct_senders",
              "distinct_recipients", "new_counterparty_share", "spike_ratio", "cash_crypto_share",
              "own_account_transfers", "high_risk_counterparties"]
HIT_COLS = ["run_date", "entity_id", "rule_id", "rule_name", "points", "evidence"]
# is_laundering is deliberately absent: reviewer pages must never see labels
TXN_COLS = ["txn_ts", "from_bank", "from_account", "to_bank", "to_account", "amount_paid_usd",
            "amount_received_usd", "payment_currency", "receiving_currency", "payment_format",
            "from_entity_id", "to_entity_id"]
OVERRIDE_COLS = ["entity_id", "decision", "note", "reviewer", "decided_at",
                 "decided_on_run_date", "suppress_until"]

# validate.py metric name -> older dashboard name. Both are returned, so pages can use either.
_METRIC_ALIASES = {
    "customers_scored": "n_customers",
    "laundering_customers": "n_laundering_customers",
    "capture_rate_high": "capture_top_1pct",
    "capture_rate_high_or_medium": "capture_top_10pct",
    "lift_high_vs_random": "lift_top_1pct",
    "lift_high_or_medium_vs_random": "lift_top_10pct",
}

_KNOWN_BANKS = {"1": "Arbor Savings Bank", "11": "Finland Bank #0", "12": "National Bank of the East",
                "20": "China Bank #6", "70": "Oasis Thrift", "119": "Israel Bank #6"}


def bank_label():
    name = BANK_NAME or _KNOWN_BANKS.get(BANK_ID.lstrip("0") or "0", "")
    return f"{name} · Bank {BANK_ID}" if name else f"Bank {BANK_ID}"


def same_bank(series):
    """True where a bank column is our bank. Compares as integers: raw IDs carry leading zeros."""
    return pd.to_numeric(series, errors="coerce") == _bank_int()


def reason_rule_id(reason):
    """Rule ID for a reason string. Reasons look like 'Fan-in: Received from 24 different senders'."""
    if not reason:
        return None
    text = str(reason)
    if text in RULES:
        return text
    return _RULE_BY_NAME.get(text.split(":", 1)[0].strip())


def reason_plain(reason):
    """Reviewer wording for a reason, keeping the pipeline's evidence when there is any."""
    rid = reason_rule_id(reason)
    if rid is None:
        return reason
    evidence = str(reason).split(":", 1)[1].strip() if ":" in str(reason) else ""
    return f"{RULES[rid]['plain']} ({evidence})" if evidence else RULES[rid]["plain"]


# ---------------------------------------------------------------- athena ----
def _bank_int():
    try:
        return int(BANK_ID)
    except ValueError:
        raise ValueError(f"BANK_ID must be a number, got {BANK_ID!r}") from None


def _d(value):
    return date.fromisoformat(str(value)[:10]).isoformat()


def _e(value):
    if not re.fullmatch(r"[A-Za-z0-9_\-]{1,64}", str(value)):
        raise ValueError(f"Invalid entity_id: {value!r}")
    return str(value)


def _window_start(run_date):
    return (date.fromisoformat(_d(run_date)) - timedelta(days=WINDOW_DAYS - 1)).isoformat()


def _session():
    import boto3
    return boto3.Session(region_name=REGION)


def _athena(sql):
    """Run a query and read its CSV result from S3 - only boto3 and pandas needed."""
    if not S3_BUCKET:
        raise ValueError("S3_BUCKET is not set. Set it to your bucket name, or run with DEMO_MODE=1.")
    session = _session()
    athena = session.client("athena")
    qid = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": DATABASE},
        WorkGroup=WORKGROUP,
        ResultConfiguration={"OutputLocation": f"s3://{S3_BUCKET}/athena-results/"},
    )["QueryExecutionId"]
    while True:
        q = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
        state = q["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(0.4)
    if state != "SUCCEEDED":
        raise RuntimeError(f"Athena query {state.lower()}: {q['Status'].get('StateChangeReason', '')}")
    bucket, key = q["ResultConfiguration"]["OutputLocation"][len("s3://"):].split("/", 1)
    body = session.client("s3").get_object(Bucket=bucket, Key=key)["Body"]
    return _normalise(pd.read_csv(body, dtype=str, keep_default_na=False, na_values=[""]))


_NUMERIC = {"n_accounts", "score", "score_percentile", "inflow_usd", "outflow_usd",
            "pass_through_ratio", "median_hours_to_forward", "distinct_senders",
            "distinct_recipients", "new_counterparty_share", "spike_ratio", "cash_crypto_share",
            "own_account_transfers", "high_risk_counterparties", "points", "n", "value",
            "amount_paid_usd", "amount_received_usd", "n_laundering_customers", "caught_high",
            "caught_high_or_medium", "n_flagged", "n_laundering", "hits", "laundering_hits", "precision"}


def _normalise(df):
    """Plain numpy dtypes whatever the table's storage types are."""
    df = df.copy()
    for c in df.columns:
        if c in _NUMERIC:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype(float)
        elif c == "moved_to_high":
            df[c] = df[c].astype(str).str.lower().isin({"true", "1"})
        elif c == "txn_ts":
            df[c] = pd.to_datetime(df[c], errors="coerce")
        else:
            df[c] = df[c].astype(object).where(df[c].notna(), None)
            if c in {"run_date", "first_high"}:
                df[c] = df[c].astype(str).str[:10]
    # score.py writes the percentile as 0-1; the dashboard shows 0-100
    if "score_percentile" in df.columns and len(df) and df["score_percentile"].max() <= 1.0:
        df["score_percentile"] = df["score_percentile"] * 100
    return df


# ------------------------------------------------------- reviewer loaders ----
@st.cache_data(ttl=TTL, show_spinner=False)
def run_dates():
    """All run dates, oldest first."""
    if DEMO_MODE:
        return list(_demo()["runs"])
    df = _athena("SELECT DISTINCT CAST(run_date AS varchar) AS run_date FROM customer_scores ORDER BY 1")
    return df["run_date"].tolist()


def latest_run_date():
    runs = run_dates()
    return runs[-1] if runs else None


@st.cache_data(ttl=TTL, show_spinner=False)
def customer_scores(run_date):
    if DEMO_MODE:
        s = _demo()["scores"]
        return s[s.run_date == run_date].reset_index(drop=True)
    cols = ", ".join(c for c in SCORE_COLS if c != "run_date")
    return _athena(f"SELECT CAST(run_date AS varchar) AS run_date, {cols} FROM customer_scores "
                   f"WHERE CAST(run_date AS varchar) = '{_d(run_date)}'")


@st.cache_data(ttl=TTL, show_spinner=False)
def score_history(entity_id):
    if DEMO_MODE:
        s = _demo()["scores"]
        return s[s.entity_id == entity_id][["run_date", "score", "score_percentile", "tier"]].reset_index(drop=True)
    return _athena("SELECT CAST(run_date AS varchar) AS run_date, score, score_percentile, tier "
                   f"FROM customer_scores WHERE entity_id = '{_e(entity_id)}' ORDER BY 1")


@st.cache_data(ttl=TTL, show_spinner=False)
def tier_mix():
    """Customer counts per run, entity type and tier."""
    if DEMO_MODE:
        return (_demo()["scores"].groupby(["run_date", "entity_type", "tier"]).size()
                .rename("n").reset_index().astype({"n": float}))
    return _athena("SELECT CAST(run_date AS varchar) AS run_date, entity_type, tier, COUNT(*) AS n "
                   "FROM customer_scores GROUP BY 1, 2, 3")


@st.cache_data(ttl=TTL, show_spinner=False)
def first_high_dates():
    """First run in which each customer was tiered High."""
    if DEMO_MODE:
        s = _demo()["scores"]
        return s[s.tier == "High"].groupby("entity_id").run_date.min().rename("first_high").reset_index()
    return _athena("SELECT entity_id, MIN(CAST(run_date AS varchar)) AS first_high "
                   "FROM customer_scores WHERE tier = 'High' GROUP BY 1")


@st.cache_data(ttl=TTL, show_spinner=False)
def rule_hits(run_date):
    if DEMO_MODE:
        h = _demo()["hits"]
        return h[h.run_date == run_date].reset_index(drop=True)
    return _athena("SELECT CAST(run_date AS varchar) AS run_date, entity_id, rule_id, rule_name, points, evidence "
                   f"FROM rule_hits WHERE CAST(run_date AS varchar) = '{_d(run_date)}'")


@st.cache_data(ttl=TTL, show_spinner=False)
def entity_rule_hits(entity_id):
    if DEMO_MODE:
        h = _demo()["hits"]
        return h[h.entity_id == entity_id].reset_index(drop=True)
    return _athena("SELECT CAST(run_date AS varchar) AS run_date, entity_id, rule_id, rule_name, points, evidence "
                   f"FROM rule_hits WHERE entity_id = '{_e(entity_id)}'")


@st.cache_data(ttl=TTL, show_spinner=False)
def rule_hit_counts():
    """Customers hit per rule per run."""
    if DEMO_MODE:
        return (_demo()["hits"].groupby(["run_date", "rule_id", "rule_name"]).entity_id.nunique()
                .rename("n").reset_index().astype({"n": float}))
    return _athena("SELECT CAST(run_date AS varchar) AS run_date, rule_id, MAX(rule_name) AS rule_name, "
                   "COUNT(DISTINCT entity_id) AS n FROM rule_hits GROUP BY 1, 2")


@st.cache_data(ttl=TTL, show_spinner=False)
def transactions(entity_id, run_date):
    """The customer's transactions seen by our bank in the scoring window ending on run_date."""
    if DEMO_MODE:
        return _demo_transactions(entity_id, run_date)
    e, bank = _e(entity_id), _bank_int()
    return _athena(f"""
        SELECT {", ".join(TXN_COLS)}
        FROM curated_transactions
        WHERE CAST("date" AS varchar) BETWEEN '{_window_start(run_date)}' AND '{_d(run_date)}'
          AND (from_entity_id = '{e}' OR to_entity_id = '{e}')
          AND (TRY_CAST(from_bank AS bigint) = {bank} OR TRY_CAST(to_bank AS bigint) = {bank})
        ORDER BY txn_ts DESC""")


def annotate_transactions(tx, entity_id):
    """Add the customer's point of view: direction, own account, counterparty."""
    tx = tx.copy()
    out = tx.from_entity_id == entity_id
    inn = tx.to_entity_id == entity_id
    tx["direction"] = np.select([out & inn, out, inn], ["Own accounts", "Out", "In"], default="Other")
    tx["our_account"] = np.where(out, tx.from_account, tx.to_account)
    tx["counterparty_entity"] = np.where(out, tx.to_entity_id, tx.from_entity_id)
    tx["counterparty_bank"] = np.where(out, tx.to_bank, tx.from_bank)
    tx["counterparty_account"] = np.where(out, tx.to_account, tx.from_account)
    tx["amount_usd"] = np.where(out, tx.amount_paid_usd, tx.amount_received_usd)
    return tx


def accounts_at_bank(tx, entity_id):
    """The customer's accounts at our bank, as seen in the window's transactions."""
    sent = tx[(tx.from_entity_id == entity_id) & same_bank(tx.from_bank)]
    recv = tx[(tx.to_entity_id == entity_id) & same_bank(tx.to_bank)]
    legs = pd.concat([
        pd.DataFrame({"account": sent.from_account, "ts": sent.txn_ts, "out_usd": sent.amount_paid_usd, "in_usd": 0.0}),
        pd.DataFrame({"account": recv.to_account, "ts": recv.txn_ts, "out_usd": 0.0, "in_usd": recv.amount_received_usd}),
    ])
    if legs.empty:
        return pd.DataFrame(columns=["account", "transactions", "in_usd", "out_usd", "first_seen", "last_seen"])
    return (legs.groupby("account").agg(transactions=("ts", "size"), in_usd=("in_usd", "sum"),
                                        out_usd=("out_usd", "sum"), first_seen=("ts", "min"),
                                        last_seen=("ts", "max"))
            .reset_index().sort_values("transactions", ascending=False))


# -------------------------------------------------------------- overrides ----
_DEMO_OVERRIDES = Path(__file__).parent / ".demo_state" / "overrides.jsonl"


@st.cache_data(ttl=TTL_OVERRIDES, show_spinner=False)
def overrides():
    """Every reviewer decision ever recorded, oldest first."""
    records = []
    if DEMO_MODE:
        if _DEMO_OVERRIDES.exists():
            records = [json.loads(x) for x in _DEMO_OVERRIDES.read_text(encoding="utf-8").splitlines() if x.strip()]
    else:
        if not S3_BUCKET:
            raise ValueError("S3_BUCKET is not set, so reviewer decisions cannot be read.")
        s3 = _session().client("s3")
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=S3_BUCKET, Prefix=OVERRIDES_PREFIX):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith((".json", ".jsonl")):
                    body = s3.get_object(Bucket=S3_BUCKET, Key=obj["Key"])["Body"].read().decode("utf-8")
                    records += [json.loads(x) for x in body.splitlines() if x.strip()]
    df = pd.DataFrame(records, columns=OVERRIDE_COLS)
    return df.sort_values("decided_at").reset_index(drop=True)


def current_overrides(ov, as_of=None):
    """Latest decision per customer, with `suppressed` = still hidden from the queue on run `as_of`.
    `as_of` defaults to the latest run date (simulated), not today's real date."""
    cur = ov.drop_duplicates("entity_id", keep="last").copy()
    as_of = _d(as_of) if as_of else (latest_run_date() or date.today().isoformat())
    cur["suppressed"] = cur.suppress_until.fillna("").astype(str).str[:10] >= as_of
    return cur


def save_override(entity_id, decision, note, reviewer, run_date=None):
    """Append one decision record. decision is 'dismiss' or 'escalate_edd'.
    A dismissal hides the customer for DISMISS_DAYS simulated days from the run it was made on."""
    now = datetime.now(timezone.utc)
    run = _d(run_date) if run_date else (latest_run_date() or now.date().isoformat())
    record = {
        "entity_id": _e(entity_id),
        "decision": decision,
        "note": note.strip(),
        "reviewer": reviewer.strip(),
        "decided_at": now.isoformat(timespec="seconds"),
        "decided_on_run_date": run,
        "suppress_until": ((date.fromisoformat(run) + timedelta(days=DISMISS_DAYS)).isoformat()
                           if decision == "dismiss" else None),
    }
    line = json.dumps(record) + "\n"
    if DEMO_MODE:
        _DEMO_OVERRIDES.parent.mkdir(exist_ok=True)
        with open(_DEMO_OVERRIDES, "a", encoding="utf-8") as f:
            f.write(line)
    else:
        if not S3_BUCKET:
            raise ValueError("S3_BUCKET is not set, so the decision cannot be saved.")
        # S3 has no append: one small object per decision
        key = f"{OVERRIDES_PREFIX}{now:%Y%m%dT%H%M%S%f}_{record['entity_id']}.json"
        _session().client("s3").put_object(Bucket=S3_BUCKET, Key=key, Body=line.encode("utf-8"),
                                           ContentType="application/json")
    overrides.clear()
    return record


# ------------------------------------------- validation (Scorecard page only) ----
def _with_metric_aliases(df):
    """Return validate.py's metric names plus the older dashboard names, whichever exist."""
    if df.empty:
        return df
    rows = [df]
    for new, old in _METRIC_ALIASES.items():
        for src, dst in ((new, old), (old, new)):
            have = df[df.metric == src]
            if not have.empty and (df.metric == dst).sum() == 0:
                rows.append(have.assign(metric=dst))
    return pd.concat(rows, ignore_index=True)


@st.cache_data(ttl=TTL, show_spinner=False)
def validation_summary(run_date):
    if DEMO_MODE:
        v = _demo()["val_summary"]
        v = v[v.run_date == run_date].reset_index(drop=True)
    else:
        v = _athena("SELECT CAST(run_date AS varchar) AS run_date, metric, value FROM validation_summary "
                    f"WHERE CAST(run_date AS varchar) = '{_d(run_date)}'")
    return _with_metric_aliases(v)


@st.cache_data(ttl=TTL, show_spinner=False)
def validation_typology(run_date):
    if DEMO_MODE:
        v = _demo()["val_typology"]
        v = v[v.run_date == run_date].reset_index(drop=True)
    else:
        v = _athena("SELECT CAST(run_date AS varchar) AS run_date, typology, n_laundering_customers, "
                    "caught_high, caught_high_or_medium FROM validation_typology "
                    f"WHERE CAST(run_date AS varchar) = '{_d(run_date)}'")
    v = v.copy()
    v["typology"] = v["typology"].astype(str).str.upper()     # validate.py writes 'Fan-In'
    return v


@st.cache_data(ttl=TTL, show_spinner=False)
def rule_validation(run_date):
    """Per rule: customers flagged, and how many of them touched a laundering transaction in the window."""
    if DEMO_MODE:
        v = _demo()["rule_val"]
        return v[v.run_date == run_date].drop(columns="run_date").reset_index(drop=True)
    v = _athena("SELECT rule_id, rule_name, hits AS n_flagged, laundering_hits AS n_laundering "
                f"FROM validation_rules WHERE CAST(run_date AS varchar) = '{_d(run_date)}'")
    v.insert(2, "points", v["rule_id"].map(lambda r: float(RULES.get(r, {}).get("points", np.nan))))
    return v


# ------------------------------------------------------------- demo data ----
def _seed(text):
    return int(hashlib.md5(text.encode()).hexdigest()[:8], 16)


@lru_cache(maxsize=1)
def _demo():
    """One coherent fake portfolio: features drive rule hits, hits drive scores, scores drive tiers.
    Output tables use exactly the pipeline's schemas and value formats."""
    rng = np.random.default_rng(459)
    n = 2500
    runs = [d.date().isoformat() for d in pd.date_range("2022-09-05", periods=10)]
    last = len(runs) - 1
    ids = np.array([f"80{v:06X}0" for v in rng.choice(0xFFFFFF, n, replace=False)])
    etype = rng.choice(ENTITY_TYPES, n, p=[.36, .34, .27, .03])
    n_acc = rng.choice([1, 2, 3, 4, 5, 6], n, p=[.80, .11, .05, .02, .01, .01])
    tiebreak = rng.random(n)

    # 60 laundering customers (hidden label) and 30 odd-but-legitimate ones (false positives)
    pick = rng.choice(n, 90, replace=False)
    laund = np.zeros(n, bool); laund[pick[:60]] = True
    odd = np.zeros(n, bool); odd[pick[60:]] = True
    prof = np.full(n, -1); prof[pick] = rng.integers(0, len(TYPOLOGIES), 90)
    start = np.zeros(n, int); start[pick[:60]] = rng.integers(0, len(runs), 60)
    start[pick[:6]] = last; prof[pick[:6]] = [2, 3, 4, 5, 2, 4]     # guaranteed movers in the latest run
    start[pick[6:12]] = last - 1
    stealth = laund & (rng.random(n) < .3)                           # laundering the rules miss
    stealth[pick[:12]] = False
    n_acc[(prof == TYPOLOGIES.index("STACK")) & (n_acc < 2)] = 2

    # persistent per-customer behaviour; each run adds a little jitter
    size = rng.lognormal(10.5, 1.2, n)
    b_ptr = np.clip(rng.normal(.5, .2, n), 0, 1.1)
    b_hours = rng.lognormal(np.log(110), .6, n)
    b_send, b_recv = rng.poisson(3, n) + 1, rng.poisson(3, n) + 1
    b_new, b_cash = rng.beta(2, 6, n), rng.beta(1, 10, n)
    b_own = np.where(n_acc > 1, rng.poisson(.8, n), 0)
    b_hrc, b_uturn = rng.poisson(.05, n), rng.binomial(1, .015, n)
    b_struct = np.where(rng.random(n) < .01, rng.integers(3, 6, n), 0)
    boost = rng.integers(10, 26, n)
    hot_ptr, hot_hours = rng.uniform(.92, 1.0, n), rng.uniform(3, 40, n)

    points = np.array([r["points"] for r in RULES.values()])
    rule_ids = list(RULES)
    scores, hits, val_summary, val_typology, rule_val = [], [], [], [], []
    prev_tier = None
    for r, run in enumerate(runs):
        on = (odd | (laund & (start <= r))) & ~stealth
        fresh = r <= start + 1
        inflow = size * rng.lognormal(0, .15, n)
        ptr = np.clip(b_ptr + rng.normal(0, .04, n), 0, 1.2)
        hours = b_hours * rng.lognormal(0, .15, n)
        senders = np.maximum(1, b_send + rng.integers(-1, 2, n))
        recips = np.maximum(1, b_recv + rng.integers(-1, 2, n))
        new_share = np.clip(b_new + rng.normal(0, .05, n), 0, 1)
        spike = rng.lognormal(0, .3, n)
        cashc = np.clip(b_cash + rng.normal(0, .03, n), 0, 1)
        own, hrc, uturn = b_own.copy(), b_hrc.copy(), b_uturn.copy()

        def m(name):
            return on & (prof == TYPOLOGIES.index(name))

        inflow[on] *= 2.5
        k = m("FAN-IN"); senders[k] += boost[k]; new_share[k] = np.maximum(new_share[k], .8); spike[k & fresh] *= 6
        k = m("FAN-OUT"); recips[k] += boost[k]; new_share[k] = np.maximum(new_share[k], .8); spike[k & fresh] *= 6
        k = m("GATHER-SCATTER"); senders[k] += boost[k]; recips[k] += boost[k]; ptr[k] = hot_ptr[k]; hours[k] = hot_hours[k]
        k = m("SCATTER-GATHER"); senders[k] += boost[k] // 2 + 5; recips[k] += boost[k] // 2 + 5; ptr[k] = hot_ptr[k]; hours[k] = hot_hours[k]
        k = m("CYCLE"); uturn[k] += 1; ptr[k] = hot_ptr[k]; hours[k] = hot_hours[k]
        k = m("STACK"); ptr[k] = hot_ptr[k]; hours[k] = hot_hours[k]; own[k] += 3; hrc[k] += 2
        k = m("BIPARTITE"); hrc[k] += 2; new_share[k] = np.maximum(new_share[k], .78); recips[k] += 6
        k = m("RANDOM"); spike[k & fresh] *= 5; cashc[k] = np.maximum(cashc[k], .6)
        n_cp = senders + recips
        n_new = np.round(new_share * n_cp).astype(int)

        # same conditions and evidence wording as score.py
        fired = {
            "R1": ((inflow >= 1e4) & (ptr >= .9) & (ptr <= 1.1) & (hours <= 48),
                   lambda i: f"{ptr[i] * 100:.0f}% of ${inflow[i]:,.0f} received was sent on, median {hours[i]:.1f}h later"),
            "R2": (uturn >= 1, lambda i: f"{uturn[i]} payment(s) came back from the same counterparty within 7 days"),
            "R3": (b_struct >= 3, lambda i: f"{b_struct[i]} cash payments of $8,000-9,999 totalling ${b_struct[i] * 9200:,.0f}"),
            "R4": (senders >= 10, lambda i: f"Received from {senders[i]} different senders"),
            "R5": (recips >= 10, lambda i: f"Paid {recips[i]} different recipients"),
            "R6": (hrc >= 2, lambda i: f"Transacts with {hrc[i]} customers rated High last run"),
            "R7": ((n_new >= 5) & (new_share >= .7), lambda i: f"{n_new[i]} of {n_cp[i]} recent counterparties are new"),
            "R8": ((spike >= 5) & (inflow >= 1e4),
                   lambda i: f"Daily value up {spike[i]:.1f}x on the previous days (${inflow[i]:,.0f} recently)"),
            "R9": ((cashc >= .5) & (inflow >= 1e4), lambda i: f"{cashc[i] * 100:.0f}% of ${inflow[i]:,.0f} moved in cash or Bitcoin"),
            "R13": (own >= 3, lambda i: f"{own[i]} transfers of $5k+ between own accounts"),
        }
        hitmat = np.column_stack([fired[rid][0] for rid in rule_ids])
        evidence = {}
        for j, rid in enumerate(rule_ids):
            idx = np.flatnonzero(hitmat[:, j])
            ev = [fired[rid][1](i) for i in idx]
            evidence.update({(i, rid): e for i, e in zip(idx, ev)})
            hits.append(pd.DataFrame({"run_date": run, "entity_id": ids[idx], "rule_id": rid,
                                      "rule_name": RULES[rid]["name"], "points": float(points[j]),
                                      "evidence": ev}))

        score = hitmat @ points + tiebreak                      # rules decide, tiebreak only breaks ties
        order = np.lexsort((-tiebreak, -score))
        n_high, n_med = int(np.ceil(.01 * n)), int(np.ceil(.10 * n))
        has_hit = hitmat.any(axis=1)
        tier = np.full(n, "Low", dtype=object)
        tier[order[:n_med]] = "Medium"
        tier[order[:n_high]] = "High"
        tier[~has_hit] = "Low"
        # RULES is ordered by points, so the first two hits are the top two reasons
        def reason(i, k):
            rids = [rule_ids[j] for j in np.flatnonzero(hitmat[i])]
            return f"{RULES[rids[k]]['name']}: {evidence[(i, rids[k])]}" if len(rids) > k else None
        rank = np.empty(n, int); rank[order] = np.arange(n)
        scores.append(pd.DataFrame({
            "run_date": run, "entity_id": ids, "entity_type": etype, "n_accounts": n_acc.astype(float),
            "score": score.round(4), "score_percentile": (1 - rank / n) * 100,
            "tier": tier, "prev_tier": prev_tier if prev_tier is not None else [None] * n,
            "moved_to_high": (tier == "High") & (prev_tier != "High") if prev_tier is not None else (tier == "High"),
            "reason_1": [reason(i, 0) for i in range(n)],
            "reason_2": [reason(i, 1) for i in range(n)],
            "inflow_usd": inflow.round(2), "outflow_usd": (inflow * ptr).round(2),
            "pass_through_ratio": ptr.round(3), "median_hours_to_forward": hours.round(1),
            "distinct_senders": senders.astype(float), "distinct_recipients": recips.astype(float),
            "new_counterparty_share": new_share.round(3), "spike_ratio": spike.round(2),
            "cash_crypto_share": cashc.round(3), "own_account_transfers": own.astype(float),
            "high_risk_counterparties": hrc.astype(float),
        }))
        prev_tier = tier

        # hidden labels -> validation tables (same metric names as validate.py)
        active = laund & (start <= r)
        hi, hm = tier == "High", tier != "Low"
        pos = int(active.sum())
        tp_h, tp_hm = int((active & hi).sum()), int((active & hm).sum())
        base = pos / n
        prec_h = tp_h / max(hi.sum(), 1)
        prec_hm = tp_hm / max(hm.sum(), 1)
        for metric, value in [("customers_scored", n), ("laundering_customers", pos),
                              ("high_count", hi.sum()), ("medium_count", hm.sum() - hi.sum()),
                              ("captured_high", tp_h), ("captured_high_or_medium", tp_hm),
                              ("capture_rate_high", tp_h / pos if pos else 0),
                              ("capture_rate_high_or_medium", tp_hm / pos if pos else 0),
                              ("precision_high", prec_h), ("precision_high_or_medium", prec_hm),
                              ("base_rate", base),
                              ("lift_high_vs_random", prec_h / base if base else 0),
                              ("lift_high_or_medium_vs_random", prec_hm / base if base else 0)]:
            val_summary.append((run, metric, float(value)))
        for t, name in enumerate(TYPOLOGIES):
            k = active & (prof == t)
            val_typology.append((run, name, float(k.sum()), float((k & hi).sum()), float((k & hm).sum())))
        for j, rid in enumerate(rule_ids):
            rule_val.append((run, rid, RULES[rid]["name"], float(points[j]),
                             float(hitmat[:, j].sum()), float((hitmat[:, j] & active).sum())))

    return {
        "runs": runs,
        "scores": pd.concat(scores, ignore_index=True)[SCORE_COLS],
        "hits": pd.concat(hits, ignore_index=True)[HIT_COLS],
        "val_summary": pd.DataFrame(val_summary, columns=["run_date", "metric", "value"]),
        "val_typology": pd.DataFrame(val_typology, columns=["run_date", "typology", "n_laundering_customers",
                                                            "caught_high", "caught_high_or_medium"]),
        "rule_val": pd.DataFrame(rule_val, columns=["run_date", "rule_id", "rule_name", "points",
                                                    "n_flagged", "n_laundering"]),
    }


_EXT_BANKS = ["1", "10", "11", "20", "22", "23", "119", "220", "1292", "21749"]


def _demo_transactions(entity_id, run_date):
    """Transactions consistent with the customer's scored features for that run."""
    s = _demo()["scores"]
    run = s[s.run_date == run_date]
    me = run[run.entity_id == entity_id]
    if me.empty:
        return pd.DataFrame(columns=TXN_COLS)
    me = me.iloc[0]
    rng = np.random.default_rng(_seed(entity_id + run_date))
    own = [f"8{v:08X}" for v in np.random.default_rng(_seed(entity_id)).integers(0, 16 ** 8, int(me.n_accounts))]
    others = run[run.entity_id != entity_id]
    all_ids, high_ids = others.entity_id.to_numpy(), others[others.tier == "High"].entity_id.to_numpy()

    def party(force_high):
        acct = f"8{rng.integers(0, 16 ** 8):08X}"
        if force_high and len(high_ids):
            return BANK_ID, acct, rng.choice(high_ids)
        if rng.random() < .35:
            return BANK_ID, acct, rng.choice(all_ids)
        return rng.choice([b for b in _EXT_BANKS if b != BANK_ID]), acct, f"80{rng.integers(0, 0xFFFFFF):06X}1"

    hr = int(me.high_risk_counterparties)
    senders = [party(i < (hr + 1) // 2) for i in range(min(int(me.distinct_senders), 60))]
    recips = [party(i < hr // 2) for i in range(min(int(me.distinct_recipients), 60))]
    both = int(.15 * min(len(senders), len(recips)))
    if both:
        recips[-both:] = senders[-both:]

    end = pd.Timestamp(run_date) + pd.Timedelta(days=1)
    rows = []

    def stamp():
        return end - pd.Timedelta(minutes=int(rng.integers(1, WINDOW_DAYS * 24 * 60)))

    def fmt():
        if rng.random() < me.cash_crypto_share:
            return rng.choice(["Cash", "Bitcoin"])
        return rng.choice(["ACH", "Cheque", "Credit Card", "Wire"], p=[.35, .35, .2, .1])

    def emit(parties, total, inbound):
        if not parties:
            return
        k = rng.integers(1, 4, len(parties))
        amounts = iter((rng.dirichlet(np.ones(k.sum())) * total).round(2))
        for (bank, acct, ent), reps in zip(parties, k):
            for _ in range(reps):
                amt, cur, mine = next(amounts), rng.choice(["US Dollar", "Euro", "Yuan"], p=[.8, .15, .05]), rng.choice(own)
                rows.append((stamp(), bank, acct, BANK_ID, mine, amt, amt, cur, cur, fmt(), ent, entity_id) if inbound
                            else (stamp(), BANK_ID, mine, bank, acct, amt, amt, cur, cur, fmt(), entity_id, ent))

    emit(senders, me.inflow_usd, True)
    emit(recips, me.outflow_usd, False)
    if len(own) > 1:
        for _ in range(int(me.own_account_transfers)):
            a, b = rng.choice(own, 2, replace=False)
            amt = round(float(rng.uniform(5000, 40000)), 2)
            rows.append((stamp(), BANK_ID, a, BANK_ID, b, amt, amt, "US Dollar", "US Dollar", "ACH", entity_id, entity_id))
    return pd.DataFrame(rows, columns=TXN_COLS).sort_values("txn_ts", ascending=False).reset_index(drop=True)


# =========================================================================== UI ====
import altair as alt   # ships with Streamlit

TIER_COLOURS = {"High": "#B42318", "Medium": "#B54708", "Low": "#98A2B3"}
NAVY = "#1F2A44"
EMPTY_SCORES = pd.DataFrame(columns=SCORE_COLS)
DECISION_LABEL = {"dismiss": "Dismissed", "escalate_edd": "Escalated to EDD"}


def _style():
    st.markdown(f"""
    <style>
      @import url('https://fonts.googleapis.com/css2?family=Public+Sans:wght@400;600;700&display=swap');
      html, body, .stApp, .stMarkdown, button, input, textarea, select, label {{
        font-family: 'Public Sans', 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
      }}
      h1, h2, h3 {{ color: {NAVY}; letter-spacing: -0.01em; }}
      .tier {{ display: inline-block; padding: 1px 10px; border-radius: 3px; color: #fff;
               font-weight: 600; font-size: 0.85rem; }}
      .tier-high {{ background: {TIER_COLOURS['High']}; }}
      .tier-medium {{ background: {TIER_COLOURS['Medium']}; }}
      .tier-low {{ background: {TIER_COLOURS['Low']}; }}
      .reason {{ border-left: 3px solid {NAVY}; padding: 4px 0 4px 12px; margin: 6px 0; }}
      .reason small {{ color: #475467; }}
      .labels-note {{ background: #F2F4F7; border-left: 3px solid #475467; padding: 8px 12px; }}
    </style>""", unsafe_allow_html=True)


def _badge(tier):
    tier = tier or "Low"
    return f"<span class='tier tier-{tier.lower()}'>{tier}</span>"


def _fmt_date(d):
    if not d:
        return "-"
    d = date.fromisoformat(str(d)[:10])
    return f"{d.day} {d:%b %Y}"   # no %-d: it fails on Windows


def _money(x):
    if x is None or pd.isna(x):
        return "-"
    x = float(x)
    if abs(x) >= 1e6:
        return f"${x / 1e6:,.1f}M"
    if abs(x) >= 1e3:
        return f"${x / 1e3:,.0f}k"
    return f"${x:,.0f}"


def _safe(fn, *args, default=None):
    try:
        return fn(*args)
    except Exception as exc:  # show the problem instead of a stack trace
        st.error(f"Could not load data ({fn.__name__}): {exc}")
        return default


def _tier_scale():
    return alt.Scale(domain=TIERS, range=[TIER_COLOURS[t] for t in TIERS])


def _style_tiers(df, col="Tier"):
    def colour(v):
        return f"color: {TIER_COLOURS.get(v, '#000')}; font-weight: 600"
    return df.style.map(colour, subset=[col])


def _latest_decisions(cur_ov):
    if cur_ov.empty:
        return {}
    return dict(zip(cur_ov.entity_id, cur_ov.decision))


# --------------------------------------------------------------- review queue ----
def tab_queue(run, scores, cur_ov):
    hidden = set(cur_ov.loc[cur_ov.suppressed & (cur_ov.decision == "dismiss"), "entity_id"]) if not cur_ov.empty else set()
    decisions = _latest_decisions(cur_ov)
    queue = scores[scores.tier.isin(["High", "Medium"])]
    visible = queue[~queue.entity_id.isin(hidden)]

    c = st.columns(4)
    c[0].metric("High", int((visible.tier == "High").sum()))
    c[1].metric("Medium", int((visible.tier == "Medium").sum()))
    c[2].metric("Newly High this run", int(visible.moved_to_high.sum()))
    c[3].metric("Dismissed and hidden", int(queue.entity_id.isin(hidden).sum()))

    hits = _safe(rule_hits, run, default=pd.DataFrame(columns=HIT_COLS))
    f = st.columns(3)
    tiers = f[0].multiselect("Tier", ["High", "Medium"], default=["High", "Medium"])
    types = f[1].multiselect("Customer type", sorted(visible.entity_type.dropna().unique()))
    rule_opts = [r for r in RULES if r in set(hits.rule_id)]
    rules = f[2].multiselect("Rule triggered", rule_opts, format_func=lambda r: f"{r} {RULES[r]['name']}")

    v = visible[visible.tier.isin(tiers)]
    if types:
        v = v[v.entity_type.isin(types)]
    if rules:
        v = v[v.entity_id.isin(hits[hits.rule_id.isin(rules)].entity_id)]

    first = _safe(first_high_dates, default=pd.DataFrame(columns=["entity_id", "first_high"]))
    v = v.merge(first, on="entity_id", how="left").sort_values("score", ascending=False)
    days_high = (pd.Timestamp(run) - pd.to_datetime(v.first_high)).dt.days
    view = pd.DataFrame({
        "Tier": v.tier.values,
        "Customer": v.entity_id.values,
        "Type": v.entity_type.values,
        "Score": v.score.round(1).values,
        "Main reason": [reason_plain(r) for r in v.reason_1],
        "Second reason": [reason_plain(r) or "" for r in v.reason_2],
        "Accounts": v.n_accounts.astype(int).values,
        "Days at High": np.where(v.tier.values == "High", days_high.values, np.nan),
        "Review status": [DECISION_LABEL.get(decisions.get(e), "") for e in v.entity_id],
    })

    if view.empty:
        st.info("No customers match these filters for this run.")
        return
    st.caption("Select a row to open that customer in the Customer tab.")
    event = st.dataframe(
        _style_tiers(view), hide_index=True, width="stretch",
        on_select="rerun", selection_mode="single-row", key="queue_table",
        column_config={
            "Main reason": st.column_config.TextColumn(width="large"),
            "Second reason": st.column_config.TextColumn(width="large"),
            "Days at High": st.column_config.NumberColumn(format="%d"),
        })
    if event.selection.rows:
        st.session_state["entity"] = view.iloc[event.selection.rows[0]]["Customer"]
        st.success(f"Customer {st.session_state['entity']} selected. Open the Customer tab to review.")


# ---------------------------------------------------------------- moved to high ----
def tab_movers(run, prev_run, scores):
    movers = scores[scores.moved_to_high].sort_values("score", ascending=False)
    if prev_run is None:
        st.info("This is the first scoring run, so there is no previous tier to compare against.")
        return
    st.markdown(f"Customers rated High on {_fmt_date(run)} who were not High on {_fmt_date(prev_run)}.")
    if movers.empty:
        st.info("Nobody moved to High in this run.")
        return
    now = _safe(rule_hits, run, default=pd.DataFrame(columns=HIT_COLS))
    before = _safe(rule_hits, prev_run, default=pd.DataFrame(columns=HIT_COLS))
    now_r = now.groupby("entity_id").rule_id.apply(set).to_dict()
    before_r = before.groupby("entity_id").rule_id.apply(set).to_dict()

    def new_rules(e):
        added = sorted(now_r.get(e, set()) - before_r.get(e, set()), key=list(RULES).index)
        return ", ".join(RULES[r]["name"] for r in added if r in RULES) or "Score rose on existing rules"

    view = pd.DataFrame({
        "Customer": movers.entity_id.values,
        "Type": movers.entity_type.values,
        "Previous tier": movers.prev_tier.fillna("Not scored").values,
        "Score": movers.score.round(1).values,
        "What changed": [new_rules(e) for e in movers.entity_id],
        "Main reason": [reason_plain(r) for r in movers.reason_1],
    })
    event = st.dataframe(view, hide_index=True, width="stretch", on_select="rerun",
                         selection_mode="single-row", key="movers_table",
                         column_config={"Main reason": st.column_config.TextColumn(width="large")})
    if event.selection.rows:
        st.session_state["entity"] = view.iloc[event.selection.rows[0]]["Customer"]
        st.success(f"Customer {st.session_state['entity']} selected. Open the Customer tab to review.")


# ------------------------------------------------------------------ portfolio ----
def tab_portfolio(run):
    mix = _safe(tier_mix, default=pd.DataFrame(columns=["run_date", "entity_type", "tier", "n"]))
    if mix.empty:
        st.info("No scores yet.")
        return
    left, right = st.columns(2)
    with left:
        st.subheader("Tiers by customer type")
        include_low = st.toggle("Include Low tier", value=False)
        m = mix[mix.run_date == run]
        if not include_low:
            m = m[m.tier != "Low"]
        st.altair_chart(
            alt.Chart(m).mark_bar().encode(
                x=alt.X("n:Q", title="Customers"), y=alt.Y("entity_type:N", title=None, sort="-x"),
                color=alt.Color("tier:N", scale=_tier_scale(), title="Tier"),
                tooltip=["entity_type", "tier", "n"]).properties(height=260),
            width="stretch")
    with right:
        st.subheader("High and Medium over time")
        t = mix[mix.tier != "Low"].groupby(["run_date", "tier"], as_index=False).n.sum()
        st.altair_chart(
            alt.Chart(t).mark_line(point=True).encode(
                x=alt.X("run_date:T", title=None), y=alt.Y("n:Q", title="Customers"),
                color=alt.Color("tier:N", scale=_tier_scale(), title="Tier"),
                tooltip=["run_date", "tier", "n"]).properties(height=300),
            width="stretch")

    st.subheader("Which rules are firing")
    counts = _safe(rule_hit_counts, default=pd.DataFrame(columns=["run_date", "rule_id", "rule_name", "n"]))
    if counts.empty:
        st.info("No rule hits recorded yet.")
        return
    counts = counts.assign(rule=counts.rule_id + " " + counts.rule_name)
    a, b = st.columns(2)
    with a:
        st.caption(f"Customers per rule on {_fmt_date(run)}")
        st.altair_chart(
            alt.Chart(counts[counts.run_date == run]).mark_bar(color=NAVY).encode(
                x=alt.X("n:Q", title="Customers"), y=alt.Y("rule:N", title=None, sort="-x"),
                tooltip=["rule", "n"]).properties(height=300),
            width="stretch")
    with b:
        st.caption("Customers per rule across runs")
        st.altair_chart(
            alt.Chart(counts).mark_line().encode(
                x=alt.X("run_date:T", title=None), y=alt.Y("n:Q", title="Customers"),
                color=alt.Color("rule:N", title=None), tooltip=["run_date", "rule", "n"]).properties(height=300),
            width="stretch")


# ------------------------------------------------------------------- customer ----
def _network(tx, scores):
    ext = tx[tx.direction.isin(["In", "Out"])]
    if ext.empty:
        return None
    agg = (ext.groupby("counterparty_entity")
              .agg(amount=("amount_usd", "sum"), payments=("amount_usd", "size"),
                   flow=("direction", lambda s: " and ".join(sorted(set(s)))),
                   banks=("counterparty_bank", lambda s: ", ".join(sorted({str(b) for b in s}))))
              .reset_index().sort_values("amount", ascending=False).head(40))
    tiers = dict(zip(scores.entity_id, scores.tier))
    agg["group"] = [f"Our customer, {tiers[e]}" if e in tiers else "Not our customer" for e in agg.counterparty_entity]
    k = len(agg)
    angle = np.linspace(0, 2 * np.pi, k, endpoint=False)
    agg["x"], agg["y"], agg["cx"], agg["cy"] = np.cos(angle), np.sin(angle), 0.0, 0.0
    agg["amount_label"] = agg.amount.map(_money)
    groups = ["Our customer, High", "Our customer, Medium", "Our customer, Low", "Not our customer"]
    colours = [TIER_COLOURS["High"], TIER_COLOURS["Medium"], "#667085", "#D0D5DD"]
    axis = alt.Scale(domain=[-1.35, 1.35])
    edges = alt.Chart(agg).mark_rule(color="#98A2B3", opacity=0.5).encode(
        x=alt.X("cx:Q", scale=axis, axis=None), y=alt.Y("cy:Q", scale=axis, axis=None),
        x2="x:Q", y2="y:Q", strokeWidth=alt.StrokeWidth("amount:Q", scale=alt.Scale(range=[0.5, 6]), legend=None))
    nodes = alt.Chart(agg).mark_circle(opacity=1, stroke="#fff", strokeWidth=1).encode(
        x=alt.X("x:Q", scale=axis, axis=None), y=alt.Y("y:Q", scale=axis, axis=None),
        size=alt.Size("amount:Q", scale=alt.Scale(range=[60, 900]), legend=None),
        color=alt.Color("group:N", scale=alt.Scale(domain=groups, range=colours), title="Counterparty"),
        tooltip=[alt.Tooltip("counterparty_entity:N", title="Counterparty"), alt.Tooltip("group:N", title="Who"),
                 alt.Tooltip("flow:N", title="Money"), alt.Tooltip("amount_label:N", title="Total"),
                 alt.Tooltip("payments:Q", title="Payments"), alt.Tooltip("banks:N", title="Their bank")])
    centre = alt.Chart(pd.DataFrame({"x": [0.0], "y": [0.0], "label": ["This customer"]}))
    dot = centre.mark_circle(size=1100, color=NAVY).encode(x=alt.X("x:Q", scale=axis, axis=None),
                                                         y=alt.Y("y:Q", scale=axis, axis=None))
    text = centre.mark_text(dy=28, color=NAVY, fontWeight="bold").encode(x="x:Q", y="y:Q", text="label:N")
    return (edges + nodes + dot + text).properties(height=460).configure_view(strokeWidth=0)


def tab_customer(run, scores, ov, cur_ov):
    if scores.empty:
        st.info("No customers scored in this run.")
        return
    ordered = scores.sort_values("score", ascending=False)
    ids = ordered.entity_id.tolist()
    tier_of = dict(zip(ordered.entity_id, ordered.tier))
    wanted = st.session_state.get("entity")
    entity = st.selectbox("Customer", ids, index=ids.index(wanted) if wanted in ids else 0,
                          format_func=lambda e: f"{e} ({tier_of.get(e, '')})",
                          help="Ordered by risk score. Type to search by customer ID.")
    st.session_state["entity"] = entity
    me = ordered[ordered.entity_id == entity].iloc[0]

    st.markdown(f"### {entity} &nbsp; {_badge(me.tier)}", unsafe_allow_html=True)
    prev = f", previously {me.prev_tier}" if me.prev_tier and me.prev_tier != me.tier else ""
    st.caption(f"{me.entity_type}, {int(me.n_accounts)} account(s) at {bank_label()}{prev}.")
    c = st.columns(5)
    c[0].metric("Risk score", f"{me.score:.1f}")
    c[1].metric("Riskier than", f"{me.score_percentile:.0f}% of customers")
    c[2].metric("Money in", _money(me.inflow_usd))
    c[3].metric("Money out", _money(me.outflow_usd))
    c[4].metric("Counterparties", int(me.distinct_senders + me.distinct_recipients))

    hits = _safe(entity_rule_hits, entity, default=pd.DataFrame(columns=HIT_COLS))
    now = hits[hits.run_date == run].copy()
    left, right = st.columns([3, 2])
    with left:
        st.subheader("Why this customer is flagged")
        if now.empty:
            st.write("No rules triggered in this run.")
        for _, h in now.sort_values("points", ascending=False).iterrows():
            plain = RULES.get(h.rule_id, {}).get("plain", h.rule_name)
            st.markdown(f"<div class='reason'><b>{h.rule_name}</b> ({int(h.points)} points): {plain}<br>"
                        f"<small>{h.evidence}</small></div>", unsafe_allow_html=True)
    with right:
        st.subheader("Score over time")
        hist = _safe(score_history, entity, default=pd.DataFrame(columns=["run_date", "score", "tier"]))
        if not hist.empty:
            base = alt.Chart(hist).encode(x=alt.X("run_date:T", title=None), y=alt.Y("score:Q", title="Score"))
            st.altair_chart((base.mark_line(color="#98A2B3") +
                             base.mark_circle(size=90).encode(color=alt.Color("tier:N", scale=_tier_scale(), title="Tier"),
                                                              tooltip=["run_date", "score", "tier"]))
                            .properties(height=230), width="stretch")

    st.subheader("Review decision")
    mine = ov[ov.entity_id == entity] if not ov.empty else ov
    if not mine.empty:
        last = mine.iloc[-1]
        until = f" until {_fmt_date(last.suppress_until)}" if last.suppress_until else ""
        st.info(f"{DECISION_LABEL.get(last.decision, last.decision)} by {last.reviewer or 'unknown'} on run "
                f"{_fmt_date(last.decided_on_run_date)}{until}. Note: {last.note or '-'}")
    note = st.text_area("Note for the file", key=f"note_{entity}",
                        placeholder="What you checked and why you reached this decision")
    reviewer = st.session_state.get("reviewer", "").strip()
    b1, b2, _ = st.columns([1, 1, 3])
    for col, decision, label in ((b1, "dismiss", f"Dismiss for {DISMISS_DAYS} days"),
                                 (b2, "escalate_edd", "Escalate to EDD")):
        if col.button(label, key=f"{decision}_{entity}", width="stretch"):
            if not reviewer:
                st.warning("Enter your name in the sidebar before recording a decision.")
            elif not note.strip():
                st.warning("Add a note explaining the decision.")
            else:
                try:
                    save_override(entity, decision, note, reviewer, run_date=run)
                    st.success(f"{DECISION_LABEL[decision]} recorded for {entity}.")
                    st.rerun()
                except Exception as exc:
                    st.error(f"Could not save the decision: {exc}")
    st.caption("Decisions are stored separately from the scores, so the model's rating and the "
               "reviewer's judgement both stay on record.")

    tx = _safe(transactions, entity, run, default=pd.DataFrame(columns=TXN_COLS))
    if tx is None or tx.empty:
        st.info("No transactions found for this customer in the scoring window.")
        return
    tx = annotate_transactions(tx, entity)
    st.subheader("Who they deal with")
    st.caption("Largest 40 counterparties in the scoring window. Line width and dot size show the amount; "
               "colour shows whether the counterparty banks with us and their current tier.")
    chart = _network(tx, scores)
    if chart is not None:
        st.altair_chart(chart, width="stretch")

    a, b = st.columns([2, 3])
    with a:
        st.subheader("Accounts at our bank")
        acc = accounts_at_bank(tx, entity)
        st.dataframe(acc.assign(in_usd=acc.in_usd.map(_money), out_usd=acc.out_usd.map(_money))
                     .rename(columns={"account": "Account", "transactions": "Payments", "in_usd": "In",
                                      "out_usd": "Out", "first_seen": "First seen", "last_seen": "Last seen"}),
                     hide_index=True, width="stretch")
    with b:
        st.subheader("Transactions")
        dirs = st.multiselect("Direction", ["In", "Out", "Own accounts"], default=["In", "Out", "Own accounts"])
        show = tx[tx.direction.isin(dirs)]
        st.dataframe(pd.DataFrame({
            "Time": show.txn_ts, "Direction": show.direction, "Amount (USD)": show.amount_usd.round(2),
            "Format": show.payment_format, "Currency": show.payment_currency,
            "Our account": show.our_account, "Counterparty": show.counterparty_entity,
            "Their bank": show.counterparty_bank}),
            hide_index=True, width="stretch", height=320,
            column_config={"Amount (USD)": st.column_config.NumberColumn(format="$%.2f")})


# ---------------------------------------------------------------- scorecard ----
def tab_scorecard(run):
    st.markdown("<div class='labels-note'>This tab uses the dataset's laundering labels to measure how well the "
                "rules work. Reviewers never see these labels on the other tabs.</div>", unsafe_allow_html=True)
    vs = _safe(validation_summary, run, default=pd.DataFrame(columns=["metric", "value"]))
    if vs is None or vs.empty:
        st.info("No validation results for this run yet. They are written by validate.py after scoring.")
        return
    m = dict(zip(vs.metric, vs.value))
    c = st.columns(4)
    c[0].metric("Laundering customers in High", f"{m.get('capture_rate_high', 0):.0%}")
    c[1].metric("In High or Medium", f"{m.get('capture_rate_high_or_medium', 0):.0%}")
    c[2].metric("High tier vs random", f"{m.get('lift_high_vs_random', 0):.1f}x")
    c[3].metric("High tier precision", f"{m.get('precision_high', 0):.0%}")
    st.caption(f"{int(m.get('captured_high', 0))} of {int(m.get('laundering_customers', 0))} laundering customers "
               f"rated High, out of {int(m.get('customers_scored', 0))} customers scored. A random pick of the same "
               f"size would contain about {m.get('base_rate', 0) * m.get('high_count', 0):.1f}.")

    left, right = st.columns(2)
    with left:
        st.subheader("How precise each rule is")
        rv = _safe(rule_validation, run, default=pd.DataFrame())
        if rv is not None and not rv.empty:
            rv = rv.assign(precision=np.where(rv.n_flagged > 0, rv.n_laundering / rv.n_flagged, 0.0),
                           rule=rv.rule_id + " " + rv.rule_name)
            st.altair_chart(
                alt.Chart(rv).mark_bar(color=NAVY).encode(
                    x=alt.X("precision:Q", title="Share of flagged customers who are laundering",
                            axis=alt.Axis(format="%")),
                    y=alt.Y("rule:N", title=None, sort="-x"),
                    tooltip=["rule", "n_flagged", "n_laundering", alt.Tooltip("precision:Q", format=".0%")])
                .properties(height=300), width="stretch")
            st.dataframe(pd.DataFrame({
                "Rule": rv.rule, "Points": rv.points, "Customers flagged": rv.n_flagged,
                "Of which laundering": rv.n_laundering, "Precision": rv.precision,
                "Designed to catch": [", ".join(RULES.get(r, {}).get("targets", [])) or "-" for r in rv.rule_id]}),
                hide_index=True, width="stretch",
                column_config={"Precision": st.column_config.NumberColumn(format="%.2f")})
    with right:
        st.subheader("Which laundering schemes are caught")
        vt = _safe(validation_typology, run, default=pd.DataFrame())
        if vt is not None and not vt.empty:
            long = pd.concat([
                vt.assign(outcome="High", n=vt.caught_high),
                vt.assign(outcome="Medium", n=vt.caught_high_or_medium - vt.caught_high),
                vt.assign(outcome="Missed", n=vt.n_laundering_customers - vt.caught_high_or_medium),
            ])
            st.altair_chart(
                alt.Chart(long).mark_bar().encode(
                    x=alt.X("n:Q", title="Laundering customers"),
                    y=alt.Y("typology:N", title=None, sort="-x"),
                    color=alt.Color("outcome:N", title=None, scale=alt.Scale(
                        domain=["High", "Medium", "Missed"],
                        range=[TIER_COLOURS["High"], TIER_COLOURS["Medium"], "#D0D5DD"])),
                    order=alt.Order("outcome:N"),
                    tooltip=["typology", "outcome", "n"]).properties(height=300),
                width="stretch")


# ----------------------------------------------------------------------- main ----
def main():
    st.set_page_config(page_title="KYC customer risk review", page_icon="🏦", layout="wide")
    _style()
    st.title("Customer risk review")

    runs = _safe(run_dates, default=[]) or []
    if not runs:
        st.info("No scoring runs found yet. Run the pipeline for at least one day, "
                "or start the app with DEMO_MODE=1 to see sample data.")
        return

    with st.sidebar:
        st.header("Review settings")
        run = st.selectbox("Scoring run", runs[::-1], format_func=_fmt_date,
                           help="Each daily run scores every customer on the last "
                                f"{WINDOW_DAYS} days of transactions.")
        st.text_input("Your name", key="reviewer", help="Recorded with every decision you make.")
        if st.button("Refresh data", width="stretch"):
            st.cache_data.clear()
            st.rerun()
        st.caption(f"Bank: {bank_label()}")
        st.caption("Showing generated demo data." if DEMO_MODE else f"Athena database: {DATABASE}")

    i = runs.index(run)
    prev_run = runs[i - 1] if i > 0 else None
    st.markdown(f"**{bank_label()}**. Scoring run of {_fmt_date(run)}, covering transactions from "
                f"{_fmt_date(_window_start(run))} to {_fmt_date(run)}.")

    scores = _safe(customer_scores, run, default=EMPTY_SCORES)
    ov = _safe(overrides, default=pd.DataFrame(columns=OVERRIDE_COLS))
    cur_ov = current_overrides(ov, as_of=run)

    queue, movers, portfolio, customer, scorecard = st.tabs(
        ["Review queue", "Moved to High", "Portfolio", "Customer", "Rule scorecard"])
    with queue:
        tab_queue(run, scores, cur_ov)
    with movers:
        tab_movers(run, prev_run, scores)
    with portfolio:
        tab_portfolio(run)
    with customer:
        tab_customer(run, scores, ov, cur_ov)
    with scorecard:
        tab_scorecard(run)


def _running_in_streamlit():
    try:
        from streamlit.runtime.scriptrunner import get_script_run_ctx
        return get_script_run_ctx() is not None
    except Exception:
        return False


if __name__ == "__main__":
    if _running_in_streamlit():
        main()
    else:
        print("This is a Streamlit app. Start it with:\n"
              "  streamlit run q2_streamlit.py --server.address 0.0.0.0 --server.port 8501\n"
              "Add DEMO_MODE=1 in front to use sample data.")
