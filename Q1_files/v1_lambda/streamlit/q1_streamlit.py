"""Q1 live AML alert dashboard - single-file Streamlit app.

Run (same as the Week 5 lab, on EC2 or your laptop):
    pip install streamlit boto3 pandas
    export S3_BUCKET=<your-bucket> BANK_ID=11          # Windows: set S3_BUCKET=... / set BANK_ID=11
    streamlit run q1_streamlit.py --server.address 0.0.0.0 --server.port 8501
Demo data (no AWS needed):
    DEMO_MODE=1 streamlit run q1_streamlit.py

Optional env vars: AWS_REGION (us-east-1), ALERTS_TABLE (q1-alerts), STATE_TABLE (q1-state),
REFRESH_SECONDS (3), BANK_NAME.

Where the data comes from:
  live alerts + decisions   DynamoDB q1-alerts   (written by the q1-score Lambda, decided here)
  simulated clock           DynamoDB q1-state    (META#clock = latest payment time streamed)
  model + evaluations       S3 q1/models/, q1/evaluation/

Confirm / Dismiss updates the alert's status in q1-alerts. That status is the only label the
retraining pipeline uses for live payments. The hidden _sim_truth field is never shown here.

Dates: payment times are SIMULATED (2022). "New" badges use real time since the alert was created.
"""
import json
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import streamlit as st


def _flag(name):
    return os.getenv(name, "").strip().lower() in {"1", "true", "yes", "on"}


DEMO_MODE = _flag("DEMO_MODE")
S3_BUCKET = os.getenv("S3_BUCKET", "")
REGION = os.getenv("AWS_REGION", "us-east-1")
BANK_ID = os.getenv("BANK_ID", "11").strip()
BANK_NAME = os.getenv("BANK_NAME", "").strip()
ALERTS_TABLE = os.getenv("ALERTS_TABLE", "q1-alerts")
STATE_TABLE = os.getenv("STATE_TABLE", "q1-state")
REFRESH = int(os.getenv("REFRESH_SECONDS", "3"))
NEW_SECONDS = 30            # real seconds an alert keeps its "New" badge
TTL_MODEL = 60              # seconds; model info changes only after retraining

_KNOWN_BANKS = {"1": "Arbor Savings Bank", "11": "Finland Bank #0", "12": "National Bank of the East",
                "20": "China Bank #6", "70": "Oasis Thrift", "119": "Israel Bank #6"}
STATUS_LABEL = {"open": "Open", "confirmed": "Confirmed laundering", "dismissed": "Dismissed"}
ALERT_COLS = ["txn_id", "txn_ts", "bank", "side", "from_account", "to_account", "amount", "currency",
              "payment_format", "risk", "threshold", "urgency", "priority", "reasons", "status",
              "model_version", "created_real", "decided_by", "decided_real", "note"]


def bank_label():
    name = BANK_NAME or _KNOWN_BANKS.get(BANK_ID.lstrip("0") or "0", "")
    return f"{name} · Bank {BANK_ID}" if name else f"Bank {BANK_ID}"


# ------------------------------------------------------------------ AWS ----
@st.cache_resource
def _aws():
    import boto3
    s = boto3.Session(region_name=REGION)
    ddb = s.resource("dynamodb")
    return ddb.Table(ALERTS_TABLE), ddb.Table(STATE_TABLE), s.client("s3")


def _to_frame(items):
    df = pd.DataFrame(items)
    for c in ALERT_COLS:
        if c not in df.columns:
            df[c] = None
    df = df[ALERT_COLS].copy()
    for c in ["amount", "risk", "threshold", "priority", "created_real", "decided_real"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype(float)
    df["reasons"] = df["reasons"].apply(lambda r: list(r) if isinstance(r, (list, tuple)) else [])
    return df


def load_alerts():
    """Every alert (open and decided). Not cached: this is the live view."""
    if DEMO_MODE:
        return _to_frame(_demo_visible())
    alerts, _, _ = _aws()
    items, kw = [], {}
    while True:
        r = alerts.scan(**kw)
        items += r["Items"]
        if "LastEvaluatedKey" not in r:
            break
        kw["ExclusiveStartKey"] = r["LastEvaluatedKey"]
    return _to_frame(items)


def sim_clock():
    if DEMO_MODE:
        v = _demo_visible()
        return max((a["txn_ts"] for a in v), default=None)
    _, state, _ = _aws()
    item = state.get_item(Key={"pk": "META#clock"}).get("Item")
    return item["ts"] if item else None


def decide(txn_id, status, reviewer, note=""):
    """Record an investigator's decision on the alert. This is the label retraining uses."""
    now = int(time.time())
    if DEMO_MODE:
        state = _demo_state()
        state[txn_id] = {"status": status, "decided_by": reviewer, "decided_real": now, "note": note}
        _DEMO_FILE.write_text(json.dumps(state), encoding="utf-8")
        return
    alerts, _, _ = _aws()
    alerts.update_item(
        Key={"txn_id": txn_id},
        UpdateExpression="SET #s = :s, decided_by = :b, decided_real = :r, note = :n",
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={":s": status, ":b": reviewer, ":r": now, ":n": note})


@st.cache_data(ttl=TTL_MODEL, show_spinner=False)
def model_info():
    """Current best model, every archived version, and every retraining comparison."""
    if DEMO_MODE:
        return _demo_model()
    if not S3_BUCKET:
        raise ValueError("S3_BUCKET is not set, so model details cannot be read.")
    _, _, s3 = _aws()

    def read(key):
        return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())

    try:
        best = read("q1/models/best/meta.json")
    except s3.exceptions.NoSuchKey:
        best = None
    versions, evals = [], []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=S3_BUCKET, Prefix="q1/models/archive/"):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith("/meta.json"):
                versions.append(read(obj["Key"]))
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=S3_BUCKET, Prefix="q1/evaluation/"):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith("result.json"):
                evals.append(read(obj["Key"]))
    return {"best": best, "versions": versions, "evals": evals}


# ------------------------------------------------------------- demo data ----
_DEMO_FILE = Path(__file__).parent / ".demo_state" / "q1_decisions.json"
_REASON_POOL = [
    "Hours since sender last received money: 0.75", "Payment / money just received: 0.98",
    "Share of sender's 1-day inflow passed on: 0.97", "First payment between these accounts",
    "Receiver: payments received, last 1 day = 9", "Sender: new-counterparty payments sent, last 7 days = 6",
    "Hours since receiver paid the sender (round trip): 20.50", "Burst in receiver's incoming payments: 4.20",
    "Payment amount: 9,640.00 US Dollar", "Currency conversion", "Just under 10,000",
]


@st.cache_resource
def _demo_alerts():
    """A fixed set of fake alerts that 'arrive' over ~10 real minutes after the app starts."""
    rng = np.random.default_rng(459)
    start, n = time.time(), 160
    sim0 = datetime(2022, 9, 10)
    offsets = np.sort(rng.uniform(0, 600, n))
    out = []
    for i, off in enumerate(offsets):
        risk = float(rng.uniform(0.55, 0.99))
        urg = "High" if rng.random() < 0.3 else "Normal"
        side = rng.choice(["sender", "receiver", "both"], p=[.45, .45, .1])
        other = f"{rng.choice(['1', '20', '119', '220'])}_8{rng.integers(0, 16 ** 7):07X}"
        mine = f"{BANK_ID}_8{rng.integers(0, 16 ** 7):07X}"
        out.append({
            "txn_id": f"demo{i:04d}", "txn_ts": (sim0 + timedelta(minutes=int(off * 6))).isoformat(),
            "bank": BANK_ID, "side": side,
            "from_account": mine if side != "receiver" else other,
            "to_account": other if side == "sender" else mine,
            "amount": round(float(rng.lognormal(8.5, 1.2)), 2),
            "currency": rng.choice(["US Dollar", "Euro", "Yuan"], p=[.8, .15, .05]),
            "payment_format": rng.choice(["ACH", "Cheque", "Wire", "Cash", "Bitcoin"]),
            "risk": risk, "threshold": 0.55, "urgency": urg,
            "priority": risk * (1.5 if urg == "High" else 1.0),
            "reasons": list(rng.choice(_REASON_POOL, 3, replace=False)), "status": "open",
            "model_version": "demo-v1", "created_real": start + off})
    return out


def _demo_state():
    if _DEMO_FILE.exists():
        return json.loads(_DEMO_FILE.read_text(encoding="utf-8"))
    _DEMO_FILE.parent.mkdir(exist_ok=True)
    return {}


def _demo_visible():
    now, decided = time.time(), _demo_state()
    return [{**a, **decided.get(a["txn_id"], {})} for a in _demo_alerts() if a["created_real"] <= now]


def _demo_model():
    meta = {"version": "demo-v1", "bank": BANK_ID, "threshold": 0.55, "alert_rate": 0.01,
            "trained_at": "2022-09-10T00:00:00", "n_train": 48210, "n_train_pos": 61,
            "valid_precision": 0.31, "valid_recall": 0.58}
    v2 = {**meta, "version": "demo-v2", "trained_at": "2022-09-14T00:00:00", "valid_precision": 0.29}
    ev = {"new_version": "demo-v2", "best_version": "demo-v1",
          "bench": {"payments": 9120, "positives": 18,
                    "new": {"alerts": 91, "caught": 9, "recall": 0.5, "precision": 0.099},
                    "best": {"alerts": 91, "caught": 10, "recall": 0.556, "precision": 0.11}},
          "recent": {"payments": 3050, "positives": 2}}
    return {"best": meta, "versions": [meta, v2], "evals": [ev]}


# =========================================================================== UI ====
import altair as alt   # ships with Streamlit

NAVY = "#1F2A44"
RISK_RED, AMBER, GREY = "#B42318", "#B54708", "#98A2B3"


def _style():
    st.markdown(f"""
    <style>
      @import url('https://fonts.googleapis.com/css2?family=Public+Sans:wght@400;600;700&display=swap');
      html, body, .stApp, .stMarkdown, button, input, textarea, select, label {{
        font-family: 'Public Sans', 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
      }}
      h1, h2, h3 {{ color: {NAVY}; letter-spacing: -0.01em; }}
      .pill {{ display: inline-block; padding: 1px 9px; border-radius: 3px; color: #fff;
               font-weight: 600; font-size: 0.8rem; margin-right: 6px; }}
      .p-risk {{ background: {RISK_RED}; }}
      .p-urgent {{ background: {AMBER}; }}
      .p-new {{ background: #027A48; }}
      .p-side {{ background: {NAVY}; }}
      .reason {{ border-left: 3px solid {NAVY}; padding: 2px 0 2px 10px; margin: 3px 0; color: #344054; }}
      .meta {{ color: #475467; font-size: 0.9rem; }}
    </style>""", unsafe_allow_html=True)


def _money(x, cur=""):
    if x is None or pd.isna(x):
        return "-"
    return f"{float(x):,.2f} {cur}".strip()


def _sim(ts):
    return str(ts).replace("T", " ")[:16] if ts else "-"


def _safe(fn, *args, default=None):
    try:
        return fn(*args)
    except Exception as exc:  # show the problem instead of a stack trace
        st.error(f"Could not load data ({fn.__name__}): {exc}")
        return default


def _on_decide(txn_id, status):
    reviewer = st.session_state.get("reviewer", "").strip()
    if not reviewer:
        st.toast("Enter your name in the sidebar before deciding.", icon="⚠️")
        return
    note = st.session_state.get(f"note_{txn_id}", "").strip()
    try:
        decide(txn_id, status, reviewer, note)
        st.toast(f"{STATUS_LABEL[status]}: {txn_id[:12]}", icon="✅")
    except Exception as exc:
        st.toast(f"Could not save the decision: {exc}", icon="❌")


# ----------------------------------------------------------------- live queue ----
def _alert_card(a, now):
    new = now - a["created_real"] < NEW_SECONDS if pd.notna(a["created_real"]) else False
    pills = (f"<span class='pill p-risk'>Risk {a['risk']:.2f}</span>"
             + ("<span class='pill p-urgent'>High urgency</span>" if a["urgency"] == "High" else "")
             + (f"<span class='pill p-side'>Our bank: {a['side']}</span>")
             + ("<span class='pill p-new'>NEW</span>" if new else ""))
    with st.container(border=True):
        left, right = st.columns([5, 1.3])
        with left:
            st.markdown(pills, unsafe_allow_html=True)
            st.markdown(f"**{a['from_account']} → {a['to_account']}** · "
                        f"{_money(a['amount'], a['currency'])} · {a['payment_format']}")
            st.markdown(f"<div class='meta'>Payment time {_sim(a['txn_ts'])} (simulated) · "
                        f"alert threshold {a['threshold']:.3f} · model {a['model_version']}</div>",
                        unsafe_allow_html=True)
            st.markdown("".join(f"<div class='reason'>{r}</div>" for r in a["reasons"]), unsafe_allow_html=True)
        with right:
            st.text_input("Note", key=f"note_{a['txn_id']}", placeholder="Optional", label_visibility="collapsed")
            st.button("Confirm laundering", key=f"c_{a['txn_id']}", type="primary", width="stretch",
                      on_click=_on_decide, args=(a["txn_id"], "confirmed"))
            st.button("Dismiss", key=f"d_{a['txn_id']}", width="stretch",
                      on_click=_on_decide, args=(a["txn_id"], "dismissed"))


def _alerts_over_time(df):
    t = df.assign(hour=pd.to_datetime(df.txn_ts).dt.floor("h"),
                  status=df.status.map(STATUS_LABEL).fillna("Open"))
    agg = t.groupby(["hour", "status"]).size().rename("n").reset_index()
    return (alt.Chart(agg).mark_bar().encode(
        x=alt.X("hour:T", title="Simulated time"), y=alt.Y("n:Q", title="Alerts"),
        color=alt.Color("status:N", title=None, scale=alt.Scale(
            domain=list(STATUS_LABEL.values()), range=[RISK_RED, NAVY, GREY])),
        tooltip=["hour:T", "status:N", "n:Q"]).properties(height=170))


@st.fragment(run_every=f"{REFRESH}s")
def tab_live():
    df = _safe(load_alerts, default=_to_frame([]))
    clock = _safe(sim_clock, default=None)
    now = time.time()
    open_ = df[df.status == "open"]
    c = st.columns(5)
    c[0].metric("Simulated time", _sim(clock))
    c[1].metric("Open alerts", len(open_))
    c[2].metric("New in last 30s", int((now - df.created_real < NEW_SECONDS).sum()))
    c[3].metric("Confirmed", int((df.status == "confirmed").sum()))
    c[4].metric("Dismissed", int((df.status == "dismissed").sum()))
    if df.empty:
        st.info("No alerts yet. Start the producer; alerts appear here as payments are scored.")
        return
    st.altair_chart(_alerts_over_time(df), width="stretch")

    f = st.columns([2, 2, 1])
    urg = f[0].multiselect("Urgency", ["High", "Normal"], default=["High", "Normal"], key="f_urg")
    side = f[1].multiselect("Our bank is the", ["sender", "receiver", "both"],
                            default=["sender", "receiver", "both"], key="f_side")
    top = f[2].number_input("Show", 5, 100, 20, step=5, key="f_top")
    v = open_[open_.urgency.isin(urg) & open_.side.isin(side)].sort_values("priority", ascending=False)
    st.caption(f"{len(v)} open alerts match. Ranked by priority = risk × urgency (High urgency × 1.5).")
    for _, a in v.head(int(top)).iterrows():
        _alert_card(a, now)


# ------------------------------------------------------------------ decided ----
@st.fragment(run_every=f"{REFRESH * 1}s")
def tab_decided():
    df = _safe(load_alerts, default=_to_frame([]))
    d = df[df.status != "open"].sort_values("decided_real", ascending=False)
    if d.empty:
        st.info("No decisions yet. Confirm or dismiss alerts on the Live queue tab.")
        return
    c = st.columns(3)
    c[0].metric("Decided", len(d))
    c[1].metric("Confirmed laundering", int((d.status == "confirmed").sum()))
    c[2].metric("Share confirmed", f"{(d.status == 'confirmed').mean():.0%}")
    view = pd.DataFrame({
        "Decision": d.status.map(STATUS_LABEL), "Payment time": d.txn_ts.map(_sim),
        "From": d.from_account, "To": d.to_account, "Amount": d.amount.round(2), "Currency": d.currency,
        "Risk": d.risk.round(3), "Decided by": d.decided_by.fillna("-"), "Note": d.note.fillna(""),
        "Alert ID": d.txn_id})
    event = st.dataframe(view, hide_index=True, width="stretch", on_select="rerun",
                         selection_mode="single-row", key="decided_table")
    st.caption("These decisions are the labels retraining uses for live payments. "
               "Select a row to change a decision.")
    if event.selection.rows:
        row = view.iloc[event.selection.rows[0]]
        b = st.columns([1, 1, 1, 3])
        for col, status, label in ((b[0], "confirmed", "Mark confirmed"), (b[1], "dismissed", "Mark dismissed"),
                                   (b[2], "open", "Reopen")):
            col.button(label, key=f"re_{status}", width="stretch", on_click=_on_decide,
                       args=(row["Alert ID"], status))




# --------------------------------------------------------------------- model ----
def tab_model():
    info = _safe(model_info, default={"best": None, "versions": [], "evals": []})
    best = info["best"]
    if not best:
        st.info("No model in models/best yet. Run the q1-pipeline Step Function with {\"mode\": \"setup\"}.")
        return
    st.subheader("Model in use")
    c = st.columns(4)
    c[0].metric("Version", best["version"][:13])
    c[1].metric("Alert threshold", f"{best['threshold']:.3f}")
    c[2].metric("Validation precision", f"{best.get('valid_precision', 0):.0%}")
    c[3].metric("Validation recall", f"{best.get('valid_recall', 0):.0%}")
    st.caption(f"Trained {best.get('trained_at', '-')} on {best.get('n_train', 0):,} payments "
               f"({best.get('n_train_pos', 0)} laundering). Alerts on the top "
               f"{best.get('alert_rate', 0.01):.0%} of payments.")

    st.subheader("Retraining comparisons")
    if not info["evals"]:
        st.info("No retraining runs yet. Start q1-pipeline with {\"mode\": \"retrain\"}.")
    rows = []
    for e in info["evals"]:
        for name in ["bench", "recent"]:
            r = e.get(name, {})
            rows.append({"New version": e["new_version"][:13], "Compared with": e["best_version"][:13],
                         "Data": "Fixed benchmark" if name == "bench" else "Recent reviewed days",
                         "Payments": r.get("payments", 0), "Laundering cases": r.get("positives", 0),
                         "New recall": (r.get("new") or {}).get("recall"),
                         "Current recall": (r.get("best") or {}).get("recall"),
                         "Promoted": "Yes" if best["version"] == e["new_version"] else "No"})
    if rows:
        st.dataframe(pd.DataFrame(rows), hide_index=True, width="stretch",
                     column_config={"New recall": st.column_config.NumberColumn(format="%.2f"),
                                    "Current recall": st.column_config.NumberColumn(format="%.2f")})
        st.caption("Both models alert on the same top 1% of the same payments. A set with fewer than 3 "
                   "laundering cases is not used for the decision.")

    st.subheader("All versions")
    vers = pd.DataFrame(info["versions"])
    if not vers.empty:
        vers = vers.sort_values("trained_at", ascending=False)
        st.dataframe(pd.DataFrame({
            "Version": vers.version, "Trained": vers.trained_at, "Threshold": vers.threshold.round(3),
            "Validation precision": vers.get("valid_precision"), "Validation recall": vers.get("valid_recall"),
            "In use": np.where(vers.version == best["version"], "✓", "")}),
            hide_index=True, width="stretch")


# ----------------------------------------------------------------------- main ----
def main():
    st.set_page_config(page_title="Live AML alerts", page_icon="🏦", layout="wide")
    _style()
    st.title("Live laundering alerts")
    with st.sidebar:
        st.header("Investigator")
        st.text_input("Your name", key="reviewer", help="Recorded with every decision you make.")
        st.caption(f"Bank: {bank_label()}")
        st.caption("Showing generated demo data." if DEMO_MODE else
                   f"Alerts table: {ALERTS_TABLE} · refresh every {REFRESH}s")
        if st.button("Refresh model info", width="stretch"):
            st.cache_data.clear()
            st.rerun()
    st.markdown(f"**{bank_label()}**. Payments are scored as they stream in; alerts above the "
                f"model's threshold appear on the live queue within seconds.")
    live, decided, model = st.tabs(["Live queue", "Decisions", "Model"])
    with live:
        tab_live()
    with decided:
        tab_decided()
    with model:
        tab_model()


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
              "  streamlit run q1_streamlit.py --server.address 0.0.0.0 --server.port 8501\n"
              "Add DEMO_MODE=1 in front to use sample data.")
