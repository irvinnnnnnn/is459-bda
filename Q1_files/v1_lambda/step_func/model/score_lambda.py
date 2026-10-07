"""
IS459 G2 - Q1 live scoring Lambda (trigger: Kinesis aml-transactions).

For each payment: clean -> skip if it doesn't involve our bank -> read account history from
DynamoDB -> build the notebook's features -> score with the model in models/best/ ->
update account history -> write an alert if the score is above the bank's threshold.

The model is scored with a small pure-Python tree walker over LightGBM's JSON dump,
so no LightGBM layer is needed. is_laundering is removed in parse() and never used for
features or scoring; it is only copied onto alerts as _sim_truth for the demo simulator.

Environment variables: BUCKET, BANK_ID (e.g. 11), EVENTS_TABLE, STATE_TABLE, ALERTS_TABLE
"""
import base64
import json
import math
import os
import struct
import time
from datetime import datetime, timedelta
from decimal import Decimal

WINDOWS = [("1D", 1), ("5D", 5), ("7D", 7)]
MODEL_PREFIX = "q1/models/best/"
RELOAD_EVERY_S = 60


# ============================================================ model (pure-Python LightGBM)
def _f32(v):
    if v != v or math.isinf(v):
        return v
    try:
        return struct.unpack("f", struct.pack("f", v))[0]
    except OverflowError:
        return math.copysign(math.inf, v)


def _go_left(v, node):
    mt = node.get("missing_type", "None")
    if v != v and mt != "NaN":
        v = 0.0
    if (mt == "Zero" and -1e-35 <= v <= 1e-35) or (mt == "NaN" and v != v):
        return node["default_left"]
    return v <= node["threshold"]


class Model:
    def __init__(self, dump, meta):
        self.meta = meta
        self.names = dump["feature_names"]
        self.trees = [t["tree_structure"] for t in dump["tree_info"]]
        self.threshold = float(meta["threshold"])
        self.version = meta["version"]

    def predict(self, x):
        """x: list of floats in self.names order (NaN = missing). Returns (prob, contributions)."""
        x = [_f32(float(v)) for v in x]
        raw, contrib = 0.0, {}
        for node in self.trees:
            while "split_index" in node:
                f = node["split_feature"]
                child = node["left_child"] if _go_left(x[f], node) else node["right_child"]
                cval = child["internal_value"] if "split_index" in child else child["leaf_value"]
                contrib[f] = contrib.get(f, 0.0) + cval - node["internal_value"]
                node = child
            raw += node["leaf_value"]
        return 1.0 / (1.0 + math.exp(-raw)), contrib


# ============================================================ state storage
def _dec(v):
    return Decimal(str(v)) if isinstance(v, float) else v


class DynamoStore:
    def __init__(self):
        import boto3
        from boto3.dynamodb.conditions import Key
        self.Key = Key
        self.ddb = boto3.resource("dynamodb")
        self.client = self.ddb.meta.client
        self.ev_name, self.st_name = os.environ["EVENTS_TABLE"], os.environ["STATE_TABLE"]
        self.events_t = self.ddb.Table(self.ev_name)
        self.alerts_t = self.ddb.Table(os.environ["ALERTS_TABLE"])
        self.state_t = self.ddb.Table(self.st_name)

    def events(self, acct, t):
        """This account's payments in the 7 days strictly before t."""
        lo = (t - timedelta(days=7)).isoformat() + "#~"
        kw = {"KeyConditionExpression": self.Key("acct").eq(acct) & self.Key("sk").between(lo, t.isoformat()),
              "ConsistentRead": True}
        items = []
        while True:
            r = self.events_t.query(**kw)
            items += r["Items"]
            if "LastEvaluatedKey" not in r:
                break
            kw["ExclusiveStartKey"] = r["LastEvaluatedKey"]
        return [{"ts": datetime.fromisoformat(i["sk"].split("#")[0]), "dir": i["sk"].rsplit("#", 1)[1],
                 "amt": float(i["amt"]), "fresh": int(i["fresh"])} for i in items]

    def get_state(self, pks):
        req = {self.st_name: {"Keys": [{"pk": k} for k in set(pks)], "ConsistentRead": True}}
        found = {}
        while req:
            r = self.client.batch_get_item(RequestItems=req)
            for it in r["Responses"].get(self.st_name, []):
                found[it["pk"]] = {k: (float(v) if isinstance(v, Decimal) else v) for k, v in it.items()}
            req = r.get("UnprocessedKeys") or None
        return found

    def write(self, events, states):
        req = {self.ev_name: [{"PutRequest": {"Item": {k: _dec(v) for k, v in e.items()}}} for e in events],
               self.st_name: [{"PutRequest": {"Item": {k: _dec(v) for k, v in s.items()}}} for s in states]}
        for attempt in range(8):
            r = self.client.batch_write_item(RequestItems=req)
            req = r.get("UnprocessedItems") or {}
            if not req:
                return
            time.sleep(0.05 * 2 ** attempt)

    def put_alert(self, item):
        self.alerts_t.put_item(Item={k: _dec(v) for k, v in item.items()})

    def set_clock(self, ts_iso):
        self.state_t.update_item(Key={"pk": "META#clock"}, UpdateExpression="SET #t = :t",
                                 ConditionExpression="attribute_not_exists(#t) OR #t < :t",
                                 ExpressionAttributeNames={"#t": "ts"},
                                 ExpressionAttributeValues={":t": ts_iso})


class MemoryStore:
    """Same interface as DynamoStore, for local testing."""
    def __init__(self):
        self.ev, self.st, self.alerts = {}, {}, []

    def events(self, acct, t):
        lo = t - timedelta(days=7)
        return [dict(e) for e in self.ev.get(acct, []) if lo < e["ts"] < t]

    def get_state(self, pks):
        return {k: dict(self.st[k]) for k in pks if k in self.st}

    def write(self, events, states):
        for e in events:
            ts, _, d = e["sk"].split("#")
            self.ev.setdefault(e["acct"], []).append({"ts": datetime.fromisoformat(ts), "dir": d,
                                                      "amt": e["amt"], "fresh": e["fresh"]})
        for s in states:
            self.st[s["pk"]] = dict(s)

    def put_alert(self, item):
        self.alerts.append(item)

    def set_clock(self, ts_iso):
        self.st["META#clock"] = {"pk": "META#clock", "ts": ts_iso}


# ============================================================ features (match glue_q1.build_features)
def _before(last_ts, prev_ts, t):
    """Latest timestamp strictly before t, from a (last, prev) pair."""
    for s in (last_ts, prev_ts):
        if s:
            v = datetime.fromisoformat(s)
            if v < t:
                return v, s
    return None, None


def build_features(p, ev_src, ev_dst, state):
    t, src, dst = p["ts"], p["src"], p["dst"]
    amt, amt_in = p["amt_paid"], p["amt_recv"]
    f = {}
    for w, days in WINDOWS:
        lo = t - timedelta(days=days)
        for prefix, evs, direction in [("s_out", ev_src, "out"), ("s_in", ev_src, "in"),
                                       ("r_in", ev_dst, "in"), ("r_out", ev_dst, "out")]:
            sel = [e for e in evs if e["dir"] == direction and lo < e["ts"] < t]
            f[f"{prefix}_n_{w}"] = float(len(sel))
            f[f"{prefix}_amt_{w}"] = float(sum(e["amt"] for e in sel))
            f[f"{prefix}_fresh_{w}"] = float(sum(e["fresh"] for e in sel))

    li = state.get(f"LASTIN#{src}", {})
    li_ts, li_key = _before(li.get("last_ts"), li.get("prev_ts"), t)
    if li_ts is None:
        f["s_hrs_since_in"], f["amt_match"] = math.nan, math.nan
    else:
        f["s_hrs_since_in"] = (t - li_ts).total_seconds() / 3600
        in_amt = li["last_amt"] if li_key == li.get("last_ts") else li["prev_amt"]
        f["amt_match"] = amt / in_amt if in_amt else (math.inf if amt > 0 else math.nan)
    s_in1 = f["s_in_amt_1D"]
    f["s_pass_1D"] = (f["s_out_amt_1D"] + amt) / s_in1 if s_in1 > 0 else 0.0

    rev = state.get(f"PAIR#{dst}|{src}", {})
    rv_ts, _ = _before(rev.get("last_ts"), rev.get("prev_ts"), t)
    f["cyc2_hrs"] = (t - rv_ts).total_seconds() / 3600 if rv_ts else math.nan

    f["s_burst"] = f["s_out_n_1D"] / ((f["s_out_n_7D"] - f["s_out_n_1D"]) / 6 + 1)
    f["r_burst"] = f["r_in_n_1D"] / ((f["r_in_n_7D"] - f["r_in_n_1D"]) / 6 + 1)

    pair = state.get(f"PAIR#{src}|{dst}")
    f["new_pair"] = 0.0 if pair else 1.0
    fresh = 1 if (not pair or t - datetime.fromisoformat(pair["last_ts"]) > timedelta(days=5)) else 0
    f["round_amt"] = 1.0 if amt % 100 == 0 else 0.0
    f["just_under_10k"] = 1.0 if 9000 <= amt < 10000 else 0.0
    f["log_amt"] = math.log1p(amt)
    f["cross_bank"] = 1.0 if p["from_bank"] != p["to_bank"] else 0.0
    f["self_transfer"] = 1.0 if src == dst else 0.0
    f["fx_mismatch"] = 1.0 if p["pay_cur"] != p["recv_cur"] else 0.0
    return f, fresh


def state_updates(p, state, fresh):
    """New DynamoDB items after this payment: two events, last inflow of receiver, the pair."""
    iso = p["ts"].isoformat()
    events = [{"acct": p["src"], "sk": f"{iso}#{p['txn_id']}#out", "amt": p["amt_paid"], "fresh": fresh},
              {"acct": p["dst"], "sk": f"{iso}#{p['txn_id']}#in", "amt": p["amt_recv"], "fresh": fresh}]
    k = f"LASTIN#{p['dst']}"
    li = dict(state.get(k) or {"pk": k})
    if "last_ts" not in li or iso > li["last_ts"]:
        if "last_ts" in li:
            li["prev_ts"], li["prev_amt"] = li["last_ts"], li["last_amt"]
        li["last_ts"], li["last_amt"] = iso, p["amt_recv"]
    else:                                     # same minute: latest payment wins, prev unchanged
        li["last_amt"] = p["amt_recv"]
    k = f"PAIR#{p['src']}|{p['dst']}"
    pr = dict(state.get(k) or {"pk": k})
    if "last_ts" not in pr or iso > pr["last_ts"]:
        if "last_ts" in pr:
            pr["prev_ts"] = pr["last_ts"]
        pr["last_ts"] = iso
    return events, [li, pr]


# ============================================================ scoring
WHO = {"s": "Sender", "r": "Receiver"}
DIRECTION = {"out": "sent", "in": "received"}
STAT = {"n": "payments", "amt": "amount", "fresh": "new-counterparty payments"}
SPECIAL = {"s_hrs_since_in": "Hours since sender last received money",
           "amt_match": "Payment / money just received",
           "s_pass_1D": "Share of sender's 1-day inflow passed on",
           "cyc2_hrs": "Hours since receiver paid the sender (round trip)",
           "s_burst": "Burst in sender's outgoing payments", "r_burst": "Burst in receiver's incoming payments"}
FLAGS = {"new_pair": ("First payment between these accounts", "Accounts have paid each other before"),
         "round_amt": ("Round amount", "Not a round amount"),
         "just_under_10k": ("Just under 10,000", "Not just under 10,000"),
         "cross_bank": ("Cross-bank payment", "Same-bank payment"),
         "self_transfer": ("Self transfer", "Not a self transfer"),
         "fx_mismatch": ("Currency conversion", "Same currency")}


def describe(name, value, p):
    """Plain-language reason for one feature."""
    if name == "log_amt":
        return f"Payment amount: {p['amt_paid']:,.2f} {p['pay_cur']}"
    if name in FLAGS:
        return FLAGS[name][0] if value == 1 else FLAGS[name][1]
    if name in SPECIAL:
        v = "none before" if value != value else (f"{value:,.2f}" if abs(value) < 1e9 else "very large")
        return f"{SPECIAL[name]}: {v}"
    prefix, stat, w = name.rsplit("_", 2)
    who, direction = prefix.split("_")
    days = w[:-1]
    shown = f"{value:,.2f}" if stat == "amt" else f"{value:,.0f}"
    return f"{WHO[who[0]]}: {STAT[stat]} {DIRECTION[direction]}, last {days} day{'s' if days != '1' else ''} = {shown}"


def score(model, feats, p, bank):
    """Sender's-bank view and/or receiver's-bank view (other bank's side blanked), max of the two."""
    meta, names = model.meta, model.names
    cross = p["from_bank"] != p["to_bank"]
    views = []
    if p["from_bank"] == bank:
        views.append(set(meta["r_cols"]) if cross else set())
    if p["to_bank"] == bank:
        views.append(set(meta["s_cols"]) if cross else set())
    best = None
    for blank in views:
        x = [math.nan if n in blank else feats[n] for n in names]
        prob, contrib = model.predict(x)
        if best is None or prob > best[0]:
            best = (prob, contrib, x)
    prob, contrib, x = best
    top = sorted(((c, i) for i, c in contrib.items() if c > 0), reverse=True)[:3]
    reasons = [describe(names[i], x[i], p) for _, i in top]
    return prob, reasons


def urgency(feats, p, bank):
    receiver_forwards = p["to_bank"] == bank and feats["r_out_n_1D"] > 0
    sender_passes_on = (p["from_bank"] == bank and feats["s_pass_1D"] >= 0.8
                        and feats["s_hrs_since_in"] == feats["s_hrs_since_in"] and feats["s_hrs_since_in"] <= 24)
    return "High" if receiver_forwards or sender_passes_on else "Normal"


def parse(rec, bank):
    """Clean one Kinesis record. Returns None if malformed. Drops is_laundering from features."""
    try:
        r = json.loads(base64.b64decode(rec["kinesis"]["data"]).decode().strip())
        fb, tb = str(int(r["from_bank"])), str(int(r["to_bank"]))
        p = {"txn_id": r["txn_id"], "ts": datetime.fromisoformat(r["txn_ts"]),
             "from_bank": fb, "to_bank": tb, "from_account": r["from_account"], "to_account": r["to_account"],
             "src": f"{fb}_{r['from_account']}", "dst": f"{tb}_{r['to_account']}",
             "amt_paid": float(r["amount_paid"]), "amt_recv": float(r["amount_received"]),
             "pay_cur": r["payment_currency"], "recv_cur": r["receiving_currency"],
             "payment_format": r.get("payment_format", "")}
        if p["amt_paid"] < 0 or p["amt_recv"] < 0:
            return None
        p["_truth"] = int(r.get("is_laundering", 0))      # demo simulator only, never a feature
        return p
    except (KeyError, ValueError, TypeError, json.JSONDecodeError):
        return None


def process(p, store, model, bank):
    ev_src = store.events(p["src"], p["ts"])
    ev_dst = ev_src if p["dst"] == p["src"] else store.events(p["dst"], p["ts"])
    state = store.get_state([f"LASTIN#{p['src']}", f"PAIR#{p['src']}|{p['dst']}", f"PAIR#{p['dst']}|{p['src']}",
                             f"LASTIN#{p['dst']}"])
    feats, fresh = build_features(p, ev_src, ev_dst, state)
    alert = None
    if model is not None:
        prob, reasons = score(model, feats, p, bank)
        if prob >= model.threshold:
            urg = urgency(feats, p, bank)
            alert = {"txn_id": p["txn_id"], "txn_ts": p["ts"].isoformat(), "bank": bank,
                     "side": "both" if p["from_bank"] == p["to_bank"] else
                             ("sender" if p["from_bank"] == bank else "receiver"),
                     "from_account": p["src"], "to_account": p["dst"],
                     "amount": p["amt_paid"], "currency": p["pay_cur"], "payment_format": p["payment_format"],
                     "risk": round(prob, 6), "threshold": round(model.threshold, 6),
                     "urgency": urg, "priority": round(prob * (1.5 if urg == "High" else 1.0), 6),
                     "reasons": reasons, "status": "open", "model_version": model.version,
                     "created_real": int(time.time()), "_sim_truth": p["_truth"]}
            store.put_alert(alert)
    events, states = state_updates(p, state, fresh)
    store.write(events, states)
    return feats, alert


# ============================================================ handler
_model, _etag, _checked, _store = None, None, 0.0, None


def load_model(s3, bucket):
    global _model, _etag, _checked
    if time.time() - _checked < RELOAD_EVERY_S and _model is not None:
        return _model
    _checked = time.time()
    try:
        etag = s3.head_object(Bucket=bucket, Key=MODEL_PREFIX + "meta.json")["ETag"]
    except Exception:
        print("[score] no model in models/best yet - updating state only")
        return None
    if etag != _etag:
        meta = json.loads(s3.get_object(Bucket=bucket, Key=MODEL_PREFIX + "meta.json")["Body"].read())
        dump = json.loads(s3.get_object(Bucket=bucket, Key=MODEL_PREFIX + "model.json")["Body"].read())
        _model, _etag = Model(dump, meta), etag
        check_parity(s3, bucket, _model)
        print(f"[score] loaded model {meta['version']} threshold={meta['threshold']:.4f}")
    return _model


def check_parity(s3, bucket, model):
    try:
        ref = json.loads(s3.get_object(Bucket=bucket, Key=MODEL_PREFIX + "parity.json")["Body"].read())
        diff = max(abs(model.predict([math.nan if v is None else v for v in row])[0] - pr)
                   for row, pr in zip(ref["x"], ref["prob"]))
        print(f"[score] parity check vs LightGBM: max prob difference {diff:.2e}")
    except Exception as e:
        print(f"[score] parity check skipped: {e}")


def handler(event, context):
    import boto3
    global _store
    bank = str(int(os.environ["BANK_ID"]))
    _store = _store or DynamoStore()
    model = load_model(boto3.client("s3"), os.environ["BUCKET"])
    seen = mine = alerts = bad = 0
    clock = None
    for rec in event["Records"]:
        p = parse(rec, bank)
        if p is None:
            bad += 1
            continue
        seen += 1
        iso = p["ts"].isoformat()
        clock = iso if clock is None or iso > clock else clock
        if bank not in (p["from_bank"], p["to_bank"]):
            continue                                  # filter: not our bank
        mine += 1
        try:
            _, alert = process(p, _store, model, bank)
            alerts += alert is not None
        except Exception as e:                        # never block the shard on one bad record
            print(f"[score] error on {p['txn_id']}: {e}")
    if clock:
        try:
            _store.set_clock(clock)
        except Exception:
            pass                                      # clock already ahead
    print(f"[score] records={len(event['Records'])} parsed={seen} bad={bad} ours={mine} alerts={alerts}")
    return {"ours": mine, "alerts": alerts}
