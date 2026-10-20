"""Q1 only: the features and the model (PIPELINE_SPEC.md 3.2). Pure Python, no third-party imports.

One copy, used by every Q1 program that needs it, so they cannot drift apart:
  side_features()    per-account features from state    the Flink job, and the Glue feature job's parity check
  FEATURES           the feature list and its order     Flink, the Glue jobs, the scored table's columns
  Model              LightGBM scoring + exact TreeSHAP  the Flink job

Why the model is scored in pure Python: Managed Flink then needs no native library. train.py saves
the booster's JSON dump plus parity samples (probabilities and SHAP values from LightGBM itself),
and Model.parity() checks this code against them every time a model is loaded.

Q2 does not use this file. The cleaning rules both pipelines share are in common/aml_clean.py.
"""
import math
import struct

from aml_clean import DAY

WINDOWS = [("1D", 1), ("5D", 5), ("7D", 7)]
PREFIXES = ["s_out", "s_in", "r_in", "r_out"]
FRESH_DAYS = 5          # a payment is "fresh" if the pair has not transacted in this many days
PAIR_DAYS = 30          # how long a pair of accounts is remembered (new_pair, cyc2_hrs)
DISTINCT = ["s_in_distinct_7D", "s_out_distinct_7D", "r_in_distinct_7D", "r_out_distinct_7D"]
FEATURES = ([f"{p}_{c}_{w}" for w, _ in WINDOWS for p in PREFIXES for c in ("n", "amt", "fresh")] +
            ["s_hrs_since_in", "s_pass_1D", "s_burst", "r_burst", "amt_match", "cyc2_hrs",
             "round_amt", "just_under_10k", "new_pair", "log_amt", "cross_bank", "self_transfer",
             "fx_mismatch"] + DISTINCT)
# Blanked when the account is at another bank: we cannot see its other payments.
S_COLS = [f for f in FEATURES if f.startswith("s_")] + ["amt_match"]
R_COLS = [f for f in FEATURES if f.startswith("r_")]
# The 7-day summary stored on each alert item (spec 3.2, "features").
SUMMARY = ["s_in_n_7D", "s_in_amt_7D", "s_out_n_7D", "s_out_amt_7D", "s_in_distinct_7D", "s_out_distinct_7D",
           "r_in_n_7D", "r_in_amt_7D", "r_out_n_7D", "r_out_amt_7D", "r_in_distinct_7D", "r_out_distinct_7D"]
NAN = float("nan")


def add_keys(p):
    """Q1's account keys on a cleaned payment: bank and account together identify one account.

    Features are kept per account, and the same account number can exist at two banks.
    """
    p["src"] = f"{p['from_bank']}_{p['from_account']}"
    p["dst"] = f"{p['to_bank']}_{p['to_account']}"
    return p


# ================================================================= features ====
# Per-account state, a plain dict so it pickles small:
#   ev    [[ts, is_in, counterparty, amount, fresh], ...]  this account's payments, last 7 days
#   lin   [last_ts, last_amt, prev_ts, prev_amt]           its latest inflow, and the one before
#   pout  {counterparty: [last_ts, prev_ts]}               when it last paid each counterparty
#   pin   {counterparty: [last_ts, prev_ts]}               when each counterparty last paid it
#   mx    latest event time seen
# "prev" is always the latest time strictly before "last", so "the latest one strictly before t"
# can be answered even when several payments share t (timestamps are to the minute).
def new_state():
    return {"ev": [], "lin": None, "pout": {}, "pin": {}, "mx": 0}


def _bump(pair, t):
    if pair is None:
        return [t, None]
    if t > pair[0]:
        return [t, pair[0]]
    if t < pair[0] and (pair[1] is None or t > pair[1]):
        return [pair[0], t]
    return pair


def _before(pair, t):
    """Latest time in [last, prev] strictly before t, within PAIR_DAYS; else None."""
    if pair:
        for v in pair:
            if v is not None and v < t:
                return v if t - v <= PAIR_DAYS * DAY else None
    return None


def _is_fresh(pair, t):
    return pair is None or t - pair[0] > FRESH_DAYS * DAY


def side_features(st, role, p):
    """Features for one side of payment p from that account's state, then the state update.

    role "s": st belongs to the sender (p["src"]).  role "r": st belongs to the receiver.
    Every figure uses only payments strictly before p, so features are point-in-time correct.
    Returns the feature dict for that side.
    """
    t, f = p["ts"], {}
    sending = role == "s"
    cp = p["dst"] if sending else p["src"]
    mine, other = ("s_out", "s_in") if sending else ("r_out", "r_in")
    lo7 = t - 7 * DAY
    sums = {(d, w): [0.0, 0.0, 0.0] for d in (0, 1) for w, _ in WINDOWS}
    seen = (set(), set())
    for ts, d, c, amt, fresh in st["ev"]:
        if ts >= t or ts <= lo7:
            continue
        seen[d].add(c)
        age = t - ts
        for w, days in WINDOWS:
            if age < days * DAY:
                s = sums[(d, w)]
                s[0] += 1.0
                s[1] += amt
                s[2] += fresh
    for w, _ in WINDOWS:
        for prefix, d in ((mine, 0), (other, 1)):
            n, a, fr = sums[(d, w)]
            f[f"{prefix}_n_{w}"], f[f"{prefix}_amt_{w}"], f[f"{prefix}_fresh_{w}"] = n, a, fr
    f[f"{role}_in_distinct_7D"], f[f"{role}_out_distinct_7D"] = float(len(seen[1])), float(len(seen[0]))

    if sending:
        amt = p["amount_paid"]
        lin = st["lin"]
        at = None
        if lin:
            at = (lin[0], lin[1]) if lin[0] < t else ((lin[2], lin[3]) if lin[2] is not None else None)
        f["s_hrs_since_in"] = (t - at[0]) / 3600.0 if at else NAN
        f["amt_match"] = amt / at[1] if at and at[1] > 0 else NAN
        f["s_pass_1D"] = (f["s_out_amt_1D"] + amt) / f["s_in_amt_1D"] if f["s_in_amt_1D"] > 0 else 0.0
        f["s_burst"] = f["s_out_n_1D"] / ((f["s_out_n_7D"] - f["s_out_n_1D"]) / 6.0 + 1.0)
        pair = st["pout"].get(cp)
        if pair is not None and t - pair[0] > PAIR_DAYS * DAY:
            pair = None
        fresh = 1.0 if _is_fresh(pair, t) else 0.0
        f["new_pair"] = 1.0 if pair is None else 0.0
        back = _before(st["pin"].get(cp), t)
        f["cyc2_hrs"] = (t - back) / 3600.0 if back is not None else NAN
        st["ev"].append([t, 0, cp, amt, fresh])
        st["pout"][cp] = _bump(pair, t)
    else:
        amt = p["amount_received"]
        f["r_burst"] = f["r_in_n_1D"] / ((f["r_in_n_7D"] - f["r_in_n_1D"]) / 6.0 + 1.0)
        pair = st["pin"].get(cp)
        if pair is not None and t - pair[0] > PAIR_DAYS * DAY:
            pair = None
        st["ev"].append([t, 1, cp, amt, 1.0 if _is_fresh(pair, t) else 0.0])
        st["pin"][cp] = _bump(pair, t)
        lin = st["lin"]
        if lin is None:
            st["lin"] = [t, amt, None, None]
        elif t > lin[0]:
            st["lin"] = [t, amt, lin[0], lin[1]]
        elif t == lin[0]:
            lin[1] = amt                      # same minute: the latest payment wins
        elif lin[2] is None or t >= lin[2]:
            lin[2], lin[3] = t, amt
    if t > st["mx"]:
        st["mx"] = t
    return f


def prune(st, now_ts=None):
    """Drop what can no longer affect a feature. Returns True if the state is now empty."""
    ref = max(st["mx"], now_ts or 0)
    lo = ref - 7 * DAY
    st["ev"] = [e for e in st["ev"] if e[0] > lo]
    old = ref - PAIR_DAYS * DAY
    for k in ("pout", "pin"):
        dead = [c for c, v in st[k].items() if v[0] < old]
        for c in dead:
            del st[k][c]
    return not (st["ev"] or st["pout"] or st["pin"])


def txn_features(p):
    amt = p["amount_paid"]
    return {"log_amt": math.log1p(amt), "round_amt": 1.0 if amt % 100 == 0 else 0.0,
            "just_under_10k": 1.0 if 9000 <= amt < 10000 else 0.0,
            "cross_bank": 1.0 if p["from_bank"] != p["to_bank"] else 0.0,
            "self_transfer": 1.0 if p["src"] == p["dst"] else 0.0,
            "fx_mismatch": 1.0 if p["payment_currency"] != p["receiving_currency"] else 0.0}


class LocalFeatures:
    """All account states in one dict. For tests, the parity check and the local runner."""

    def __init__(self):
        self.acct = {}

    def features(self, p):
        s = side_features(self.acct.setdefault(p["src"], new_state()), "s", p)
        r = side_features(self.acct.setdefault(p["dst"], new_state()), "r", p)
        for k in (p["src"], p["dst"]):
            if len(self.acct[k]["ev"]) > 64:
                prune(self.acct[k])
        return {**s, **r, **txn_features(p)}


# ==================================================================== model ====
def _f32(v):
    """LightGBM compares float32 inputs against thresholds; match it at the boundaries."""
    if v != v or math.isinf(v):
        return v
    try:
        return struct.unpack("f", struct.pack("f", v))[0]
    except OverflowError:
        return math.copysign(math.inf, v)


class Model:
    """A LightGBM binary classifier from booster.dump_model(), scored and explained in pure Python."""

    def __init__(self, dump, meta):
        self.meta = meta
        self.names = dump["feature_names"]
        self.version = str(meta["version"])
        self.threshold = float(meta["threshold"])
        self.s_cols, self.r_cols = set(meta["s_cols"]), set(meta["r_cols"])
        self.trees = [self._flatten(t["tree_structure"]) for t in dump["tree_info"]]

    @staticmethod
    def _flatten(root):
        """Tree as parallel lists: feature, threshold, left, right, default_left, missing, value, count."""
        feat, thr, left, right, dleft, miss, val, cnt = [], [], [], [], [], [], [], []

        def add(node):
            i = len(feat)
            for lst in (feat, thr, left, right, dleft, miss, val, cnt):
                lst.append(None)
            if "split_index" in node:
                if node.get("decision_type", "<=") != "<=":
                    raise ValueError("only numerical splits are supported")
                feat[i], thr[i] = node["split_feature"], node["threshold"]
                dleft[i] = bool(node["default_left"])
                miss[i] = {"None": 0, "Zero": 1, "NaN": 2}[node.get("missing_type", "None")]
                cnt[i] = float(node["internal_count"])
                left[i], right[i] = add(node["left_child"]), add(node["right_child"])
            else:
                feat[i], val[i], cnt[i] = -1, node["leaf_value"], float(node.get("leaf_count", 0))
            return i

        add(root)
        return feat, thr, left, right, dleft, miss, val, cnt

    @staticmethod
    def _left(v, thr, dleft, miss):
        if v != v and miss != 2:
            v = 0.0
        if (miss == 1 and -1e-35 <= v <= 1e-35) or (miss == 2 and v != v):
            return dleft
        return v <= thr

    def raw(self, x):
        total = 0.0
        go = self._left
        for feat, thr, left, right, dleft, miss, val, _ in self.trees:
            i = 0
            while feat[i] >= 0:
                i = left[i] if go(x[feat[i]], thr[i], dleft[i], miss[i]) else right[i]
            total += val[i]
        return total

    def predict(self, x):
        """x: floats in self.names order, NaN = missing. Returns the probability."""
        return 1.0 / (1.0 + math.exp(-self.raw([_f32(float(v)) for v in x])))

    # ---- exact TreeSHAP (Lundberg et al.), the algorithm LightGBM runs for pred_contrib=True
    def shap(self, x):
        """Per-feature contributions to the raw score (log-odds), in self.names order."""
        x = [_f32(float(v)) for v in x]
        phi = [0.0] * len(self.names)
        for tree in self.trees:
            if tree[0][0] >= 0:
                self._shap(tree, x, phi, 0, [], 1.0, 1.0, -1)
        return phi

    def _shap(self, tree, x, phi, node, parent, pz, po, pf):
        feat, thr, left, right, dleft, miss, val, cnt = tree
        path = [e[:] for e in parent]                       # [feature, zero_frac, one_frac, weight]
        d = len(path)
        path.append([pf, pz, po, 1.0 if d == 0 else 0.0])
        for i in range(d - 1, -1, -1):
            path[i + 1][3] += po * path[i][3] * (i + 1) / (d + 1)
            path[i][3] = pz * path[i][3] * (d - i) / (d + 1)
        f = feat[node]
        if f < 0:
            leaf = val[node]
            for i in range(1, d + 1):
                one, zero = path[i][2], path[i][1]
                nxt, total = path[d][3], 0.0
                for j in range(d - 1, -1, -1):
                    if one != 0:
                        tmp = nxt * (d + 1) / ((j + 1) * one)
                        total += tmp
                        nxt = path[j][3] - tmp * zero * (d - j) / (d + 1)
                    else:
                        total += path[j][3] / zero / ((d - j) / (d + 1))
                phi[path[i][0]] += total * (one - zero) * leaf
            return
        hot, cold = ((left[node], right[node]) if self._left(x[f], thr[node], dleft[node], miss[node])
                     else (right[node], left[node]))
        w = cnt[node]
        iz, io = 1.0, 1.0
        k = next((i for i in range(d + 1) if path[i][0] == f), None)
        if k is not None:                                   # already split on this feature: undo it
            iz, io = path[k][1], path[k][2]
            one, zero, nxt = io, iz, path[d][3]
            for j in range(d - 1, -1, -1):
                if one != 0:
                    tmp = path[j][3]
                    path[j][3] = nxt * (d + 1) / ((j + 1) * one)
                    nxt = tmp - path[j][3] * zero * (d - j) / (d + 1)
                else:
                    path[j][3] = path[j][3] * (d + 1) / (zero * (d - j))
            for j in range(k, d):
                path[j][0], path[j][1], path[j][2] = path[j + 1][0], path[j + 1][1], path[j + 1][2]
            path.pop()
        self._shap(tree, x, phi, hot, path, iz * cnt[hot] / w, io, f)
        self._shap(tree, x, phi, cold, path, iz * cnt[cold] / w, 0.0, f)

    # ---- scoring one payment
    def vector(self, feats, blank=()):
        return [NAN if n in blank else feats.get(n, NAN) for n in self.names]

    def score(self, feats, p, our_bank):
        """Our bank's view of the payment: sender's, receiver's, or both; the score is the higher.

        For a cross-bank payment the other bank's features are blanked. Returns (prob, x, blank).
        """
        cross = p["from_bank"] != p["to_bank"]
        views = []
        if p["from_bank"] == our_bank:
            views.append(self.r_cols if cross else set())
        if p["to_bank"] == our_bank:
            views.append(self.s_cols if cross else set())
        best = None
        for blank in views or [set()]:
            x = self.vector(feats, blank)
            prob = self.predict(x)
            if best is None or prob > best[0]:
                best = (prob, x, blank)
        return best

    def reasons(self, x, blank, cross, top=3):
        """The three features that pushed this payment's score up the most (spec 3.2 step 6).

        "The other party is at another bank" is context shown on the payment, not a risk factor,
        so blanked features and cross_bank are never picked.
        """
        skip = set(blank) | ({"cross_bank"} if cross else set())
        phi = self.shap(x)
        ranked = sorted(((c, i) for i, c in enumerate(phi) if c > 0 and self.names[i] not in skip), reverse=True)
        return [{"feature": self.names[i], "value": None if x[i] != x[i] else float(x[i]),
                 "contribution": round(c, 4)} for c, i in ranked[:top]]

    def parity(self, ref):
        """Largest difference from LightGBM's own output on the saved samples: (probability, SHAP)."""
        dp = ds = 0.0
        for n, (row, prob) in enumerate(zip(ref["x"], ref["prob"])):
            x = [NAN if v is None else v for v in row]
            dp = max(dp, abs(self.predict(x) - prob))
            if n < len(ref.get("shap", [])):
                ds = max(ds, max(abs(a - b) for a, b in zip(self.shap(x), ref["shap"][n])))
        return dp, ds


def is_audit(txn_id, rate):
    """Deterministic random sample, so a replayed or retried payment gets the same answer."""
    h = 0
    for ch in txn_id:
        h = (h * 131 + ord(ch)) & 0xFFFFFFFF
    return (h * 2654435761 & 0xFFFFFFFF) / 4294967296.0 < rate


def alert_item(p, feats, prob, model, reasons, audit, review_days=7):
    """The DynamoDB item for an alert or audit item (spec 3.2, "DynamoDB alerts").

    created_at and expires_at are in event time: they move with the payment's own clock, so the
    7-day review window is the same whether the feed is live or an accelerated replay.
    """
    return {"txn_id": p["txn_id"], "is_audit": bool(audit), "txn_ts": p["ts"],
            "from_bank": p["from_bank"], "from_account": p["from_account"],
            "to_bank": p["to_bank"], "to_account": p["to_account"],
            "amount": p["amount_paid"], "currency": p["payment_currency"],
            "amount_received": p["amount_received"], "receiving_currency": p["receiving_currency"],
            "payment_format": p["payment_format"], "score": round(prob, 8),
            "threshold": round(model.threshold, 8), "model_version": model.version, "reasons": reasons,
            "features": {k: (None if feats[k] != feats[k] else feats[k]) for k in SUMMARY},
            "status": "open", "created_at": p["ts"], "expires_at": p["ts"] + review_days * DAY}
