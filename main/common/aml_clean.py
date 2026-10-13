"""What counts as a valid payment record. Shared by Q1 and Q2 (PIPELINE_SPEC.md 3.2 and 4.3).

Both pipelines read the same record: Q1's Flink job takes it from Kinesis, and Q2's Glue job takes
the copy Firehose landed in raw/transactions/. The spec requires them to accept and reject the same
records ("the same rules as Flink"), so the rules live here once and both call clean().

Shared, because they mean the same thing in both questions:
  clean()            rules 1 to 5 below: is this a usable payment record?
  involves_bank()    the bank filter: the feed has every bank's payments, both pipelines keep ours
  one row per txn_id a txn_id is one payment. The batch jobs drop repeats (spark_common.apply_clean).
                     Flink cannot without unbounded state, so readers of its output keep one row per txn_id.

Not here, because only one pipeline wants them:
  Q1  the sender / receiver account keys used for features (Q1_files/q1_core.py)
  Q2  dropping moves within one account, the customer and FX joins, and their checks (Q2_files/glue/q2_etl.py)

Pure Python, no third-party imports, so it runs inside Flink, Spark and plain scripts alike.
"""
import json
import math
from datetime import datetime, timezone

DAY = 86400
TXN_FIELDS = ["txn_id", "txn_ts", "from_bank", "from_account", "to_bank", "to_account", "amount_paid",
              "payment_currency", "amount_received", "receiving_currency", "payment_format"]


class Reject(Exception):
    """A record that fails a cleaning rule. The message is the reason written to dlq/."""


def parse_ts(s):
    """ISO-8601 timestamp -> epoch seconds. A timestamp without a zone is taken as UTC."""
    d = datetime.fromisoformat(str(s).strip().replace("Z", "+00:00"))
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return int(d.timestamp())


def iso(ts):
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")


def day_of(ts):
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d")


def clean(r):
    """The cleaning rules. r is a dict as received; returns a cleaned dict or raises Reject.

      1. every field in TXN_FIELDS except payment_format is present and not blank
      2. txn_ts is an ISO-8601 timestamp
      3. bank IDs are whole numbers; leading zeros are removed ("011" and "11" are the same bank)
      4. amounts are numbers, finite and not negative
      5. text fields are trimmed
    Only the fields in TXN_FIELDS are returned. Anything else on the record, such as the demo
    data's is_laundering, is left for the caller to carry or drop.
    """
    if not isinstance(r, dict):
        raise Reject("not_a_json_object")
    for k in TXN_FIELDS:
        if k != "payment_format" and (r.get(k) is None or str(r[k]).strip() == ""):
            raise Reject(f"missing_{k}")
    try:
        ts = parse_ts(r["txn_ts"])
    except (ValueError, OverflowError, OSError):
        raise Reject("bad_txn_ts")
    try:
        fb, tb = str(int(str(r["from_bank"]).strip())), str(int(str(r["to_bank"]).strip()))
    except ValueError:
        raise Reject("bad_bank_id")
    try:
        paid, recv = float(r["amount_paid"]), float(r["amount_received"])
    except (ValueError, TypeError):
        raise Reject("bad_amount")
    if not (math.isfinite(paid) and math.isfinite(recv)):
        raise Reject("bad_amount")
    if paid < 0 or recv < 0:
        raise Reject("negative_amount")
    return {"txn_id": str(r["txn_id"]).strip(), "ts": ts, "from_bank": fb,
            "from_account": str(r["from_account"]).strip(), "to_bank": tb,
            "to_account": str(r["to_account"]).strip(), "amount_paid": paid, "payment_currency": str(r["payment_currency"]).strip(),
            "amount_received": recv, "receiving_currency": str(r["receiving_currency"]).strip(),
            "payment_format": str(r.get("payment_format") or "").strip()}


def involves_bank(p, bank):
    """The bank filter: is our bank the sender's or the receiver's bank on this cleaned payment?

    The feed carries every bank's payments. Another bank's payment is not a bad record, so it is
    filtered out quietly and never written to dlq/. Which bank is "ours" is set once, in
    common/settings.json.
    """
    return bank in (p["from_bank"], p["to_bank"])


def clean_json(line):
    """clean() for a raw JSON string."""
    try:
        r = json.loads(line)
    except (ValueError, TypeError):
        raise Reject("invalid_json")
    return clean(r)
