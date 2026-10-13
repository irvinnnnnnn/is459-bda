"""Q1 scoring and output, shared by the Flink job (main.py) and the local runner (local_runner.py).

Scorer   loads the approved model named in models/best/manifest.json and re-checks the manifest
         every few seconds, so a newly promoted model is picked up without a restart (spec 3.2 step 8).
         score() turns one payment and its features into the row for curated/live/scored/ and, for
         alerts and audit items, the DynamoDB item with its SHAP risk factors.
Outputs  write_scored() / write_dlq() write one S3 object per batch; put_alert() writes DynamoDB.
"""
import io
import json
import time
import uuid
from decimal import Decimal

import aml_clean
import q1_core as core

SCORED_PREFIX = "curated/live/scored/"
DLQ_PREFIX = "dlq/"
MANIFEST_KEY = "models/best/manifest.json"


def scored_columns():
    """(name, type) for every column of curated/live/scored/, in order. The Glue table is built from this."""
    return ([("txn_id", "string"), ("txn_ts", "timestamp"), ("from_bank", "string"), ("from_account", "string"),
             ("to_bank", "string"), ("to_account", "string"), ("amount_paid", "double"),
             ("payment_currency", "string"), ("amount_received", "double"), ("receiving_currency", "string"),
             ("payment_format", "string"), ("score", "double"), ("threshold", "double"), ("is_alert", "boolean"),
             ("is_audit", "boolean"), ("model_version", "string"),
             ("arrived_at", "timestamp"), ("scored_at", "timestamp")] + [(f, "double") for f in core.FEATURES])


class Scorer:
    def __init__(self, bucket, our_bank, alerts_table=None, audit_rate=0.003, region=None, poll_seconds=30,
                 backstop_days=30, log=print):
        import boto3
        self.bucket, self.our_bank, self.audit_rate = bucket, str(int(our_bank)), float(audit_rate)
        self.poll_seconds, self.backstop_days, self.log = poll_seconds, backstop_days, log
        self.s3 = boto3.client("s3", region_name=region)
        self.table = boto3.resource("dynamodb", region_name=region).Table(alerts_table) if alerts_table else None
        self.model, self._etag, self._checked = None, None, 0.0

    # ---- model hot-reload
    def _get(self, key):
        return json.loads(self.s3.get_object(Bucket=self.bucket, Key=key)["Body"].read())

    def current_model(self):
        if time.time() - self._checked < self.poll_seconds:
            return self.model
        self._checked = time.time()
        try:
            etag = self.s3.head_object(Bucket=self.bucket, Key=MANIFEST_KEY)["ETag"]
        except Exception as exc:                       # no approved model yet: features are still logged
            if self.model is None:
                self.log(f"[q1] no model manifest yet ({type(exc).__name__}); scoring is off until one is promoted")
            return self.model
        if etag == self._etag:
            return self.model
        try:
            manifest = self._get(MANIFEST_KEY)
            prefix = manifest["artifact"]
            meta = self._get(prefix + "meta.json")
            meta.update({k: manifest[k] for k in ("version", "threshold") if k in manifest})
            model = core.Model(self._get(prefix + "model.json"), meta)
            try:
                dp, ds = model.parity(self._get(prefix + "parity.json"))
                self.log(f"[q1] parity with LightGBM: probability {dp:.1e}, SHAP {ds:.1e}")
            except Exception as exc:
                self.log(f"[q1] parity check skipped: {exc}")
            self.model, self._etag = model, etag
            self.log(f"[q1] loaded model {model.version}, threshold {model.threshold:.6f}")
        except Exception as exc:                       # keep serving the model we already have
            self.log(f"[q1] could not load the model in the manifest: {exc}")
        return self.model

    # ---- one payment
    def score(self, p, feats, arrived_ms):
        """Returns (scored_row, alert_item or None)."""
        model = self.current_model()
        row = {k: p[k] for k in ("txn_id", "from_bank", "from_account", "to_bank", "to_account", "amount_paid",
                                 "payment_currency", "amount_received", "receiving_currency", "payment_format")}
        row.update(txn_ts=p["ts"] * 1000, score=None, threshold=None, is_alert=False, is_audit=False,
                   model_version=None, arrived_at=arrived_ms)
        row.update({f: (None if feats[f] != feats[f] else feats[f]) for f in core.FEATURES})
        item = None
        if model is None:
            row["scored_at"] = int(time.time() * 1000)
            return row, None
        prob, x, blank = model.score(feats, p, self.our_bank)
        row["scored_at"] = int(time.time() * 1000)      # time to flag stops here; SHAP runs after
        alert = prob >= model.threshold
        audit = not alert and core.is_audit(p["txn_id"], self.audit_rate)
        row.update(score=prob, threshold=model.threshold, is_alert=alert, is_audit=audit,
                   model_version=model.version)
        if alert or audit:
            reasons = model.reasons(x, blank, p["from_bank"] != p["to_bank"])
            item = core.alert_item(p, feats, prob, model, reasons, audit)
            # DynamoDB TTL is only a backstop for a failed label export, and it runs on the real
            # clock, so it is set from the real time and never from the (possibly replayed) event time.
            item["ttl_at"] = int(time.time()) + self.backstop_days * aml_clean.DAY
        return row, item

    def put_alert(self, item):
        """Written once per payment; a replay after a restart must not undo a decision."""
        from botocore.exceptions import ClientError
        try:
            self.table.put_item(Item=_ddb(item), ConditionExpression="attribute_not_exists(txn_id)")
        except ClientError as exc:
            if exc.response["Error"]["Code"] != "ConditionalCheckFailedException":
                raise


def _ddb(v):
    if isinstance(v, float):
        return Decimal(repr(v))
    if isinstance(v, dict):
        return {k: _ddb(x) for k, x in v.items() if x is not None}
    if isinstance(v, (list, tuple)):
        return [_ddb(x) for x in v]
    return v


def write_scored(s3, bucket, rows, tag="0"):
    """One Parquet object per event date. Returns the keys written."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    kinds = {"string": pa.string(), "double": pa.float64(), "boolean": pa.bool_(), "timestamp": pa.timestamp("ms")}
    cols = scored_columns()
    schema = pa.schema([(n, kinds[t]) for n, t in cols])
    by_day, keys = {}, []
    for r in rows:
        by_day.setdefault(aml_clean.day_of(r["txn_ts"] // 1000), []).append(r)
    for day, part in by_day.items():
        table = pa.table({n: pa.array([r.get(n) for r in part], type=kinds[t]) for n, t in cols}, schema=schema)
        buf = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        key = f"{SCORED_PREFIX}date={day}/part-{tag}-{uuid.uuid4().hex}.parquet"
        s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())
        keys.append(key)
    return keys


def write_dlq(s3, bucket, lines, tag="0"):
    """Rejected records as JSON lines: {raw, error, source, at}."""
    if not lines:
        return None
    key = f"{DLQ_PREFIX}source=flink/date={aml_clean.day_of(int(time.time()))}/part-{tag}-{uuid.uuid4().hex}.jsonl"
    s3.put_object(Bucket=bucket, Key=key, Body=("\n".join(lines) + "\n").encode())
    return key


def dlq_line(raw, error):
    return json.dumps({"raw": raw if isinstance(raw, str) else repr(raw), "error": error, "source": "flink",
                       "at": aml_clean.iso(int(time.time()))})
