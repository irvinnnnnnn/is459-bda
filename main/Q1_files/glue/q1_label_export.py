"""Q1 daily label export (spec 3.2): DynamoDB alerts -> curated/live/labels/. Glue Python shell job.

For every alert and audit item whose 7-day review window has closed:
  1. write one row with its final outcome (confirmed, dismissed, or expired if nobody reviewed it)
  2. then delete the item from DynamoDB
S3 first, delete second: if the delete fails, the next run exports the item again, and training
keeps the latest row per txn_id, so a duplicate is harmless and nothing is ever lost.

"Closed" is judged against the demo's simulated clock when one is running, otherwise the real time.

Arguments: --bucket --alerts_table --region
"""
import io
import json
import sys
import time
import uuid
from datetime import datetime, timezone

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from awsglue.utils import getResolvedOptions

SCHEMA = pa.schema([("txn_id", pa.string()), ("outcome", pa.string()), ("is_audit", pa.bool_()),
                    ("reviewer", pa.string()), ("reason", pa.string()), ("note", pa.string()),
                    ("decided_at", pa.timestamp("ms")), ("created_at", pa.timestamp("ms")),
                    ("expires_at", pa.timestamp("ms")), ("score", pa.float64()), ("threshold", pa.float64()),
                    ("model_version", pa.string()), ("exported_at", pa.timestamp("ms"))])


def sim_now(s3, bucket):
    try:
        st = json.loads(s3.get_object(Bucket=bucket, Key="state/sim_clock.json")["Body"].read())
    except s3.exceptions.NoSuchKey:
        return time.time()
    if not st.get("running"):
        return float(st["sim_anchor"])
    return float(st["sim_anchor"]) + (time.time() - float(st["wall_anchor"])) * float(st["speed"])


def ms(v):
    return None if v is None else int(float(v) * 1000)


def main():
    a = getResolvedOptions(sys.argv, ["bucket", "alerts_table", "region"])
    s3 = boto3.client("s3", region_name=a["region"])
    table = boto3.resource("dynamodb", region_name=a["region"]).Table(a["alerts_table"])
    now = sim_now(s3, a["bucket"])

    closed, kw = [], {"FilterExpression": "expires_at <= :now", "ExpressionAttributeValues": {":now": int(now)}}
    while True:
        r = table.scan(**kw)
        closed += r["Items"]
        if "LastEvaluatedKey" not in r:
            break
        kw["ExclusiveStartKey"] = r["LastEvaluatedKey"]
    print(f"[labels] as of {datetime.fromtimestamp(now, timezone.utc):%Y-%m-%d %H:%M} UTC: "
          f"{len(closed)} items past their review window")
    if not closed:
        return

    rows = [{"txn_id": it["txn_id"],
             "outcome": it["status"] if it.get("status") in ("confirmed", "dismissed") else "expired",
             "is_audit": bool(it.get("is_audit")), "reviewer": it.get("reviewer"), "reason": it.get("reason"),
             "note": it.get("note"), "decided_at": ms(it.get("decided_at")), "created_at": ms(it.get("created_at")),
             "expires_at": ms(it.get("expires_at")),
             "score": float(it["score"]) if it.get("score") is not None else None,
             "threshold": float(it["threshold"]) if it.get("threshold") is not None else None,
             "model_version": it.get("model_version"), "exported_at": int(now * 1000)} for it in closed]
    buf = io.BytesIO()
    pq.write_table(pa.Table.from_pylist(rows, schema=SCHEMA), buf, compression="snappy")
    day = datetime.fromtimestamp(now, timezone.utc).strftime("%Y-%m-%d")
    key = f"curated/live/labels/export_date={day}/labels-{uuid.uuid4().hex}.parquet"
    s3.put_object(Bucket=a["bucket"], Key=key, Body=buf.getvalue())
    counts = {o: sum(r["outcome"] == o for r in rows) for o in ("confirmed", "dismissed", "expired")}
    print(f"[labels] wrote s3://{a['bucket']}/{key}: {counts}")

    with table.batch_writer() as w:                    # only after the rows are safely in S3
        for it in closed:
            w.delete_item(Key={"txn_id": it["txn_id"]})
    print(f"[labels] deleted {len(closed)} items from {a['alerts_table']}")


if __name__ == "__main__":
    main()
