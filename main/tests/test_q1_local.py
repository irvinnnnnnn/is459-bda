"""Q1 end to end on your laptop, with S3 replaced by a dictionary. No AWS needed.

    pip install lightgbm pandas numpy pyarrow scikit-learn boto3
    python main/producer/producer.py prepare --csv <HI-Small_Trans.csv>     (once)
    python main/tests/test_q1_local.py

Runs the real train.py and evaluate.py on features from q1_core, then replays the live days through
q1_scoring.Scorer exactly as Flink does: model loaded from the manifest, payments scored, alerts and
audit items built with SHAP risk factors, scored rows written as Parquet and read back.
"""
import argparse
import io
import json
import os
import sys
import tempfile
import time

import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
for sub in ("common", "Q1_files", os.path.join("Q1_files", "flink"), os.path.join("Q1_files", "sagemaker"), "tests"):
    sys.path.insert(0, os.path.join(HERE, "..", sub))
os.environ["SKIP_PIP"] = "1"

import boto3  # noqa: E402

import config  # noqa: E402
import q1_core as core  # noqa: E402
from test_core import DATA, load  # noqa: E402


class FakeS3:
    class exceptions:
        class NoSuchKey(Exception):
            pass

    def __init__(self):
        self.objects = {}

    def put_object(self, Bucket, Key, Body, **kw):
        self.objects[Key] = Body if isinstance(Body, bytes) else Body.encode()

    def get_object(self, Bucket, Key):
        if Key not in self.objects:
            raise self.exceptions.NoSuchKey(Key)
        return {"Body": io.BytesIO(self.objects[Key])}

    def head_object(self, Bucket, Key):
        if Key not in self.objects:
            raise self.exceptions.NoSuchKey(Key)
        return {"ETag": str(hash(self.objects[Key]))}

    def upload_file(self, path, bucket, key):
        with open(path, "rb") as fh:
            self.objects[key] = fh.read()

    def download_fileobj(self, bucket, key, fh):
        fh.write(self.objects[key])

    def copy(self, src, dst):
        self.objects[dst] = self.objects[src]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", default=DATA)
    ap.add_argument("--bank", default="11")
    ap.add_argument("--live-start", default=config.SETTINGS["live_start"])
    a = ap.parse_args()
    bank = str(int(a.bank))
    rows, _ = load(a.data_dir, bank)
    lf = core.LocalFeatures()
    feats = [lf.features(p) for p in rows]
    df = pd.DataFrame(feats)[core.FEATURES]
    df.insert(0, "txn_id", [p["txn_id"] for p in rows])
    df["txn_ts"] = pd.to_datetime([p["ts"] for p in rows], unit="s")
    df["from_bank"], df["to_bank"] = [p["from_bank"] for p in rows], [p["to_bank"] for p in rows]
    df["label"], df["source"] = [p["label"] for p in rows], "historical"
    is_live = df.txn_ts >= pd.Timestamp(a.live_start)
    hist = df[~is_live].reset_index(drop=True)
    n = len(hist)

    s3 = FakeS3()
    boto3.client = lambda *args, **kw: s3
    tmp = tempfile.mkdtemp()
    for name, part in (("train", hist[:int(n * .6)]), ("val", hist[int(n * .6):int(n * .8)]), ("test", hist[int(n * .8):])):
        os.makedirs(os.path.join(tmp, name))
        part.to_parquet(os.path.join(tmp, name, "part.parquet"))
        os.environ[{"train": "SM_CHANNEL_TRAIN", "val": "SM_CHANNEL_VAL", "test": "TEST_DIR"}[name]] = os.path.join(tmp, name)
    os.environ["SM_MODEL_DIR"] = os.path.join(tmp, "model")

    import evaluate
    import train
    for run_id in ("run-a", "run-b"):                 # run-b meets run-a as the champion
        sys.argv = ["x", "--bucket", "b", "--bank", bank, "--run_id", run_id]
        train.main()
        evaluate.main()
        ev = json.loads(s3.objects[f"models/artifacts/run={run_id}/evaluation.json"])
        print(f"{run_id}: promote={ev['promote']}  {ev['reason']}")
        if ev["promote"]:
            s3.copy(f"models/artifacts/run={run_id}/manifest.json", "models/best/manifest.json")
    assert json.loads(s3.objects["models/best/manifest.json"])["version"] == "run-a", "an identical retrain must not be promoted"

    # ---- live days through the scorer Flink uses
    import q1_scoring
    scorer = q1_scoring.Scorer.__new__(q1_scoring.Scorer)
    scorer.bucket, scorer.our_bank, scorer.audit_rate, scorer.poll_seconds = "b", bank, 0.003, 30
    scorer.backstop_days, scorer.log, scorer.s3, scorer.table = 30, print, s3, None
    scorer.model, scorer._etag, scorer._checked = None, None, 0.0
    out, items, t0 = [], [], time.perf_counter()
    for i in is_live[is_live].index:
        row, item = scorer.score(rows[i], feats[i], int(time.time() * 1000))
        out.append(row)
        if item:
            items.append(item)
    dt = (time.perf_counter() - t0) / max(len(out), 1)
    alerts = [it for it in items if not it["is_audit"]]
    caught = sum(rows[i]["label"] for i, r in zip(is_live[is_live].index, out) if r["is_alert"])
    total = int(df.label[is_live].sum())
    print(f"live: {len(out)} payments scored at {dt * 1e3:.2f} ms each (including SHAP for alerts), {len(alerts)} alerts "
          f"({len(alerts) / len(out):.1%}), {len(items) - len(alerts)} audit items; {caught} of {total} laundering payments flagged")
    assert alerts and all(len(it["reasons"]) <= 3 for it in items)
    assert all(r["feature"] != "cross_bank" for it in items for r in it["reasons"])
    print("example alert:", json.dumps({k: alerts[0][k] for k in ("txn_id", "score", "threshold", "reasons")}))
    q1_scoring._ddb(alerts[0])                         # every value has a DynamoDB type

    keys = q1_scoring.write_scored(s3, "b", out, tag="test")
    back = pd.concat(pd.read_parquet(io.BytesIO(s3.objects[k])) for k in keys)
    assert len(back) == len(out) and list(back.columns) == [c for c, _ in q1_scoring.scored_columns()]
    assert str(back.txn_ts.dtype).startswith("datetime64") and back.is_alert.sum() == len(alerts)
    print(f"scored rows: {len(keys)} Parquet files, {len(back.columns)} columns, read back OK:", sorted(keys)[0])
    print("OK")


if __name__ == "__main__":
    main()
