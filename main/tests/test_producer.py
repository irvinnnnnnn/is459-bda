"""Both producers against fake Kinesis and S3: timing, the shared clock, resume, and the stress rate.

    python main/producer/producer.py prepare --csv <HI-Small_Trans.csv>     (once)
    python main/tests/test_producer.py
"""
import glob
import io
import json
import os
import signal
import sys
import time
from argparse import Namespace
from datetime import datetime, timedelta

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path[:0] = [os.path.join(HERE, "..", "common"), os.path.join(HERE, "..", "producer")]
import boto3  # noqa: E402

import config  # noqa: E402
import producer  # noqa: E402
import stress  # noqa: E402
from sim_clock import CLOCK_KEY, sim_now  # noqa: E402


class FakeS3:
    class exceptions:
        class NoSuchKey(Exception):
            pass
    objects = {}

    first_put = None

    def put_object(self, Bucket, Key, Body, **kw):
        FakeS3.first_put = FakeS3.first_put or time.time()
        self.objects[Key] = Body

    def get_object(self, Bucket, Key):
        if Key not in self.objects:
            raise self.exceptions.NoSuchKey(Key)
        return {"Body": io.BytesIO(self.objects[Key])}


class FakeKinesis:
    def __init__(self):
        self.records, self.interrupt_at = [], None

    def put_records(self, StreamName, Records):
        now = time.time()
        self.records += [(now, json.loads(r["Data"]), r["PartitionKey"]) for r in Records]
        if self.interrupt_at and len(self.records) >= self.interrupt_at:     # stands in for Ctrl+C mid-replay
            self.interrupt_at = None
            signal.raise_signal(signal.SIGINT)
        return {"FailedRecordCount": 0, "Records": [{} for _ in Records]}


def main():
    s3, kinesis = FakeS3(), FakeKinesis()
    boto3.client = lambda name, **kw: {"s3": s3, "kinesis": kinesis}.get(name, object())
    config.load = lambda required=(): {"bucket": "b", "stream": "s", "region": "us-east-1", "our_bank": "11",
                                       "live_start": config.SETTINGS["live_start"]}
    files = sorted(glob.glob(os.path.join(producer.DATA, "live", "date=*.csv")))
    if not files:
        sys.exit("No prepared data. Run: python main/producer/producer.py prepare --csv <transactions CSV>")
    day = os.path.basename(files[0])[5:15]             # the first live day of whatever dataset was prepared
    next_day = (datetime.fromisoformat(day) + timedelta(days=1)).strftime("%Y-%m-%d")
    fx_file = os.path.join(HERE, ".tmp_fx.csv")
    with open(fx_file, "w") as fh:
        fh.write(f"date,currency,usd_per_unit,source\n{day},Euro,1.01,ecb\n{day},US Dollar,1.0,base\n")
    base = dict(data_dir=producer.DATA, start=None, end_date=day, resume=False, stream=None, labels=False,
                q2="off", flush_wait=0, tick=0.01, dry_run=False, fx=fx_file)

    # 1. a whole simulated day (every bank's payments) in about 12 real seconds
    speed = 86400 / 12
    with open(files[0]) as fh:
        expected = sum(1 for _ in fh) - 1
    t0 = time.time()
    producer.cmd_replay(Namespace(speed=speed, **base))
    took, n = time.time() - t0, len(kinesis.records)
    clock = json.loads(s3.objects[CLOCK_KEY])
    print(f"replayed {n} payments of {day} in {took:.1f}s at speed {speed:g}")
    assert n == expected and 11 < took < 16, (n, expected, took)
    assert not clock["running"] and abs(clock["sim_anchor"] - producer.epoch(next_day)) < speed, clock
    # the day's FX rates went to their own day folder, as the day started
    fx = s3.objects[f"raw/fx/date={day}/fx.csv"].decode()
    assert fx == "currency,usd_per_unit,source\nEuro,1.01,ecb\nUS Dollar,1.0,base\n", fx
    # every payment left when the simulated clock reached its own timestamp (never early, at most a tick late)
    start_wall = FakeS3.first_put                      # the moment the producer started the clock
    late = [(w - start_wall) * speed - (producer.epoch(r["txn_ts"]) - producer.epoch(day)) for w, r, _ in kinesis.records]
    print(f"send time vs the payment's own time, in simulated seconds: min {min(late):.0f}, max {max(late):.0f}")
    assert min(late) > -speed * 0.02 and max(late) < 60 + speed * 0.5, (min(late), max(late))
    assert all(key == r["from_account"] for _, r, key in kinesis.records), "partition key must be the account"
    assert len({r["txn_id"] for _, r, _ in kinesis.records}) == n, "txn_id must be unique"

    # 2. a reader computes the same "now" from the saved clock; a paused clock stands still
    a = sim_now(clock)
    time.sleep(0.2)
    assert sim_now(clock) == a

    # 3. stop part-way, then --resume: nothing is sent twice and nothing is skipped
    kinesis.records.clear()
    s3.objects.clear()
    kinesis.interrupt_at = expected // 5
    producer.cmd_replay(Namespace(speed=speed, **base))
    first = len(kinesis.records)
    paused_at = json.loads(s3.objects[CLOCK_KEY])["sim_anchor"]
    producer.cmd_replay(Namespace(speed=speed, **{**base, "resume": True}))
    ids = [r["txn_id"] for _, r, _ in kinesis.records]
    print(f"interrupted after {first} payments at simulated {producer.stamp(paused_at)}, resumed to {len(ids)}")
    assert 0 < first < expected and len(ids) == expected and len(set(ids)) == expected
    os.remove(fx_file)

    # 4. stress: the rate asked for is the rate sent
    kinesis.records.clear()
    stress.RUNS = os.path.join(HERE, ".tmp_stress_runs")
    stress.cmd_run(Namespace(rate=200.0, duration=3.0, stages=None, accounts=500, event_speed=60, shards=1,
                             stream=None, run_id="t1"))
    rate = len(kinesis.records) / 3.0
    print(f"stress: asked for 200/s, sent {rate:.0f}/s")
    assert 190 <= rate <= 210, rate
    assert all(r["txn_id"].startswith("stress-t1-") and r["from_account"].startswith("S") for _, r, _ in kinesis.records)
    os.remove(os.path.join(stress.RUNS, "t1.json"))
    os.rmdir(stress.RUNS)
    print("OK")


if __name__ == "__main__":
    main()
