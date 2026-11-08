#!/usr/bin/env python3
"""Measure the payment rate each IBM AML dataset would put on the pipeline, and how much of one Kinesis shard
and one Flink KPU it would use.

Timestamps in the files are to the minute, and the producer spreads each minute's payments evenly over it,
so the rate in a second = that minute's payments / 60. Only the live days are counted (the last 30% of busy
days, the same rule as producer.suggest_live_start); history days are trained on in batch and never streamed.

Capacity of one unit (same constants as main/scripts/estimate.py):
  Kinesis shard : 1,000 records/s (AWS write limit). Every bank's payments go into Kinesis.
  Flink KPU     : 100 of our bank's payments/s (stress test, SHAP included). Flink drops other banks' payments
                  before the heavy work, so only bank 11's share loads the KPU.
"""
import argparse
import json
from collections import Counter, defaultdict

OUR_BANK = 11
SHARD_PER_S = 1000
KPU_OUR_BANK_PER_S = 100


def live_days(day_counts):
    sizes = sorted(day_counts.values())
    typical = sizes[len(sizes) // 2]
    busy = sorted(d for d, n in day_counts.items() if n >= typical / 10)
    return busy[-max(1, round(len(busy) * 0.3)):]


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--csv", required=True)
    p.add_argument("--out", required=True)
    a = p.parse_args()

    per_min, per_min_ours = Counter(), Counter()
    with open(a.csv, encoding="utf-8") as fh:
        next(fh)
        for line in fh:
            ts, from_bank, _, to_bank, _ = line.split(",", 4)
            per_min[ts] += 1
            try:
                if int(from_bank) == OUR_BANK or int(to_bank) == OUR_BANK:
                    per_min_ours[ts] += 1
            except ValueError:
                pass

    day_counts = defaultdict(int)
    for ts, n in per_min.items():
        day_counts[ts[:10]] += n
    live = set(live_days(day_counts))

    mins = sorted((ts, n) for ts, n in per_min.items() if ts[:10] in live)
    total = sum(n for _, n in mins)
    ours = sum(per_min_ours[ts] for ts, _ in mins)
    seconds = len(live) * 86400
    rates = sorted(n / 60 for _, n in mins)
    peak_ts, peak_n = max(mins, key=lambda x: x[1])
    peak_ours_ts = max((ts for ts, _ in mins), key=lambda t: per_min_ours[t])
    by_day = {d: day_counts[d] for d in sorted(live)}
    peak_day = max(by_day, key=by_day.get)

    res = {
        "csv": a.csv.replace("\\", "/").split("/")[-1],
        "live_days": sorted(live),
        "payments_live": total,
        "payments_live_our_bank": ours,
        "our_bank_share": round(ours / total, 4),
        "avg_per_s": round(total / seconds, 1),
        "busiest_day": peak_day,
        "busiest_day_avg_per_s": round(by_day[peak_day] / 86400, 1),
        "p99_minute_per_s": round(rates[int(len(rates) * 0.99)], 1),
        "peak_minute": peak_ts,
        "peak_per_s": round(peak_n / 60, 1),
        "peak_our_bank_per_s": round(per_min_ours[peak_ours_ts] / 60, 1),
        "shard_used_at_peak": round(peak_n / 60 / SHARD_PER_S, 3),
        "kpu_used_at_peak": round(per_min_ours[peak_ours_ts] / 60 / KPU_OUR_BANK_PER_S, 3),
        "payments_per_live_day": by_day,
    }
    with open(a.out, "w") as fh:
        json.dump(res, fh, indent=1)
    print(json.dumps({k: v for k, v in res.items() if k != "payments_per_live_day"}, indent=1))


if __name__ == "__main__":
    main()
