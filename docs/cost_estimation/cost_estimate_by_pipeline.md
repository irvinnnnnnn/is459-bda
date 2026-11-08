# Cost estimate by pipeline

Source: `python main/scripts/estimate.py --run annex/run_medium.json` (default mode: the live days streamed in real time, 24 h a day). Full output: `cost_estimate_full.txt`.


## Pipeline 1 (real-time flagging)

| Line | HI-Small | HI-Medium | HI-Large |
|---|---:|---:|---:|
| Managed Flink: KPU-hours | $15.84 | $26.40 | $153.12 |
| Managed Flink: running storage GB-months | $0.49 | $0.82 | $4.77 |
| DynamoDB alerts table: million write request units (alerts) | $0.00 | $0.00 | $0.01 |
| DynamoDB alerts table: million read request units (label export) | $0.00 | $0.00 | $0.00 |
| DynamoDB alerts table: million write request units (label export) | $0.00 | $0.00 | $0.01 |
| q1-features-historical: DPU-hours | $0.06 | $0.28 | $1.60 |
| q1-build-training: DPU-hours | $0.01 | $0.02 | $0.06 |
| q1-train: instance-hours | $0.00 | $0.00 | $0.02 |
| q1-eval: instance-hours | $0.00 | $0.00 | $0.01 |
| q1-label-export: DPU-hours | $0.00 | $0.00 | $0.01 |
| Athena: Q1 investigator app: TB scanned | $0.01 | $0.05 | $0.30 |
| DynamoDB: Q1 investigator app: million read request units | $0.04 | $1.83 | $18.55 |
| DynamoDB: Q1 investigator app: million write request units | $0.00 | $0.00 | $0.01 |
| **Subtotal** | **$16.45** | **$29.40** | **$178.47** |

## Pipeline 2 (daily KYC scoring)

| Line | HI-Small | HI-Medium | HI-Large |
|---|---:|---:|---:|
| q2-etl: DPU-hours | $0.10 | $0.20 | $1.13 |
| q2-emr: instance-hours | $0.23 | $0.39 | $2.25 |
| q2-emr: EMR fee instance-hours | $0.06 | $0.10 | $0.56 |
| Athena: Q2 KYC app: TB scanned | $0.01 | $0.01 | $0.08 |
| **Subtotal** | **$0.40** | **$0.70** | **$4.02** |

## Shared (ingest, storage, app server)

| Line | HI-Small | HI-Medium | HI-Large |
|---|---:|---:|---:|
| Kinesis Data Streams: shard-hours | $1.04 | $1.74 | $10.09 |
| EC2 apps server: t3.small instance-hours | $1.46 | $2.43 | $14.07 |
| EC2 apps server: public IPv4 address-hours | $0.35 | $0.58 | $3.38 |
| EC2 apps server: EBS GB-months | $0.06 | $0.10 | $0.59 |
| Kinesis Data Streams: million PUT payload units | $0.02 | $0.15 | $0.73 |
| Data Firehose: GB ingested (5 KB per record) | $0.19 | $1.49 | $7.20 |
| Data Firehose: GB converted to Parquet | $0.12 | $0.92 | $4.47 |
| Data Firehose: GB dynamically partitioned | $0.01 | $0.07 | $0.36 |
| S3 storage: GB-months | $0.00 | $0.01 | $0.37 |
| **Subtotal** | **$3.25** | **$7.49** | **$41.26** |

## Summary

| | HI-Small | HI-Medium | HI-Large |
|---|---:|---:|---:|
| Pipeline 1 (real-time flagging) | $16.45 | $29.40 | $178.47 |
| Pipeline 2 (daily KYC scoring) | $0.40 | $0.70 | $4.02 |
| Shared (ingest, storage, app server) | $3.25 | $7.49 | $41.26 |
| **Total** | **$20.10** | **$37.59** | **$223.75** |

| Scale | HI-Small | HI-Medium | HI-Large |
|---|---:|---:|---:|
| Live days streamed (real time) | 3 | 5 | 29 |
| Payments streamed (all banks) | 1.35 M | 10.74 M | 52.05 M |
| History payments (training) | 3.73 M | 21.15 M | 127.61 M |
| Hours running | 72 | 120 | 696 |
