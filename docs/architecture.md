# AML Pipeline Architecture (Draft)

This page describes the rough end-to-end flow shown in [`BDA-Combine.drawio.png`](BDA-Combine.drawio.png). It is a high-level orientation, not a deployment runbook. For detailed requirements and the agreed design, use [`PIPELINE_SPEC.md`](../PIPELINE_SPEC.md).

## Q1: Real-Time Transaction Alerts

1. The bank's payment system sends transaction events to **Kinesis Data Streams**, partitioned by account so each account's events stay ordered.
2. **Managed Apache Flink** cleans each event, sends invalid records to a dead-letter prefix, maintains rolling account activity, computes features, and scores transactions with the current model. It uses event time and seeds its state with recent history when first started.
3. Flink sends every scored record through **Firehose** into the curated live transaction data in **S3**. It also writes flagged transactions and a random sample of unflagged transactions to **DynamoDB** for immediate investigator review. These items include transaction details, model information, risk factors, and a recent activity summary.
4. Investigators use the **Streamlit** app to review, claim, and decide alerts. Decisions are captured as append-only events in S3 through DynamoDB Streams and Lambda. DynamoDB keeps the short-lived operational queue; S3 keeps the durable history.

## Q1: Model Training and Promotion

An initial or scheduled **Step Functions** workflow prepares a training snapshot with Glue. It combines labelled historical transactions and reviewed live transactions, then makes time-based train, validation, and test splits. SageMaker trains a candidate model and evaluates it against the current champion on the same test split. A better candidate is registered; after approval, its S3 manifest is updated. Flink polls that manifest and hot-loads the approved model and alert threshold.

## Q2: Daily KYC Prioritisation

Q2 reuses Q1's curated transaction data rather than creating a second payment ingestion and cleaning path. Daily account and foreign-exchange CSV inputs are added to S3. Glue validates the inputs, links transactions to customers, converts amounts to USD, and writes curated customer transactions. A scheduled Step Functions workflow starts a transient EMR Spark job, which aggregates customer activity, applies the configured rules, and writes customer scores, tiers, evidence, and queue status to S3.

KYC analysts view the prioritised EDD queue in Streamlit, with Athena reading the latest scores and relevant run history. Claims and decisions are held in DynamoDB for the live workflow and exported to append-only S3 decision history for future runs.

## Demo Scope and Status

The diagram shows the intended AWS architecture. In the demo, `producer.py` replays the IBM AML dataset in place of a live core banking feed; provided account and FX CSVs stand in for bank and market data. Local Streamlit dashboards and snapshot builders are implemented. The planned Q1 v2 Flink and Q2 v2 EMR jobs, and the dashboards' live AWS connections, are not yet implemented. See the repository README for runnable local demos.

The drawing is a rough flow and some labels may lag the specification. In particular, Q2 consumes Q1's curated transaction output; S3 prefixes and component behavior should follow `PIPELINE_SPEC.md` when they differ from the image.
