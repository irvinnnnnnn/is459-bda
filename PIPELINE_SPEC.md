# IS459 AML Project — Pipeline Specification (Q1 + Q2)

This is the agreed design for both business-question pipelines of the IS459 Big Data Architecture group project (Group 2, SMU). It is the source of truth for agents working on this repo: build to it, and do not reintroduce anything listed under "Rejected" without asking the user first.

Status: design agreed Oct 2026. Items under "Open decisions" are not settled.

Revised 7 Oct 2026 (Q1 investigator app): SHAP risk factors added to Flink (§3.2 step 6); the DynamoDB alert item now carries the full transaction, the risk factors and the 7-day summary, so Athena is only used on request; decisions carry a reason code.

---

## 1. Context and constraints

- **Course:** IS459 Big Data Architecture. Needs an end-to-end AWS pipeline from ingestion to visualisation, with a live demo.
- **Dataset:** IBM Transactions for Anti Money Laundering (Kaggle), synthetic.
  - The pipeline runs on **HI-Large**; only HI-Small is available locally for testing.
  - Laundering is ~0.1% of transactions, so the classes are heavily imbalanced.
  - The data has **no geography, no account-opening dates, no customer income and no locations**. MAS red flags that depend on these can't be implemented directly.
- **Budget:** $250 total AWS spend. Stop always-on resources (Managed Flink, EMR) when not demoing.
- **Framing:** both questions are framed from a single bank's own view. Diagrams describe a real-world system; demo-only stand-ins are marked explicitly (§2.4).
- **Q1:** real-time flagging of likely laundering transactions, so investigators can act before money moves on (ML model, streaming).
- **Q2:** which customers KYC should prioritise for **enhanced due diligence (EDD)**, based on combined behaviour across all their accounts (rules, daily batch, analytical dashboard).
  - EDD is the deeper review required by MAS for higher-risk customers: source of funds and wealth, beneficial ownership, purpose of activity, more frequent monitoring, often senior approval.

---

## 2. Shared conventions

### 2.1 Core principles

1. **Apps read DynamoDB; pipelines read S3.**
   - DynamoDB holds small, current, interactive state for Streamlit (instant reads, conditional writes).
   - Every batch or streaming job reads only from S3.
   - An app may also run *on-demand, read-only* Athena queries for history it doesn't need instantly.
2. **S3 is the single source of truth, and its event logs are append-only.**
   - S3 objects can't be edited in place, so changes are recorded as **new** small files (one per Lambda batch). They are never appended to an existing file and never rewritten.
   - Current state = the latest event per key.
   - DynamoDB changes reach S3 via **DynamoDB Streams → Lambda → S3**.
3. **Parquet, not CSV,** for every curated or analytical dataset:
   - column pruning makes Athena scans cheaper
   - columns are typed
   - files are compressed

   CSV appears only in `raw/` as received. Note: Athena can't `UPDATE` plain S3 tables in *any* format; row-level updates would need Iceberg, which is not used (§5).
4. **No Glue crawlers.** Schemas are defined explicitly in setup.
   - Glue jobs register partitions as they write (`enableUpdateCatalog`).
   - Firehose output uses Athena **partition projection**.
   - Firehose Parquet conversion needs the table to exist *before* Firehose starts.

### 2.2 Storage

One S3 bucket, `s3://is459-aml-project/`, with separate prefixes:

| Prefix | Pipeline | Written by | Contents |
|---|---|---|---|
| `historical/` | Q1 | one-off upload | Bank's labelled case history (HI-Large, labelled) |
| `curated/historical/` | Q1 (shared) | Q1 Glue feature job | Historical transactions + features + label (Parquet) |
| `curated/live/` | Q1 (shared) | Firehose (from Flink) | Live transactions + features + score + `model_version` + `arrived_at` + `scored_at` (Parquet, partitioned by date). Written once, never updated |
| `dlq/` | Q1 | Flink | Malformed or unprocessable records + error reason |
| `labels/` | Q1 | Lambda (from DynamoDB Streams) | Append-only alert events: `txn_id`, event (created / confirmed / dismissed / reopened / expired), `is_audit`, reviewer, reason code, note, timestamp. Partitioned by date |
| `training/run=<date>/train/`, `val/`, `test/` | Q1 | training Glue job | Training snapshot for one run |
| `models/artifacts/run=<date>/` | Q1 | SageMaker training | Model artifact |
| `models/best/manifest.json` | Q1 | promotion step | Pointer to the approved artifact |
| `raw/` | Q2 | daily uploads | Accounts + FX CSVs, as received |
| `curated/customer_txns/` | Q2 | Q2 Glue job | Transactions + `customer_id` + USD amount (Parquet, partitioned by date) |
| `decisions/` | Q2 | Lambda (from DynamoDB Streams) | Append-only analyst decisions (§4.5). Partitioned by date |
| `scores/run_date=<date>/` | Q2 | EMR | One row per customer: score, tier, rules fired, evidence, `in_queue`, `queue_reason` |
| `rule_hits/run_date=<date>/` | Q2 | EMR | One row per (customer, rule fired) with evidence text |
| `rule_eval/run_date=<date>/` | Q2 | EMR | Per-rule and per-tier precision against `is_laundering` (§4.4) |
| `state/cp_first_seen/` | Q2 | EMR | Counterparty first-seen dates for the new-counterparty rule. Keep only the latest snapshot(s) |

### 2.3 Small-file note

The Lambdas write one small file per batch. At project volume (tens to hundreds of events a day) this is fine; Athena and Spark read a whole prefix as one table.

Production improvement (mention, don't build): Lambda → Firehose → S3 to buffer into larger Parquet files, or a nightly compaction step.

### 2.4 Demo stand-ins

Diagrams show the real-world entity, with a red dashed "DEMO" box saying what simulates it.

| Real-world entity | Demo stand-in |
|---|---|
| Core banking / payment system (transaction events) | `producer.py` replays HI-Large in event-time order into Kinesis |
| Bank's labelled case history | IBM AML HI-Large (synthetic, complete labels) |
| Customer master DB (core banking / KYC) | `accounts.csv` (IBM dataset) uploaded straight to `raw/` |
| yfinance FX rates API | FX CSV pre-fetched via yfinance, uploaded to `raw/` |

AML investigators and KYC analysts are real Streamlit users in the design. A demo may simulate their decisions using the dataset's hidden labels; that is not drawn on the diagrams.

---

## 3. Q1 — Real-time flagging (streaming + ML)

### 3.1 Flow summary

```
Core banking → Kinesis → Managed Apache Flink ─┬─▶ Firehose → curated/live/  (Parquet, written once)
                                               └─▶ DynamoDB (alerts + audit items, TTL 7 days)
                                                      ⇅ Streamlit (Alerts tab · Audit tab) ⇄ AML investigators
                                                      └─▶ DynamoDB Streams → Lambda → labels/  (append-only)

historical/ → Glue feature job → curated/historical/

Step Functions (initial run + every 6 months):
  Glue job (curated/live + curated/historical ⨝ labels/) → training/run=<date>/
    → SageMaker training → models/artifacts/
    → SageMaker Processing (champion vs challenger on test/) → better? → Model Registry
    → models/best/manifest.json → Flink polls + hot-reloads
```

### 3.2 Components

**Kinesis Data Streams**
- Partition key = **account ID**, so per-account ordering holds.

**Managed Apache Flink** (the single streaming ETL; no other component cleans live data)
1. Extract each transaction from Kinesis.
2. Clean it. Bad rows go to `dlq/` with the error reason, never dropped silently.
3. Compute features from per-account keyed state (7-day windows):
   - counts, amounts, last inflow, pairs
   - **distinct senders into the account and distinct receivers from it** (fan-in / fan-out). Distinct receivers needs a second keyBy on the receiver.
4. Use **event-time windows with watermarks**, never processing time. The replay compresses months into minutes, and real feeds arrive late or out of order.
5. Score in-process with the current model.
6. **Explain (SHAP), for alert and audit items only.** For each transaction at or above the threshold, and each one drawn into the audit sample, compute per-transaction SHAP values (LightGBM TreeSHAP) and keep the **three features with the largest positive contribution** as its risk factors.
   - This is per transaction, not per model: two alerts with the same score can have different risk factors.
   - It is not run on the other ~99% of traffic, so it adds no meaningful time to flagging.
   - **"Other bank" is context, not a risk factor.** For a cross-bank payment the other bank's features are blanked, and the model learns from the blanks: together with the `cross_bank` flag they mean "the other party is at another bank". That is true of most payments, so it is shown on the payment (the counterparty's bank) and **never picked as one of the three risk factors**. Risk factors are limited to behaviour an investigator can check. The score itself is unaffected.
7. Output:
   - every scored record → Firehose → `curated/live/` (includes `model_version`, `arrived_at` = Kinesis arrival time, `scored_at`)
   - flagged transactions + a small random **audit sample** of unflagged ones → DynamoDB, with their risk factors and 7-day summary (see "DynamoDB alerts")
8. Model hot-reload: poll `models/best/manifest.json`. When the version changes, load the artifact it points to and broadcast it to operators. The manifest also carries the alert **threshold**, which Flink stamps on each item.
9. State is checkpointed.

Features are **point-in-time correct** by construction: Flink only sees the past, and training reuses Flink's logged features for live rows.

**Flink state seeding (one-off, at first start)**
- Load the last 7 days of history into state before live traffic.
- Arrow on the diagram: historical → Flink, labelled "Flink reads last 7 days at first start".

**DynamoDB alerts** (small operational store, not the system of record)
- Two item types, distinguished by `is_audit`: **alerts** (model-flagged) and **audit items** (random unflagged sample).
- Each item carries **everything the alert page shows without a query**, so opening an alert is instant. Written by Flink at scoring time:
  - **the transaction:** `txn_id`, `txn_ts`, `from_bank`, `from_account`, `to_bank`, `to_account`, `amount` + `currency` (paid), `amount_received` + `receiving_currency`, `payment_format`
  - **the model output:** `score`, `threshold`, `model_version`
  - **`reasons`:** the three SHAP risk factors, each stored as `{feature, value, contribution}` (e.g. `{r_out_n_5D, 95, 1.8}`). Flink stores the feature name and value, **not a sentence**; the app holds one fixed sentence per feature, so wording can change without re-scoring and stays valid after retraining (the feature list is fixed by the feature code).
  - **`features`:** the 7-day summary for both accounts, taken from the feature vector Flink already holds: payments in / out, total amount in / out, distinct senders / receivers. This replaces an Athena query for the first look at the customer.
  - **workflow fields**, written by Streamlit: `status` (open / confirmed / dismissed), `claimed_by` (the investigator who has taken an open item; a conditional write so two people can't take the same one, cleared when the decision is recorded), `reviewer`, `reason` (the reason code chosen with the decision), `note`, `decided_at`; plus `created_at` and `expires_at` from Flink
- Other alerts on the same customer are found in this table too (same account, last 7 days); no Athena needed.
- GSI on status + score.
- **TTL: 7 days from creation for both alerts and audit items.** After 7 days an alert is no longer actionable for real-time interception, and the table stays small.
- **DynamoDB Streams → Lambda → `labels/`:**
  - every creation, status change and **TTL expiry** is written as an event, with the `reason` code and `note` when there is one
  - a decision that is undone or reopened is written as a `reopened` event
  - TTL deletions are recorded as `expired`, never filtered out: every alert must have a recorded outcome (compliance), and the expiry rate is a capacity signal

**Streamlit (investigator UI)**
- **Three pages, switched in the sidebar:**
  - **Alert queue:** model-flagged items, highest model risk first
  - **Audit sample:** the random sample
  - **Reviewed:** decisions from the last 7 days, which can be changed or reopened
- Clicking a row opens the alert page: the payment, the risk factors, the customer's 7-day summary, other alerts on the customer, and the decision form.
- A decision is **Escalate as suspicious** (stored as `confirmed`) or **Close as not suspicious** (stored as `dismissed`), with a reason code and an optional comment.
- **Everything above comes from DynamoDB (instant).** The queue, the alert page, the 7-day summary and the related alerts need no query.
- **Athena is used only on request:** the customer's full payment list (up to 30 days) is an on-demand query on `curated/live/`. Note that `curated/live/` lags by the Firehose buffer (about a minute).
- History older than 7 days comes from `labels/` via Athena.

**Glue feature job (historical lane, batch)**
- Reads `historical/` and computes the **same features as Flink** with Spark window functions over each account's past 7 days, strictly past-only.
- Writes `curated/historical/`.
- **Parity check:** compute features for a small sample in Spark and in Flink and confirm they match.

**Training workflow: one Step Functions workflow for both the initial run and retrains**
- **Trigger:** EventBridge, every 6 months. Manual run for the demo.
- **Step 1: Glue job (build training set)**
  1. Read `labels/` and keep the **latest event per `txn_id`**. Map it to a label:
     - confirmed → 1
     - dismissed → 0
     - audit item → whatever the reviewer found
     - open, reopened or expired → dropped
  2. Inner-join with `curated/live/` on `txn_id` (broadcast join on the small labels side). Unreviewed live rows are not used, and there is no pseudo-labelling.
  3. Union with `curated/historical/` (already labelled).
  4. Keep a **rolling window of the last N months**; historical data ages out naturally.
  5. **Time-based split 60 / 20 / 20**: oldest → train, next → val, newest → test.
  6. Write `training/run=<date>/`. The Model Registry entry points to this path, so each model links to its exact data.
  - On the initial run there are no live labels yet, so it trains on history only.
- **Step 2: SageMaker training** reads `train/` + `val/` (val is for early stopping and threshold) and writes the artifact to `models/artifacts/run=<date>/`.
- **Step 3: SageMaker Processing** loads the challenger and champion artifacts from `models/` and scores both on the **same `test/`** split. Metrics: PR-AUC and recall at a fixed alert budget.
- **Step 4: Choice ("better?")**
  - No → keep the champion.
  - Yes → register in the **SageMaker Model Registry** (artifact path, training data path, metrics, approval status).
- **Step 5: On approval,** update `models/best/manifest.json`.
- The test split never reaches the training job.

### 3.3 Label policy

- **Labels:** confirmed = 1; dismissed = 0 (valuable hard negatives); audit = the reviewer's finding.
- **The audit sample** has two jobs:
  1. It catches laundering the model missed.
  2. It measures the miss rate.
  - It's reviewed in its own tab (not blind). Known trade-off: reviewers know these items weren't flagged. Mitigate with a fixed audit routine (e.g. a daily quota).
- **Low live-label volume** is handled by:
  - historical data in the rolling window
  - a larger audit sample
  - optionally skipping a retrain if too few new labels arrived
  - the champion-vs-challenger gate
- **Honest limitations:**
  - early hops of a laundering pattern can't be caught in real time
  - synthetic data with instant perfect labels means retraining may not show a measurable improvement
  - per-transaction features only partly capture network-shaped laundering
  - SHAP risk factors explain what the model reacted to, not whether the payment is laundering; a factor such as "first payment between these accounts" is weak on its own and matters in combination
  - model scores are small probabilities (laundering is ~0.1% of payments): an alert means "in the top 1% by risk", not "probably laundering"

### 3.4 Metrics and monitoring (essential set only; all near-free)

| Metric | Source |
|---|---|
| Kinesis iterator age | CloudWatch alarm |
| Throughput (records/sec) | Kinesis incoming records |
| Time to flag, p50 / p99 (`arrived_at` → `scored_at`) | Athena on `curated/live/` |
| % of alerts / audit items expired unreviewed | Athena on `labels/` |
| Training / retrain duration | Step Functions run history |

Flink backpressure: Flink dashboard only, no alarm.

---

## 4. Q2 — KYC prioritisation for EDD (daily batch + rules)

### 4.1 What it answers

"Which customers should KYC review first for enhanced due diligence?"
- The output is a **ranked queue with reasons**.
- The value comes from **combining all of a customer's accounts**. Each account can look normal alone, while the combined picture (e.g. many senders, money out within 48h, amounts under a threshold) is suspicious.

### 4.2 Flow summary

```
NIGHT (automatic, daily) — EventBridge → Step Functions:
  Customer master DB ─(daily export)─▶ raw/ ─┐
  yfinance FX ───────(daily fetch)───▶ raw/ ─┤
  curated/live/ + curated/historical/ (Q1) ──┴─▶ Glue job → curated/customer_txns/
     → Transient EMR (reads customer_txns + decisions/) → scores/, rule_hits/, rule_eval/

DAY (KYC analysts):
  Athena (WHERE in_queue) → Streamlit queue ⇄ KYC analysts
  Streamlit ⇄ DynamoDB decisions (current only) → DynamoDB Streams → Lambda → decisions/ (append-only)
```

The nightly loop answers "who looks risky?". The daytime loop records "we've dealt with them". They connect when EMR reads `decisions/` the next night.

### 4.3 Components

**Shared ingestion with Q1**
- No separate stream, crawler or transaction cleaning; Q2 reads Q1's `curated/` and ignores Q1-only columns.
- Q1 must run (or be backfilled) before Q2 can be demoed.

**Reference data in `raw/` (CSV)**
- Accounts (account → customer mapping) from the customer master DB.
- Daily FX rates from yfinance, plus a one-off historical backfill.
- yfinance notes:
  - it's an unofficial library
  - it needs **historical** rates for the transaction dates
  - it needs a mapping from IBM currency names (incl. Bitcoin) to Yahoo tickers

**Glue job (ETL, incremental: new dates only, after a one-off backfill)**
1. Read raw accounts + FX.
2. Clean them (dedupe, standardise currency codes) and **validate**, failing the run if any check fails:
   - every account maps to a customer
   - every currency has a rate
   - row counts are non-zero
3. Join to the transactions, adding `customer_id` and the USD amount, using the account mapping **as of the transaction date**.
4. Write `curated/customer_txns/`.
5. Do not re-clean the transactions.

**Transient EMR (Spark)**
- Created and terminated per run by Step Functions, with spot workers.
- **All Q2 decision logic lives here** (§4.4 and §4.5).
- Writes `scores/`, `rule_hits/`, `rule_eval/` and `state/`.

**Athena**
- Read-only.
- Queue query: `WHERE run_date = <today> AND in_queue ORDER BY score DESC`.
- **Change vs previous run:** join today's and the previous `scores/` partition per customer to get the score change and the rules new today.
- Customer drill-down: query `curated/customer_txns/` on demand.

**Streamlit (prioritised EDD queue)**
- Cache the queue query (`st.cache_data`) until the next nightly run, and enable Athena query-result reuse.
- Combine the cached queue rows (Athena) with each customer's live status (claimed by X, cleared until …) from DynamoDB.
- Three views:

| View | Shows | Answers |
|---|---|---|
| Priority queue | Customers with `in_queue`, ranked: score, change since last run, top rules fired, number of accounts, `queue_reason` | Who do we review first? |
| Customer detail | Rules fired with evidence, the customer's accounts, score trend, transaction drill-down | Why is this customer here? |
| Overview | Count above EDD threshold, which rules fire most, score distribution, rule precision from `rule_eval/` | How big is the workload, and are rules too noisy? |

**DynamoDB decisions** (current state only)
- **One item per customer:** latest decision, analyst, timestamp, cleared-until, and the **snapshot** (score + rules fired at review time).
- Decision types:
  - **claim:** in review by analyst X; conditional write, so two analysts can't claim the same customer
  - **EDD needed**
  - **cleared until <date>**
- **TTL = cleared-until date**, for housekeeping only. EMR does not rely on TTL.
- **DynamoDB Streams → Lambda → `decisions/`** (append-only history, audit trail). EMR reads `decisions/`, never DynamoDB.

### 4.4 Rules (EMR)

- Window: 7 days ending on `run_date` (recent period: 3 days).
- Behaviour rules that need prior history (R7 spike, R9 new counterparties) only fire when the window is complete (`window_complete`).
- Each rule hit carries an **evidence string** for analysts.

| # | Rule | Logic | Points | MAS grounding |
|---|---|---|---|---|
| R1 | Rapid pass-through | Inflow ≥ $10k, outflow/inflow 0.9–1.1, median hours from receipt to next payment out ≤ 48 | 25 | Assets withdrawn immediately after deposit without business reason |
| R2 | U-turn / round-trip | Paid a counterparty ≥ $1k and ≥ 80% came back from the same counterparty within 7 days | 20 | U-turn transactions |
| R3 | Collection from many senders (fan-in) | ≥ 10 distinct senders, or top 1% for the entity type with ≥ 5 | 15 | Payments to a single account from many different people without explanation |
| R4 | Gather-and-scatter (mule) | R3 **and** ≥ 10 distinct recipients **and** pass-through ≥ 0.8 | +15 bonus | Scam money-mule pattern (fan-in + immediate withdrawal) |
| R5 | Dispersal to many recipients (fan-out) | ≥ 10 distinct recipients, or top 1% for the type with ≥ 5 | 10 | Unusual destination of funds (layering) |
| R6 | Structuring below threshold | ≥ 3 transactions in a band just under the threshold, totalling ≥ the threshold. Threshold is a parameter (see note) | 20 | Splitting to avoid reporting / CDD thresholds |
| R7 | Activity inconsistent with profile | Recent daily value ≥ 5× own prior average and ≥ $10k, **or** inflow ≥ 99th percentile for the entity type | 15 | Activity inconsistent with the customer's profile |
| R8 | Cash / crypto concentration | ≥ 50% of value in Cash or Bitcoin, total ≥ $10k | 10 | Cash-heavy activity; digital-asset risk |
| R9 | New-counterparty burst | ≥ 5 new counterparties making up ≥ 70% of recent ones | 10 | Proxy for mule onboarding / newly active accounts (no account-open date in the data) |
| R10 | Own-account shuffling | ≥ 3 transfers of ≥ $5k between own accounts | 10 | Movement across multiple accounts without clear purpose |
| R11 | Currency churn | ≥ 30% of value converted between currencies, total ≥ $10k | 10 | Layering via conversion |
| R12 | Linked to customers needing EDD | Transacted in the window with ≥ 1 counterparty whose latest decision is "EDD needed" | 15 | Dealings with parties already assessed as higher risk |

- **R12 uses analyst decisions, not last run's tier.** This avoids the self-reinforcing loop where High customers keep each other High.
- **Structuring threshold:** MAS Notice 626 requires CDD for occasional transactions exceeding S$20,000 (verify against the current notice). Amounts are in USD, so set the band as e.g. 80–99.9% of the threshold converted to USD, and state the choice on the slide. The previous $8,000–9,999 band follows the US $10k rule.
- **Score** = sum of rule points + peer composite (0–1 tie-breaker from peer percentiles within entity type).
- **Tiers** (capacity-based prioritisation, not a verdict):
  - top 1% → High
  - next 9% → Medium
  - both require at least one rule to have fired
  - everyone else → Low
- **Rule evaluation (`rule_eval/`):** per rule — customers fired on and the share with any `is_laundering` transaction (precision); per tier — the share involved in laundering. `is_laundering` is used **only** for evaluation, never as a scoring input.
- **Scalability:**
  - Avoid a global unpartitioned window for ranking on large data; compute the 1% / 10% cutoffs with `approxQuantile` and tier by threshold.
  - Watch R2's self-join for skew.
- **R9 state:** keep only the latest `state/cp_first_seen/` snapshot(s). Runs must be processed in date order.

### 4.5 Queue logic (`in_queue`) — computed by EMR

Scoring never stops: every customer is scored every night, regardless of decisions. Decisions only affect whether they appear in the queue.

For each customer, EMR reads their latest decision from `decisions/`:

| Latest decision | `in_queue` |
|---|---|
| None | true if tier is High or Medium |
| Cleared, today < cleared-until | **false**, unless a rule fired that is not in the decision's snapshot, or the score rose well above the snapshot score (e.g. +20) → true |
| Cleared, cleared-until passed | treat as no decision: true if High or Medium |
| EDD needed | false (moved to the EDD process; still scored and visible in customer detail) |
| Claimed (in review) | true; Streamlit shows who claimed it |

- EMR also writes a `queue_reason`, e.g. "new rule since review: structuring", "clearance expired", or "new High".
- **Clearance period:**
  - **Default 7 days**, matching the rule window: a clearance covers exactly the data the analyst reviewed. Once the window has fully rolled over, every transaction behind the score is unreviewed.
  - Analysts may choose **30 or 90 days** for a confirmed, stable, legitimate pattern, to avoid weekly re-reviews. The new-rule and score-jump checks still bring the customer back early.

### 4.6 Orchestration

EventBridge (daily) → Step Functions:
1. Glue job
2. Transient EMR (create → run → terminate)

---

## 5. Rejected (do not reintroduce without asking)

| Rejected | Reason |
|---|---|
| Separate "Initial Training" pipeline | The same Step Function covers the initial run and retrains |
| Second Firehose / Glue "clean data" job on Q1's live path | Flink is the single cleaner |
| Glue crawlers | We own all schemas |
| Lambda "Evaluate Results" step | Replaced by Processing + a Choice state |
| Backfilling history through Flink in batch mode | Replaced by the Glue feature job + parity check |
| Pseudo-labelling low-score unflagged transactions | The model would grade its own homework; the audit sample is used instead |
| Score-drift alarm | Replaced by the operational metrics |
| Training reads a DynamoDB export | Replaced by Streams → Lambda → `labels/` |
| **Lambda writing labels into `curated/live/`** | Parquet in S3 can't be edited per row; labels go to the separate `labels/` log and are joined at training |
| Storing curated data as CSV | No column pruning, untyped, larger; CSV also can't be updated by Athena |
| Athena `UPDATE` per click / Iceberg for labels | Plain S3 tables can't be updated; Iceberg adds rewrite and compaction overhead and loses event history. Optional future X-factor only, via a daily batch MERGE |
| Blind review (audit items hidden among alerts) | Team chose separate Alerts and Audit tabs |
| Different TTLs for alerts and audit items | One 7-day TTL for both |
| Filtering TTL deletions out of the stream | Expiries must be recorded |
| Separate Kinesis / raw / crawler / cleaning pipeline for Q2 | Duplicates Q1 ingestion |
| Analysts editing scores in S3 | Decisions are stored separately; scores are never edited |
| EMR reading decisions directly from DynamoDB | Pipelines read S3 (`decisions/`) |
| Q2 R6 "linked to last run's High tier" | Self-reinforcing; replaced by R12 (analyst-confirmed EDD) |
| Clearing a customer permanently | Clearances expire and are overridden by new rules or a score jump |
| Separate Glue Data Quality step, FX Lambda, extra CloudWatch for Q2 | Validation lives in the Glue job |
| Live RDS instance for the customer DB in the demo | A Glue job in a VPC loses internet access (yfinance), and a NAT gateway costs ~$1/day |

---

## 6. Open decisions

- **Flink language:** PyFlink (load LightGBM directly) or Java (export as LightGBM text or ONNX). This decides the training artifact format **and how SHAP is computed**:
  - PyFlink: LightGBM's built-in `predict(pred_contrib=True)` gives exact TreeSHAP.
  - Java: needs a TreeSHAP implementation over the LightGBM text model; ONNX alone does not give SHAP values.
  - The current `score_lambda.py` uses a simpler tree-path attribution, not SHAP. It must be replaced, not reused.
- **Flink seeding mechanism:**
  - Real world: hybrid source (S3 last 7 days → Kinesis); check connector support.
  - Demo fallback: `producer.py` replays 7 days early with alerts suppressed.
- **DynamoDB Streams → S3:** Lambda (current choice) or Kinesis Data Streams for DynamoDB → Firehose (no code, ~$10+/month).
- **Values not yet fixed:**
  - Q1: N (rolling window), audit sample rate, alert threshold / budget, minimum new labels to retrain
  - Q2: score-jump margin for re-surfacing, structuring threshold
- **SageMaker input format:** built-in LightGBM (CSV) or a custom script (Parquet).
- **EMR sizing:** node count and instance types.
- **Q2 code paths:** the current scoring script reads `curated/` + `reference/accounts/` and writes `results/customer_scores/`, `results/rule_hits/`. Align the code with this spec, or this spec with the code.

---

## 7. Diagram files

- `q1.drawio`: Q1 ("IS459 BDA AML Project Business Question 1")
- `q2.drawio`: Q2 ("IS459 BDA AML Project Business Question 2")

Legend conventions:
- solid arrow = data flow
- dashed arrow = control / model flow
- red dashed box = demo stand-in
- grey dashed box = provided by the Q1 pipeline
- "Apps read DynamoDB; pipelines read S3"
