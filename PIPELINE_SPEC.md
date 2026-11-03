# IS459 AML Project — Pipeline Specification (Q1 + Q2)

This is the agreed design for both business-question pipelines of the IS459 Big Data Architecture group project (Group 2, SMU). It is the source of truth for agents working on this repo: build to it, and do not reintroduce anything listed under "Rejected" without asking the user first.

Status: design agreed Oct 2026. Items under "Open decisions" are not settled.

Revised 7 Oct 2026 (Q1 investigator app): SHAP risk factors added to Flink (§3.2 step 6); the DynamoDB alert item now carries the full transaction, the risk factors and the 7-day summary, so Athena is only used on request; decisions carry a reason code.

Revised 13 Oct 2026 (architecture update, to match the proposal diagrams):
- Q1: Kinesis also lands every transaction as received in `raw/transactions/` via Firehose.
- Q1: Flink writes scored rows directly to `curated/live/scored/`.
- Q1: alert outcomes reach S3 through a daily Glue label export into `curated/live/labels/`, replacing DynamoDB Streams → Lambda → `labels/`. Both Q1 live datasets sit under `curated/live/` as two separate Athena tables.
- Q2: reads `raw/transactions/` and cleans it, instead of reading Flink's output.
- Q2: KYC decisions are written by Streamlit straight to `decisions/` in S3 (no DynamoDB decisions table) and read back through Athena.
- Q2: Athena also serves the customer's full transaction history from `curated/customer_txns/`.

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

1. **Pipelines read S3. Each app reads the store that matches how fast it needs answers.**
   - **Q1 (real-time):** the investigator app reads and writes DynamoDB for small, current, interactive state (instant reads, conditional writes). It runs *on-demand, read-only* Athena queries only for history it doesn't need instantly.
   - **Q2 (daily batch):** the KYC app reads through Athena and writes decisions straight to S3. A few seconds' delay changes nothing for a daily process, and it avoids a second store to keep in sync.
   - Every batch or streaming job reads only from S3. The single exception is the Q1 daily label export, which is the bridge from DynamoDB into S3.
2. **S3 is the single source of truth, and its event logs are append-only.**
   - S3 objects can't be edited in place, so changes are recorded as **new** small files. They are never appended to an existing file and never rewritten.
   - Current state = the latest event per key.
   - Q1 alert outcomes reach S3 through the **daily Glue label export** (DynamoDB → `curated/live/labels/`).
   - Q2 decisions are written to S3 directly by Streamlit.
3. **Parquet, not CSV,** for every curated, analytical or log dataset:
   - column pruning makes Athena scans cheaper
   - columns are typed
   - files are compressed

   CSV appears only for the Q2 reference files in `raw/accounts/` and `raw/fx/`, as received. Raw transactions land as Parquet (Firehose converts on arrival). Note: Athena can't `UPDATE` plain S3 tables in *any* format; row-level updates would need Iceberg, which is not used (§5).
4. **No Glue crawlers.** Schemas are defined explicitly in setup.
   - Glue jobs register partitions as they write (`enableUpdateCatalog`).
   - Streaming outputs (`raw/transactions/` from Firehose, `curated/live/scored/` from Flink) use Athena **partition projection**.
   - Firehose Parquet conversion needs the table to exist *before* Firehose starts.
   - Each dataset under `curated/live/` is its own table pointing at its own sub-prefix. Never point one table at `curated/live/` itself, or it would read both datasets.

### 2.2 Storage

One S3 bucket, `s3://is459-aml-project/`, with separate prefixes:

| Prefix | Pipeline | Written by | Contents |
|---|---|---|---|
| `raw/transactions/` | Q1 + Q2 (shared) | Firehose (from Kinesis) | Every transaction as received, uncleaned (Parquet, partitioned by date). Q2's transaction input; also allows replay |
| `historical/` | Q1 | one-off upload | Bank's labelled case history (HI-Large, labelled) |
| `curated/historical/` | Q1 + Q2 (shared) | Q1 Glue feature job | Historical transactions + features + label (Parquet) |
| `curated/live/scored/` | Q1 | Flink (S3 file sink) | Live transactions + features + score + `model_version` + `arrived_at` + `scored_at` (Parquet, partitioned by date). Written once, never updated |
| `curated/live/labels/` | Q1 | daily Glue label export | One row per alert / audit item once its 7-day review window has closed: `txn_id`, final outcome (confirmed / dismissed / expired), `is_audit`, reviewer, reason code, note, `decided_at`, `created_at`, score, `model_version`. Append-only, partitioned by export date |
| `dlq/` | Q1 + Q2 | Flink; Q2 Glue job | Malformed or unprocessable records + error reason + source |
| `training/run=<date>/train/`, `val/`, `test/` | Q1 | training Glue job | Training snapshot for one run |
| `models/artifacts/run=<date>/` | Q1 | SageMaker training | Model artifact |
| `models/best/manifest.json` | Q1 | promotion step | Pointer to the approved artifact |
| `raw/accounts/`, `raw/fx/` | Q2 | daily uploads | Accounts + FX CSVs, as received (`raw/fx/date=<day>/fx.csv`: one file per day) |
| `curated/customer_txns/` | Q2 | Q2 Glue job | Cleaned transactions + `customer_id` + USD amount (Parquet, partitioned by date) |
| `decisions/` | Q2 | Streamlit (directly) | Append-only analyst events (§4.3): claims, releases, decisions, reopens. Partitioned by date |
| `scores/run_date=<date>/` | Q2 | EMR | One row per customer: score, tier, rules fired, evidence, `in_queue`, `queue_reason` |
| `rule_hits/run_date=<date>/` | Q2 | EMR | One row per (customer, rule fired) with evidence text |
| `rule_eval/run_date=<date>/` | Q2 | EMR | Per-rule and per-tier precision against `is_laundering` (§4.4) |
| `state/cp_first_seen/` | Q2 | EMR | Counterparty first-seen dates for the new-counterparty rule. Keep only the latest snapshot(s) |

### 2.3 Small-file note

- Q2 Streamlit writes one small file per analyst event.
- The Q1 label export writes one small file set per day.
- At project volume (tens to hundreds of events a day) this is fine; Athena and Spark read a whole prefix as one table.

Production improvement (mention, don't build): a nightly compaction step for `decisions/`.

### 2.4 Demo stand-ins

Diagrams show the real-world entity, with a red dashed "DEMO" box saying what simulates it.

| Real-world entity | Demo stand-in |
|---|---|
| Core banking / payment system (transaction events) | `producer.py` replays HI-Large in event-time order into Kinesis |
| Bank's labelled case history | IBM AML HI-Large (synthetic, complete labels) |
| Customer master DB (core banking / KYC) | `accounts.csv` (IBM dataset) uploaded straight to `raw/accounts/` |
| Daily FX rates feed | Rates pre-fetched from the Frankfurter API, sent to `raw/fx/` one day at a time |

AML investigators and KYC analysts are real Streamlit users in the design. A demo may simulate their decisions using the dataset's hidden labels; that is not drawn on the diagrams.

---

## 3. Q1 — Real-time flagging (streaming + ML)

### 3.1 Flow summary

```
Core banking → Kinesis ─┬─▶ Firehose → raw/transactions/  (Parquet, as received; also Q2's input)
                        └─▶ Managed Apache Flink ─┬─▶ curated/live/scored/  (Parquet, written once)
                                                  ├─▶ dlq/
                                                  └─▶ DynamoDB alerts (alerts + audit items, 7-day review window)
                                                         ⇅ Streamlit (Alert queue · Audit sample · Reviewed) ⇄ AML investigators

EventBridge (daily) → Glue label export:
  DynamoDB items whose 7-day window has closed → curated/live/labels/ (append-only) → delete them from DynamoDB

historical/ → Glue feature job → curated/historical/
historical/ ┄▶ Flink (last 7 days, at first start)

Step Functions (initial run + every 6 months):
  Glue job (curated/live/scored + curated/historical ⨝ curated/live/labels) → training/run=<date>/
    → SageMaker training → models/artifacts/
    → SageMaker Processing (champion vs challenger on test/) → better? → Model Registry
    → models/best/manifest.json → Flink polls + hot-reloads
```

On the diagram, `S3 curated/live/` is one bucket icon covering both `scored/` and `labels/`.

### 3.2 Components

**Kinesis Data Streams**
- Partition key = **account ID**, so per-account ordering holds.
- Two consumers: Firehose (raw landing) and Flink.

**Firehose (raw landing)**
- Reads Kinesis and writes every transaction **as received** to `raw/transactions/`, converted to Parquet against the predefined `raw_transactions` table and partitioned by date.
- It does **not** clean or filter. Records that fail format conversion go to Firehose's error prefix.
- Purpose: an untouched copy for replay or reprocessing, and the transaction input for Q2 (§4).

**Managed Apache Flink** (the single cleaner on Q1's live path)
1. Extract each transaction from Kinesis.
2. Clean it. Bad rows go to `dlq/` with the error reason, never dropped silently. The cleaning rules are shared with the Q2 Glue job (§4.3).
3. Compute features from per-account keyed state (7-day windows):
   - counts, amounts, last inflow, pairs
   - **distinct senders into the account and distinct receivers from it** (fan-in / fan-out). Distinct receivers needs a second keyBy on the receiver.
4. Use **event-time windows with watermarks**, never processing time. The replay compresses months into minutes, and real feeds arrive late or out of order.
5. Score in-process with the current model.
6. **Explain (SHAP), for alert and audit items only.** For each transaction at or above the threshold, and each one drawn into the audit sample, compute per-transaction SHAP values (LightGBM TreeSHAP). Keep the **three features with the largest positive contribution** as its risk factors.
   - This is per transaction, not per model: two alerts with the same score can have different risk factors.
   - It is not run on the other ~99% of traffic, so it adds no meaningful time to flagging.
   - **"Other bank" is context, not a risk factor.** For a cross-bank payment the other bank's features are blanked, and the model learns from the blanks: together with the `cross_bank` flag they mean "the other party is at another bank".
     - That is true of most payments, so it is shown on the payment (the counterparty's bank) and **never picked as one of the three risk factors**.
     - Risk factors are limited to behaviour an investigator can check.
     - The score itself is unaffected.
7. Output:
   - every scored record → `curated/live/scored/` through Flink's S3 file sink (Parquet bulk format; files are committed on each checkpoint). Includes `model_version`, `arrived_at` = Kinesis arrival time, `scored_at`
   - flagged transactions + a small random **audit sample** of unflagged ones → DynamoDB, with their risk factors and 7-day summary (see "DynamoDB alerts")
8. Model hot-reload: poll `models/best/manifest.json`. When the version changes, load the artifact it points to and broadcast it to operators. The manifest also carries the alert **threshold**, which Flink stamps on each item.
9. State is checkpointed.

Features are **point-in-time correct** by construction: Flink only sees the past, and training reuses Flink's logged features for live rows.

**Flink state seeding (one-off, at first start)**
- Load the last 7 days of history into state before live traffic.
- Arrow on the diagram: historical → Flink, labelled "Seed Flink state (last 7 days, at startup)".

**DynamoDB alerts** (small operational store, not the system of record)
- Two item types, distinguished by `is_audit`: **alerts** (model-flagged) and **audit items** (random unflagged sample).
- Each item carries **everything the alert page shows without a query**, so opening an alert is instant. Flink writes it at scoring time:
  - **the transaction:** `txn_id`, `txn_ts`, `from_bank`, `from_account`, `to_bank`, `to_account`, `amount` + `currency` (paid), `amount_received` + `receiving_currency`, `payment_format`
  - **the model output:** `score`, `threshold`, `model_version`
  - **`reasons`:** the three SHAP risk factors, each stored as `{feature, value, contribution}` (e.g. `{r_out_n_5D, 95, 1.8}`).
    - Flink stores the feature name and value, **not a sentence**.
    - The app holds one fixed sentence per feature, so wording can change without re-scoring and stays valid after retraining (the feature list is fixed by the feature code).
  - **`features`:** the 7-day summary for both accounts, taken from the feature vector Flink already holds: payments in / out, total amount in / out, distinct senders / receivers. This replaces an Athena query for the first look at the customer.
  - **workflow fields**, written by Streamlit:
    - `status` (open / confirmed / dismissed)
    - `claimed_by`: the investigator who has taken an open item. It is a conditional write, so two people can't take the same one, and it is cleared when the decision is recorded.
    - `reviewer`, `reason` (the reason code chosen with the decision), `note`, `decided_at`
  - plus `created_at` and `expires_at` from Flink
- Other alerts on the same customer are found in this table too (same account, last 7 days); no Athena needed.
- GSI on status + score.
- **Review window: 7 days from creation, for both alerts and audit items** (`expires_at`).
  - After 7 days an alert is no longer actionable for real-time interception.
  - Streamlit hides it and no longer accepts or changes its decision, so the outcome is final.
- **Removal:** the daily label export deletes items after writing them to S3. A DynamoDB TTL attribute (`ttl_at`, a few days after `expires_at`) is only a **backstop** in case the export fails, so no item is deleted before it is recorded.

**Daily label export (Glue job, EventBridge daily)**
1. Read the alerts table and select items whose `expires_at` has passed.
2. Write one row per item to `curated/live/labels/` with its **final outcome**:
   - `confirmed` or `dismissed` if reviewed
   - **`expired`** if still open, never dropped: every alert must have a recorded outcome (compliance), and the expiry rate is a capacity signal
   - plus reason code, note, reviewer and timestamps
3. Then delete those items from DynamoDB.
   - Order matters: S3 first, delete second.
   - If the delete fails, the next run exports them again. Training keeps the latest row per `txn_id`, so duplicates are harmless.
- **Trade-off:** only the final outcome is kept. Changes and reopens within the 7-day window are not logged separately (the earlier per-event Streams → Lambda log did that; see §5).

**Streamlit (investigator UI)**
- **Three pages, switched in the sidebar** (plus Metrics):
  - **Alert queue:** model-flagged items, highest model risk first
  - **Audit sample:** the random sample
  - **Reviewed:** decisions from the last 7 days, which can be changed or reopened until the window closes
- Clicking a row opens the alert page: the payment, the risk factors, the customer's 7-day summary, other alerts on the customer, and the decision form.
- A decision is **Escalate as suspicious** (stored as `confirmed`) or **Close as not suspicious** (stored as `dismissed`), with a reason code and an optional comment.
- **Everything above comes from DynamoDB (instant).** The queue, the alert page, the 7-day summary and the related alerts need no query.
- **Athena is used only on request:**
  - The customer's full payment list (up to 30 days) is an on-demand query on `curated/live/scored/`. Note that it lags by the Flink checkpoint interval (about a minute).
  - History older than 7 days comes from `curated/live/labels/`.

**Glue feature job (historical lane, batch)**
- Reads `historical/` and computes the **same features as Flink** with Spark window functions over each account's past 7 days, strictly past-only.
- Writes `curated/historical/`.
- **Parity check:** compute features for a small sample in Spark and in Flink and confirm they match.

**Training workflow: one Step Functions workflow for both the initial run and retrains**
- **Trigger:** EventBridge, every 6 months. Manual run for the demo.
- **Step 1: Glue job (build training set)**
  1. Read `curated/live/labels/` and keep the **latest row per `txn_id`**. Map it to a label, for alerts and audit items alike:
     - confirmed → 1
     - dismissed → 0
     - expired → dropped
  2. Inner-join with `curated/live/scored/` on `txn_id` (broadcast join on the small labels side). Unreviewed live rows are not used, and there is no pseudo-labelling.
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

- **Labels:** confirmed = 1; dismissed = 0 (valuable hard negatives); audit = the reviewer's finding; expired = no label.
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
  - SHAP risk factors explain what the model reacted to, not whether the payment is laundering. A factor such as "first payment between these accounts" is weak on its own and matters in combination.
  - model scores are small probabilities (laundering is ~0.1% of payments): an alert means "in the top 1% by risk", not "probably laundering"
  - labels are final outcomes only; how often decisions were changed within the 7-day window is not recorded

### 3.4 Metrics and monitoring (essential set only; all near-free)

| Metric | Source |
|---|---|
| Kinesis iterator age | CloudWatch alarm |
| Throughput (records/sec) | Kinesis incoming records |
| Time to flag, p50 / p99 (`arrived_at` → `scored_at`) | Athena on `curated/live/scored/` |
| % of alerts / audit items expired unreviewed | Athena on `curated/live/labels/` |
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
  Customer master DB ─(daily export)─▶ raw/accounts/ ─┐
  FX (Frankfurter) ─────(daily)─▶ raw/fx/ ───────────┤
  raw/transactions/ (Q1 Firehose) + curated/historical/ (Q1) ──┴─▶ Glue job → curated/customer_txns/
     → Transient EMR (reads customer_txns + decisions/) → scores/, rule_hits/, rule_eval/

DAY (KYC analysts):
  Athena: scores/ (WHERE in_queue) + latest event per customer from decisions/ → Streamlit queue ⇄ KYC analysts
  Athena: curated/customer_txns/ → customer's full transaction history (on demand)
  Streamlit → decisions/  (append-only, written directly)
```

The nightly loop answers "who looks risky?". The daytime loop records "we've dealt with them". They connect when EMR reads `decisions/` the next night.

### 4.3 Components

**Shared ingestion with Q1**
- Q2 reuses Q1's Kinesis → Firehose landing (`raw/transactions/`) and `curated/historical/`. There is no separate stream, Firehose or crawler.
- Q2 does **not** depend on Flink or the model. Only the ingestion (Kinesis + Firehose) must run, or `raw/transactions/` be backfilled, before Q2 can be demoed.

**Reference data (CSV)**
- `raw/accounts/`: account → customer mapping, from the customer master DB.
- `raw/fx/date=<day>/fx.csv`: one small file per day (`currency,usd_per_unit,source`). The history days are uploaded once; the producer sends each live day's file as that simulated day starts. The Glue job reads only the days it processes, so the rate table it broadcasts holds just those days.
- FX source notes:
  - Frankfurter API v2 (free, no key): central-bank and official rates for all 14 currencies; Bitcoin from the Swiss Federal Tax Administration
  - a day with no published rate (Bitcoin on weekends) reuses the last one before it (`source` = `frankfurter-carried`)

**Glue job (ETL, incremental: new dates only, after a one-off backfill)**
1. Read the raw accounts + FX, and the new date partitions of `raw/transactions/`.
2. Clean the transactions using **the same rules as Flink**. Rejected rows go to `dlq/` with the reason. `curated/historical/` is already clean and is not re-cleaned.
3. Clean the reference data (dedupe, standardise currency codes) and **validate**, failing the run if any check fails:
   - every account maps to a customer
   - every currency has a rate
   - row counts are non-zero
4. Join to the transactions, adding `customer_id` and the USD amount, using the account mapping **as of the transaction date**.
5. Write `curated/customer_txns/`.

**Transient EMR (Spark)**
- Created and terminated per run by Step Functions, with spot workers.
- **All Q2 decision logic lives here** (§4.4 and §4.5).
- Reads `curated/customer_txns/` and `decisions/`.
- Writes `scores/`, `rule_hits/`, `rule_eval/` and `state/`.

**Athena** (read-only; schemas from the Glue Data Catalog)
- Queue query: `WHERE run_date = <today> AND in_queue ORDER BY score DESC`.
- **Latest decision per customer:** the latest event per `customer_id` in `decisions/` (window function over the recent date partitions).
- **Change vs previous run:** join today's and the previous `scores/` partition per customer to get the score change and the rules new today.
- **Customer full history:** query `curated/customer_txns/` for one customer on demand (the Athena → `curated/customer_txns/` arrow on the diagram).

**Streamlit (prioritised EDD queue)**
- **Reads through Athena only.**
  - Cache the queue query (`st.cache_data`) until the next nightly run, and enable Athena query-result reuse.
  - Re-run the latest-decision query after each write. S3 is read-after-write consistent, so a new decision file is visible to the next query.
- Combine the cached queue rows with each customer's current state (in review by X, cleared until …) from `decisions/`.
- **Writes decisions directly to `decisions/`** (one small Parquet file per event).
- Three views:

| View | Shows | Answers |
|---|---|---|
| Priority queue | Customers with `in_queue`, ranked: score, change since last run, top rules fired, number of accounts, `queue_reason` | Who do we review first? |
| Customer detail | Rules fired with evidence, the customer's accounts, score trend, full transaction history (Athena on `curated/customer_txns/`) | Why is this customer here? |
| Overview | Count above EDD threshold, which rules fire most, score distribution, rule precision from `rule_eval/` | How big is the workload, and are rules too noisy? |

**`decisions/` (append-only event log, written by Streamlit)**
- One event per row:
  - `customer_id`, `event`, `analyst`, `at`
  - `cleared_until` (for clearances)
  - `note`
  - the **snapshot** (score + rules fired at review time)
- Event types:
  - **claimed / released:** analyst X is reviewing the customer, or has stopped
  - **edd_needed**
  - **cleared** (with cleared-until)
  - **reopened:** undoes the latest decision
- Current state per customer:
  - **decision** = the latest `edd_needed` / `cleared` / `reopened` event (`reopened` = no decision)
  - **claim** = the latest `claimed` / `released` event, ignored once a decision is recorded after it
- **Claims are not an atomic lock.**
  - Before claiming or deciding, the app re-reads the customer's latest events and refuses if another analyst holds an open claim.
  - Two analysts claiming within the same few seconds could both succeed. This is acceptable at KYC volume (see §6 for a strict alternative).
- No TTL is needed: EMR evaluates clearance expiry from `cleared_until`.
- This log is also the full audit trail for compliance.

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
- **Structuring threshold:** MAS Notice 626 requires CDD for occasional transactions exceeding S$20,000 (verify against the current notice).
  - Amounts are in USD, so set the band as e.g. 80–99.9% of the threshold converted to USD, and state the choice on the slide.
  - The previous $8,000–9,999 band follows the US $10k rule.
- **Score** = sum of rule points + peer composite (0–1 tie-breaker from peer percentiles within entity type).
- **Tiers** (capacity-based prioritisation, not a verdict):
  - top 1% → High
  - next 9% → Medium
  - both require at least one rule to have fired
  - everyone else → Low
- **Rule evaluation (`rule_eval/`):**
  - per rule: customers fired on, and the share with any `is_laundering` transaction (precision)
  - per tier: the share involved in laundering
  - `is_laundering` is used **only** for evaluation, never as a scoring input
- **Scalability:**
  - Avoid a global unpartitioned window for ranking on large data; compute the 1% / 10% cutoffs with `approxQuantile` and tier by threshold.
  - Watch R2's self-join for skew.
- **R9 state:** keep only the latest `state/cp_first_seen/` snapshot(s). Runs must be processed in date order.

### 4.5 Queue logic (`in_queue`) — computed by EMR

Scoring never stops: every customer is scored every night, regardless of decisions. Decisions only affect whether they appear in the queue.

For each customer, EMR reads their latest decision from `decisions/` (claims and releases don't change it):

| Latest decision | `in_queue` |
|---|---|
| None, or reopened | true if tier is High or Medium |
| Cleared, today < cleared-until | **false**, unless a rule fired that is not in the decision's snapshot, or the score rose well above the snapshot score (e.g. +20) → true |
| Cleared, cleared-until passed | treat as no decision: true if High or Medium |
| EDD needed | false (moved to the EDD process; still scored and visible in customer detail) |

An open claim doesn't change `in_queue`; Streamlit shows who is reviewing.

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
| A cleaning step on Q1's live path other than Flink | Flink is the single cleaner for Q1. The raw Firehose stores as received and does not clean |
| Glue crawlers | We own all schemas |
| Lambda "Evaluate Results" step | Replaced by Processing + a Choice state |
| Backfilling history through Flink in batch mode | Replaced by the Glue feature job + parity check |
| Pseudo-labelling low-score unflagged transactions | The model would grade its own homework; the audit sample is used instead |
| Score-drift alarm | Replaced by the operational metrics |
| Training reads DynamoDB directly | Training reads `curated/live/labels/`, written by the daily label export |
| DynamoDB Streams → Lambda → `labels/` (per-event alert log) | Replaced by the daily Glue label export: simpler, batch, and outcomes are final once the 7-day window closes. Trade-off: intermediate changes aren't logged |
| **Writing labels into the scored files** | Parquet in S3 can't be edited per row. Labels go to their own sub-prefix, `curated/live/labels/`, and are joined to `curated/live/scored/` at training |
| One Athena table over all of `curated/live/` | It would mix scored rows and labels; each sub-prefix is its own table |
| Storing curated data as CSV | No column pruning, untyped, larger; CSV also can't be updated by Athena |
| Athena `UPDATE` per click / Iceberg for labels | Plain S3 tables can't be updated; Iceberg adds rewrite and compaction overhead and loses event history. Optional future X-factor only, via a daily batch MERGE |
| Blind review (audit items hidden among alerts) | Team chose separate Alerts and Audit tabs |
| Different review windows for alerts and audit items | One 7-day window for both |
| Dropping unreviewed alerts without a record | The label export writes them as `expired`; every alert has a recorded outcome |
| Separate Kinesis / Firehose / crawler for Q2 | Q2 shares Q1's ingestion (`raw/transactions/`) |
| DynamoDB decisions table + sync to S3 for Q2 | KYC is a daily batch process, so instant updates aren't needed. Decisions go straight to S3 as an append-only log: full audit trail, what EMR reads, no extra table or sync |
| Analysts editing scores in S3 | Decisions are stored separately; scores are never edited |
| Q2 R6 "linked to last run's High tier" | Self-reinforcing; replaced by R12 (analyst-confirmed EDD) |
| Clearing a customer permanently | Clearances expire and are overridden by new rules or a score jump |
| Separate Glue Data Quality step, FX Lambda, extra CloudWatch for Q2 | Validation lives in the Glue job |
| Live RDS instance for the customer DB in the demo | A Glue job in a VPC loses internet access (the FX API), and a NAT gateway costs ~$1/day |

---

## 6. Open decisions

- **Flink language:** PyFlink (load LightGBM directly) or Java (export as LightGBM text or ONNX). This decides the training artifact format **and how SHAP is computed**:
  - PyFlink: LightGBM's built-in `predict(pred_contrib=True)` gives exact TreeSHAP.
  - Java: needs a TreeSHAP implementation over the LightGBM text model; ONNX alone does not give SHAP values.
  - The current `score_lambda.py` uses a simpler tree-path attribution, not SHAP. It must be replaced, not reused.
- **Flink → `curated/live/scored/` sink:** Flink's S3 file sink (as drawn; files commit on checkpoint) or Flink → Firehose → S3 (managed buffering, extra cost).
- **Flink seeding mechanism:**
  - Real world: hybrid source (S3 last 7 days → Kinesis); check connector support.
  - Demo fallback: `producer.py` replays 7 days early with alerts suppressed.
- **Shared transaction cleaning:** one set of cleaning rules used by both Flink and the Q2 Glue job (shared module, or a written rule list plus a parity test on a sample). Otherwise the two can drift.
- **Q2 claim locking:** event-based claims (current, small race) or a strict lock via S3 conditional writes (`If-None-Match` on a per-customer claim object).
- **Values not yet fixed:**
  - Q1: N (rolling window), audit sample rate, alert threshold / budget, minimum new labels to retrain, DynamoDB backstop TTL (`ttl_at` = `expires_at` + ? days)
  - Q2: score-jump margin for re-surfacing, structuring threshold
- **SageMaker input format:** built-in LightGBM (CSV) or a custom script (Parquet).
- **EMR sizing:** node count and instance types.
- **Code alignment:**
  - Q2 scoring script: it currently reads `curated/` + `reference/accounts/` and writes `results/customer_scores/`, `results/rule_hits/`. Align the code with this spec, or this spec with the code.
  - `q2_streamlit.py`: AWS mode is still described as DynamoDB decisions with conditional writes. It must write events to `decisions/` and read state via Athena.
  - `q1_streamlit.py`: AWS mode must hide items by `expires_at`, not rely on TTL deletion. `LIVE_TABLE` must point at the table over `curated/live/scored/`.

---

## 7. Diagram files

- `q1.drawio`: Q1 ("IS459 BDA AML Project Business Question 1")
- `q2.drawio`: Q2 ("IS459 BDA AML Project Business Question 2")

Notes:
- Q1: the `S3 curated/live/` icon covers both sub-prefixes. Flink's "scored records" land in `curated/live/scored/`, and the daily Glue job's output lands in `curated/live/labels/`.
- Q1: `S3 raw/` on the diagram is `raw/transactions/` in this spec.
- Q2: add the arrow Athena → `curated/customer_txns/` (customer's full transaction history).
- Not drawn (fine to leave off): `dlq/`, `rule_hits/`, `rule_eval/`, `state/`.

Legend conventions:
- solid arrow = data flow
- dashed arrow = control / model flow
- red dashed box = demo stand-in
- grey dashed box = provided by the Q1 pipeline
- "Pipelines read S3; the Q1 app reads DynamoDB, the Q2 app reads Athena"

---

## 8. As built (13 Oct 2026)

The code in `main/` implements sections 2 to 4. How to run it is in `README.md`. This section records how the open decisions in section 6 were settled, and where the build differs from the text above.

Open decisions, as settled:

| Decision | Choice | Why |
|---|---|---|
| Flink language | PyFlink | No Java build is needed, and the feature code is one Python module shared with the Glue jobs |
| Model format and SHAP | LightGBM's JSON dump, scored and explained (exact TreeSHAP) in pure Python by `Q1_files/q1_core.py` | Managed Flink then needs no native library. `train.py` saves LightGBM's own probabilities and SHAP values for a sample, and Flink checks itself against them each time it loads a model |
| Flink seeding | Flink reads `state/flink_seed/` (the last 7 days of history, written by the Glue feature job) as a second source at first start | Matches the diagram's arrow from `historical/` to Flink. Start Flink a minute before the producer so the seed is loaded first |
| Shared cleaning | One function, `clean()` in `common/aml_clean.py`, run by Flink and as a UDF by the Q2 Glue job. It holds only the rules that mean the same in both questions: a valid payment record involving our bank, one row per `txn_id`. Rules only one pipeline wants stay in that pipeline (Q1: account keys for features; Q2: dropping moves within one account, the customer and FX checks) | The two cannot drift, and `common/` holds nothing that only one pipeline uses |
| Q2 claim locking | Event-based claims (the small race is accepted) | As in 4.3 |
| Values | N = 6 months, audit rate 0.3%, alert budget 1%, DynamoDB backstop 30 days, score jump +20, structuring threshold US$14,300 | Terraform variables or constants in the code |
| SageMaker input | Custom script, Parquet | |
| EMR sizing | 1 master + 2 core `m5.xlarge`, on demand | The Learner Lab does not allow spot. `emr_use_spot = true` restores the spec |

Differences from the text above:

- **Scored rows sink.** Flink writes `curated/live/scored/` itself, as drawn, but from a Python operator that buffers rows in checkpointed state and writes one Parquet file every 30 seconds, not through Flink's file sink. The file sink's Parquet format needs a Java build with Hadoop libraries. Delivery is at least once: a restart can write a row twice, so readers keep one row per `txn_id`.
- **Model hot-reload.** Each scoring task polls `models/best/manifest.json` every 30 seconds and reloads when it changes. The effect is the same as broadcast state, without a second source.
- **Partitions.** Every table uses Athena partition projection, including the ones Glue jobs write, so nothing has to register partitions.
- **Label export schedule.** The daily label export is started by a Glue scheduled trigger, because an EventBridge rule cannot target a Glue job directly.
- **Time in the demo.** `created_at` and `expires_at` on an alert are in event time (the payment's own clock), and the apps and the label export read "now" from the producer's simulated clock (`state/sim_clock.json`). `ttl_at` stays on the real clock. Decision events carry `written_at` (real time) to order events made at the same simulated moment.
- **`new_pair` and `cyc2_hrs`** look back 30 days, so Flink's state stays bounded. The Glue feature job uses the same 30 days.
- **All banks are streamed.** The producer sends every bank's payments, and `raw/transactions/` holds them all. Flink, the Q1 historical feature job and the Q2 Glue job each keep the payments that involve our bank (`involves_bank()` in `common/aml_clean.py`). Other banks' payments are filtered out, not sent to `dlq/`. The bank is set once, in `main/common/settings.json`.
- **FX rates.** A rate applies to its own day. The Q2 Glue job fails if any payment date has no rate; it does not fall back to a nearby date. `producer.py fx` fetches rates from the Frankfurter API for the prepared dataset's whole date range.
- **Code layout.** `main/common/` holds only what both pipelines use: the cleaning rules, the simulated clock, and config. Q1's feature and model code is `main/Q1_files/q1_core.py`.
- **Q2 validation** fails the run if an account at our bank has no customer. A counterparty at another bank that is not in the customer file is kept, with the id `EXT-<bank>-<account>`.
- **Q2 customer accounts** shown in the app are the accounts the customer used in the 7-day window.
- **IAM.** Every service runs as `LabRole`, because the Learner Lab cannot create roles.

Not yet run on AWS: see `README.md` for the tests that run on a laptop, and what they cover.
