-- IS459 G2 Q2 - Athena tables and views (database: aml_q2)
-- Replace is459-g2-aml-project everywhere, then run each statement separately in the Athena query editor.
-- Partition projection = no crawlers and no MSCK REPAIR needed.

CREATE EXTERNAL TABLE IF NOT EXISTS aml_q2.curated_transactions (
  `txn_id` string,
  `txn_ts` timestamp,
  `txn_date` string,
  `from_bank` string,
  `from_account` string,
  `from_entity_id` string,
  `from_entity_type` string,
  `to_bank` string,
  `to_account` string,
  `to_entity_id` string,
  `to_entity_type` string,
  `amount_paid` double,
  `payment_currency` string,
  `amount_paid_usd` double,
  `amount_received` double,
  `receiving_currency` string,
  `amount_received_usd` double,
  `payment_format` string,
  `is_laundering` int
)
PARTITIONED BY (`date` string)
STORED AS PARQUET
LOCATION 's3://is459-g2-aml-project/curated/'
TBLPROPERTIES (
  'projection.enabled'='true',
  'projection.date.type'='date',
  'projection.date.format'='yyyy-MM-dd',
  'projection.date.range'='2022-09-01,2022-12-31',
  'projection.date.interval'='1',
  'projection.date.interval.unit'='DAYS',
  'storage.location.template'='s3://is459-g2-aml-project/curated/date=${date}/'
);

CREATE EXTERNAL TABLE IF NOT EXISTS aml_q2.customer_scores (
  `entity_id` string,
  `entity_type` string,
  `n_accounts` bigint,
  `score` double,
  `score_percentile` double,
  `points` int,
  `tier` string,
  `prev_tier` string,
  `moved_to_high` boolean,
  `reason_1` string,
  `reason_2` string,
  `rules_triggered` string,
  `inflow_usd` double,
  `outflow_usd` double,
  `pass_through_ratio` double,
  `median_hours_to_forward` double,
  `distinct_senders` bigint,
  `distinct_recipients` bigint,
  `new_counterparty_share` double,
  `spike_ratio` double,
  `cash_crypto_share` double,
  `own_account_transfers` bigint,
  `high_risk_counterparties` bigint,
  `round_trips` bigint,
  `window_start` string,
  `window_end` string,
  `window_complete` boolean
)
PARTITIONED BY (`run_date` string)
STORED AS PARQUET
LOCATION 's3://is459-g2-aml-project/results/customer_scores/'
TBLPROPERTIES (
  'projection.enabled'='true',
  'projection.run_date.type'='date',
  'projection.run_date.format'='yyyy-MM-dd',
  'projection.run_date.range'='2022-09-01,2022-12-31',
  'projection.run_date.interval'='1',
  'projection.run_date.interval.unit'='DAYS',
  'storage.location.template'='s3://is459-g2-aml-project/results/customer_scores/run_date=${run_date}/'
);

CREATE EXTERNAL TABLE IF NOT EXISTS aml_q2.rule_hits (
  `entity_id` string,
  `rule_id` string,
  `rule_name` string,
  `points` int,
  `evidence` string
)
PARTITIONED BY (`run_date` string)
STORED AS PARQUET
LOCATION 's3://is459-g2-aml-project/results/rule_hits/'
TBLPROPERTIES (
  'projection.enabled'='true',
  'projection.run_date.type'='date',
  'projection.run_date.format'='yyyy-MM-dd',
  'projection.run_date.range'='2022-09-01,2022-12-31',
  'projection.run_date.interval'='1',
  'projection.run_date.interval.unit'='DAYS',
  'storage.location.template'='s3://is459-g2-aml-project/results/rule_hits/run_date=${run_date}/'
);

CREATE EXTERNAL TABLE IF NOT EXISTS aml_q2.validation_summary (
  `metric` string,
  `value` double
)
PARTITIONED BY (`run_date` string)
STORED AS PARQUET
LOCATION 's3://is459-g2-aml-project/results/validation_summary/'
TBLPROPERTIES (
  'projection.enabled'='true',
  'projection.run_date.type'='date',
  'projection.run_date.format'='yyyy-MM-dd',
  'projection.run_date.range'='2022-09-01,2022-12-31',
  'projection.run_date.interval'='1',
  'projection.run_date.interval.unit'='DAYS',
  'storage.location.template'='s3://is459-g2-aml-project/results/validation_summary/run_date=${run_date}/'
);

CREATE EXTERNAL TABLE IF NOT EXISTS aml_q2.validation_typology (
  `typology` string,
  `n_laundering_customers` bigint,
  `caught_high` bigint,
  `caught_high_or_medium` bigint
)
PARTITIONED BY (`run_date` string)
STORED AS PARQUET
LOCATION 's3://is459-g2-aml-project/results/validation_typology/'
TBLPROPERTIES (
  'projection.enabled'='true',
  'projection.run_date.type'='date',
  'projection.run_date.format'='yyyy-MM-dd',
  'projection.run_date.range'='2022-09-01,2022-12-31',
  'projection.run_date.interval'='1',
  'projection.run_date.interval.unit'='DAYS',
  'storage.location.template'='s3://is459-g2-aml-project/results/validation_typology/run_date=${run_date}/'
);

CREATE EXTERNAL TABLE IF NOT EXISTS aml_q2.validation_rules (
  `rule_id` string,
  `rule_name` string,
  `hits` bigint,
  `laundering_hits` bigint,
  `precision` double
)
PARTITIONED BY (`run_date` string)
STORED AS PARQUET
LOCATION 's3://is459-g2-aml-project/results/validation_rules/'
TBLPROPERTIES (
  'projection.enabled'='true',
  'projection.run_date.type'='date',
  'projection.run_date.format'='yyyy-MM-dd',
  'projection.run_date.range'='2022-09-01,2022-12-31',
  'projection.run_date.interval'='1',
  'projection.run_date.interval.unit'='DAYS',
  'storage.location.template'='s3://is459-g2-aml-project/results/validation_rules/run_date=${run_date}/'
);

-- Reviewer decisions written by Streamlit as one JSON object per line.
-- suppress_until is a SIMULATED date (run_date + N days), not today's real date.
CREATE EXTERNAL TABLE IF NOT EXISTS aml_q2.overrides (
  `entity_id` string,
  `decision` string,
  `note` string,
  `reviewer` string,
  `decided_at` string,
  `decided_on_run_date` string,
  `suppress_until` string
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://is459-g2-aml-project/state/overrides/';

-- ---------- views

CREATE OR REPLACE VIEW aml_q2.v_latest_run AS
SELECT max(run_date) AS run_date
FROM aml_q2.customer_scores;

CREATE OR REPLACE VIEW aml_q2.v_first_high AS
SELECT entity_id, min(run_date) AS first_high_date
FROM aml_q2.customer_scores
WHERE tier = 'High'
GROUP BY entity_id;

CREATE OR REPLACE VIEW aml_q2.v_review_queue AS
SELECT s.*, f.first_high_date
FROM aml_q2.customer_scores s
JOIN aml_q2.v_latest_run l ON s.run_date = l.run_date
LEFT JOIN aml_q2.v_first_high f ON s.entity_id = f.entity_id
WHERE s.tier IN ('High', 'Medium')
  AND NOT EXISTS (
    SELECT 1 FROM aml_q2.overrides o
    WHERE o.entity_id = s.entity_id
      AND o.decision = 'dismiss'
      AND o.suppress_until >= s.run_date
  );

CREATE OR REPLACE VIEW aml_q2.v_moved_to_high AS
SELECT s.*
FROM aml_q2.customer_scores s
JOIN aml_q2.v_latest_run l ON s.run_date = l.run_date
WHERE s.moved_to_high;

CREATE OR REPLACE VIEW aml_q2.v_tier_distribution AS
SELECT run_date, entity_type, tier, count(*) AS n_customers
FROM aml_q2.customer_scores
GROUP BY run_date, entity_type, tier;
