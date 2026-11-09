# AML Transaction Monitoring and KYC Prioritisation on AWS

An end-to-end anti-money-laundering (AML) system built on AWS for the IS459 Big Data Architecture group project (Group 2, SMU). It uses the synthetic [IBM Transactions for Anti Money Laundering](https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml) dataset and takes the view of a single bank.

| | Question | Approach |
|---|---|---|
| **Q1** | Which payments look like laundering, right now? | Streaming: each payment is scored by a LightGBM model within seconds, and high-risk payments become alerts for investigators |
| **Q2** | Which customers should KYC prioritise for enhanced due diligence? | Daily batch: each customer's weekly activity is evaluated against 12 rules and ranked |

Each question has a Streamlit application for reviewing results and recording decisions. The full design is in [PIPELINE_SPEC.md](PIPELINE_SPEC.md).

## Architecture

```
producer.py ──► Kinesis ──► Managed Flink ──► DynamoDB alerts ──► Q1 app (investigators)
(dataset replay)    │          (features,          │
                    │           scoring)           └──► S3 curated/live/scored
                    │
                    └──► Firehose ──► S3 raw ──► Glue ──► EMR (rules, ranking) ──► Q2 app (KYC analysts)
```

**Q1, real-time flagging.** Managed Flink computes 53 per-account features over rolling 1, 5 and 7-day windows, scores each payment, and explains each alert with SHAP risk factors. Alerts and a random audit sample go to DynamoDB for review. Investigator decisions are exported daily to S3 and feed retraining.

**Q1, model training.** A Step Functions workflow builds training features with Glue, trains and evaluates a challenger on SageMaker, and promotes it only if it beats the current model. Flink hot-loads the approved model.

**Q2, daily KYC scoring.** Glue cleans the day's transactions and converts them to USD. A transient EMR Spark cluster aggregates each customer's activity, applies the rules and writes scores, tiers and evidence to S3, queried through Athena.

### AWS services

| Layer | Services |
|---|---|
| Ingestion | Kinesis Data Streams, Data Firehose |
| Processing | Managed Service for Apache Flink, Glue, EMR |
| Machine learning | SageMaker (training, model registry) |
| Storage and query | S3 (Parquet), Glue Data Catalog, Athena, DynamoDB |
| Orchestration | Step Functions, EventBridge, Lambda |
| Applications | Streamlit on EC2 |
| Infrastructure | Terraform |

## Repository structure

```
├── PIPELINE_SPEC.md          Design specification
├── data/                     Dataset files (git-ignored except fx_rates.csv)
├── docs/                     Architecture diagram, cost estimates, throughput and model results
└── main/
    ├── infra/                Terraform root: one file per area, plus Step Functions (sfn/) and Lambda (lambda/)
    ├── scripts/              build.py, pipeline.py (operations), cost.py, estimate.py
    ├── producer/             producer.py (dataset replay), stress.py (load testing)
    ├── common/               Shared by Q1 and Q2: payment cleaning, simulated clock, config, settings.json
    ├── Q1_files/             q1_core.py (features, scoring), flink/, glue/, sagemaker/, streamlit/
    ├── Q2_files/             q2_common.py, glue/, emr/, streamlit/
    └── tests/                Local tests (no AWS required)
```

## Getting started

### Prerequisites

- Python 3.11+
- [Terraform](https://developer.hashicorp.com/terraform/install) 1.5+
- [AWS CLI](https://aws.amazon.com/cli/) v2
- An AWS account, with an IAM user that has `AdministratorAccess`
- `HI-Small_Trans.csv` and `HI-Small_accounts.csv` from the Kaggle dataset, placed in `data/`

### Installation

```bash
python -m venv .venv
source .venv/bin/activate            # Windows: .venv\Scripts\activate
pip install boto3 pandas streamlit
```

### AWS credentials

1. In the AWS console, create an access key for the IAM user (**IAM → Users → Security credentials → Create access key**).
2. Run `aws configure` and enter the access key ID, secret access key, region `us-east-1` and output format `json`. This saves the key to `~/.aws/credentials` (Windows: `C:\Users\<you>\.aws\credentials`).
3. Confirm the account:

   ```bash
   aws sts get-caller-identity
   ```

Terraform and the scripts read the key from that file, and the stack deploys to that account. No account ID is hard-coded.

### Configuration

```bash
cp main/infra/terraform.tfvars.example main/infra/terraform.tfvars
```

The file sets `manage_iam_resources = true`, so Terraform creates the project's IAM roles, and the stream and Flink capacity.

The bank and the live start date are set in [main/common/settings.json](main/common/settings.json) (defaults: bank `11`, `2022-09-08`). See [Configuration reference](#configuration-reference).

### Deployment

```bash
python main/scripts/build.py                     # package code for Terraform
cd main/infra && terraform init && terraform apply && cd ../..

python main/producer/producer.py prepare --csv data/HI-Small_Trans.csv
python main/producer/producer.py upload --accounts data/HI-Small_accounts.csv --fx data/fx_rates.csv
python main/scripts/pipeline.py setup            # build features and train the first model (~20 min)
python main/scripts/pipeline.py status           # expect Flink READY and an approved model
```

`prepare` splits the dataset at `live_start`: earlier days become training history and later days are replayed live.

## Usage

### Running a demo

```bash
python main/scripts/pipeline.py start            # start Flink; allow ~3 min to load recent history
python main/producer/producer.py replay --labels --q2 async
python main/scripts/pipeline.py apps             # print the Q1 and Q2 app URLs (http)
python main/scripts/pipeline.py stop             # stop Flink when finished
```

The producer drives a simulated clock that both apps follow. `--labels` and `--q2 async` run the daily label export and Q2 job at each simulated midnight. Press `Ctrl+C` to pause and `replay --resume` to continue.

### Replay speed

`--speed N` runs the clock N times faster than real time (default 60). For HI-Small:

| `--speed` | One simulated day | Three busy days | Payments/second |
|---|---|---|---|
| 24 | 1 h | 3 h | 28 |
| **60** | **24 min** | **72 min** | **71** |
| 120 | 12 min | 36 min | 142 |
| 288 | 5 min | 15 min | 341 |
| 720 | 2 min | 6 min | 853 |
| 1440 | 1 min | 3 min | 1,705 |

`--day-minutes N` sets the length of a day directly, and `--dry-run` previews the rate without sending. Each Q2 run takes about 12 real minutes at any speed; `--q2 wait` pauses the clock while it runs. Above speed 800, increase `kinesis_shards`.

### Load testing

```bash
python main/producer/stress.py run --rate 20 --duration 120
python main/producer/stress.py run --stages 20:60,50:60,100:60
python main/producer/stress.py report --run-id <id>    # p50/p99 scoring latency per minute
python main/producer/stress.py cleanup --run-id <id>
```

Stress payments are excluded from both apps. A steadily rising p99 indicates `flink_parallelism` should be increased.

### Applications

Both apps run on one EC2 instance and are open to anyone with the URL. App changes are deployed with `terraform apply`. To run an app locally against AWS:

```bash
cd main/Q1_files/streamlit
Q1_MODE=aws streamlit run q1_streamlit.py              # Windows: set Q1_MODE=aws
```

For Q2, use `main/Q2_files/streamlit`, `Q2_MODE` and `--server.port 8502`.

## Configuration reference

### settings.json

| Key | Description | Default |
|---|---|---|
| `our_bank` | Bank whose view the system takes (ID without leading zeros) | `"11"` |
| `live_start` | Split date: earlier payments train the model, later ones are replayed | `"2022-09-08"` |

`producer.py dates --csv <file>` lists a dataset's dates and suggests a `live_start`. Changing either value after deployment requires `terraform apply`, `pipeline.py reset --yes` and `pipeline.py setup`; changing `live_start` also requires re-running `prepare` and `upload`.

### terraform.tfvars

| Variable | Description | Default |
|---|---|---|
| `manage_iam_resources` | Create the project's IAM roles and instance profiles | `true` |
| `kinesis_shards` | Stream shards, 1,000 records/second each | `1` |
| `flink_parallelism` | Flink parallelism (KPUs) | `1` |

All variables are documented in [main/infra/variables.tf](main/infra/variables.tf).

### Other datasets

The HI-Medium, HI-Large and LI datasets cover different dates. Set `live_start` from `producer.py dates`, run `prepare`, fetch matching exchange rates from the [Frankfurter API](https://frankfurter.dev) with `producer.py fx` (replaces `data/fx_rates.csv`), then `upload`, `terraform apply` and `pipeline.py setup`.

## Operations

| Task | Command |
|---|---|
| Clear demo results, keep the stack and model | `pipeline.py stop`, `pipeline.py reset --yes`, `pipeline.py start --fresh` |
| Retrain and promote if better | `pipeline.py retrain` |
| Run Q2 for one day | `pipeline.py q2 --run-date 2022-09-08` |
| Export investigator labels | `pipeline.py labels` |
| Control the simulated clock | `producer.py clock show` / `pause` / `resume` |
| Cost of a past run | `cost.py --since <time> --runs --save run.json` |
| Project costs to larger datasets | `estimate.py --run run.json --mode live\|full\|month\|demo` |
| Tear down | `cd main/infra && terraform destroy` |

All scripts accept `--help`. Avoid destroying the stack while a Q2 run is in progress (`q2-daily-kyc-scoring` in Step Functions), as its EMR cluster may be left running.

After a teardown, redeploy with `build.py`, `terraform apply`, `upload` and `pipeline.py setup`. When moving to a different AWS account, destroy the stack first; otherwise delete `main/infra/terraform.tfstate*` before applying.

## Testing

```bash
pip install lightgbm scikit-learn
python main/producer/producer.py prepare --csv data/HI-Small_Trans.csv
python -m pytest main/tests
```

The tests cover feature computation, model scoring and training, and the producers. Components that run on AWS are not covered by local tests.
