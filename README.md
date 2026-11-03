# is459-bda

IS459 Big Data Architecture, Group 2. An anti-money-laundering (AML) system on AWS, built on the IBM AML dataset. It answers two questions for one bank:

- **Q1: which payments look like laundering, right now?** Each payment is scored within seconds. The riskiest become alerts for investigators.
- **Q2: which customers should KYC review first?** Every night, each customer's week of activity is checked against 12 rules and ranked.

Each question has a web app where staff see the results and record decisions.

The design is in [PIPELINE_SPEC.md](PIPELINE_SPEC.md). This README is how to build it and run it.


## How it fits together

```
your laptop                          AWS
-----------                          ---
producer.py  -- payments -->  Kinesis --> Flink --> alerts ---------> Q1 app
(replays the dataset                  \                                (investigators)
 on a fast clock)                      --> S3 --> nightly job ------> Q2 app
                                                  (rules, ranking)     (KYC analysts)
```

- **Terraform** creates everything on the AWS side from the files in `main/infra/`. You describe what you want in files and it builds it, so nobody clicks through the console.
- **The producer** pretends to be the bank's payment feed. It replays the dataset faster than real time, so several days pass in about an hour.
- **Two helper scripts** run on your laptop: `producer.py` sends payments, `pipeline.py` starts and stops things.

## Set up (once)

Run every command from the repo root.

### 1. Install the tools

| Tool | Install | Check |
|---|---|---|
| Python 3.11+ | python.org | `python --version` |
| Terraform | Windows: `winget install Hashicorp.Terraform` · Mac: `brew install terraform` | `terraform version` |
| AWS CLI | Windows: `winget install Amazon.AWSCLI` · Mac: `brew install awscli` | `aws --version` |

Open a new terminal after installing, then:

```
python -m venv .venv
.venv\Scripts\activate                 (Mac/Linux: source .venv/bin/activate)
pip install boto3 pandas streamlit
```

### 2. Connect to your AWS lab account

Do this at the start of **every** lab session, because the credentials expire when the session ends.

1. Start the Learner Lab and wait for the green dot.
2. Click **AWS Details**, then **Show** next to AWS CLI.
3. Copy the whole block into `C:\Users\<you>\.aws\credentials` (Mac/Linux: `~/.aws/credentials`), replacing what is there.
4. Check it worked: `aws sts get-caller-identity` prints your account number.

That file is the only thing that decides which AWS account gets used.

### 3. Build everything on AWS

The system is set up for bank 11, with 8 Sep 2022 as the first live day. To use a different bank or date, edit [settings.json](#settingsjson-which-bank-and-which-dates) before you build.

```
python main/scripts/build.py
cd main/infra
terraform init
terraform apply
cd ../..
```

| Command | What it does | How long |
|---|---|---|
| `build.py` | Packages the code so Terraform can upload it | 1 minute the first time |
| `terraform init` | Downloads Terraform's AWS plugin. Only needed once | 1 minute |
| `terraform apply` | Shows what it will create, then asks. Type `yes` | 3 minutes |

### 4. Load the data and train the first model

The dataset is not in git. Download `HI-Small_Trans.csv` and `HI-Small_accounts.csv` from Kaggle ("IBM Transactions for Anti Money Laundering") and put them in the [data/](data/) folder at the repo root. Put `fx_rates.csv` (exchange rates) there too if a teammate has one; if not, the middle command below makes it.

```
python main/producer/producer.py prepare --csv data/HI-Small_Trans.csv
python main/producer/producer.py fx                    # only if you have no data/fx_rates.csv
python main/producer/producer.py upload --accounts data/HI-Small_accounts.csv --fx data/fx_rates.csv
python main/scripts/pipeline.py setup
```

| Command | What it does | How long |
|---|---|---|
| `prepare` | Splits the dataset: days before 8 Sep 2022 are history for training, later days are replayed live | 1 minute |
| `fx` | Downloads exchange rates for the dataset's dates (Frankfurter API: central-bank rates, Bitcoin from the Swiss tax administration) into `data/fx_rates.csv`. Days with no published rate take the last one before (`source` = `frankfurter-carried`) | 1 minute |
| `upload` | Sends the history, the customer list and the history days' exchange rates to AWS. Each live day's rates are sent by `replay` as that simulated day starts | 1 minute |
| `setup` | Trains the first model | 20 minutes |

Setup is done. You only repeat it after a `terraform destroy`.

## Run a demo

**1. Start the real-time scoring** (about 3 minutes):

```
python main/scripts/pipeline.py start
```

**2. Start sending payments.** Leave this running in its own terminal:

```
python main/producer/producer.py replay --labels --q2 async
```

**3. Open the two apps.** This prints their addresses:

```
python main/scripts/pipeline.py apps
```

```
  Q1 investigators  http://3.90.12.34:8501
  Q2 KYC analysts   http://3.90.12.34:8502
```

Use `http`, not `https`. Alerts appear in the Q1 app within a minute or two. The Q2 app fills in after the first simulated day ends (about 35 real minutes at the default speed).

**4. When you finish, stop the scoring.** It is the one thing that costs money for every hour it runs:

```
python main/scripts/pipeline.py stop
```

Useful while a demo runs:

| Do this | To |
|---|---|
| `Ctrl+C` in the producer terminal | Pause the demo. The clock in both apps pauses too |
| `producer.py replay --resume --labels --q2 async` | Carry on from where you paused |
| `pipeline.py status` | Check scoring is running and a model is loaded |

## Starting again

| | **Reset** | **Destroy** |
|---|---|---|
| What it does | Clears the demo's results. Keeps AWS, the history and the model | Deletes everything on AWS |
| Ready again in | 5 minutes | 25 minutes |
| Costs while you are away | About $0.90 a day | Nothing |
| Use it | Between demos in one sitting | When you are done for the day |

**Reset:**

```
python main/scripts/pipeline.py stop
python main/scripts/pipeline.py reset --yes
python main/scripts/pipeline.py start --fresh
```

**Destroy:**

```
cd main/infra
terraform destroy
cd ../..
```

To come back after a destroy, repeat [step 3](#3-build-everything-on-aws) (skip `terraform init`) and [step 4](#4-load-the-data-and-train-the-first-model) (skip `prepare`), then start a demo.

> **Before destroying,** check that no Q2 nightly run is in progress (Step Functions console, `q2-daily-kyc-scoring`). Destroying mid-run can leave its cluster running and billing. If that happens, terminate the cluster in the EMR console.

## Replay speed

The producer runs a **simulated clock** that is faster than the real one. `--speed N` means N times faster:

- 1 real second = N simulated seconds
- 1 simulated day takes 1440 ÷ N real minutes

| `--speed` | 1 real minute is | 1 simulated day takes | The 3 busy days take | Payments sent per second |
|---|---|---|---|---|
| 24 | 24 minutes | 1 hour | 3 hours | 28 |
| **60** (default) | **1 hour** | **24 minutes** | **72 minutes** | **71** |
| 120 | 2 hours | 12 minutes | 36 minutes | 142 |
| 288 | 4.8 hours | 5 minutes | 15 minutes | 341 |
| 720 | 12 hours | 2 minutes | 6 minutes | 853 |
| 1440 | 1 day | 1 minute | 3 minutes | 1,705 |

```
python main/producer/producer.py replay --speed 120 --labels --q2 async
python main/producer/producer.py replay --day-minutes 5 --labels --q2 async     # or say how long a day should take
python main/producer/producer.py replay --dry-run --speed 300                   # preview a speed; sends nothing
```

Three things to know:

- **Both apps follow the producer's clock.** The producer saves its clock to AWS, and the apps read it. So "expires in 6 days" on an alert counts simulated days.
- **The Q2 nightly job takes about 12 real minutes**, whatever the speed. At high speeds the Q2 app lags several simulated days behind. Use `--q2 wait` in place of `--q2 async` to pause the clock while the job runs.
- **Above speed 800 you need more capacity.** Set `kinesis_shards = 2` (see [Settings](#settings)).

The figures are for HI-Small, which has three busy days (8 to 10 Sep) and then almost nothing.

## Stress test

Sends made-up payments at a rate you choose, to see how fast the pipeline can score. Scoring must be running (`pipeline.py start`).

```
python main/producer/stress.py run --rate 20 --duration 120            # 20 a second for 2 minutes
python main/producer/stress.py run --stages 20:60,50:60,100:60         # step the rate up, 60 seconds each
python main/producer/stress.py report --run-id <id>                    # wait a minute after the run
python main/producer/stress.py cleanup --run-id <id>
```

`run` prints the run id. `report` shows, minute by minute, how long payments took to score: the typical time (p50) and the slowest 1% (p99), in milliseconds.

- **p99 stays flat as the rate goes up:** the pipeline has room to spare.
- **p99 keeps climbing:** it cannot keep up at that rate. Raise `flink_parallelism` (see [Settings](#settings)).

Stress payments never show up in either app.

## Settings

There are two places: `settings.json` for which bank and which dates, and `terraform.tfvars` for the AWS side.

### settings.json: which bank and which dates

Open [main/common/settings.json](main/common/settings.json) in any editor. It has two values you can change:

```json
{
  "our_bank": "11",
  "live_start": "2022-09-08"
}
```

| Setting | Meaning | How to write it |
|---|---|---|
| `our_bank` | The bank whose view the system takes. The producer sends every bank's payments, and the pipelines keep only this bank's | The bank's ID in quotes, without leading zeros: `"11"`, not `"011"` |
| `live_start` | The dividing date. Payments before it are history that trains the model. Payments from it onwards are replayed live | A date in quotes, as `"YYYY-MM-DD"` |

Keep the quotes and the comma, and leave the `_about` line alone. Save the file, then run the commands for what you changed.

**If you have not built anything yet,** edit the file before [step 3](#3-build-everything-on-aws) and carry on as normal. Nothing else is needed.

**If the system is already built and you changed `our_bank`:**

```
cd main/infra
terraform apply
cd ../..
python main/scripts/pipeline.py stop
python main/scripts/pipeline.py reset --yes
python main/scripts/pipeline.py setup
python main/scripts/pipeline.py start --fresh
```

**If the system is already built and you changed `live_start`,** the dataset has to be split again first, so add `prepare` and `upload`:

```
python main/producer/producer.py prepare --csv data/HI-Small_Trans.csv
python main/producer/producer.py upload --accounts data/HI-Small_accounts.csv --fx data/fx_rates.csv
cd main/infra
terraform apply
cd ../..
python main/scripts/pipeline.py stop
python main/scripts/pipeline.py reset --yes
python main/scripts/pipeline.py setup
python main/scripts/pipeline.py start --fresh
```

Both take about 25 minutes, because `setup` trains the model again: a model is built for one bank and one stretch of history.

Not sure which date to use? `python main/producer/producer.py dates --csv <file>` shows the dates a dataset covers and suggests a `live_start`. `prepare` refuses a date that leaves no history or nothing to replay.

### terraform.tfvars: sizes and switches

In `main/infra/`, copy `terraform.tfvars.example` to `terraform.tfvars`, edit it, and run `terraform apply`.

| Setting | Meaning | Default |
|---|---|---|
| `kinesis_shards` | Capacity of the payment stream: 1,000 payments a second each | 1 |
| `flink_parallelism` | How many payments can be scored at once. Each step adds about $0.11 an hour | 1 |
| `apps_on_ec2` | Host the two apps on AWS | true |
| `apps_allowed_cidr` | Who can open the apps | anyone |

The full list, with explanations, is in `main/infra/variables.tf`.

## The two apps

Terraform puts both apps on one small AWS server, so there is nothing to run on your laptop.

- **Anyone with the address can open them.** They have no login. The data is synthetic, but stop the server when you are not demoing: `pipeline.py apps stop`, and later `pipeline.py apps start`.
- **The address changes** whenever the server restarts, which happens with each lab session. `pipeline.py apps` prints the current one.
- **To change an app,** edit its file and run `terraform apply`. The new version is live in about 4 minutes.

To run an app on your laptop instead, which is quicker when you are changing how it looks:

```
cd main/Q1_files/streamlit
set Q1_MODE=aws                        (Mac/Linux: export Q1_MODE=aws)
streamlit run q1_streamlit.py
```

For Q2, use `main/Q2_files/streamlit`, `Q2_MODE` and `q2_streamlit.py --server.port 8502`.

To try an app with no AWS at all, copy `zArchive/Q1_files/v2/demo_snapshot.json` (or `zArchive/Q2_files/v2/streamlit/demo_snapshot.json`) into the app's folder and run it without setting the mode.

## Using a different dataset

The other IBM datasets cover different dates, so check before you prepare one:

```
python main/producer/producer.py dates --csv data/HI-Large_Trans.csv
```

It prints the dates covered and suggests a `live_start`. Then:

1. Set `live_start` in `settings.json`.
2. Run `prepare` on the new file.
3. Fetch exchange rates for the new dates. A rates file made for HI-Small only covers September 2022. This replaces `data/fx_rates.csv`:
   ```
   python main/producer/producer.py fx
   ```
4. Run `upload` with the new accounts file and `--fx data/fx_rates.csv`, then `terraform apply` and `pipeline.py setup`.

The larger datasets have far more payments a day. Run `replay --dry-run` to see the rate before choosing a speed.

## Switching AWS accounts

1. Run `terraform destroy` while still on the old account.
2. Paste the new account's credentials ([step 2](#2-connect-to-your-aws-lab-account)).
3. Build again from [step 3](#3-build-everything-on-aws).

If you already switched without destroying, `terraform apply` will fail. Delete `main/infra/terraform.tfstate` and `terraform.tfstate.backup`, then apply again. Those files are Terraform's record of what it built, and they still describe the old account.

## What it costs

Rough figures. Three things cost money while idle; the rest only when used.

| What | Cost |
|---|---|
| Real-time scoring (Flink), while running | $0.22 an hour. **Stop it after each demo** |
| The server for the two apps | $0.50 a day |
| The payment stream (Kinesis) | $0.36 a day |
| One Q2 nightly run | $0.25 |
| One model training run | $0.15 |
| Everything else | cents |

A demo of the three busy days starts three Q2 runs, about $0.75.

## Problems

| What you see | What to do |
|---|---|
| "security token ... is invalid" or "expired" | The lab session ended. Redo [step 2](#2-connect-to-your-aws-lab-account) |
| `terraform apply` says something "already exists" | A resource with that name was made by hand earlier, outside Terraform. Delete the old one in the AWS console, or give ours a different name in `main/infra/` |
| `terraform apply` says a file in `main/build/` is missing | Run `python main/scripts/build.py` first |
| `terraform apply` fails on `aws_kinesisanalyticsv2_application` | The lab may not allow Managed Flink. Comment out that resource in `q1_streaming.tf` and the `flink_app` line in `outputs.tf`, apply again, and run `python main/Q1_files/flink/local_runner.py` in place of `pipeline.py start` |
| An app's page does not load | Get the current address from `pipeline.py apps`. Use `http`. Wait 3 minutes if the server just started |
| No alerts appear | `pipeline.py status` should show scoring RUNNING and an approved model. The log is in CloudWatch under `/aws/managed-flink/q1-realtime-flagging` |
| The Q2 app says no nightly run has finished | The first simulated day has not ended yet, or the producer was started without `--q2 async` |
| The Q2 run fails with "no rates for ..." | The exchange rates do not cover those dates. See [Using a different dataset](#using-a-different-dataset), step 3 |
| The Q2 run fails at the EMR step | The error is in `s3://<bucket>/logs/emr/`. The lab allows only 9 servers at once |

## Reference

### All commands

| Command | What it does |
|---|---|
| `pipeline.py setup` | Prepare the history and train the first model |
| `pipeline.py start` / `stop` | Start or stop real-time scoring. `start --fresh` starts with empty memory |
| `pipeline.py status` | Is scoring running, and which model is in use |
| `pipeline.py apps` / `apps stop` / `apps start` | Show the apps' addresses; stop or start their server |
| `pipeline.py reset --yes` | Clear the demo's results |
| `pipeline.py retrain` | Train a new model and switch to it if it is better |
| `pipeline.py q2 --run-date 2022-09-08` | Run the Q2 nightly job for one day by hand |
| `pipeline.py labels` | Save investigators' finished decisions for the next training run |
| `producer.py prepare` / `upload` / `replay` | Split the dataset, send it to AWS, replay it |
| `producer.py dates` / `fx` | Show a dataset's dates; fetch exchange rates |
| `producer.py clock show` / `pause` / `resume` | Look at or control the simulated clock |
| `stress.py run` / `report` / `cleanup` | The stress test |

`pipeline.py` is in `main/scripts/`. `producer.py` and `stress.py` are in `main/producer/`. Add `--help` to any of them.

In `replay`, `--labels` and `--q2 async` start the two daily jobs at each simulated midnight. Without them, those jobs do not run.

### Folders

| Path | What it is |
|---|---|
| `PIPELINE_SPEC.md` | The design. Section 8 lists how it was built and where that differs |
| `main/` | The current code |
| `data/` | Where you put the downloaded dataset files. Not committed |
| `zArchive/` | The first version. For reference only |

Inside `main/`:

| Path | What it is |
|---|---|
| `infra/` | The Terraform files. One per area: storage, Q1 streaming, Q1 training, Q2, the apps |
| `scripts/` | `build.py` and `pipeline.py` |
| `producer/` | `producer.py` and `stress.py` |
| `common/` | Only what both Q1 and Q2 use: the rules for a valid payment, the simulated clock, `settings.json` |
| `Q1_files/` | Real-time scoring (`flink/`), model training (`glue/`, `sagemaker/`), the investigators' app (`streamlit/`) |
| `Q2_files/` | Data preparation (`glue/`), the nightly rules and ranking (`emr/`), the KYC app (`streamlit/`) |
| `tests/` | Checks that run on your laptop without AWS |

Three things appear when you run the commands and are not committed: `main/build/` (packaged code), `main/config.json` (the names of what Terraform built) and `main/producer/data/` (the prepared dataset).

### Tests

These run on your laptop without AWS. Run `producer.py prepare` first.

```
pip install lightgbm scikit-learn
python main/tests/test_core.py
python main/tests/test_q1_local.py
python main/tests/test_producer.py
```

They cover the scoring code, model training, and the two producers. The parts that run on AWS have no local test.
