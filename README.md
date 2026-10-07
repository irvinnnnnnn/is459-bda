# is459-bda

IS459 Big Data Architecture, Group 2: an AML pipeline on AWS using the IBM AML dataset.

- **Q1**: flag likely laundering payments in real time, for investigators to review.
- **Q2**: rank customers for KYC to review for enhanced due diligence (EDD).

## Where things are

| Path | What it is |
|---|---|
| [PIPELINE_SPEC.md](PIPELINE_SPEC.md) | **The current design for both pipelines.** This is the source of truth. Update it whenever something changes, so the code and the spec don't drift apart. |
| `Q1_files/v2/`, `Q2_files/v2/` | **The current iteration.** Everything new goes here, and it follows the spec. |
| `Q1_files/v1_lambda/`, `Q2_files/v1/` | The first version, which is what is currently built in the AWS labs. Kept for reference; don't build on it. |


## Run the dashboards

Both dashboards run on your laptop with no AWS account. They replay demo data that was built from the real HI-Small dataset.

One-time setup:

```
python -m venv .venv
.venv\Scripts\activate        (Mac/Linux: source .venv/bin/activate; Git Bash: source .venv/Scripts/activate)
pip install streamlit pandas
```

Q1, the investigator app:

```
cd Q1_files/v2/streamlit
streamlit run q1_streamlit.py
```

Q2, the KYC app:

```
cd Q2_files/v2/streamlit
streamlit run q2_streamlit.py
```

Run each command from its own folder. The dark theme is set in `.streamlit/config.toml` in that folder, and Streamlit only picks it up from the folder you run in.

The app opens at http://localhost:8501. To run both at once, add `--server.port 8502` to the second one.

## Change how a dashboard looks

1. Start the app as above and leave it running.
2. Edit `q1_streamlit.py` or `q2_streamlit.py` and save.
3. In the browser, click **Rerun** at the top right (or choose **Always rerun** once). Your change shows straight away.

Where to look in each file:

- **Colours, spacing, table and card styles**: the `CSS` block near the middle of the file.
- **Theme colours** (background, text, buttons): `.streamlit/config.toml`.
- **Page content**: one function per page. In Q1: `worklist` (the queue), `alert_page`, `reviewed`, `metrics`. In Q2: `page_queue`, `customer_page`, `page_reviewed`, `page_overview`.
- **Wording of Q1 risk factors**: the `phrase` function, one sentence per model feature.

Saving the file resets the demo, so any decisions you made in the app are cleared. That is expected.

## Demo data

Each app reads `demo_snapshot.json` in its folder. You only need to rebuild it if you change the scoring or the rules, not for layout changes.

```
python build_demo_snapshot.py
```

- **Q1** needs `pip install lightgbm`. It trains the model and scores bank 11's payments with the `v1` feature code.
- **Q2** applies the spec's rules R1 to R11 to bank 11's customers for four nightly runs.

Both scripts expect the prepared daily files (`prep_all/`, made by `producer.py prepare`) and `HI-Small_accounts.csv` somewhere in the repo. These are large and are not committed.

## What is and isn't built

- **Built**: both dashboards in demo mode, and the scripts that build their demo data.
- **Not built yet**: the `v2` Flink job (Q1) and the `v2` EMR scoring job (Q2). The demo build scripts are a working reference for their logic.
- **Not connected**: the dashboards' AWS mode. Q1 has the DynamoDB and Athena calls written but never run against real tables. Q2 has none yet.
