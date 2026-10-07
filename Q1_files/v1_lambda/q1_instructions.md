Files you need
File
Where it goes
producer.py, HI-Small_Trans.csv
Your laptop, in a Q1 folder
glue_q1.py
Pasted into the Glue job
sagemaker/train.tar.gz, sagemaker/evaluate.py
S3 q1/code/
lambdas/score_lambda.py, lambdas/promote_lambda.py
Pasted into two Lambda functions
state_machine_q1.json
Pasted into Step Functions
q1_streamlit.py
S3 q1/code/, then copied onto EC2

simulator_lambda.py isn't used for now; you make the decisions yourself. sagemaker/train.py and sagemaker/requirements.txt are already inside train.tar.gz, so you don't upload them separately.
Part 1: Laptop setup (one time)
Make a separate Q1 folder, so the producer's checkpoint doesn't clash with Q2's. Put producer.py and HI-Small_Trans.csv in it.
Open a terminal in that folder, then create a Python environment and install the requirements:
   python -m venv venv
   venv\Scripts\activate          (Windows)
   source venv/bin/activate       (Mac/Linux)
   pip install pandas boto3
Prepare daily files for all banks:
   python producer.py prepare --csv HI-Small_Trans.csv --bank-id all --out-dir prep_all
This prints the transactions per day. Check that 2022-09-10 is listed and that there are days after it. Days before it are the training history, and days from it onwards are streamed live. If you pick a different start date, use it everywhere these instructions say 2022-09-10.
 4. Check the files without sending anything:
   python producer.py replay --prepared-dir prep_all --start-date 2022-09-10 --ignore-checkpoint --dry-run --max-days 1
Part 2: AWS credentials (every lab session)
Start the Learner Lab, click AWS Details, then Show next to AWS CLI.
Copy the whole block into this file, replacing whatever is there:
Windows: C:\Users\<you>\.aws\credentials
Mac/Linux: ~/.aws/credentials
Create the .aws folder if it doesn't exist. The first line must be [default]. These credentials expire when the lab session ends, so repeat this step every session.
Note your account ID. Open IAM → Roles → LabRole. The ARN looks like arn:aws:iam::123456789012:role/LabRole, and the 12 digits are your account ID.
Part 3: S3
Use your existing bucket from Q2. If you don't have one, open S3 → Create bucket, name it e.g. is459-g2-aml-<yourname>, keep the defaults and create it.
In the bucket, create the folders q1/history/ and q1/code/.
Upload HI-Small_Trans.csv to q1/history/. This is the labelled history; the Glue job only uses dates before 2022-09-10.
Upload train.tar.gz, evaluate.py and q1_streamlit.py to q1/code/.
Part 4: DynamoDB
Open DynamoDB → Tables → Create table. Make these three tables, and for each one, under Table settings, choose Customize settings and set Capacity mode to On-demand. The default capacity is too small for seeding.
Table name
Partition key
Sort key
q1-events
acct (String)
sk (String)
q1-state
pk (String)
none
q1-alerts
txn_id (String)
none




Wait until all three show Active.
Part 5: Kinesis Data Stream and Firehose
If aml-transactions and aml-to-s3 from Q2 still exist, skip to Part 6.
Otherwise, open Kinesis → Data streams → Create data stream. Name it aml-transactions, choose Provisioned with 1 shard, and create it. Wait until it shows Active.
Open Amazon Data Firehose → Create Firehose stream. Set Source to Amazon Kinesis Data Streams (choose aml-transactions) and Destination to Amazon S3. Name it aml-to-s3.
Under destination settings:
Choose your bucket.
New line delimiter: Enabled
Dynamic partitioning: Enabled
Inline parsing for JSON: Enabled
Add a key named txn_date with JQ expression .txn_date
S3 bucket prefix: raw/date=!{partitionKeyFromQuery:txn_date}/
Error output prefix: raw-errors/!{firehose:error-output-type}/
Set the buffer interval to the lowest value allowed.
Under Advanced settings → Service access, choose LabRole. Create the stream.
Part 6: Glue job
Open Glue → ETL jobs → Script editor, choose Spark, click Create script, and replace everything with glue_q1.py.
Under Job details:
Name: q1-features
IAM role: LabRole
Glue version: 4.0
Worker type: G.1X, with 2 workers
Under Advanced → Job parameters, add:
Key
Value
--mode
setup
--bucket
your bucket name (without s3://)
--bank_id
11
--live_start
2022-09-10
--review_delay_hours
12
--eval_days
2
--region
us-east-1
--events_table
q1-events
--state_table
q1-state
--alerts_table
q1-alerts




Save. Don't run it on its own; the Step Function runs it in Part 8.
Part 7: Promote Lambda
Open Lambda → Create function:
Name: q1-promote
Runtime: Python 3.12
Under Change default execution role, choose Use an existing role → LabRole
In the code editor, replace everything in lambda_function.py with promote_lambda.py, then click Deploy.
Under Runtime settings → Edit, set Handler to lambda_function.handler.
Under Configuration → General configuration, set Timeout to 1 min.
Part 8: Step Functions and the first model
Open state_machine_q1.json and replace every <BUCKET> with your bucket name and every <ACCOUNT_ID> with the 12 digits from step 7.
Open Step Functions → Create state machine → Blank, switch to Code, and paste the JSON. Under Config, set Name to q1-pipeline and Execution role to LabRole. Save.
Click Start execution with this input:
    {"mode": "setup"}

It takes about 10–15 minutes: Glue, then SageMaker training (which spends a few minutes starting its instance), then Promote.
30. When every step is green, check:
 - q1/models/best/ contains model.txt, model.json, meta.json and parity.json.
 - q1-state and q1-events have items (DynamoDB → Explore items).
 - The Glue run log has lines starting with [q1], showing row counts per split and "seeded … events".
Part 9: Scoring Lambda
Create another function:
Name: q1-score
Runtime: Python 3.12
Execution role: LabRole
Replace lambda_function.py with score_lambda.py and click Deploy. Set Handler to lambda_function.handler.
Under General configuration, set Memory to 512 MB and Timeout to 5 min.
Under Configuration → Environment variables, add:
Key
Value
BUCKET
your bucket name
BANK_ID
11
EVENTS_TABLE
q1-events
STATE_TABLE
q1-state
ALERTS_TABLE
q1-alerts




Click Add trigger → Kinesis → aml-transactions, with Batch size 100 and Starting position Latest. Leave everything else as default.
 Use Latest, so it only reads what you stream from now on and not old Q2 records still in the stream.
Part 10: Streamlit on EC2
Open EC2 → Launch instance:
Name: q1-streamlit
AMI: Amazon Linux 2023
Instance type: t3.small
Key pair: vockey
Under Network settings, click Edit and keep the SSH rule. Click Add security group rule and set Type to Custom TCP, Port 8501, Source My IP.
Under Advanced details, set IAM instance profile to LabInstanceProfile. This is what lets the app read DynamoDB and S3 without credentials. Launch the instance.
When it's Running, select it → Connect → EC2 Instance Connect → Connect. In the terminal:
    sudo dnf install -y python3-pip
    pip3 install --user streamlit boto3 pandas
    aws s3 cp s3://<BUCKET>/q1/code/q1_streamlit.py .
    export S3_BUCKET=<BUCKET> BANK_ID=11 AWS_REGION=us-east-1
    nohup python3 -m streamlit run q1_streamlit.py --server.address 0.0.0.0 --server.port 8501 > streamlit.log 2>&1 &
Copy the instance's Public IPv4 address and open http://<public-ip>:8501 in your browser. Use http, not https. You should see "No alerts yet".
Enter your name in the sidebar. It's recorded with every decision.
If the page doesn't load, run cat streamlit.log in the terminal and check that the port 8501 rule from step 37 is there.
To check the layout without AWS first, run pip install streamlit, then DEMO_MODE=1 streamlit run q1_streamlit.py on your laptop. On Windows, run set DEMO_MODE=1 first. Fake alerts arrive over about 10 minutes.
Part 11: Stream and review alerts live
On your laptop, in the Q1 folder with the venv active, stream the first live day:
    python producer.py replay --prepared-dir prep_all --start-date 2022-09-10 --max-days 1 --ignore-checkpoint --flush-wait 5
Watch the Live queue tab. Alerts should start appearing within a minute or two, ranked by priority, with the newest marked NEW. Simulated time moves forward as payments arrive.
On a few alerts, add a note if you like, then click Confirm laundering or Dismiss. They leave the queue and appear on the Decisions tab, where you can select a row to change or reopen them.
If no alerts appear after about 3 minutes, open q1-score → Monitor → View CloudWatch logs. Each batch logs a line like [score] records=100 parsed=100 ours=3 alerts=0, and the first run also shows loaded model … and a parity check line, which should be near 0. Send me what you see.
To stream the following days, run the same command without --max-days 1 and without --ignore-checkpoint. It continues from the checkpoint.
Part 12 (optional): Retrain
After a few days have streamed and you've decided some alerts, start a new q1-pipeline execution with:
    {"mode": "retrain"}

The steps run Glue → Train → Evaluate → Promote.
49. Check the Model tab: click Refresh model info in the sidebar. It shows the comparison and whether the new version was promoted. You can also open q1/evaluation/<execution name>/result.json.
 Only alerts you decided count as live labels, and alerts from the last 12 simulated hours are left out because they may not be reviewed yet. With only a few confirmed cases, the "recent reviewed days" comparison is skipped and the fixed benchmark decides. If a new model is promoted, q1-score switches to it within a minute, and new alerts show the new version.

