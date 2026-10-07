Part 1: Laptop setup (one time)
Put these in one project folder on your laptop:
producer.py and requirements.txt
HI-Small_Trans.csv, HI-Small_accounts.csv, HI-Small_Patterns.txt, fx_rates.csv
Open a terminal in that folder, then create a Python environment and install the requirements:
   python -m venv venv
   venv\Scripts\activate          (Windows)
   source venv/bin/activate       (Mac/Linux)
   pip install -r requirements.txt
Prepare bank 12's daily files:
   python producer.py prepare --csv HI-Small_Trans.csv --bank-id 12 --out-dir prep12
This creates a prep12 folder with one file per day and prints the daily counts.
 4. Check the files without sending anything:
   python producer.py replay --prepared-dir prep12 --dry-run --max-days 1
Part 2: AWS credentials on your laptop (every lab session)
Start the Learner Lab, click AWS Details, then Show next to AWS CLI.
Copy the whole block (aws_access_key_id, aws_secret_access_key, aws_session_token) into this file, replacing whatever is there:
Windows: C:\Users\<you>\.aws\credentials
Mac/Linux: ~/.aws/credentials
Create the .aws folder if it doesn't exist. The first line must be [default]. These credentials expire when the lab session ends, so repeat this step every session.
Part 3: S3
Open S3 → Create bucket. Name it e.g. is459-g2-aml-<yourname>, keep the defaults and create it.
Open the bucket and use Create folder to make: reference, raw, curated, results, state, scripts, emr-logs, athena-results.
Inside reference, create the folders accounts, fx and patterns, then upload:
HI-Small_accounts.csv to reference/accounts/
fx_rates.csv to reference/fx/
HI-Small_Patterns.txt to reference/patterns/
Upload score.py and validate.py to scripts/.
Part 4: Kinesis Data Stream
Open Kinesis → Data streams → Create data stream.
Name it aml-transactions, choose Provisioned with 1 shard, and create it.
Wait until the status shows Active.
Part 5: Firehose
Open Amazon Data Firehose → Create Firehose stream.
Set Source to Amazon Kinesis Data Streams (choose aml-transactions) and Destination to Amazon S3.
Name it aml-to-s3.
Under destination settings:
Choose your bucket.
New line delimiter: Enabled.
Dynamic partitioning: Enabled.
Inline parsing for JSON: Enabled.
Add a key named txn_date with JQ expression .txn_date.
S3 bucket prefix: raw/date=!{partitionKeyFromQuery:txn_date}/
Error output prefix: raw-errors/!{firehose:error-output-type}/
Set the buffer interval to the lowest value allowed and note the number.
Under Advanced settings → Service access, choose LabRole.
Create the stream.
Part 6: First streaming test
From your laptop, send one day:
    python producer.py replay --prepared-dir prep12 --max-days 1 --flush-wait 120

Set `--flush-wait` a bit above the buffer interval you noted in step 18.
22. In S3, check that files appear under raw/date=2022-09-01/.
 23. If nothing appears after about 5 minutes, check the raw-errors/ folder and the Monitoring tab of both streams, then tell me what you see.
The producer remembers the last day it sent in .producer_checkpoint.json. The next replay continues from Sept 2.
Part 7: Glue ETL job
Open Glue → Databases → Add database and name it aml_q2.
Open Glue → ETL jobs → Script editor, choose Spark, click Create script, and replace everything with glue_etl.py.
Under Job details:
Name: aml-q2-etl
IAM role: LabRole
Glue version: 4.0
Worker type: G.1X with 2 workers
Under Advanced → Job parameters, add:
--run_date = 2022-09-01
--bucket = your bucket name (just the name, without s3://)
Save, then click Run.
When it succeeds, check that Parquet files appear in curated/date=2022-09-01/. The run log shows a line starting with [etl] with row counts.
Part 8: Load a full week and test EMR by hand
Stream days 2–7 from your laptop:
    python producer.py replay --prepared-dir prep12 --end-date 2022-09-07 --flush-wait 120
Run the Glue job once for each of those dates by changing --run_date to 2022-09-02, then -03, and so on up to -07.
Create an EMR cluster the same way as in Week 6:
Spark
1 primary and 1 core node, both m5.xlarge
Week 6 roles
Log URI s3://<bucket>/emr-logs/
Note the release version and role names it uses.
When the cluster is Waiting, go to Steps → Add step:
Type: Custom JAR
JAR location: command-runner.jar
Arguments:
      spark-submit --deploy-mode cluster s3://<bucket>/scripts/score.py --bucket <bucket> --run-date 2022-09-07 --bank-id 12 --window-days 7
When it completes, add another step with the same arguments, replacing score.py with validate.py.
Check that results/ now contains customer_scores, rule_hits and the validation_ folders.
Terminate the cluster.
Part 9: Step Functions
Open state_machine.json and replace every <BUCKET> with your bucket name. If your Week 6 cluster used a different release version or role names, change those too.
Open Step Functions → Create state machine → Blank, switch to Code, and paste the JSON.
Under Config:
Name: aml-q2-daily
Execution role: LabRole
Save it.
Test it: click Start execution with input {"run_date": "2022-09-07"} and watch every step turn green. The EMR cluster takes several minutes to start.
Copy the state machine ARN from its details page.
Part 10: Run the full flow from your laptop
Run:
    python producer.py replay --prepared-dir prep12 --start-date 2022-09-08 --sfn-arn <ARN> --wait-for-sfn --flush-wait 120
For each day, the producer streams the transactions, waits for Firehose, starts the daily Step Functions run, and waits for it to finish before moving to the next day. Watch the runs in the Step Functions console.
Part 11: Athena
Open Athena → Query editor → Settings → Manage and set the result location to s3://<bucket>/athena-results/.
Open athena.sql and replace every <BUCKET>.
Run each statement one at a time with database aml_q2 selected.
Test with:
    SELECT * FROM aml_q2.v_review_queue ORDER BY score DESC LIMIT 20
Part 12: Streamlit
Generate the app using the Streamlit prompt, with the addition from my last message.
Run it on your laptop with DEMO_MODE=1 first to check the layout.
Then set the bucket, database and bank environment variables and run it against Athena. It uses the same ~/.aws/credentials.
Starting over
Delete the contents of raw/, curated/, results/ and state/ in S3, and delete .producer_checkpoint.json on your laptop.
After every session
Make sure no EMR cluster is running.
Delete the Kinesis stream if you won't use it again soon. It bills every hour it exists.
Check the lab budget.

