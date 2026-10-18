# 💲 Optional, ungraded: run SPK-07 at 50 GB on EMR Serverless

> This is **optional and not graded**. It is the only part of the platform that can cost real money,
> so it is opt-in and spelled out end to end. **Nothing here calls AWS for you** — you run the
> commands yourself after reading the estimate, and you run the teardown when done.

## What it does

Runs the SPK-07 partition-sizing job on ~50 GB of generated commerce data on **EMR Serverless**, so
you can observe real shuffle behavior, file sizes, and cost at a scale your 32 GB laptop can't reach.

## Cost estimate (read before running)

EMR Serverless bills per **vCPU-second** and **GB-hour** of workers while the job runs, plus S3
storage/requests. Rough order-of-magnitude for a ~50 GB shuffle-heavy job (us-east-1 list prices,
re-check the current [EMR Serverless pricing page](https://aws.amazon.com/emr/pricing/) before you
run — prices change):

| Item | Rough basis | Order-of-magnitude |
|---|---|---|
| Compute (vCPU + memory) | a few hundred vCPU-seconds + GB-seconds for a ~5–15 min job | **~$1–3** |
| S3 storage (50 GB, short-lived) | GB-month prorated to hours | **< $0.10** |
| S3 requests | PUT/GET during the run | **< $0.10** |

Expect **a few US dollars** for a single run if you tear everything down promptly. This is an
estimate, not a quote — **check the official pricing calculator for your region/date.** The platform
does not estimate your actual bill; use the AWS Pricing Calculator.

## Required tagging

Tag **every** resource you create with `project=de-mastery` so the spend is attributable:

```bash
--tags project=de-mastery
```

The example commands below already include it.

## Run it (you execute these; the platform does not)

Prerequisites: the AWS CLI configured with a profile you control, an S3 bucket you own, and an IAM
job-execution role for EMR Serverless. Set these environment variables first (no secrets are stored
in the repo):

```bash
export DE_AWS_PROFILE=your-profile
export DE_AWS_REGION=us-east-1
export DE_S3_BUCKET=your-bucket
export DE_EMR_ROLE_ARN=arn:aws:iam::<acct>:role/your-emr-serverless-role
```

1. Generate ~50 GB of data locally (scale ≈ 50) and upload to S3, or generate directly in S3.
2. Package the SPK-07 job (`entryPoint` = your job's `main.py`) and upload it to
   `s3://$DE_S3_BUCKET/jobs/`.
3. Create a right-sized application and submit the job:

```bash
aws emr-serverless create-application \
  --release-label emr-7.2.0 --type SPARK \
  --name de-mastery-spk07 \
  --tags project=de-mastery \
  --profile "$DE_AWS_PROFILE" --region "$DE_AWS_REGION"

# capture the application-id, then:
aws emr-serverless start-job-run \
  --application-id "$APP_ID" \
  --execution-role-arn "$DE_EMR_ROLE_ARN" \
  --tags project=de-mastery \
  --job-driver '{"sparkSubmit":{"entryPoint":"s3://'"$DE_S3_BUCKET"'/jobs/main.py",
     "sparkSubmitParameters":"--conf spark.sql.shuffle.partitions=400 --conf spark.sql.adaptive.enabled=true"}}' \
  --profile "$DE_AWS_PROFILE" --region "$DE_AWS_REGION"
```

Right-sizing: start with modest driver/executor sizes and `spark.dynamicAllocation.enabled=true`;
400 shuffle partitions targets ~128 MB post-shuffle files for 50 GB. Adjust and re-measure.

## Tear it down (always)

When the run finishes, stop paying immediately:

```bash
bash teardown.sh "$APP_ID"
```

`teardown.sh` cancels any running job, waits for the application to be STOPPED, and deletes it. It
does **not** touch your data bucket (you decide when to delete the 50 GB) and it never runs
automatically.
