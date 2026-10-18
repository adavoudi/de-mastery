# 💲 Optional, ungraded: run SPK-07 at 50 GB on EMR Serverless

> This is **optional and not graded**. It is the only part of the platform that can cost real money.
> So it is opt-in, and every step is written out. **Nothing here calls AWS for you.** You run the
> commands yourself after you read the estimate. You run the teardown when you finish.

## What it does

It runs the SPK-07 partition-sizing job on ~50 GB of generated commerce data on **EMR Serverless**. You
can observe real shuffle behavior, file sizes, and cost at a scale that your 32 GB laptop cannot reach.

## Cost estimate (read before running)

EMR Serverless charges per **vCPU-hour** and per **GB-hour** of workers while the job runs. It bills per second, with a 1-minute minimum. It also bills
S3 storage and requests. This is a rough order of magnitude for a ~50 GB job with many shuffles
(us-east-1 list prices). Prices change, so check the current
[EMR Serverless pricing page](https://aws.amazon.com/emr/pricing/) before you run:

| Item | Rough basis | Order-of-magnitude |
|---|---|---|
| Compute (vCPU + memory) | tens of vCPU-hours + GB-hours (many workers for ~5–15 min) | **~$1–3** |
| S3 storage (50 GB, short-lived) | GB-month prorated to hours | **< $0.10** |
| S3 requests | PUT/GET during the run | **< $0.10** |

Expect **a few US dollars** for a single run if you tear everything down promptly. This is an
estimate, not a quote. **Check the official pricing calculator for your region and date.** The platform
does not estimate your actual bill. Use the AWS Pricing Calculator.

## Required tagging

Tag **every** resource that you create with `project=de-mastery`, so that you can attribute the spend:

```bash
--tags project=de-mastery
```

The example commands below already include it.

## Run it (you execute these; the platform does not)

Prerequisites:
- the AWS CLI, configured with a profile that you control
- an S3 bucket that you own
- an IAM job-execution role for EMR Serverless

First, set these environment variables (the repo stores no secrets):

```bash
export DE_AWS_PROFILE=your-profile
export DE_AWS_REGION=us-east-1
export DE_S3_BUCKET=your-bucket
export DE_EMR_ROLE_ARN=arn:aws:iam::<acct>:role/your-emr-serverless-role
```

1. Generate ~50 GB of data locally (scale ≈ 50) and upload it to S3. You can also generate it directly in S3.
2. Package the SPK-07 job (`entryPoint` = the `main.py` of your job) and upload it to
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

Right-sizing: start with modest driver/executor sizes and `spark.dynamicAllocation.enabled=true`.
400 shuffle partitions gives post-shuffle files of ~128 MB for 50 GB. Adjust the settings and measure again.

## Tear it down (always)

When the run finishes, stop paying immediately:

```bash
bash teardown.sh "$APP_ID"
```

`teardown.sh` cancels any running job, waits until the application is STOPPED, and deletes it. It
does **not** touch your data bucket (you decide when to delete the 50 GB). It never runs
automatically.
