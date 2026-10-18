#!/usr/bin/env bash
# Optional, ungraded teardown for the 💲 EMR Serverless run (SPK-12/02).
#
# You invoke this yourself after the benchmark. It cancels any running job, waits for
# the application to stop, and DELETES the EMR Serverless application so it stops
# accruing charges. It does NOT delete your S3 data (you decide that) and it is NEVER
# run by the test suite or the platform.
#
# Usage:
#   export DE_AWS_PROFILE=your-profile DE_AWS_REGION=us-east-1
#   bash teardown.sh <application-id>
set -euo pipefail

APP_ID="${1:-}"
PROFILE="${DE_AWS_PROFILE:?set DE_AWS_PROFILE}"
REGION="${DE_AWS_REGION:?set DE_AWS_REGION}"

if [[ -z "$APP_ID" ]]; then
  echo "usage: bash teardown.sh <application-id>" >&2
  exit 2
fi

echo "Cancelling any running job runs for application $APP_ID ..."
RUNS=$(aws emr-serverless list-job-runs --application-id "$APP_ID" \
         --states RUNNING SCHEDULED PENDING SUBMITTED \
         --query 'jobRuns[].id' --output text \
         --profile "$PROFILE" --region "$REGION" 2>/dev/null || true)
for r in $RUNS; do
  echo "  cancel job run $r"
  aws emr-serverless cancel-job-run --application-id "$APP_ID" --job-run-id "$r" \
    --profile "$PROFILE" --region "$REGION" || true
done

echo "Stopping application $APP_ID ..."
aws emr-serverless stop-application --application-id "$APP_ID" \
  --profile "$PROFILE" --region "$REGION" || true

echo "Waiting for application to reach STOPPED ..."
for _ in $(seq 1 30); do
  STATE=$(aws emr-serverless get-application --application-id "$APP_ID" \
            --query 'application.state' --output text \
            --profile "$PROFILE" --region "$REGION" 2>/dev/null || echo "UNKNOWN")
  echo "  state=$STATE"
  [[ "$STATE" == "STOPPED" || "$STATE" == "TERMINATED" ]] && break
  sleep 10
done

echo "Deleting application $APP_ID ..."
aws emr-serverless delete-application --application-id "$APP_ID" \
  --profile "$PROFILE" --region "$REGION"

echo "Done. The EMR Serverless application is deleted. Your S3 data was left untouched."
