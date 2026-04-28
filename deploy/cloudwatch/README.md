# CV Pipeline CloudWatch Resources

Everything the Phase 8 observability layer registers:

- **`alarms.json`** — 3 alarms on the `UBall/CV` namespace
- **`dashboard.json`** — 6-widget operations dashboard

## Prerequisites

- SNS topic `cv-pipeline-alerts` created in Phase 0 with your email subscribed.
- Its ARN substituted into every `PLACEHOLDER_SNS_TOPIC_ARN` in `alarms.json`.

## Apply

```bash
REGION=us-east-1
SNS_TOPIC_ARN="arn:aws:sns:us-east-1:840102831548:cv-pipeline-alerts"

# 1. Substitute the SNS ARN + register each alarm
jq --arg arn "$SNS_TOPIC_ARN" \
   '(.[].AlarmActions[], .[].OKActions[]?) |= (if . == "PLACEHOLDER_SNS_TOPIC_ARN" then $arn else . end)' \
   deploy/cloudwatch/alarms.json > /tmp/alarms-expanded.json

jq -c '.[]' /tmp/alarms-expanded.json | while read -r a; do
  name=$(echo "$a" | jq -r .AlarmName)
  echo "$a" > /tmp/one-alarm.json
  aws cloudwatch put-metric-alarm --region "$REGION" \
    --cli-input-json file:///tmp/one-alarm.json
  echo "registered: $name"
done

# 2. Register the dashboard
aws cloudwatch put-dashboard --region "$REGION" \
  --dashboard-name "UBall-CV-Pipeline" \
  --dashboard-body "$(cat deploy/cloudwatch/dashboard.json)"
```

## Verify

```bash
# Alarms should appear in the "Alarms" console
aws cloudwatch describe-alarms --region "$REGION" \
  --alarm-name-prefix "UBall-CV-" \
  --query 'MetricAlarms[].{name:AlarmName,state:StateValue}' --output table

# Dashboard URL
echo "https://${REGION}.console.aws.amazon.com/cloudwatch/home?region=${REGION}#dashboards:name=UBall-CV-Pipeline"
```

## What each alarm does

| Alarm | Fires when | Typical root cause |
|---|---|---|
| `UBall-CV-JobFailure` | any fusion or merge Batch job has `CVJobFailure >= 1` summed over 1h | OOM, model weight corruption, Firebase outage, UBall API outage |
| `UBall-CV-DispatchUnhandledError` | Flask dispatch endpoint raises an exception twice in 10 min | boto3 creds expired, Batch queue disabled, Firebase/network issue on Jetson |
| `UBall-CV-NeedsReviewStreak` | merge output is flagged `needs_review=1` three or more times in a day | Operators forgetting to set **Attacking hoop at tip-off** on check-in page |

## Runbook

For triage steps, see `docs/CV_PIPELINE_RUNBOOK.md`.
