#!/usr/bin/env bash
set -euo pipefail

if ! command -v aws >/dev/null 2>&1; then
  echo "aws CLI is required" >&2
  exit 1
fi

if ! aws sts get-caller-identity >/dev/null 2>&1; then
  echo "No AWS credentials found. Run: aws configure sso (recommended) or aws configure" >&2
  exit 1
fi

if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required. Install with: brew install jq" >&2
  exit 1
fi

MONTHLY_LIMIT="${MONTHLY_LIMIT:-75}"
BUDGET_NAME="${BUDGET_NAME:-OrgLens-Monthly-Guardrail}"
ALERT_EMAIL="${ALERT_EMAIL:-}"
ALARM_THRESHOLD="${ALARM_THRESHOLD:-60}"

if [[ -z "$ALERT_EMAIL" ]]; then
  echo "Set ALERT_EMAIL before running, e.g. ALERT_EMAIL=you@example.com" >&2
  exit 1
fi

ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"
REGION="${AWS_REGION:-ap-south-2}"
BILLING_REGION="us-east-1"
TOPIC_NAME="orglens-cost-alerts"
TOPIC_ARN="$(aws sns create-topic --name "$TOPIC_NAME" --region "$BILLING_REGION" --query TopicArn --output text)"

aws sns subscribe \
  --topic-arn "$TOPIC_ARN" \
  --protocol email \
  --notification-endpoint "$ALERT_EMAIL" \
  --region "$BILLING_REGION" >/dev/null

BUDGET_FILE="/tmp/orglens-budget.json"
NOTIFICATIONS_FILE="/tmp/orglens-budget-notifications.json"

cat > "$BUDGET_FILE" <<EOF
{
  "BudgetName": "$BUDGET_NAME",
  "BudgetLimit": {
    "Amount": "$MONTHLY_LIMIT",
    "Unit": "USD"
  },
  "TimeUnit": "MONTHLY",
  "BudgetType": "COST"
}
EOF

cat > "$NOTIFICATIONS_FILE" <<EOF
[
  {
    "Notification": {
      "NotificationType": "ACTUAL",
      "ComparisonOperator": "GREATER_THAN",
      "Threshold": 80,
      "ThresholdType": "PERCENTAGE"
    },
    "Subscribers": [
      {
        "SubscriptionType": "EMAIL",
        "Address": "$ALERT_EMAIL"
      },
      {
        "SubscriptionType": "SNS",
        "Address": "$TOPIC_ARN"
      }
    ]
  },
  {
    "Notification": {
      "NotificationType": "ACTUAL",
      "ComparisonOperator": "GREATER_THAN",
      "Threshold": 95,
      "ThresholdType": "PERCENTAGE"
    },
    "Subscribers": [
      {
        "SubscriptionType": "EMAIL",
        "Address": "$ALERT_EMAIL"
      },
      {
        "SubscriptionType": "SNS",
        "Address": "$TOPIC_ARN"
      }
    ]
  },
  {
    "Notification": {
      "NotificationType": "FORECASTED",
      "ComparisonOperator": "GREATER_THAN",
      "Threshold": 100,
      "ThresholdType": "PERCENTAGE"
    },
    "Subscribers": [
      {
        "SubscriptionType": "EMAIL",
        "Address": "$ALERT_EMAIL"
      },
      {
        "SubscriptionType": "SNS",
        "Address": "$TOPIC_ARN"
      }
    ]
  }
]
EOF

if aws budgets describe-budget --account-id "$ACCOUNT_ID" --budget-name "$BUDGET_NAME" --region "$BILLING_REGION" >/dev/null 2>&1; then
  aws budgets update-budget --account-id "$ACCOUNT_ID" --new-budget file://"$BUDGET_FILE" --region "$BILLING_REGION" >/dev/null
else
  aws budgets create-budget \
    --account-id "$ACCOUNT_ID" \
    --budget file://"$BUDGET_FILE" \
    --notifications-with-subscribers file://"$NOTIFICATIONS_FILE" \
    --region "$BILLING_REGION" >/dev/null
fi

aws cloudwatch put-metric-alarm \
  --alarm-name "OrgLens-Estimated-Charges" \
  --namespace AWS/Billing \
  --metric-name EstimatedCharges \
  --dimensions Name=Currency,Value=USD \
  --statistic Maximum \
  --period 21600 \
  --evaluation-periods 1 \
  --threshold "$ALARM_THRESHOLD" \
  --comparison-operator GreaterThanThreshold \
  --alarm-actions "$TOPIC_ARN" \
  --region "$BILLING_REGION" >/dev/null

echo "Cost guardrails configured"
echo "Account: $ACCOUNT_ID"
echo "Budget: $BUDGET_NAME ($MONTHLY_LIMIT USD/month)"
echo "Alarm: OrgLens-Estimated-Charges at $ALARM_THRESHOLD USD"
echo "SNS Topic: $TOPIC_ARN"
echo "Billing region: $BILLING_REGION"
echo "IMPORTANT: Confirm SNS email subscription from your inbox"
