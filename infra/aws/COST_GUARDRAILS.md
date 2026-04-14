# AWS Cost Guardrails for $100 Credit Protection

This guide sets hard monitoring guardrails before deploying OrgLens Layers 2-5.

## Current Access Status

AWS CLI is installed locally, but credentials are not configured.
You must authenticate first.

## 1) Authenticate CLI

Recommended:

```bash
aws configure sso
```

Alternative (access key):

```bash
aws configure
```

Verify:

```bash
aws sts get-caller-identity
```

## 2) Install helper dependency

```bash
brew install jq
```

## 3) Apply guardrails (must do before deployment)

```bash
cd /Users/divyaraj/Documents/Semester\ 6/OrgLens
chmod +x infra/aws/cost_guardrails.sh
AWS_PROFILE=AdministratorAccess-772721871316 \
AWS_REGION=ap-south-2 \
ALERT_EMAIL=divyarajdeepak2356@gmail.com \
MONTHLY_LIMIT=75 \
ALARM_THRESHOLD=60 \
infra/aws/cost_guardrails.sh
```

Why these values:

- Credit limit is $100.
- Budget at $75 gives enough reaction time.
- Billing alarm at $60 warns early.

Notes:

- Billing budget/alarm APIs are managed in `us-east-1` by the script.
- Deployment region remains `ap-south-2`.

## 4) Enforce low-cost deployment defaults

- Single EC2 host (avoid multi-node)
- ARM instance class preferred (`t4g.small` or smaller)
- gp3 EBS 20-30 GB only
- No NAT Gateway for dev/test (use public subnet + SG hardening)
- Stop EC2 nightly when idle

## 5) Daily spend check

```bash
aws ce get-cost-and-usage \
  --time-period Start=$(date -u +%Y-%m-01),End=$(date -u -v+1d +%Y-%m-%d) \
  --granularity DAILY \
  --metrics UnblendedCost
```

## 6) Budget breach playbook

If spend exceeds 80%:

1. Stop nonessential EC2 instances.
2. Scale down Prometheus retention or stop Grafana for non-working hours.
3. Review CloudWatch logs retention and reduce to 3-7 days.
4. Confirm no large data transfer or snapshots are accumulating.
