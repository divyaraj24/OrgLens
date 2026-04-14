#!/usr/bin/env bash
set -euo pipefail

# Cost-aware local orchestrator for AWS stack setup.
# It runs local prechecks first, then (optionally) provisions remote EC2 stack.

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DEPLOY_SCRIPT="$REPO_ROOT/infra/aws/deploy_ec2_stack.sh"
GUARDRAILS_SCRIPT="$REPO_ROOT/infra/aws/cost_guardrails.sh"
PUSH_SSM_SCRIPT="$REPO_ROOT/infra/aws/push_ssm_secrets.sh"

MAX_DAILY_USD="${MAX_DAILY_USD:-0.50}"
INSTANCE_TYPE="${INSTANCE_TYPE:-t4g.micro}"
VOLUME_SIZE_GB="${VOLUME_SIZE_GB:-20}"
ALERT_EMAIL="${ALERT_EMAIL:-}"
APPLY_GUARDRAILS="${APPLY_GUARDRAILS:-1}"
SKIP_DEPLOY="${SKIP_DEPLOY:-0}"
SSM_PREFIX="${ORGLENS_SSM_PREFIX:-/orglens/prod}"

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1" >&2
    exit 1
  fi
}

require_cmd aws
require_cmd bash

estimate_daily_compute_usd() {
  case "$1" in
    t4g.nano) echo "0.10" ;;
    t4g.micro) echo "0.21" ;;
    t4g.small) echo "0.42" ;;
    t4g.medium) echo "0.84" ;;
    *) echo "1.20" ;;
  esac
}

estimate_daily_ebs_usd() {
  local gb="$1"
  # gp3 ~ $0.08/GB-month -> daily ~= gb * 0.08 / 30
  awk -v g="$gb" 'BEGIN { printf "%.3f", (g * 0.08) / 30.0 }'
}

compare_gt() {
  awk -v a="$1" -v b="$2" 'BEGIN { exit !(a>b) }'
}

main() {
  echo "== OrgLens AWS setup precheck =="
  echo "Instance type   : $INSTANCE_TYPE"
  echo "Volume (GB)     : $VOLUME_SIZE_GB"
  echo "Max daily budget: $MAX_DAILY_USD USD"

  local compute ebs total
  compute="$(estimate_daily_compute_usd "$INSTANCE_TYPE")"
  ebs="$(estimate_daily_ebs_usd "$VOLUME_SIZE_GB")"
  total="$(awk -v c="$compute" -v e="$ebs" 'BEGIN { printf "%.3f", c+e }')"

  echo "Estimated compute/day : $compute USD"
  echo "Estimated EBS/day     : $ebs USD"
  echo "Estimated total/day   : $total USD"

  if compare_gt "$total" "$MAX_DAILY_USD"; then
    echo "Refusing deploy: estimated daily cost ($total) exceeds MAX_DAILY_USD ($MAX_DAILY_USD)." >&2
    echo "Try INSTANCE_TYPE=t4g.micro and VOLUME_SIZE_GB=20." >&2
    exit 2
  fi

  echo "Validating AWS identity..."
  aws sts get-caller-identity >/dev/null

  if [[ "$APPLY_GUARDRAILS" == "1" ]]; then
    if [[ -z "$ALERT_EMAIL" ]]; then
      echo "Skipping cost guardrails: ALERT_EMAIL not set." >&2
    else
      echo "Applying AWS cost guardrails..."
      MONTHLY_LIMIT="${MONTHLY_LIMIT:-75}" \
      ALARM_THRESHOLD="${ALARM_THRESHOLD:-60}" \
      ALERT_EMAIL="$ALERT_EMAIL" \
      "$GUARDRAILS_SCRIPT"
    fi
  fi

  if [[ "$SKIP_DEPLOY" == "1" ]]; then
    echo "SKIP_DEPLOY=1 set. Precheck complete."
    exit 0
  fi

  if [[ -x "$PUSH_SSM_SCRIPT" ]]; then
    echo "Syncing local secrets to SSM Parameter Store..."
    ORGLENS_SSM_PREFIX="$SSM_PREFIX" "$PUSH_SSM_SCRIPT" --prefix "$SSM_PREFIX" --env-file "$REPO_ROOT/.env.aws"
  fi

  echo "Launching remote stack provisioning..."
  INSTANCE_TYPE="$INSTANCE_TYPE" \
  VOLUME_SIZE_GB="$VOLUME_SIZE_GB" \
  ORGLENS_SSM_PREFIX="$SSM_PREFIX" \
  "$DEPLOY_SCRIPT"
}

main "$@"
