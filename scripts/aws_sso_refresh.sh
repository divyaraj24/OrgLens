#!/usr/bin/env bash
set -euo pipefail

PROFILE="${AWS_PROFILE:-AdministratorAccess-772721871316}"
REGION="${AWS_REGION:-ap-south-2}"
FORCE_LOGIN="false"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --profile)
      PROFILE="$2"
      shift 2
      ;;
    --region)
      REGION="$2"
      shift 2
      ;;
    --force)
      FORCE_LOGIN="true"
      shift
      ;;
    *)
      echo "Unknown argument: $1" >&2
      echo "Usage: $0 [--profile PROFILE] [--region REGION] [--force]" >&2
      exit 2
      ;;
  esac
done

export AWS_PAGER=""
export AWS_REGION="$REGION"
export AWS_DEFAULT_REGION="$REGION"

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1" >&2
    exit 1
  fi
}

require_cmd aws

sts_ok() {
  aws --no-cli-pager --profile "$PROFILE" sts get-caller-identity >/dev/null 2>&1
}

if [[ "$FORCE_LOGIN" == "true" ]]; then
  echo "Forcing AWS SSO login for profile $PROFILE"
  aws --no-cli-pager sso login --profile "$PROFILE"
else
  if sts_ok; then
    echo "AWS session already valid for profile $PROFILE"
  else
    echo "AWS session expired or missing for profile $PROFILE. Starting SSO login..."
    aws --no-cli-pager sso login --profile "$PROFILE"
  fi
fi

if ! sts_ok; then
  echo "AWS session is still invalid after login. Check SSO portal, account, and role mapping." >&2
  exit 1
fi

ARN="$(aws --no-cli-pager --profile "$PROFILE" sts get-caller-identity --query Arn --output text)"
ACCOUNT_ID="$(aws --no-cli-pager --profile "$PROFILE" sts get-caller-identity --query Account --output text)"

echo "AWS session ready"
echo "Profile: $PROFILE"
echo "Region: $REGION"
echo "Account: $ACCOUNT_ID"
echo "Identity: $ARN"
