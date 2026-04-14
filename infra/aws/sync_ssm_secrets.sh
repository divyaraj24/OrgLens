#!/usr/bin/env bash
set -euo pipefail

# Sync OrgLens secrets from AWS SSM Parameter Store into an env file.
# Usage:
#   infra/aws/sync_ssm_secrets.sh [--prefix /orglens/prod] [--env-file .env.aws]

PREFIX="${ORGLENS_SSM_PREFIX:-/orglens/prod}"
ENV_FILE=".env.aws"
REGION="${AWS_REGION:-ap-south-2}"
PROFILE="${AWS_PROFILE:-}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --prefix)
      PREFIX="$2"
      shift 2
      ;;
    --env-file)
      ENV_FILE="$2"
      shift 2
      ;;
    --region)
      REGION="$2"
      shift 2
      ;;
    --profile)
      PROFILE="$2"
      shift 2
      ;;
    *)
      echo "Unknown argument: $1" >&2
      echo "Usage: $0 [--prefix /orglens/prod] [--env-file .env.aws] [--region ap-south-2] [--profile name]" >&2
      exit 2
      ;;
  esac
done

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Missing env file: $ENV_FILE" >&2
  exit 1
fi

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1" >&2
    exit 1
  fi
}

require_cmd aws

aws_cmd() {
  if [[ -n "$PROFILE" ]]; then
    aws --no-cli-pager --profile "$PROFILE" --region "$REGION" "$@"
  else
    aws --no-cli-pager --region "$REGION" "$@"
  fi
}

set_env_var() {
  local key="$1"
  local value="$2"
  local escaped_value
  escaped_value="${value//\\/\\\\}"
  escaped_value="${escaped_value//&/\\&}"
  escaped_value="${escaped_value//#/\\#}"
  if grep -q "^${key}=" "$ENV_FILE"; then
    sed -i.bak "s#^${key}=.*#${key}=${escaped_value}#" "$ENV_FILE"
  else
    printf '%s=%s\n' "$key" "$value" >> "$ENV_FILE"
  fi
}

fetch_secret() {
  local key="$1"
  aws_cmd ssm get-parameter --name "$PREFIX/$key" --with-decryption --query 'Parameter.Value' --output text 2>/dev/null || true
}

echo "Validating AWS identity..."
aws_cmd sts get-caller-identity >/dev/null

echo "Syncing secrets from SSM prefix: $PREFIX"
for key in ORGLENS_GITHUB_TOKEN ORGLENS_WEBHOOK_SECRET ORGLENS_API_KEY ORGLENS_INGEST_SIGNING_SECRET GRAFANA_ADMIN_PASSWORD; do
  value="$(fetch_secret "$key")"
  if [[ -z "$value" || "$value" == "None" ]]; then
    echo "Skipping $key (not found in SSM)" >&2
    continue
  fi
  set_env_var "$key" "$value"
  echo "Updated $key in $ENV_FILE"
done

rm -f "$ENV_FILE.bak"
echo "Done"
