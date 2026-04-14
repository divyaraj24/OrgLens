#!/usr/bin/env bash
set -euo pipefail

# Push selected OrgLens secrets from local .env.aws to AWS SSM Parameter Store.
# Usage:
#   infra/aws/push_ssm_secrets.sh [--prefix /orglens/prod] [--env-file .env.aws]

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

read_env_value() {
  local key="$1"
  awk -F= -v k="$key" '$1==k {print substr($0, index($0, "=")+1); exit}' "$ENV_FILE"
}

put_secret() {
  local key="$1"
  local value
  value="$(read_env_value "$key")"
  if [[ -z "$value" || "$value" == "replace_me" ]]; then
    echo "Skipping $key (missing or placeholder)" >&2
    return 0
  fi

  aws_cmd ssm put-parameter \
    --name "$PREFIX/$key" \
    --type SecureString \
    --overwrite \
    --value "$value" >/dev/null

  echo "Stored $PREFIX/$key"
}

echo "Validating AWS identity..."
aws_cmd sts get-caller-identity >/dev/null

echo "Pushing secrets to SSM prefix: $PREFIX"
put_secret ORGLENS_GITHUB_TOKEN
put_secret ORGLENS_WEBHOOK_SECRET
put_secret ORGLENS_API_KEY
put_secret ORGLENS_INGEST_SIGNING_SECRET
put_secret GRAFANA_ADMIN_PASSWORD

echo "Done"
