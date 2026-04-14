#!/usr/bin/env bash
set -euo pipefail

export AWS_PAGER=""

PROFILE="${AWS_PROFILE:-}"
REGION="${AWS_REGION:-ap-south-2}"
APPLY="false"
DB_ID="${DB_ID:-orglens-postgres}"
DB_NAME="${DB_NAME:-orglens}"
DB_USER="${DB_USER:-orglens}"
DB_PASSWORD="${DB_PASSWORD:-}"
DB_CLASS="${DB_CLASS:-db.t4g.micro}"
DB_STORAGE_GB="${DB_STORAGE_GB:-20}"
VPC_ID="${VPC_ID:-}"
APP_SG_NAME="${APP_SG_NAME:-orglens-stack-sg}"
SUBNET_IDS="${SUBNET_IDS:-}"
WRITE_ENV_FILE="${WRITE_ENV_FILE:-}"

usage() {
  cat <<EOF
Usage: $0 [options]

Options:
  --apply                  Execute changes (default is dry-run)
  --region <region>        AWS region (default: ${REGION})
  --profile <profile>      AWS profile (optional)
  --db-id <id>             RDS instance identifier (default: ${DB_ID})
  --db-name <name>         Initial DB name (default: ${DB_NAME})
  --db-user <user>         Master username (default: ${DB_USER})
  --db-password <pass>     Master password (required with --apply)
  --db-class <class>       Instance class (default: ${DB_CLASS})
  --db-storage-gb <n>      Allocated storage (default: ${DB_STORAGE_GB})
  --vpc-id <vpc-id>        VPC ID (default: resolve default VPC)
  --subnet-ids <a,b,c>     Comma-separated subnet IDs (default: all VPC subnets)
  --app-sg-name <name>     SG used by OrgLens app EC2/ECS tasks (default: ${APP_SG_NAME})
  --write-env-file <path>  Write POSTGRES_HOST/POSTGRES_PORT to env file after provisioning

Notes:
  - Dry-run by default. Add --apply to create resources.
  - Provisions PostgreSQL 16 with private access, SG-restricted to app SG.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --apply) APPLY="true"; shift ;;
    --region) REGION="$2"; shift 2 ;;
    --profile) PROFILE="$2"; shift 2 ;;
    --db-id) DB_ID="$2"; shift 2 ;;
    --db-name) DB_NAME="$2"; shift 2 ;;
    --db-user) DB_USER="$2"; shift 2 ;;
    --db-password) DB_PASSWORD="$2"; shift 2 ;;
    --db-class) DB_CLASS="$2"; shift 2 ;;
    --db-storage-gb) DB_STORAGE_GB="$2"; shift 2 ;;
    --vpc-id) VPC_ID="$2"; shift 2 ;;
    --subnet-ids) SUBNET_IDS="$2"; shift 2 ;;
    --app-sg-name) APP_SG_NAME="$2"; shift 2 ;;
    --write-env-file) WRITE_ENV_FILE="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown arg: $1" >&2; usage; exit 2 ;;
  esac
done

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing command: $1" >&2
    exit 1
  fi
}

require_cmd aws

aws_cli() {
  if [[ -n "$PROFILE" ]]; then
    aws --no-cli-pager --profile "$PROFILE" --region "$REGION" "$@"
  else
    aws --no-cli-pager --region "$REGION" "$@"
  fi
}

set_env_var() {
  local env_file="$1"
  local key="$2"
  local value="$3"
  if grep -q "^${key}=" "$env_file"; then
    sed -i.bak "s#^${key}=.*#${key}=${value}#" "$env_file"
  else
    printf '%s=%s\n' "$key" "$value" >> "$env_file"
  fi
}

echo "Validating identity..."
if ! aws_cli sts get-caller-identity >/dev/null 2>&1; then
  if [[ "$APPLY" == "true" ]]; then
    echo "AWS authentication failed. Re-authenticate and retry." >&2
    exit 1
  fi
  cat <<EOF
[dry-run without AWS auth]
- AWS credentials are not currently available.
- Re-authenticate to resolve VPC/subnets and security groups dynamically.
- Apply mode requires active AWS credentials.
EOF
  exit 0
fi

if [[ -z "$VPC_ID" ]]; then
  VPC_ID="$(aws_cli ec2 describe-vpcs --filters Name=isDefault,Values=true --query 'Vpcs[0].VpcId' --output text)"
fi
if [[ "$VPC_ID" == "None" || -z "$VPC_ID" ]]; then
  echo "No VPC found. Provide --vpc-id." >&2
  exit 1
fi

if [[ -z "$SUBNET_IDS" ]]; then
  SUBNET_IDS="$(aws_cli ec2 describe-subnets --filters Name=vpc-id,Values="$VPC_ID" --query 'Subnets[].SubnetId' --output text | tr '\t' ',')"
fi
if [[ -z "$SUBNET_IDS" ]]; then
  echo "No subnets found. Provide --subnet-ids." >&2
  exit 1
fi

APP_SG_ID="$(aws_cli ec2 describe-security-groups --filters Name=vpc-id,Values="$VPC_ID" Name=group-name,Values="$APP_SG_NAME" --query 'SecurityGroups[0].GroupId' --output text 2>/dev/null || true)"
if [[ "$APP_SG_ID" == "None" || -z "$APP_SG_ID" ]]; then
  echo "App security group not found: $APP_SG_NAME" >&2
  exit 1
fi

DB_SG_NAME="${DB_ID}-sg"
DB_SUBNET_GROUP="${DB_ID}-subnets"

if [[ "$APPLY" != "true" ]]; then
  cat <<EOF
[dry-run summary]
- Would create DB subnet group: ${DB_SUBNET_GROUP} with subnets ${SUBNET_IDS}
- Would create/reuse DB SG: ${DB_SG_NAME} and allow ingress 5432 from ${APP_SG_NAME}
- Would create RDS PostgreSQL: ${DB_ID} (${DB_CLASS}, ${DB_STORAGE_GB}GB)
- Optional env update target: ${WRITE_ENV_FILE:-<none>}
EOF
  exit 0
fi

if [[ -z "$DB_PASSWORD" ]]; then
  echo "--db-password is required with --apply" >&2
  exit 2
fi

aws_cli rds create-db-subnet-group \
  --db-subnet-group-name "$DB_SUBNET_GROUP" \
  --db-subnet-group-description "OrgLens RDS subnets" \
  --subnet-ids "${SUBNET_IDS//,/ }" >/dev/null 2>&1 || true

DB_SG_ID="$(aws_cli ec2 describe-security-groups --filters Name=vpc-id,Values="$VPC_ID" Name=group-name,Values="$DB_SG_NAME" --query 'SecurityGroups[0].GroupId' --output text 2>/dev/null || true)"
if [[ "$DB_SG_ID" == "None" || -z "$DB_SG_ID" ]]; then
  DB_SG_ID="$(aws_cli ec2 create-security-group --vpc-id "$VPC_ID" --group-name "$DB_SG_NAME" --description "OrgLens RDS SG" --query GroupId --output text)"
fi
aws_cli ec2 authorize-security-group-ingress --group-id "$DB_SG_ID" --ip-permissions "IpProtocol=tcp,FromPort=5432,ToPort=5432,UserIdGroupPairs=[{GroupId=$APP_SG_ID,Description=OrgLens-App-to-RDS}]" >/dev/null 2>&1 || true

aws_cli rds create-db-instance \
  --db-instance-identifier "$DB_ID" \
  --engine postgres \
  --engine-version 16.3 \
  --db-instance-class "$DB_CLASS" \
  --allocated-storage "$DB_STORAGE_GB" \
  --storage-type gp3 \
  --master-username "$DB_USER" \
  --master-user-password "$DB_PASSWORD" \
  --db-name "$DB_NAME" \
  --db-subnet-group-name "$DB_SUBNET_GROUP" \
  --vpc-security-group-ids "$DB_SG_ID" \
  --publicly-accessible false \
  --backup-retention-period 7 \
  --deletion-protection \
  --no-multi-az >/dev/null 2>&1 || true

echo "Waiting for RDS availability..."
aws_cli rds wait db-instance-available --db-instance-identifier "$DB_ID"

DB_ENDPOINT="$(aws_cli rds describe-db-instances --db-instance-identifier "$DB_ID" --query 'DBInstances[0].Endpoint.Address' --output text)"
DB_PORT="$(aws_cli rds describe-db-instances --db-instance-identifier "$DB_ID" --query 'DBInstances[0].Endpoint.Port' --output text)"

if [[ -n "$WRITE_ENV_FILE" ]]; then
  if [[ ! -f "$WRITE_ENV_FILE" ]]; then
    echo "Env file not found: $WRITE_ENV_FILE" >&2
    exit 1
  fi
  set_env_var "$WRITE_ENV_FILE" "POSTGRES_HOST" "$DB_ENDPOINT"
  set_env_var "$WRITE_ENV_FILE" "POSTGRES_PORT" "$DB_PORT"
  set_env_var "$WRITE_ENV_FILE" "POSTGRES_DB" "$DB_NAME"
  set_env_var "$WRITE_ENV_FILE" "POSTGRES_USER" "$DB_USER"
  rm -f "$WRITE_ENV_FILE.bak"
fi

cat <<EOF
RDS provisioned.
Endpoint: ${DB_ENDPOINT}:${DB_PORT}
Next step:
- Ensure runtime uses POSTGRES_HOST=${DB_ENDPOINT} and POSTGRES_PORT=${DB_PORT}
- Rotate DB password into SSM/Secrets Manager and restart services
EOF
