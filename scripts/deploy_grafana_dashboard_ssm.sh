#!/usr/bin/env bash
set -euo pipefail

PROFILE="${AWS_PROFILE:-AdministratorAccess-772721871316}"
REGION="${AWS_REGION:-ap-south-2}"
HOST="${ORGLENS_AWS_HOST:-18.60.214.55}"
INSTANCE_ID=""
REMOTE_ROOT="${ORGLENS_REMOTE_ROOT:-/opt/orglens}"
DRY_RUN="false"

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
    --host)
      HOST="$2"
      shift 2
      ;;
    --instance-id)
      INSTANCE_ID="$2"
      shift 2
      ;;
    --remote-root)
      REMOTE_ROOT="$2"
      shift 2
      ;;
    --dry-run)
      DRY_RUN="true"
      shift
      ;;
    *)
      echo "Unknown argument: $1" >&2
      echo "Usage: $0 [--profile PROFILE] [--region REGION] [--host IP] [--instance-id ID] [--remote-root PATH] [--dry-run]" >&2
      exit 2
      ;;
  esac
done

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DASHBOARD_LOCAL="$REPO_ROOT/infra/aws/grafana/dashboards/orglens-3layer-overview.json"
DATASOURCE_LOCAL="$REPO_ROOT/infra/aws/grafana/provisioning/datasources/datasources.yml"

for cmd in aws jq tar curl; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "Missing required command: $cmd" >&2
    exit 1
  fi
done

export AWS_PAGER=""
export AWS_DEFAULT_REGION="$REGION"

echo "Running local preflight checks..."
jq . "$DASHBOARD_LOCAL" >/dev/null
if ! grep -q 'type: prometheus' "$DATASOURCE_LOCAL" || ! grep -q 'type: postgres' "$DATASOURCE_LOCAL"; then
  echo "Prometheus/PostgreSQL datasource definitions not found in datasources.yml" >&2
  exit 1
fi

aws --no-cli-pager --profile "$PROFILE" --region "$REGION" sts get-caller-identity >/dev/null

if [[ -z "$INSTANCE_ID" ]]; then
  INSTANCE_ID="$(aws --no-cli-pager --profile "$PROFILE" --region "$REGION" ec2 describe-instances \
    --filters Name=ip-address,Values="$HOST" Name=instance-state-name,Values=running \
    --query 'Reservations[].Instances[].InstanceId' --output text)"
fi

if [[ -z "$INSTANCE_ID" || "$INSTANCE_ID" == "None" ]]; then
  echo "Could not resolve running instance id for host $HOST" >&2
  exit 1
fi

PING_STATUS="$(aws --no-cli-pager --profile "$PROFILE" --region "$REGION" ssm describe-instance-information \
  --filters Key=InstanceIds,Values="$INSTANCE_ID" \
  --query 'InstanceInformationList[0].PingStatus' --output text)"
if [[ "$PING_STATUS" != "Online" ]]; then
  echo "Instance $INSTANCE_ID is not SSM-online (status=$PING_STATUS)" >&2
  exit 1
fi

ACCOUNT_ID="$(aws --no-cli-pager --profile "$PROFILE" --region "$REGION" sts get-caller-identity --query Account --output text)"
BUCKET_NAME="orglens-deploy-${ACCOUNT_ID}-${REGION}"
TIMESTAMP="$(date -u +%Y%m%dT%H%M%SZ)"
ARCHIVE_PATH="/tmp/orglens-grafana-${TIMESTAMP}.tgz"
S3_KEY="ops/grafana-${TIMESTAMP}.tgz"

TMP_DIR="/tmp/orglens-grafana-${TIMESTAMP}"
rm -rf "$TMP_DIR"
mkdir -p "$TMP_DIR/infra/aws/grafana/dashboards" "$TMP_DIR/infra/aws/grafana/provisioning/datasources"
cp "$DASHBOARD_LOCAL" "$TMP_DIR/infra/aws/grafana/dashboards/orglens-3layer-overview.json"
cp "$DATASOURCE_LOCAL" "$TMP_DIR/infra/aws/grafana/provisioning/datasources/datasources.yml"

tar -czf "$ARCHIVE_PATH" -C "$TMP_DIR" .

if ! aws --no-cli-pager --profile "$PROFILE" --region "$REGION" s3 ls "s3://$BUCKET_NAME" >/dev/null 2>&1; then
  aws --no-cli-pager --profile "$PROFILE" --region "$REGION" s3api create-bucket \
    --bucket "$BUCKET_NAME" \
    --create-bucket-configuration "LocationConstraint=$REGION" >/dev/null
fi

aws --no-cli-pager --profile "$PROFILE" --region "$REGION" s3 cp "$ARCHIVE_PATH" "s3://$BUCKET_NAME/$S3_KEY" >/dev/null
PRESIGNED_URL="$(aws --no-cli-pager --profile "$PROFILE" --region "$REGION" s3 presign "s3://$BUCKET_NAME/$S3_KEY" --expires-in 3600)"

if [[ "$DRY_RUN" == "true" ]]; then
  echo "Dry run successful"
  echo "Instance: $INSTANCE_ID"
  echo "Bundle uploaded: s3://$BUCKET_NAME/$S3_KEY"
  exit 0
fi

read -r -d '' SSM_SCRIPT <<'EOF' || true
set -euo pipefail
TS="$(date -u +%Y%m%dT%H%M%SZ)"
WORKDIR="/tmp/orglens-dashboard-deploy-$TS"
mkdir -p "$WORKDIR"
curl -fsSL 'REPLACE_PRESIGNED_URL' -o "$WORKDIR/deploy.tgz"
tar -xzf "$WORKDIR/deploy.tgz" -C "$WORKDIR"

DASH_DIR="REPLACE_REMOTE_ROOT/infra/aws/grafana/dashboards"
DS_DIR="REPLACE_REMOTE_ROOT/infra/aws/grafana/provisioning/datasources"
mkdir -p "$DASH_DIR" "$DS_DIR"

if [[ -f "$DASH_DIR/orglens-3layer-overview.json" ]]; then
  cp "$DASH_DIR/orglens-3layer-overview.json" "$DASH_DIR/orglens-3layer-overview.json.bak.$TS"
fi
if [[ -f "$DS_DIR/datasources.yml" ]]; then
  cp "$DS_DIR/datasources.yml" "$DS_DIR/datasources.yml.bak.$TS"
fi

cp "$WORKDIR/infra/aws/grafana/dashboards/orglens-3layer-overview.json" "$DASH_DIR/orglens-3layer-overview.json"
cp "$WORKDIR/infra/aws/grafana/provisioning/datasources/datasources.yml" "$DS_DIR/datasources.yml"

cd REPLACE_REMOTE_ROOT
docker compose -f infra/aws/docker-compose.minimal.yml --env-file .env.aws up -d --no-deps grafana prometheus layer2-core >/dev/null

for _ in $(seq 1 12); do
  if curl -fsS --max-time 10 http://127.0.0.1:3000/api/health >/dev/null && \
     curl -fsS --max-time 10 http://127.0.0.1:8001/health >/dev/null; then
    exit 0
  fi
  sleep 5
done

echo "Health checks did not recover in time" >&2
exit 1
EOF

SSM_SCRIPT="${SSM_SCRIPT//REPLACE_PRESIGNED_URL/$PRESIGNED_URL}"
SSM_SCRIPT="${SSM_SCRIPT//REPLACE_REMOTE_ROOT/$REMOTE_ROOT}"

SCRIPT_B64="$(printf '%s' "$SSM_SCRIPT" | base64 | tr -d '\n')"
COMMANDS_JSON="$(jq -cn --arg b64 "$SCRIPT_B64" '[
  "echo \($b64) | base64 --decode > /tmp/orglens_dashboard_deploy.sh",
  "chmod +x /tmp/orglens_dashboard_deploy.sh",
  "/bin/bash /tmp/orglens_dashboard_deploy.sh"
]')"

CMD_ID="$(aws --no-cli-pager --profile "$PROFILE" --region "$REGION" ssm send-command \
  --instance-ids "$INSTANCE_ID" \
  --document-name 'AWS-RunShellScript' \
  --comment 'OrgLens Grafana dashboard deploy' \
  --parameters commands="$COMMANDS_JSON" \
  --query 'Command.CommandId' --output text)"

echo "SSM command started: $CMD_ID"

for _ in $(seq 1 60); do
  STATUS="$(aws --no-cli-pager --profile "$PROFILE" --region "$REGION" ssm get-command-invocation \
    --command-id "$CMD_ID" --instance-id "$INSTANCE_ID" \
    --query 'Status' --output text 2>/dev/null || true)"
  case "$STATUS" in
    Success)
      echo "Deployment complete via SSM"
      echo "Grafana: http://$HOST:3000"
      echo "Layer2 health: http://$HOST:8001/health"
      exit 0
      ;;
    Failed|Cancelled|TimedOut)
      echo "SSM deployment failed with status: $STATUS" >&2
      aws --no-cli-pager --profile "$PROFILE" --region "$REGION" ssm get-command-invocation \
        --command-id "$CMD_ID" --instance-id "$INSTANCE_ID" \
        --query 'StandardErrorContent' --output text >&2 || true
      exit 1
      ;;
    *)
      sleep 5
      ;;
  esac
done

echo "Timed out waiting for SSM command completion: $CMD_ID" >&2
exit 1
