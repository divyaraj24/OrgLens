#!/usr/bin/env bash
set -euo pipefail

HOST="${ORGLENS_AWS_HOST:-}"
USER="${ORGLENS_AWS_USER:-ubuntu}"
KEY_PATH="${ORGLENS_AWS_KEY_PATH:-infra/aws/keys/orglens-ec2-key.pem}"
REMOTE_ROOT="${ORGLENS_REMOTE_ROOT:-/opt/orglens}"
MODE="${ORGLENS_DEPLOY_MODE:-auto}"
AWS_PROFILE="${AWS_PROFILE:-}"
AWS_REGION="${AWS_REGION:-ap-south-2}"
INSTANCE_ID=""
GRAFANA_URL=""
DRY_RUN="false"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --host)
      HOST="$2"
      shift 2
      ;;
    --user)
      USER="$2"
      shift 2
      ;;
    --key)
      KEY_PATH="$2"
      shift 2
      ;;
    --remote-root)
      REMOTE_ROOT="$2"
      shift 2
      ;;
    --mode)
      MODE="$2"
      shift 2
      ;;
    --profile)
      AWS_PROFILE="$2"
      shift 2
      ;;
    --region)
      AWS_REGION="$2"
      shift 2
      ;;
    --instance-id)
      INSTANCE_ID="$2"
      shift 2
      ;;
    --grafana-url)
      GRAFANA_URL="$2"
      shift 2
      ;;
    --dry-run)
      DRY_RUN="true"
      shift
      ;;
    *)
      echo "Unknown argument: $1" >&2
      echo "Usage: $0 [--host HOST] [--user USER] [--key KEY_PATH] [--remote-root PATH] [--mode auto|ssh|api|ssm] [--profile PROFILE] [--region REGION] [--instance-id ID] [--grafana-url URL] [--dry-run]" >&2
      exit 2
      ;;
  esac
done

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DASHBOARD_LOCAL="$REPO_ROOT/infra/aws/grafana/dashboards/orglens-3layer-overview.json"
DATASOURCE_LOCAL="$REPO_ROOT/infra/aws/grafana/provisioning/datasources/datasources.yml"
ENV_AWS_LOCAL="$REPO_ROOT/.env.aws"
SSM_DEPLOY_SCRIPT="$REPO_ROOT/scripts/deploy_grafana_dashboard_ssm.sh"

if [[ -z "$GRAFANA_URL" && -n "$HOST" ]]; then
  GRAFANA_URL="http://$HOST:3000"
fi

if [[ -z "$HOST" && -z "$INSTANCE_ID" && "$MODE" != "api" ]]; then
  echo "Set --host (or ORGLENS_AWS_HOST), or provide --instance-id for SSM mode." >&2
  exit 2
fi

if [[ "$MODE" == "api" && -z "$GRAFANA_URL" ]]; then
  echo "API mode requires --grafana-url or --host (ORGLENS_AWS_HOST)." >&2
  exit 2
fi

if [[ ! -f "$DASHBOARD_LOCAL" ]]; then
  echo "Missing dashboard file: $DASHBOARD_LOCAL" >&2
  exit 1
fi

if [[ ! -f "$DATASOURCE_LOCAL" ]]; then
  echo "Missing datasource file: $DATASOURCE_LOCAL" >&2
  exit 1
fi

for cmd in jq ssh scp curl; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "Missing required command: $cmd" >&2
    exit 1
  fi
done

if [[ ! -f "$KEY_PATH" ]]; then
  echo "Missing SSH key file: $KEY_PATH" >&2
  exit 1
fi

chmod 400 "$KEY_PATH" >/dev/null 2>&1 || true

echo "Running local preflight checks..."
jq . "$DASHBOARD_LOCAL" >/dev/null
if ! grep -q 'type: prometheus' "$DATASOURCE_LOCAL" || ! grep -q 'type: postgres' "$DATASOURCE_LOCAL"; then
  echo "Prometheus/PostgreSQL datasource definitions not found in datasources.yml" >&2
  exit 1
fi

if [[ "$DRY_RUN" == "true" ]]; then
  echo "Dry run successful. Local files are valid and ready for deployment."
  exit 0
fi

run_api_deploy() {
  if [[ ! -f "$ENV_AWS_LOCAL" ]]; then
    echo "Missing .env.aws for Grafana API credentials: $ENV_AWS_LOCAL" >&2
    return 1
  fi

  # shellcheck disable=SC1090
  source "$ENV_AWS_LOCAL"
  if [[ -z "${GRAFANA_ADMIN_USER:-}" || -z "${GRAFANA_ADMIN_PASSWORD:-}" ]]; then
    echo "GRAFANA_ADMIN_USER / GRAFANA_ADMIN_PASSWORD are required in .env.aws for API deploy." >&2
    return 1
  fi

  local payload
  payload="$(jq -cn --rawfile dashboard "$DASHBOARD_LOCAL" '{dashboard: ($dashboard | fromjson), overwrite: true, message: "orglens scripted deploy"}')"

  echo "Deploying dashboard through Grafana API..."
  if ! curl -fsS --max-time 30 \
    -u "$GRAFANA_ADMIN_USER:$GRAFANA_ADMIN_PASSWORD" \
    -H 'Content-Type: application/json' \
    -X POST "$GRAFANA_URL/api/dashboards/db" \
    -d "$payload" >/dev/null; then
    echo "Grafana API POST failed" >&2
    return 1
  fi

  if ! curl -fsS --max-time 30 \
    -u "$GRAFANA_ADMIN_USER:$GRAFANA_ADMIN_PASSWORD" \
    "$GRAFANA_URL/api/dashboards/uid/orglens-3layer-overview" >/dev/null; then
    echo "Grafana API verification failed" >&2
    return 1
  fi

  echo "Grafana API deployment complete"
}

run_ssm_deploy() {
  if [[ ! -x "$SSM_DEPLOY_SCRIPT" ]]; then
    echo "Missing SSM deploy helper: $SSM_DEPLOY_SCRIPT" >&2
    return 1
  fi

  local args
  args=(--host "$HOST" --remote-root "$REMOTE_ROOT" --region "$AWS_REGION")
  if [[ -n "$AWS_PROFILE" ]]; then
    args+=(--profile "$AWS_PROFILE")
  fi
  if [[ -n "$INSTANCE_ID" ]]; then
    args+=(--instance-id "$INSTANCE_ID")
  fi

  echo "Deploying through AWS SSM fallback..."
  "$SSM_DEPLOY_SCRIPT" "${args[@]}"
}

run_ssh_deploy() {
  echo "Checking remote reachability..."
  ssh -o BatchMode=yes -o StrictHostKeyChecking=accept-new -o ConnectTimeout=10 -i "$KEY_PATH" "$USER@$HOST" "echo ssh_ok" >/dev/null

  echo "Uploading Grafana provisioning files..."
  scp -o StrictHostKeyChecking=accept-new -i "$KEY_PATH" "$DASHBOARD_LOCAL" "$USER@$HOST:/tmp/orglens-3layer-overview.json"
  scp -o StrictHostKeyChecking=accept-new -i "$KEY_PATH" "$DATASOURCE_LOCAL" "$USER@$HOST:/tmp/datasources.yml"

  read -r -d '' REMOTE_CMD <<'EOF' || true
set -euo pipefail
TS="$(date -u +%Y%m%dT%H%M%SZ)"
DASH_DIR="REPLACE_REMOTE_ROOT/infra/aws/grafana/dashboards"
DS_DIR="REPLACE_REMOTE_ROOT/infra/aws/grafana/provisioning/datasources"

mkdir -p "$DASH_DIR" "$DS_DIR"

if [[ -f "$DASH_DIR/orglens-3layer-overview.json" ]]; then
  cp "$DASH_DIR/orglens-3layer-overview.json" "$DASH_DIR/orglens-3layer-overview.json.bak.$TS"
fi
if [[ -f "$DS_DIR/datasources.yml" ]]; then
  cp "$DS_DIR/datasources.yml" "$DS_DIR/datasources.yml.bak.$TS"
fi

mv /tmp/orglens-3layer-overview.json "$DASH_DIR/orglens-3layer-overview.json"
mv /tmp/datasources.yml "$DS_DIR/datasources.yml"

cd REPLACE_REMOTE_ROOT
if docker compose -f infra/aws/docker-compose.minimal.yml --env-file .env.aws ps grafana >/dev/null 2>&1; then
  docker compose -f infra/aws/docker-compose.minimal.yml --env-file .env.aws up -d --no-deps grafana >/dev/null
else
  docker compose -f infra/aws/docker-compose.minimal.yml --env-file .env.aws up -d --no-deps grafana prometheus layer2-core >/dev/null
fi

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

  REMOTE_CMD="${REMOTE_CMD//REPLACE_REMOTE_ROOT/$REMOTE_ROOT}"

  echo "Applying changes and restarting Grafana..."
  ssh -o StrictHostKeyChecking=accept-new -i "$KEY_PATH" "$USER@$HOST" "$REMOTE_CMD"
}

if [[ "$MODE" == "ssh" ]]; then
  run_ssh_deploy
elif [[ "$MODE" == "api" ]]; then
  run_api_deploy
elif [[ "$MODE" == "ssm" ]]; then
  run_ssm_deploy
else
  if ssh -o BatchMode=yes -o StrictHostKeyChecking=accept-new -o ConnectTimeout=8 -i "$KEY_PATH" "$USER@$HOST" "echo ssh_ok" >/dev/null 2>&1; then
    run_ssh_deploy
  else
    echo "SSH unavailable, trying Grafana API deployment"
    if ! run_api_deploy; then
      echo "Grafana API deploy failed, switching to SSM fallback"
      run_ssm_deploy
    fi
  fi
fi

echo "Deployment complete"
echo "Grafana: $GRAFANA_URL"
echo "Layer2 health: http://$HOST:8001/health"
