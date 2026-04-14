#!/usr/bin/env bash
set -euo pipefail

# One-command minimal pipeline runner for any public repository.
# Executes against the minimized 3-layer stack by calling APIs only:
#   1) layer1-cloud /api/backfill/start + /api/backfill/status/{job_id}
#   2) layer2-core /api/run/analytics
#   3) layer2-core observability endpoints and /metrics

REPO_URL=""
FROM_DATE_OVERRIDE=""
HOST="${ORGLENS_STACK_HOST:-127.0.0.1}"
CONFIG_PATH="${ORGLENS_REMOTE_CONFIG:-/app/config.aws.yaml}"
RUN_ALL_TIMEOUT="${ORGLENS_RUN_ALL_TIMEOUT:-0}"
SINGLE_REPO_MODE="${ORGLENS_SINGLE_REPO_MODE:-1}"
AUTO_START_LOCAL_STACK="${ORGLENS_AUTO_START_LOCAL_STACK:-1}"
COMPOSE_FILE="${ORGLENS_COMPOSE_FILE:-infra/aws/docker-compose.minimal.yml}"
COMPOSE_ENV_FILE="${ORGLENS_COMPOSE_ENV_FILE:-.env.aws}"

usage() {
  cat <<EOF
Usage:
  scripts/run_minimal_pipeline.sh --repo-url <https://github.com/owner/repo(.git)> [options]

Options:
  --from-date <ISO8601>    Override start date (default: repository created_at)
  --host <ip-or-dns>       Stack host (default: $HOST)
  --config <path>          Remote config path in container (default: $CONFIG_PATH)
  --run-timeout <seconds>  Timeout for /api/run/all call; 0 = no timeout (default: $RUN_ALL_TIMEOUT)
  --single-repo-mode <0|1> If 1, clears DB tables before run (default: $SINGLE_REPO_MODE)
  --auto-start-local <0|1> Auto-start local Docker stack when host is local (default: $AUTO_START_LOCAL_STACK)
  --compose-file <path>    Docker compose file for local stack (default: $COMPOSE_FILE)
  --compose-env <path>     Docker compose env file for local stack (default: $COMPOSE_ENV_FILE)
  -h, --help               Show help

Environment overrides:
  ORGLENS_STACK_HOST, ORGLENS_REMOTE_CONFIG, ORGLENS_RUN_ALL_TIMEOUT,
  ORGLENS_SINGLE_REPO_MODE, ORGLENS_AUTO_START_LOCAL_STACK,
  ORGLENS_COMPOSE_FILE, ORGLENS_COMPOSE_ENV_FILE
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo-url)
      REPO_URL="${2:-}"
      shift 2
      ;;
    --from-date)
      FROM_DATE_OVERRIDE="${2:-}"
      shift 2
      ;;
    --host)
      HOST="${2:-}"
      shift 2
      ;;
    --config)
      CONFIG_PATH="${2:-}"
      shift 2
      ;;
    --run-timeout)
      RUN_ALL_TIMEOUT="${2:-}"
      shift 2
      ;;
    --single-repo-mode)
      SINGLE_REPO_MODE="${2:-}"
      shift 2
      ;;
    --auto-start-local)
      AUTO_START_LOCAL_STACK="${2:-}"
      shift 2
      ;;
    --compose-file)
      COMPOSE_FILE="${2:-}"
      shift 2
      ;;
    --compose-env)
      COMPOSE_ENV_FILE="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ -z "$REPO_URL" ]]; then
  echo "Missing required argument: --repo-url" >&2
  usage
  exit 1
fi

for cmd in curl jq; do
  command -v "$cmd" >/dev/null 2>&1 || {
    echo "Missing required command: $cmd" >&2
    exit 1
  }
done

REPO_CLEAN="${REPO_URL%.git}"
OWNER_REPO="$(echo "$REPO_CLEAN" | sed -E 's#https?://github.com/##')"
if [[ "$OWNER_REPO" != */* ]]; then
  echo "Invalid GitHub repository URL: $REPO_URL" >&2
  exit 1
fi

FROM_DATE="$FROM_DATE_OVERRIDE"
if [[ -z "$FROM_DATE" ]]; then
  echo "Resolving repository creation date from GitHub API..."
  CREATED_AT="$(curl -fsSL "https://api.github.com/repos/$OWNER_REPO" | jq -r '.created_at')"
  if [[ -z "$CREATED_AT" || "$CREATED_AT" == "null" ]]; then
    echo "Failed to resolve repository creation date from GitHub API." >&2
    exit 1
  fi
  FROM_DATE="$CREATED_AT"
fi

echo "== Minimal Pipeline Run =="
echo "Repo URL : $REPO_URL"
echo "Repo Key : $OWNER_REPO"
echo "From Date: $FROM_DATE"
echo "Host     : $HOST"
echo "SingleRepoMode: $SINGLE_REPO_MODE"

IS_LOCAL_HOST=0
if [[ "$HOST" == "127.0.0.1" || "$HOST" == "localhost" ]]; then
  IS_LOCAL_HOST=1
fi

if [[ "$IS_LOCAL_HOST" == "1" && "$AUTO_START_LOCAL_STACK" == "1" ]]; then
  command -v docker >/dev/null 2>&1 || {
    echo "docker is required for local stack startup" >&2
    exit 1
  }
  echo "Starting local 3-layer stack via docker compose..."
  docker compose -f "$COMPOSE_FILE" --env-file "$COMPOSE_ENV_FILE" up -d --build postgres layer2-core layer1-cloud prometheus grafana >/dev/null
fi

wait_for_health() {
  local url="$1"
  local name="$2"
  local retries="${3:-90}"
  local i

  for ((i=1; i<=retries; i++)); do
    if curl -fsSL --max-time 5 "$url" >/dev/null 2>&1; then
      echo "$name healthy"
      return 0
    fi
    sleep 2
  done

  echo "Timed out waiting for $name at $url" >&2
  return 1
}

verify_clean_state_after_reset() {
  if [[ "$IS_LOCAL_HOST" == "1" ]]; then
    local audit
    audit="$(docker compose -f "$COMPOSE_FILE" --env-file "$COMPOSE_ENV_FILE" exec -T postgres sh -lc 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -At <<"SQL"
SELECT
  (SELECT COUNT(*) FROM raw_events),
  (SELECT COUNT(*) FROM contribution_windows),
  (SELECT COUNT(*) FROM bus_factor_scores),
  (SELECT COUNT(*) FROM ownership_drift),
  (SELECT COUNT(*) FROM succession_risk),
  (SELECT COUNT(*) FROM ingest_metrics),
  (SELECT COUNT(*) FROM ingest_queue),
  (SELECT COUNT(*) FROM pipeline_state WHERE key = $$active_repo$$);
SQL')"

    local raw cw bf drift succ metrics queue active_count
    IFS='|' read -r raw cw bf drift succ metrics queue active_count <<<"$audit"

    if [[ "${raw:-0}" != "0" || "${cw:-0}" != "0" || "${bf:-0}" != "0" || "${drift:-0}" != "0" || "${succ:-0}" != "0" || "${metrics:-0}" != "0" || "${queue:-0}" != "0" || "${active_count:-0}" != "0" ]]; then
      echo "Error: reset did not fully clear DB state (raw=$raw cw=$cw bf=$bf drift=$drift succ=$succ metrics=$metrics queue=$queue active_repo_rows=$active_count)" >&2
      exit 1
    fi
    echo "DB reset verified: clean state confirmed"
    return 0
  fi

  local repos_len
  repos_len="$(curl -fsSL --max-time 20 "http://$HOST:8001/api/repos" | jq -r '.repos | length')"
  if [[ "$repos_len" != "0" ]]; then
    echo "Error: reset verification failed remotely; /api/repos still contains entries" >&2
    exit 1
  fi

  local status_json
  status_json="$(curl -fsSL --max-time 20 "http://$HOST:8001/api/ingest/status")"
  local recv proc dup dead depth
  recv="$(echo "$status_json" | jq -r '.events_received_today // 0')"
  proc="$(echo "$status_json" | jq -r '.events_processed_today // 0')"
  dup="$(echo "$status_json" | jq -r '.duplicates_dropped_today // 0')"
  dead="$(echo "$status_json" | jq -r '.dead_letters_today // 0')"
  depth="$(echo "$status_json" | jq -r '.redis_queue_depth // 0')"
  if [[ "$recv" != "0" || "$proc" != "0" || "$dup" != "0" || "$dead" != "0" || "$depth" != "0" ]]; then
    echo "Error: reset verification failed remotely; ingest status not zeroed (received=$recv processed=$proc duplicates=$dup dead_letters=$dead queue_depth=$depth)" >&2
    exit 1
  fi
  echo "Remote reset verified: clean state confirmed"
}

# Ensure Layer 1 and Layer 2 are reachable.
wait_for_health "http://$HOST:8080/health" "layer1-cloud"
wait_for_health "http://$HOST:8001/health" "layer2-core"

if [[ "$SINGLE_REPO_MODE" == "1" ]]; then
  echo "Clearing database tables to enforce single-repo mode..."
  curl -fsSL --max-time 30 -X POST "http://$HOST:8001/api/admin/reset-repo-data" >/dev/null
  verify_clean_state_after_reset
fi

if [[ "$SINGLE_REPO_MODE" != "1" ]]; then
  echo "Error: single-repo mode is mandatory." >&2
  exit 1
fi

echo "Starting remote backfill job on layer1-cloud..."
BACKFILL_START_PAYLOAD="{\"repo_url\":\"$REPO_URL\",\"from_date\":\"$FROM_DATE\"}"
BACKFILL_START_JSON="$(curl -fsSL --max-time 60 \
  -H 'Content-Type: application/json' \
  --data "$BACKFILL_START_PAYLOAD" \
  "http://$HOST:8080/api/backfill/start")"
JOB_ID="$(echo "$BACKFILL_START_JSON" | jq -r '.job_id')"
if [[ -z "$JOB_ID" || "$JOB_ID" == "null" ]]; then
  echo "Failed to start backfill job: $BACKFILL_START_JSON" >&2
  exit 1
fi

echo "Backfill job id: $JOB_ID"
LAST_QUEUED=-1
START_TS="$(date +%s)"
while true; do
  STATUS_JSON="$(curl -fsSL --max-time 30 "http://$HOST:8080/api/backfill/status/$JOB_ID")"
  STATUS="$(echo "$STATUS_JSON" | jq -r '.status')"
  QUEUED="$(echo "$STATUS_JSON" | jq -r '.queued // 0')"

  if [[ "$QUEUED" != "$LAST_QUEUED" ]]; then
    echo "Backfill progress: queued_events=$QUEUED status=$STATUS"
    LAST_QUEUED="$QUEUED"
  fi

  if [[ "$STATUS" == "completed" ]]; then
    break
  fi
  if [[ "$STATUS" == "failed" ]]; then
    echo "Backfill failed: $(echo "$STATUS_JSON" | jq -r '.error // "unknown error"')" >&2
    exit 1
  fi

  NOW_TS="$(date +%s)"
  if [[ "$RUN_ALL_TIMEOUT" != "0" ]] && (( NOW_TS - START_TS > RUN_ALL_TIMEOUT )); then
    echo "Backfill timed out after ${RUN_ALL_TIMEOUT}s" >&2
    exit 1
  fi
  sleep 2
done

ANALYTICS_JSON="$(curl -fsSL --max-time 120 -X POST \
  -H 'Content-Type: application/json' \
  --data "{\"repo\":\"$OWNER_REPO\",\"log_level\":\"INFO\"}" \
  "http://$HOST:8001/api/run/analytics")"

echo "$ANALYTICS_JSON" >/tmp/orglens_min_run.json

# Validate observability data availability.
echo "Running verification checks..."
curl -fsSL --max-time 20 "http://$HOST:8001/health" >/dev/null
SUMMARY="$(curl -fsSL --max-time 20 "http://$HOST:8001/api/risk/summary?repo=$OWNER_REPO")"
METRIC="$(curl -fsSL --max-time 20 "http://$HOST:8001/metrics" | grep -F "repo=\"$OWNER_REPO\"" | head -n 1 || true)"
REPOS_JSON="$(curl -fsSL --max-time 20 "http://$HOST:8001/api/repos")"

echo ""
echo "== Verification Summary =="
echo "Run result     : $(cat /tmp/orglens_min_run.json)"
echo "Risk summary  : $SUMMARY"
echo "Repos in DB    : $REPOS_JSON"
if [[ -n "$METRIC" ]]; then
  echo "Sample metric : $METRIC"
else
  echo "Sample metric : (no repo-specific metric line found yet; scrape may still be catching up)"
fi

echo ""
echo "Success: minimal end-to-end pipeline completed for $OWNER_REPO"
echo "Grafana      : http://$HOST:3000/d/orglens-3layer-overview/orglens-3-layer-overview?var-repo=$OWNER_REPO"
