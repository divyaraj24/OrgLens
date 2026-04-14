#!/usr/bin/env bash
set -euo pipefail

export AWS_PAGER=""

PROFILE="${AWS_PROFILE:-}"
REGION="${AWS_REGION:-ap-south-2}"
APPLY="false"
CLUSTER_NAME="${CLUSTER_NAME:-orglens-cluster}"
ECR_PREFIX="${ECR_PREFIX:-orglens}"
TASK_EXEC_ROLE_NAME="${TASK_EXEC_ROLE_NAME:-orglens-ecs-task-exec-role}"
OUTPUT_DIR="${OUTPUT_DIR:-infra/aws/ecs}"

usage() {
  cat <<EOF
Usage: $0 [options]

Options:
  --apply                  Execute changes (default is dry-run)
  --region <region>        AWS region (default: ${REGION})
  --profile <profile>      AWS profile (optional)
  --cluster-name <name>    ECS cluster name (default: ${CLUSTER_NAME})
  --ecr-prefix <prefix>    ECR repo prefix (default: ${ECR_PREFIX})
  --task-exec-role <name>  ECS task execution role name (default: ${TASK_EXEC_ROLE_NAME})
  --output-dir <path>      Output dir for generated task defs (default: ${OUTPUT_DIR})

What this script does:
  1) Creates ECS cluster
  2) Creates ECR repos for layer1/layer2
  3) Creates CloudWatch log group /orglens/ecs
  4) Creates ECS task execution role with standard policy
  5) Generates Fargate task definition templates
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --apply) APPLY="true"; shift ;;
    --region) REGION="$2"; shift 2 ;;
    --profile) PROFILE="$2"; shift 2 ;;
    --cluster-name) CLUSTER_NAME="$2"; shift 2 ;;
    --ecr-prefix) ECR_PREFIX="$2"; shift 2 ;;
    --task-exec-role) TASK_EXEC_ROLE_NAME="$2"; shift 2 ;;
    --output-dir) OUTPUT_DIR="$2"; shift 2 ;;
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
require_cmd mkdir

aws_cli() {
  if [[ -n "$PROFILE" ]]; then
    aws --no-cli-pager --profile "$PROFILE" --region "$REGION" "$@"
  else
    aws --no-cli-pager --region "$REGION" "$@"
  fi
}

if ! ACCOUNT_ID="$(aws_cli sts get-caller-identity --query Account --output text 2>/dev/null)"; then
  if [[ "$APPLY" == "true" ]]; then
    echo "AWS authentication failed. Re-authenticate and retry." >&2
    exit 1
  fi
  ACCOUNT_ID="000000000000"
  echo "[dry-run] AWS auth unavailable; using placeholder account ID in generated templates."
fi
L1_REPO="${ECR_PREFIX}/layer1-cloud"
L2_REPO="${ECR_PREFIX}/layer2-core"
LOG_GROUP="/orglens/ecs"

mkdir -p "$OUTPUT_DIR"

if [[ "$APPLY" != "true" ]]; then
  cat <<EOF
[dry-run summary]
- Would create/reuse ECS cluster: ${CLUSTER_NAME}
- Would create/reuse ECR repos: ${L1_REPO}, ${L2_REPO}
- Would create/reuse log group: ${LOG_GROUP}
- Would create/reuse task execution role: ${TASK_EXEC_ROLE_NAME}
- Will generate task def templates in: ${OUTPUT_DIR}
EOF
else
  aws_cli ecs create-cluster --cluster-name "$CLUSTER_NAME" >/dev/null 2>&1 || true
  aws_cli ecr create-repository --repository-name "$L1_REPO" >/dev/null 2>&1 || true
  aws_cli ecr create-repository --repository-name "$L2_REPO" >/dev/null 2>&1 || true
  aws_cli logs create-log-group --log-group-name "$LOG_GROUP" >/dev/null 2>&1 || true

  TRUST_FILE="/tmp/orglens-ecs-trust-policy.json"
  cat > "$TRUST_FILE" <<'JSON'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {"Service": "ecs-tasks.amazonaws.com"},
      "Action": "sts:AssumeRole"
    }
  ]
}
JSON

  aws_cli iam create-role --role-name "$TASK_EXEC_ROLE_NAME" --assume-role-policy-document "file://$TRUST_FILE" >/dev/null 2>&1 || true
  aws_cli iam attach-role-policy --role-name "$TASK_EXEC_ROLE_NAME" --policy-arn arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy >/dev/null 2>&1 || true
fi

TASK_EXEC_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${TASK_EXEC_ROLE_NAME}"
L1_IMAGE="${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${L1_REPO}:latest"
L2_IMAGE="${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${L2_REPO}:latest"

cat > "${OUTPUT_DIR}/taskdef-layer2.json" <<EOF
{
  "family": "orglens-layer2-core",
  "networkMode": "awsvpc",
  "requiresCompatibilities": ["FARGATE"],
  "cpu": "512",
  "memory": "1024",
  "executionRoleArn": "${TASK_EXEC_ROLE_ARN}",
  "containerDefinitions": [
    {
      "name": "layer2-core",
      "image": "${L2_IMAGE}",
      "essential": true,
      "portMappings": [{"containerPort": 8001, "hostPort": 8001, "protocol": "tcp"}],
      "environment": [
        {"name": "ORGLENS_CONFIG", "value": "/app/config.aws.yaml"}
      ],
      "logConfiguration": {
        "logDriver": "awslogs",
        "options": {
          "awslogs-group": "${LOG_GROUP}",
          "awslogs-region": "${REGION}",
          "awslogs-stream-prefix": "layer2"
        }
      }
    }
  ]
}
EOF

cat > "${OUTPUT_DIR}/taskdef-layer1.json" <<EOF
{
  "family": "orglens-layer1-cloud",
  "networkMode": "awsvpc",
  "requiresCompatibilities": ["FARGATE"],
  "cpu": "512",
  "memory": "1024",
  "executionRoleArn": "${TASK_EXEC_ROLE_ARN}",
  "containerDefinitions": [
    {
      "name": "layer1-cloud",
      "image": "${L1_IMAGE}",
      "essential": true,
      "portMappings": [{"containerPort": 8080, "hostPort": 8080, "protocol": "tcp"}],
      "environment": [
        {"name": "ORGLENS_CONFIG", "value": "/app/config.aws.yaml"},
        {"name": "ORGLENS_LAYER1_API_URL", "value": "http://layer2-core:8001/api/ingest"}
      ],
      "logConfiguration": {
        "logDriver": "awslogs",
        "options": {
          "awslogs-group": "${LOG_GROUP}",
          "awslogs-region": "${REGION}",
          "awslogs-stream-prefix": "layer1"
        }
      }
    }
  ]
}
EOF

cat <<EOF
ECS/Fargate preparation complete.
Generated:
- ${OUTPUT_DIR}/taskdef-layer1.json
- ${OUTPUT_DIR}/taskdef-layer2.json

Next:
1) Build/push images to ECR.
2) Create ECS services with appropriate subnets + security groups.
3) Wire ALB listeners/target groups to ECS services.
EOF
