#!/usr/bin/env bash
set -euo pipefail

export AWS_PAGER=""

PROFILE="${AWS_PROFILE:-}"
REGION="${AWS_REGION:-ap-south-2}"
APPLY="false"
STACK_NAME="${STACK_NAME:-orglens}"
INSTANCE_ID="${INSTANCE_ID:-}"
INSTANCE_NAME="${INSTANCE_NAME:-orglens-stack}"
VPC_ID="${VPC_ID:-}"
SUBNET_IDS="${SUBNET_IDS:-}"
CERT_ARN="${CERT_ARN:-}"
PUBLIC_API_CIDR="${PUBLIC_API_CIDR:-0.0.0.0/0}"

usage() {
  cat <<EOF
Usage: $0 [options]

Options:
  --apply                  Execute changes (default is dry-run)
  --region <region>        AWS region (default: ${REGION})
  --profile <profile>      AWS profile (optional)
  --stack-name <name>      Name prefix for ALB resources (default: ${STACK_NAME})
  --instance-id <id>       EC2 instance to register as ALB target
  --instance-name <name>   EC2 Name tag when resolving instance (default: ${INSTANCE_NAME})
  --vpc-id <vpc-id>        VPC ID (default: resolve default VPC)
  --subnet-ids <a,b,c>     Comma-separated subnet IDs (default: all default VPC subnets)
  --cert-arn <arn>         ACM certificate ARN. If set, creates HTTPS listener.
  --public-api-cidr <cidr> CIDR for ALB ingress (default: ${PUBLIC_API_CIDR})

Notes:
  - Dry-run by default. Add --apply to provision resources.
  - Creates path-based routing:
      /webhook* and /api/backfill* -> Layer1 target group (8080)
      default -> Layer2 target group (8001)
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --apply) APPLY="true"; shift ;;
    --region) REGION="$2"; shift 2 ;;
    --profile) PROFILE="$2"; shift 2 ;;
    --stack-name) STACK_NAME="$2"; shift 2 ;;
    --instance-id) INSTANCE_ID="$2"; shift 2 ;;
    --instance-name) INSTANCE_NAME="$2"; shift 2 ;;
    --vpc-id) VPC_ID="$2"; shift 2 ;;
    --subnet-ids) SUBNET_IDS="$2"; shift 2 ;;
    --cert-arn) CERT_ARN="$2"; shift 2 ;;
    --public-api-cidr) PUBLIC_API_CIDR="$2"; shift 2 ;;
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

run() {
  if [[ "$APPLY" == "true" ]]; then
    "$@"
  else
    echo "[dry-run] $*"
  fi
}

echo "Validating identity..."
if ! ACCOUNT_ID="$(aws_cli sts get-caller-identity --query Account --output text 2>/dev/null)"; then
  if [[ "$APPLY" == "true" ]]; then
    echo "AWS authentication failed. Re-authenticate and retry." >&2
    exit 1
  fi
  cat <<EOF
[dry-run without AWS auth]
- AWS credentials are not currently available.
- Re-authenticate to resolve VPC/subnet/instance dynamically.
- You can still execute with explicit flags after auth:
  $0 --apply --instance-id <id> --vpc-id <vpc-id> --subnet-ids <subnet1,subnet2>
EOF
  exit 0
fi
echo "Account: ${ACCOUNT_ID}, Region: ${REGION}"

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

if [[ -z "$INSTANCE_ID" ]]; then
  INSTANCE_ID="$(aws_cli ec2 describe-instances \
    --filters Name=tag:Name,Values="$INSTANCE_NAME" Name=instance-state-name,Values=running \
    --query 'Reservations[0].Instances[0].InstanceId' --output text)"
fi
if [[ "$INSTANCE_ID" == "None" || -z "$INSTANCE_ID" ]]; then
  echo "No running instance found. Provide --instance-id." >&2
  exit 1
fi

INSTANCE_SG_ID="$(aws_cli ec2 describe-instances --instance-ids "$INSTANCE_ID" --query 'Reservations[0].Instances[0].SecurityGroups[0].GroupId' --output text)"

ALB_NAME="${STACK_NAME}-alb"
ALB_SG_NAME="${STACK_NAME}-alb-sg"
TG_L2_NAME="${STACK_NAME}-l2"
TG_L1_NAME="${STACK_NAME}-l1"

ALB_SG_ID="$(aws_cli ec2 describe-security-groups --filters Name=vpc-id,Values="$VPC_ID" Name=group-name,Values="$ALB_SG_NAME" --query 'SecurityGroups[0].GroupId' --output text 2>/dev/null || true)"
if [[ "$ALB_SG_ID" == "None" || -z "$ALB_SG_ID" ]]; then
  if [[ "$APPLY" == "true" ]]; then
    ALB_SG_ID="$(aws_cli ec2 create-security-group --vpc-id "$VPC_ID" --group-name "$ALB_SG_NAME" --description "OrgLens ALB SG" --query GroupId --output text)"
  else
    ALB_SG_ID="sg-dryrun"
    echo "[dry-run] aws ec2 create-security-group --vpc-id $VPC_ID --group-name $ALB_SG_NAME ..."
  fi
fi

run aws_cli ec2 authorize-security-group-ingress --group-id "$ALB_SG_ID" --ip-permissions "IpProtocol=tcp,FromPort=80,ToPort=80,IpRanges=[{CidrIp=$PUBLIC_API_CIDR,Description=OrgLens-ALB-HTTP}]"
if [[ -n "$CERT_ARN" ]]; then
  run aws_cli ec2 authorize-security-group-ingress --group-id "$ALB_SG_ID" --ip-permissions "IpProtocol=tcp,FromPort=443,ToPort=443,IpRanges=[{CidrIp=$PUBLIC_API_CIDR,Description=OrgLens-ALB-HTTPS}]"
fi
run aws_cli ec2 authorize-security-group-ingress --group-id "$INSTANCE_SG_ID" --ip-permissions "IpProtocol=tcp,FromPort=8001,ToPort=8001,UserIdGroupPairs=[{GroupId=$ALB_SG_ID,Description=ALB-to-L2}]"
run aws_cli ec2 authorize-security-group-ingress --group-id "$INSTANCE_SG_ID" --ip-permissions "IpProtocol=tcp,FromPort=8080,ToPort=8080,UserIdGroupPairs=[{GroupId=$ALB_SG_ID,Description=ALB-to-L1}]"

if [[ "$APPLY" == "true" ]]; then
  TG_L2_ARN="$(aws_cli elbv2 create-target-group --name "$TG_L2_NAME" --protocol HTTP --port 8001 --target-type instance --vpc-id "$VPC_ID" --health-check-path /health --query 'TargetGroups[0].TargetGroupArn' --output text 2>/dev/null || aws_cli elbv2 describe-target-groups --names "$TG_L2_NAME" --query 'TargetGroups[0].TargetGroupArn' --output text)"
  TG_L1_ARN="$(aws_cli elbv2 create-target-group --name "$TG_L1_NAME" --protocol HTTP --port 8080 --target-type instance --vpc-id "$VPC_ID" --health-check-path /health --query 'TargetGroups[0].TargetGroupArn' --output text 2>/dev/null || aws_cli elbv2 describe-target-groups --names "$TG_L1_NAME" --query 'TargetGroups[0].TargetGroupArn' --output text)"
  aws_cli elbv2 register-targets --target-group-arn "$TG_L2_ARN" --targets Id="$INSTANCE_ID",Port=8001 >/dev/null
  aws_cli elbv2 register-targets --target-group-arn "$TG_L1_ARN" --targets Id="$INSTANCE_ID",Port=8080 >/dev/null

  ALB_ARN="$(aws_cli elbv2 create-load-balancer --name "$ALB_NAME" --subnets "${SUBNET_IDS//,/ }" --security-groups "$ALB_SG_ID" --scheme internet-facing --type application --query 'LoadBalancers[0].LoadBalancerArn' --output text 2>/dev/null || aws_cli elbv2 describe-load-balancers --names "$ALB_NAME" --query 'LoadBalancers[0].LoadBalancerArn' --output text)"

  if [[ -n "$CERT_ARN" ]]; then
    LISTENER_ARN="$(aws_cli elbv2 create-listener --load-balancer-arn "$ALB_ARN" --protocol HTTPS --port 443 --certificates CertificateArn="$CERT_ARN" --default-actions Type=forward,TargetGroupArn="$TG_L2_ARN" --query 'Listeners[0].ListenerArn' --output text 2>/dev/null || aws_cli elbv2 describe-listeners --load-balancer-arn "$ALB_ARN" --query 'Listeners[?Port==`443`][0].ListenerArn' --output text)"
    aws_cli elbv2 create-listener --load-balancer-arn "$ALB_ARN" --protocol HTTP --port 80 --default-actions Type=redirect,RedirectConfig='{Protocol="HTTPS",Port="443",StatusCode="HTTP_301"}' >/dev/null 2>&1 || true
  else
    LISTENER_ARN="$(aws_cli elbv2 create-listener --load-balancer-arn "$ALB_ARN" --protocol HTTP --port 80 --default-actions Type=forward,TargetGroupArn="$TG_L2_ARN" --query 'Listeners[0].ListenerArn' --output text 2>/dev/null || aws_cli elbv2 describe-listeners --load-balancer-arn "$ALB_ARN" --query 'Listeners[?Port==`80`][0].ListenerArn' --output text)"
  fi

  aws_cli elbv2 create-rule --listener-arn "$LISTENER_ARN" --priority 10 --conditions Field=path-pattern,Values='/webhook*','/api/backfill*' --actions Type=forward,TargetGroupArn="$TG_L1_ARN" >/dev/null 2>&1 || true

  ALB_DNS="$(aws_cli elbv2 describe-load-balancers --load-balancer-arns "$ALB_ARN" --query 'LoadBalancers[0].DNSName' --output text)"
  echo "ALB ready: ${ALB_DNS}"
  echo "Layer1 paths -> /webhook, /api/backfill"
  echo "Layer2 default -> all other paths"
else
  cat <<EOF
[dry-run summary]
- Would create/reuse ALB SG: ${ALB_SG_NAME}
- Would create/reuse target groups: ${TG_L2_NAME}(8001), ${TG_L1_NAME}(8080)
- Would register instance: ${INSTANCE_ID}
- Would create/reuse ALB: ${ALB_NAME}
- Would create listener: $([[ -n "$CERT_ARN" ]] && echo HTTPS:443 || echo HTTP:80)
- Would add path rule: /webhook* and /api/backfill* -> Layer1
EOF
fi
