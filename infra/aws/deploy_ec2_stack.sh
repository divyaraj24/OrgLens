#!/usr/bin/env bash
set -euo pipefail

export AWS_PAGER=""

PROFILE="${AWS_PROFILE:-}"
REGION="${AWS_REGION:-ap-south-2}"
INSTANCE_TYPE="${INSTANCE_TYPE:-t4g.micro}"
INSTANCE_NAME="${INSTANCE_NAME:-orglens-stack}"
KEY_NAME="${KEY_NAME:-orglens-ec2-key}"
SG_NAME="${SG_NAME:-orglens-stack-sg}"
ROLE_NAME="${ROLE_NAME:-orglens-ec2-role}"
INSTANCE_PROFILE_NAME="${INSTANCE_PROFILE_NAME:-orglens-ec2-profile}"
VOLUME_SIZE_GB="${VOLUME_SIZE_GB:-20}"
SSM_PREFIX="${ORGLENS_SSM_PREFIX:-/orglens/prod}"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
KEY_DIR="$REPO_ROOT/infra/aws/keys"
mkdir -p "$KEY_DIR"

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1" >&2
    exit 1
  fi
}

require_cmd aws
require_cmd jq
require_cmd tar
require_cmd curl

aws_cli() {
  if [[ -n "$PROFILE" ]]; then
    aws --profile "$PROFILE" --region "$REGION" "$@"
  else
    aws --region "$REGION" "$@"
  fi
}

aws_iam() {
  if [[ -n "$PROFILE" ]]; then
    aws --profile "$PROFILE" iam "$@"
  else
    aws iam "$@"
  fi
}

aws_s3() {
  if [[ -n "$PROFILE" ]]; then
    aws --profile "$PROFILE" --region "$REGION" s3 "$@"
  else
    aws --region "$REGION" s3 "$@"
  fi
}

echo "Validating AWS credentials/profile..."
ACCOUNT_ID="$(aws_cli sts get-caller-identity --query Account --output text)"
USER_ARN="$(aws_cli sts get-caller-identity --query Arn --output text)"
echo "Using account: $ACCOUNT_ID"
echo "Using identity: $USER_ARN"

BUCKET_NAME="orglens-deploy-${ACCOUNT_ID}-${REGION}"
TIMESTAMP="$(date -u +%Y%m%dT%H%M%SZ)"
ARCHIVE_PATH="/tmp/orglens-deploy-${TIMESTAMP}.tgz"
S3_KEY="releases/orglens-${TIMESTAMP}.tgz"

echo "Packaging deployment bundle..."
cd "$REPO_ROOT"
tar \
  --exclude='.git' \
  --exclude='.venv' \
  --exclude='__pycache__' \
  --exclude='*.pyc' \
  --exclude='*.pyo' \
  --exclude='._*' \
  --exclude='.DS_Store' \
  --exclude='.pytest_cache' \
  --exclude='orglens_agent.egg-info' \
  -czf "$ARCHIVE_PATH" .

echo "Ensuring deployment S3 bucket exists: $BUCKET_NAME"
if ! aws_s3 ls "s3://$BUCKET_NAME" >/dev/null 2>&1; then
  aws_cli s3api create-bucket \
    --bucket "$BUCKET_NAME" \
    --create-bucket-configuration "LocationConstraint=$REGION" >/dev/null
fi

aws_cli s3api put-bucket-versioning --bucket "$BUCKET_NAME" --versioning-configuration Status=Enabled >/dev/null

echo "Uploading bundle to S3..."
aws_s3 cp "$ARCHIVE_PATH" "s3://$BUCKET_NAME/$S3_KEY" >/dev/null
PRESIGNED_URL="$(aws_cli s3 presign "s3://$BUCKET_NAME/$S3_KEY" --expires-in 86400)"

KEY_PATH="$KEY_DIR/${KEY_NAME}.pem"
echo "Ensuring key pair exists: $KEY_NAME"
if ! aws_cli ec2 describe-key-pairs --key-names "$KEY_NAME" >/dev/null 2>&1; then
  aws_cli ec2 create-key-pair --key-name "$KEY_NAME" --query KeyMaterial --output text > "$KEY_PATH"
  chmod 400 "$KEY_PATH"
  echo "Created key pair material at: $KEY_PATH"
else
  if [[ -f "$KEY_PATH" ]]; then
    chmod 400 "$KEY_PATH"
    echo "Using existing local key material at: $KEY_PATH"
  else
    echo "Key pair exists in AWS but local PEM not found at $KEY_PATH" >&2
    echo "You may not be able to SSH unless you recreate keypair or use SSM." >&2
  fi
fi

echo "Ensuring IAM role and instance profile exist..."
TRUST_POLICY_FILE="/tmp/orglens-ec2-trust-policy.json"
cat > "$TRUST_POLICY_FILE" <<'JSON'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {"Service": "ec2.amazonaws.com"},
      "Action": "sts:AssumeRole"
    }
  ]
}
JSON

if ! aws_iam get-role --role-name "$ROLE_NAME" >/dev/null 2>&1; then
  aws_iam create-role --role-name "$ROLE_NAME" --assume-role-policy-document "file://$TRUST_POLICY_FILE" >/dev/null
fi

aws_iam attach-role-policy --role-name "$ROLE_NAME" --policy-arn arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore >/dev/null
aws_iam attach-role-policy --role-name "$ROLE_NAME" --policy-arn arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess >/dev/null

SSM_SECRETS_POLICY_FILE="/tmp/orglens-ssm-secrets-policy.json"
cat > "$SSM_SECRETS_POLICY_FILE" <<JSON
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "ssm:GetParameter",
        "ssm:GetParameters",
        "ssm:GetParametersByPath"
      ],
      "Resource": [
        "arn:aws:ssm:${REGION}:${ACCOUNT_ID}:parameter${SSM_PREFIX}*"
      ]
    }
  ]
}
JSON
aws_iam put-role-policy --role-name "$ROLE_NAME" --policy-name "OrgLensSsmSecretsRead" --policy-document "file://$SSM_SECRETS_POLICY_FILE" >/dev/null

if ! aws_iam get-instance-profile --instance-profile-name "$INSTANCE_PROFILE_NAME" >/dev/null 2>&1; then
  aws_iam create-instance-profile --instance-profile-name "$INSTANCE_PROFILE_NAME" >/dev/null
fi

if ! aws_iam get-instance-profile --instance-profile-name "$INSTANCE_PROFILE_NAME" \
  --query "InstanceProfile.Roles[?RoleName=='$ROLE_NAME'] | length(@)" --output text | grep -q '^1$'; then
  aws_iam add-role-to-instance-profile --instance-profile-name "$INSTANCE_PROFILE_NAME" --role-name "$ROLE_NAME" >/dev/null || true
fi

echo "Waiting for IAM propagation..."
sleep 10

echo "Resolving default VPC/subnet..."
VPC_ID="$(aws_cli ec2 describe-vpcs --filters Name=isDefault,Values=true --query 'Vpcs[0].VpcId' --output text)"
if [[ "$VPC_ID" == "None" || -z "$VPC_ID" ]]; then
  echo "No default VPC found. Create one or set up networking manually." >&2
  exit 1
fi

SUBNET_ID="$(aws_cli ec2 describe-subnets \
  --filters Name=vpc-id,Values="$VPC_ID" Name=map-public-ip-on-launch,Values=true \
  --query 'Subnets[0].SubnetId' --output text)"
if [[ "$SUBNET_ID" == "None" || -z "$SUBNET_ID" ]]; then
  SUBNET_ID="$(aws_cli ec2 describe-subnets --filters Name=vpc-id,Values="$VPC_ID" --query 'Subnets[0].SubnetId' --output text)"
fi

echo "Ensuring security group exists: $SG_NAME"
SG_ID="$(aws_cli ec2 describe-security-groups --filters Name=vpc-id,Values="$VPC_ID" Name=group-name,Values="$SG_NAME" --query 'SecurityGroups[0].GroupId' --output text 2>/dev/null || true)"
if [[ "$SG_ID" == "None" || -z "$SG_ID" ]]; then
  SG_ID="$(aws_cli ec2 create-security-group --group-name "$SG_NAME" --description "OrgLens stack SG" --vpc-id "$VPC_ID" --query GroupId --output text)"
fi

MY_IP="$(curl -s https://checkip.amazonaws.com | tr -d '[:space:]')"
SSH_CIDR="${SSH_CIDR:-${MY_IP}/32}"
PUBLIC_API_CIDR="${PUBLIC_API_CIDR:-0.0.0.0/0}"
ADMIN_CIDR="${ADMIN_CIDR:-$SSH_CIDR}"

authorize_ingress() {
  local port="$1"
  local cidr="$2"
  aws_cli ec2 authorize-security-group-ingress \
    --group-id "$SG_ID" \
    --ip-permissions "IpProtocol=tcp,FromPort=$port,ToPort=$port,IpRanges=[{CidrIp=$cidr,Description=OrgLens-$port}]" >/dev/null 2>&1 || true
}

authorize_ingress 22 "$SSH_CIDR"
authorize_ingress 3000 "$ADMIN_CIDR"
authorize_ingress 8001 "$PUBLIC_API_CIDR"
authorize_ingress 8080 "$PUBLIC_API_CIDR"
authorize_ingress 9090 "$ADMIN_CIDR"

echo "Resolving Ubuntu ARM64 AMI via SSM..."
AMI_ID="$(aws_cli ssm get-parameter \
  --name /aws/service/canonical/ubuntu/server/24.04/stable/current/arm64/hvm/ebs-gp3/ami-id \
  --query 'Parameter.Value' --output text)"

USER_DATA_FILE="/tmp/orglens-userdata-${TIMESTAMP}.sh"
cat > "$USER_DATA_FILE" <<EOF
#!/bin/bash
set -euxo pipefail
export DEBIAN_FRONTEND=noninteractive

apt-get update
apt-get install -y docker.io curl jq openssl
if ! apt-get install -y docker-compose-v2; then
  apt-get install -y docker-compose-plugin
fi
systemctl enable docker
systemctl start docker
usermod -aG docker ubuntu || true

mkdir -p /opt/orglens
curl -fsSL "${PRESIGNED_URL}" -o /opt/orglens/deploy.tgz
tar -xzf /opt/orglens/deploy.tgz -C /opt/orglens --strip-components=1

cd /opt/orglens

# Replace placeholder secrets if present to avoid accidental public defaults.
randhex() { openssl rand -hex 24; }
if grep -q '^ORGLENS_API_KEY=replace_me$' .env.aws; then
  sed -i "s/^ORGLENS_API_KEY=.*/ORGLENS_API_KEY=\$(randhex)/" .env.aws
fi
if grep -q '^ORGLENS_INGEST_SIGNING_SECRET=replace_me$' .env.aws; then
  sed -i "s/^ORGLENS_INGEST_SIGNING_SECRET=.*/ORGLENS_INGEST_SIGNING_SECRET=\$(randhex)/" .env.aws
fi
if grep -q '^ORGLENS_GITHUB_TOKEN=replace_me$' .env.aws; then
  sed -i "s/^ORGLENS_GITHUB_TOKEN=.*/ORGLENS_GITHUB_TOKEN=/" .env.aws
fi
if grep -q '^GRAFANA_ADMIN_PASSWORD=admin$' .env.aws; then
  sed -i "s/^GRAFANA_ADMIN_PASSWORD=.*/GRAFANA_ADMIN_PASSWORD=\$(randhex)/" .env.aws
fi

if [[ -x infra/aws/sync_ssm_secrets.sh ]]; then
  ORGLENS_SSM_PREFIX="${SSM_PREFIX}" infra/aws/sync_ssm_secrets.sh --prefix "${SSM_PREFIX}" --env-file .env.aws || true
fi

docker compose -f infra/aws/docker-compose.minimal.yml --env-file .env.aws up -d --build
EOF

echo "Launching EC2 instance..."
INSTANCE_ID="$(aws_cli ec2 run-instances \
  --image-id "$AMI_ID" \
  --instance-type "$INSTANCE_TYPE" \
  --key-name "$KEY_NAME" \
  --security-group-ids "$SG_ID" \
  --subnet-id "$SUBNET_ID" \
  --associate-public-ip-address \
  --iam-instance-profile Name="$INSTANCE_PROFILE_NAME" \
  --block-device-mappings "[{\"DeviceName\":\"/dev/sda1\",\"Ebs\":{\"VolumeSize\":$VOLUME_SIZE_GB,\"VolumeType\":\"gp3\",\"DeleteOnTermination\":true}}]" \
  --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$INSTANCE_NAME},{Key=Project,Value=OrgLens}]" \
  --user-data "file://$USER_DATA_FILE" \
  --query 'Instances[0].InstanceId' --output text)"

echo "Instance launched: $INSTANCE_ID"

echo "Waiting for instance to be running..."
aws_cli ec2 wait instance-running --instance-ids "$INSTANCE_ID"

PUBLIC_IP="$(aws_cli ec2 describe-instances --instance-ids "$INSTANCE_ID" --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)"
PUBLIC_DNS="$(aws_cli ec2 describe-instances --instance-ids "$INSTANCE_ID" --query 'Reservations[0].Instances[0].PublicDnsName' --output text)"

echo "Waiting for EC2 status checks..."
aws_cli ec2 wait instance-status-ok --instance-ids "$INSTANCE_ID"

cat <<OUT

Deployment initiated successfully.
Instance ID: $INSTANCE_ID
Public IP: $PUBLIC_IP
Public DNS: $PUBLIC_DNS
Region: $REGION

Expected service URLs (allow 3-8 minutes for bootstrap + container builds):
- Layer2 health: http://$PUBLIC_IP:8001/health
- Layer1 health: http://$PUBLIC_IP:8080/health
- Prometheus: http://$PUBLIC_IP:9090
- Grafana: http://$PUBLIC_IP:3000

SSH (if PEM exists):
ssh -i "$KEY_PATH" ubuntu@$PUBLIC_IP

Notes:
- To watch cloud-init completion: sudo tail -f /var/log/cloud-init-output.log
- Current SG allows ports 22,3000,8001,8080,9090.
- `PUBLIC_API_CIDR` controls 8001/8080 exposure; `ADMIN_CIDR` controls 3000/9090 exposure.
OUT
