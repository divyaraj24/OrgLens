# OrgLens Stable Cloud Deployment (AWS)

This deployment targets AWS EC2 using the simplified 3-layer model:
- Layer 1 cloud ingestion
- Layer 2 core processing + analytics API
- Layer 3 observability (Prometheus + Grafana)

## Cloud Layer Confirmation

- Layer 1: `layer1-cloud`
- Layer 2: `layer2-core`
- Layer 3: `prometheus` + `grafana`

## Prerequisites

- Ubuntu 22.04+ EC2 instance
- Security Group open ports:
  - 8001 (Layer 2 core API + metrics)
  - 8080 (Layer 1 cloud ingest/backfill)
  - 3000 (Grafana)
  - 9090 (Prometheus, optional external)
- Docker and docker-compose installed

## 1) Prepare Environment

```bash
cp .env.aws.example .env.aws
# edit values in .env.aws
```

Minimum variables to set:

- ORGLENS_GITHUB_TOKEN
- ORGLENS_API_KEY
- ORGLENS_INGEST_SIGNING_SECRET
- POSTGRES_USER
- POSTGRES_PASSWORD
- POSTGRES_DB
- GRAFANA_ADMIN_USER
- GRAFANA_ADMIN_PASSWORD

## 2) Configure Repositories

Edit `config.aws.yaml`:

- update `repos`
- set `output.api.url` to Layer 2 core ingest endpoint

## 3) Build and Start

```bash
docker-compose -f infra/aws/docker-compose.minimal.yml --env-file .env.aws up -d --build
```

## 4) Verify Health

```bash
curl -s http://localhost:8001/health
curl -s http://localhost:8080/health
curl -s http://localhost:8001/metrics | head -n 20
```

Grafana:

- URL: http://<EC2_PUBLIC_IP>:3000
- Dashboard: OrgLens 3-Layer Overview (auto-provisioned)

## 5) Layer 1 -> Layer 2 wiring

- `ORGLENS_LAYER1_API_URL=http://layer2-core:8001/api/ingest`
- Keep API key/signing secret aligned between Layer 1 and Layer 2 config.

## Operational Notes

- Analytics are executed inside Layer 2 core via `/api/run/analytics`.
- All stateful data persists in Docker volumes (`pgdata`, `promdata`, `grafanadata`).
- For production, front Layer 1 and Layer 2 with AWS ALB + ACM TLS.
- Prefer AWS Secrets Manager or SSM Parameter Store over plaintext env files.

## Stable Auth and Deploy Workflow

Use these scripts from the repository root to avoid recurring auth/session issues and to deploy dashboard changes safely.

### 1) Refresh AWS SSO session

```bash
./scripts/aws_sso_refresh.sh --profile AdministratorAccess-772721871316 --region ap-south-2
```

### 2) Local preflight only

```bash
./scripts/deploy_grafana_dashboard.sh --host <EC2_PUBLIC_IP> --key infra/aws/keys/orglens-ec2-key.pem --dry-run
```

### 3) Deploy dashboard (auto fallback)

```bash
./scripts/deploy_grafana_dashboard.sh --host <EC2_PUBLIC_IP> --key infra/aws/keys/orglens-ec2-key.pem --mode auto
```

`--mode auto` attempts SSH first, then Grafana API, then AWS SSM fallback.

You can force SSM directly:

```bash
./scripts/deploy_grafana_dashboard.sh --host <EC2_PUBLIC_IP> --mode ssm
```

### 4) Deploy dashboard via AWS SSM (no SSH required)

```bash
./scripts/deploy_grafana_dashboard_ssm.sh --host <EC2_PUBLIC_IP>
```

This path packages Grafana files locally, uploads to S3, and applies changes on-instance through SSM.
