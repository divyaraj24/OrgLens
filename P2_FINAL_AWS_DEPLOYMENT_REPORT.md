# OrgLens P2 AWS Deployment Report

Date: 2026-04-15
Status: ACTIVE

## Executive Summary

The AWS deployment is running in a simplified 3-layer model:

1. Layer 1 cloud ingestion (`layer1-cloud`, port 8080)
2. Layer 2 core API and analytics (`layer2-core`, port 8001)
3. Observability (`prometheus`, `grafana`)

As of this report, duplicate ALB resources were cleaned and only one ALB remains in service.

## Live Infrastructure

- Region: `ap-south-2`
- Account: `772721871316`
- EC2 instance: `i-096d8bc8afcddedd4`
- Instance type: `t4g.micro` (ARM64)
- VPC: `vpc-068fd98f8f2a3cb49`
- Active ALB: `orglens-alb`
- ALB DNS: `orglens-alb-1887249332.ap-south-2.elb.amazonaws.com`

## Load Balancer and Routing

- Listener: `HTTP:80`
- Default route: Layer 2 target group (`orglens-l2` on port 8001)
- Path route: `/webhook*`, `/api/backfill*` -> Layer 1 target group (`orglens-l1` on port 8080)

## Container Services (EC2 Docker Compose)

- `layer2-core`: healthy
- `layer1-cloud`: healthy
- `postgres`: healthy
- `prometheus`: healthy
- `grafana`: healthy

## Validation Evidence (Post-Cleanup)

- AWS identity check succeeded (STS)
- ALB inventory shows only `orglens-alb`
- Target groups `orglens-l1` and `orglens-l2` are healthy
- In-instance checks:
  - `GET /health` on Layer 2 -> `{"status":"ok"}`
  - `GET /health` on Layer 1 -> `{"status":"ok"}`
  - `POST /api/run/analytics` for `divyaraj24/webhook-sanity` -> success
  - `GET /api/overview/forecast` for `divyaraj24/webhook-sanity` -> non-empty output
  - `/metrics` includes repo-scoped `orglens_*` series

## Security and Operations Notes

- Single-repo mode is enforced at runtime.
- Secrets are intentionally excluded from this report.
- Access is managed through SSM and controlled SG rules.

## Cleanup Actions Completed

- Removed extra ALB: `orglens-p2-alb`
- Removed extra target groups: `orglens-p2-l1`, `orglens-p2-l2`
- Removed stale ingress rules referencing deleted p2 ALB SG
- Deleted obsolete p2 ALB SG

## Recommended Next Steps

1. Keep `orglens-alb` as the only ingress endpoint.
2. Continue documenting operational changes in this report file only.
3. Keep cost guidance in `COST_REPORT_P2_AWS.md` aligned to the live instance type.
