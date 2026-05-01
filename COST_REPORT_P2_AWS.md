# OrgLens P2 AWS Cost Report

Date: 2026-04-15
Status: UPDATED AFTER INFRA CLEANUP

## Current Live Cost Baseline

This report reflects the current active infrastructure after removing duplicate ALB resources.

- Region: `ap-south-2`
- EC2: `t4g.micro` (single instance)
- ALB: single `orglens-alb`
- RDS: provisioned but not used by the active app path
- App runtime: Docker Compose on EC2 (`layer1-cloud`, `layer2-core`, `postgres`, `prometheus`, `grafana`)

## Estimated Recurring Cost (Order of Magnitude)

- EC2 `t4g.micro`: about `$0.04/hour` (~`$29/month` at 730h)
- ALB base: about `$0.0225/hour` (~`$16/month`)
- ALB usage/LCU: variable (light traffic typically low)
- EBS 20 GB gp2: about `$2/month`
- Data transfer: variable by usage

Conservative monthly expectation with light traffic: approximately `$55-$95/month` before taxes.

## What Changed

Completed cleanup reduced unnecessary spend by removing duplicate ingress resources:

- Deleted: `orglens-p2-alb`
- Deleted: `orglens-p2-l1`, `orglens-p2-l2` target groups
- Deleted: `orglens-p2-alb-sg`
- Kept: `orglens-alb`, `orglens-l1`, `orglens-l2`

## Cost Guardrails

1. Keep only one ALB unless multi-environment separation is intentional.
2. Use SSM for administration to avoid extra network components.
3. Keep RDS stopped/unreferenced unless explicitly switching away from local postgres.
4. Set AWS Budget alerts for monthly thresholds.

## Notes

- Costs are estimates and vary with region pricing changes, traffic, and transfer.
- This file intentionally avoids stale historical assumptions from earlier larger-instance baselines.
