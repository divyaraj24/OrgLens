# OrgLens AWS Implementation Plan (Impact-First, Cost-Aware)

Target constraints:

- Keep always-on stack below ~0.50 USD/day preferred, hard ceiling 1.00 USD/day.
- Prioritize high-impact reliability/security changes first.

## Priorities

## P0 (Implement First: High Impact, Low Cost)

1. Cost-aware baseline compute

- Keep single-node EC2 architecture for now.
- Default to `t4g.micro`, `gp3` 20 GB.
- Enforce cost precheck before remote provisioning.

2. Deployment reliability and auth stability

- Use AWS default credential chain first (env, role, optional profile).
- Remove stale hardcoded host defaults from scripts.
- Require explicit host/instance inputs for remote operations.

3. Basic security hardening with no extra service cost

- Restrict Grafana/Prometheus ports to `ADMIN_CIDR`.
- Keep API exposure configurable via `PUBLIC_API_CIDR`.

## P1 (Next: Strong Impact, Low-to-Moderate Cost)

1. Secret management

- Move runtime secrets from `.env.aws` into AWS SSM Parameter Store (SecureString) or Secrets Manager.
- Inject at bootstrap/runtime via IAM role.

2. Observability hardening

- Add CloudWatch alarms for EC2 health, disk pressure, and API health endpoints.
- Add log retention policy caps.

3. Backup and recovery

- Add scheduled EBS snapshots + restore drill docs.

## P2 (Implemented as Optional High-Cost Tracks)

1. ALB + ACM for public TLS

- Implemented via `infra/aws/p2_enable_alb_acm.sh` (dry-run by default).
- Valuable for production edge, but ongoing cost often exceeds low-budget target.

2. RDS PostgreSQL

- Implemented via `infra/aws/p2_provision_rds.sh` (dry-run by default).
- Better managed DB operations, but likely pushes daily total toward or above preferred budget.

3. ECS/Fargate migration

- Implemented preparation flow via `infra/aws/p2_prepare_ecs_fargate.sh` (dry-run by default + taskdef generation).
- Improves orchestration maturity but likely exceeds budget at always-on runtime.

## Cost Guidance

Estimated always-on baseline (approx, region-dependent):

- EC2 t4g.micro: ~0.21 USD/day
- EBS gp3 20GB: ~0.05 USD/day
- Total baseline: ~0.26 USD/day (+ small AWS API/monitoring charges)

## Execution Sequence

1. Run local precheck and budget gate:

```bash
MAX_DAILY_USD=0.50 INSTANCE_TYPE=t4g.micro VOLUME_SIZE_GB=20 SKIP_DEPLOY=1 infra/aws/setup_stack.sh
```

2. Apply guardrails and provision remote stack:

```bash
MAX_DAILY_USD=0.50 INSTANCE_TYPE=t4g.micro VOLUME_SIZE_GB=20 ALERT_EMAIL=<you@example.com> infra/aws/setup_stack.sh
```

3. Verify cloud endpoints:

- Layer2: `http://<host>:8001/health`
- Layer1: `http://<host>:8080/health`
- Grafana: `http://<host>:3000`

4. Iterate P1 items without introducing high recurring cost.

## Long-Term Session Plan (Execute One Goal at a Time)

This roadmap breaks your goals into independent sessions. Complete each session fully before moving to the next.

### Session 1: Complete Remaining Implementation Items

Objective:

- Finish all actionable items in this plan (P0 -> P1, while keeping cost constraints).

Scope:

- Finalize secret sync path reliability (bootstrap + runtime sync).
- Ensure optional profile/default credential chain behavior is consistent in all AWS scripts.
- Complete security/cost guardrail defaults and docs parity.
- Close all open checklist items in AWS deploy and runbooks.

Execution checklist:

1. Review all AWS scripts and docs for drift.
2. Resolve script portability issues (quoting, shell compatibility, dependency checks).
3. Re-run syntax checks for all touched shell scripts.
4. Commit implementation changes in focused commits by concern:
   - secret management
   - reliability/auth
   - security/cost
   - docs

Evidence to keep:

- Git commit list with concise messages.
- Script syntax-check output summary.
- Short changelog of completed plan items.

Done criteria:

- No remaining unchecked implementation tasks in this document for P0/P1.
- All deployment scripts run successfully with current defaults.

### Session 2: End-to-End Service Validation

Objective:

- Confirm the full stack is healthy from startup to API and observability endpoints.

Scope:

- Local compose and remote compose health.
- Layer 1, Layer 2, Prometheus, Grafana, PostgreSQL readiness.

Execution checklist:

1. Start stack using canonical compose command.
2. Validate container states and restart counts.
3. Validate endpoint health:
   - `/health` for Layer 1 and Layer 2
   - `/metrics` on Layer 2
   - Prometheus targets UP
   - Grafana reachable and dashboards load
4. Run smoke API flows (`/api/ingest`, status paths, summary/trend queries).
5. Save logs for failed checks and immediately fix regressions.

Evidence to keep:

- Service status table (service, endpoint, expected, actual, pass/fail).
- Timestamped health-check outputs.
- Minimal incident notes for any failure and fix.

Done criteria:

- All required services pass health checks in one clean run.
- No critical errors in logs during smoke tests.

### Session 3: Remote Dashboard Metric Validation + Screenshot Audit

Objective:

- Verify dashboard metrics are correct, complete, timeline-mapped, and visually organized.

Scope:

- Remote Grafana dashboard from EC2 host.
- Accuracy, mapping, null/missing behavior, and panel clarity.

Execution checklist:

1. Prepare a controlled test time window and known input events.
2. Trigger ingestion and analytics runs.
3. Validate each dashboard panel against source queries/API responses.
4. Check timeline alignment (panel time range vs event timestamps).
5. Verify no missing/unknown label artifacts unless expected.
6. Capture screenshots per dashboard section:
   - health + volume
   - risk distribution
   - trend state breakdown
   - what-if/forecast panels
7. Record visual QA notes (layout density, readability, legend/axis clarity).

Evidence to keep:

- Screenshot set with timestamps.
- Validation sheet: panel -> source -> expected -> observed -> result.
- List of panel fixes if mismatches are found.

Done criteria:

- All key panels pass correctness and mapping checks.
- Screenshot set is complete and usable as release evidence.

### Session 4: Test Dataset + README Achievement Metrics (with Backup)

Objective:

- Build a repeatable test dataset/process and publish real project achievement metrics in README safely.

Scope:

- Define measurable project outcomes.
- Validate numbers before README update.
- Keep rollback-ready backup copies of README.

Execution checklist:

1. Create/curate test dataset(s):
   - baseline sample
   - medium sample
   - edge-case sample
2. Run benchmark-style test passes and capture metrics:
   - ingest throughput
   - end-to-end latency
   - successful processing rate
   - dashboard freshness interval
3. Validate metric calculations and units.
4. Backup README before editing:
   - `cp README.md README.backup.<date>.md`
5. Update README with verified achievement metrics only.
6. Re-check formatting/readability and link validity.

Evidence to keep:

- Raw test outputs and summary table.
- README backup file path and timestamp.
- Final metrics block source trace (where each number came from).

Done criteria:

- README includes only reproducible, validated metrics.
- Backup exists and rollback is immediate.

## Operating Rules for These Sessions

- Run one session at a time; do not blend goals.
- Keep cost guardrails active for each remote test run.
- Always capture evidence before declaring a session complete.
- If a blocker appears, resolve in-session and update this plan before proceeding.

## Suggested Cadence

- Session 1: implementation completion
- Session 2: platform health validation
- Session 3: dashboard correctness + screenshot audit
- Session 4: achievement-metric testing and README publication

## Execution Progress (2026-04-15)

Session status:

- Session 1: completed (P2-first implementation added and validated)
- Session 2: completed (local + remote health and smoke checks)
- Session 3: completed with evidence (API correctness checks + dashboard screenshot audit)
- Session 4: completed (test sets, benchmark metrics, README update with backup)

Artifacts:

- `reports/session1_p2_implementation_20260415.md`
- `reports/session2_validation_20260415.md`
- `reports/session3_dashboard_validation_20260415.md`
- `reports/session4_achievement_metrics_20260415.json`
- `reports/dashboard_screenshots_20260415/local_dashboard_top.png`
- `reports/dashboard_screenshots_20260415/local_dashboard_mid.png`
- `reports/dashboard_screenshots_20260415/local_dashboard_bottom.png`
- `reports/dashboard_screenshots_20260415/local_dashboard_full_live_20260415.png`
- `README.backup.2026-04-15.md`
