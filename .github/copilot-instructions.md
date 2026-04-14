# OrgLens Agent — Copilot Instructions

**OrgLens** uses a simplified **3-layer architecture**:
1. Layer 1 cloud ingestion
2. Layer 2 core processing + analytics API
3. Layer 3 observability UI (Prometheus + Grafana)

---

## Build, Test, and Lint

### Installation
```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

### Running Tests
```bash
pytest tests/ -v
pytest tests/test_normalizer.py -v
pytest tests/layer2/ -v
```

### Linting
```bash
ruff check .
ruff format .
```

---

## Active Runtime Commands

### Layer 1 (Cloud Ingestion)
```bash
orglens-cloud-ingest --config config.local.yaml --host 0.0.0.0 --port 8080
```

Endpoints include:
- `POST /webhook`
- `POST /api/backfill/start`
- `GET /api/backfill/status/{job_id}`

### Layer 2 (Core Processing + Analytics + API)
```bash
orglens-core --config config.local.yaml --host 0.0.0.0 --port 8001
```

Primary endpoints:
- `POST /api/ingest`
- `GET /api/ingest/status`
- `POST /api/admin/reset-repo-data`
- `POST /api/run/all`
- `POST /api/run/analytics`
- `GET /api/risk/summary`
- `GET /api/trends/weekly`
- `GET /metrics`
- `GET /health`

### One-command orchestration
```bash
orglens-auto https://github.com/owner/repo.git --use-remote-aws
```

---

## Architecture Summary

Data flow:
```
GitHub -> Layer1 cloud ingest -> Layer2 core ingest/store/analytics -> /metrics -> Prometheus -> Grafana
```

Notes:
- Internal modules named `layer3`, `layer4`, `layer5` are implementation modules used by Layer 2 core.
- They are not separate deployment layers.

---

## Key Data Semantics

- Normalized event contract: `RawEvent` (`orglens/layers/layer1/models/raw_event.py`)
- Dedup key: stable `event_id` (`ON CONFLICT DO NOTHING`)
- Analytics outputs:
  - `contribution_windows`
  - `bus_factor_scores`
  - `ownership_drift`
  - `succession_risk`

---

## Health Checks

- Layer 1: `GET http://localhost:8080/health`
- Layer 2: `GET http://localhost:8001/health`
- Metrics: `GET http://localhost:8001/metrics`

