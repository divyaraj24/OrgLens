from __future__ import annotations

import hashlib
import hmac
import json
import time

from fastapi.testclient import TestClient

from orglens.layers.layer2.api import create_ingestion_app
from orglens.layers.layer2.status import IngestStatus


class FakeQueue:
    def __init__(self) -> None:
        self.batches = []

    async def add_batch(self, payload, source):
        self.batches.append((payload, source))
        return "1-0"

    async def ping(self):
        return True

    async def depth(self):
        return len(self.batches)


class FakeStore:
    async def ping(self):
        return True


class FakeRuntime:
    def __init__(self):
        self.api_key = "test-key"
        self.signature_secret = ""
        self.signature_tolerance_seconds = 300
        self.queue = FakeQueue()
        self.status = IngestStatus()
        self.store = FakeStore()
        self.archive = None

    async def start(self):
        return None

    async def stop(self):
        return None


def test_ingest_requires_auth_and_returns_202() -> None:
    runtime = FakeRuntime()
    app = create_ingestion_app(runtime)

    payload = [
        {
            "event_id": "e1",
            "source": "webhook",
            "repo": "django/django",
            "actor": "alice",
            "event_type": "commit",
            "target": "a.py",
            "module": "orm",
            "timestamp": "2024-01-01T00:00:00Z",
            "metadata": {},
        }
    ]

    with TestClient(app) as client:
        unauthorized = client.post("/api/ingest", json=payload)
        assert unauthorized.status_code == 401

        authorized = client.post(
            "/api/ingest",
            json=payload,
            headers={"Authorization": "Bearer test-key"},
        )
        assert authorized.status_code == 202
        assert authorized.json() == {"queued": 1}


def test_ingest_accepts_x_api_key_header() -> None:
    runtime = FakeRuntime()
    app = create_ingestion_app(runtime)

    payload = [
        {
            "event_id": "e2",
            "source": "perceval_git",
            "repo": "django/django",
            "actor": "bob",
            "event_type": "commit",
            "target": "b.py",
            "module": "http",
            "timestamp": "2024-01-01T00:00:00Z",
            "metadata": {},
        }
    ]

    with TestClient(app) as client:
        authorized = client.post(
            "/api/ingest",
            json=payload,
            headers={"X-Api-Key": "test-key"},
        )
        assert authorized.status_code == 202
        assert authorized.json() == {"queued": 1}


def test_status_endpoint_exposes_counters_and_depth() -> None:
    runtime = FakeRuntime()
    app = create_ingestion_app(runtime)

    with TestClient(app) as client:
        health = client.get("/health")
        assert health.status_code == 200
        assert health.json()["redis"] is True
        assert health.json()["postgres"] is True

        status = client.get("/api/ingest/status")
        assert status.status_code == 200
        body = status.json()
        assert "events_received_today" in body
        assert "events_processed_today" in body
        assert "duplicates_dropped_today" in body
        assert "redis_queue_depth" in body


def _signed_headers(payload: list[dict], secret: str, api_key: str) -> dict[str, str]:
    body = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    timestamp = str(int(time.time()))
    message = timestamp.encode("utf-8") + b"." + body
    digest = hmac.new(secret.encode("utf-8"), message, hashlib.sha256).hexdigest()
    return {
        "Authorization": f"Bearer {api_key}",
        "X-Orglens-Timestamp": timestamp,
        "X-Orglens-Signature": f"sha256={digest}",
    }


def test_ingest_accepts_valid_signature_when_enabled() -> None:
    runtime = FakeRuntime()
    runtime.signature_secret = "signing-secret"
    app = create_ingestion_app(runtime)

    payload = [
        {
            "event_id": "e3",
            "source": "webhook",
            "repo": "django/django",
            "actor": "carol",
            "event_type": "commit",
            "target": "c.py",
            "module": "core",
            "timestamp": "2024-01-01T00:00:00Z",
            "metadata": {},
        }
    ]

    with TestClient(app) as client:
        response = client.post(
            "/api/ingest",
            content=json.dumps(payload, separators=(",", ":"), sort_keys=True),
            headers={
                **_signed_headers(payload, "signing-secret", "test-key"),
                "Content-Type": "application/json",
            },
        )

    assert response.status_code == 202
    assert response.json() == {"queued": 1}


def test_ingest_rejects_invalid_signature_when_enabled() -> None:
    runtime = FakeRuntime()
    runtime.signature_secret = "signing-secret"
    app = create_ingestion_app(runtime)

    payload = [
        {
            "event_id": "e4",
            "source": "webhook",
            "repo": "django/django",
            "actor": "dan",
            "event_type": "commit",
            "target": "d.py",
            "module": "core",
            "timestamp": "2024-01-01T00:00:00Z",
            "metadata": {},
        }
    ]

    bad_headers = _signed_headers(payload, "wrong-secret", "test-key")
    with TestClient(app) as client:
        response = client.post(
            "/api/ingest",
            content=json.dumps(payload, separators=(",", ":"), sort_keys=True),
            headers={
                **bad_headers,
                "Content-Type": "application/json",
            },
        )

    assert response.status_code == 401
    assert response.json()["detail"] == "Invalid ingest signature"
