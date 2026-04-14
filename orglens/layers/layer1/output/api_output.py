"""
ApiOutput — sends event batches to the OrgLens cloud ingest endpoint.

POST /api/ingest
  Content-Type: application/json
  X-Api-Key: <key>
  Body: [{ ...RawEvent }, ...]

Retries with exponential back-off on transient failures (5xx / network errors).
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import time
from typing import List

import httpx

from orglens.layers.layer1.models.raw_event import RawEvent
from orglens.layers.layer1.output.base import OutputSink

log = logging.getLogger(__name__)


class ApiOutput(OutputSink):
    def __init__(
        self,
        url: str,
        api_key: str = "",
        auth_scheme: str = "x-api-key",
        signing_secret: str = "",
        timeout: float = 10.0,
        max_retries: int = 3,
    ):
        self._url = url
        self._api_key = api_key
        self._auth_scheme = auth_scheme.strip().lower() or "x-api-key"
        if self._auth_scheme not in {"x-api-key", "bearer"}:
            raise ValueError("output.api.auth_scheme must be 'x-api-key' or 'bearer'")
        self._signing_secret = signing_secret
        self._timeout = timeout
        self._max_retries = max_retries

    def _build_headers(self, body: bytes) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self._api_key:
            if self._auth_scheme == "bearer":
                headers["Authorization"] = f"Bearer {self._api_key}"
            else:
                headers["X-Api-Key"] = self._api_key

        if self._signing_secret:
            timestamp = str(int(time.time()))
            message = timestamp.encode("utf-8") + b"." + body
            digest = hmac.new(
                self._signing_secret.encode("utf-8"),
                message,
                hashlib.sha256,
            ).hexdigest()
            headers["X-Orglens-Timestamp"] = timestamp
            headers["X-Orglens-Signature"] = f"sha256={digest}"

        return headers

    async def send(self, events: List[RawEvent]) -> None:
        payload = [e.model_dump(mode="json") for e in events]
        body = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
        attempt = 0
        delay = 1.0

        async with httpx.AsyncClient(timeout=self._timeout) as client:
            while attempt <= self._max_retries:
                try:
                    headers = self._build_headers(body)
                    resp = await client.post(
                        self._url, content=body, headers=headers
                    )
                    if resp.status_code < 500:
                        if resp.status_code >= 400:
                            log.error(
                                "Ingest API rejected batch: %d %s",
                                resp.status_code, resp.text[:200],
                            )
                        else:
                            log.info(
                                "Sent %d events → %s (%d)", len(events), self._url, resp.status_code
                            )
                        return
                    log.warning(
                        "Ingest API returned %d (attempt %d/%d) — retrying in %.0fs",
                        resp.status_code, attempt + 1, self._max_retries + 1, delay,
                    )
                except httpx.RequestError as exc:
                    log.warning(
                        "Network error (attempt %d/%d): %s — retrying in %.0fs",
                        attempt + 1, self._max_retries + 1, exc, delay,
                    )

                attempt += 1
                if attempt <= self._max_retries:
                    await asyncio.sleep(delay)
                    delay *= 2

        log.error("Giving up after %d attempts (%d events lost)", self._max_retries + 1, len(events))
