"""
WebhookListener — FastAPI application that receives GitHub webhook events
in real-time and feeds them into the BatchBuffer.

Validates X-Hub-Signature-256 (HMAC-SHA256) when a webhook secret is configured.
Handles: push, pull_request, pull_request_review, issues
"""
from __future__ import annotations

import hashlib
import hmac
import logging
from typing import Callable, Optional

from fastapi import FastAPI, Header, HTTPException, Request, status

log = logging.getLogger(__name__)


def create_webhook_app(
    normalizer,
    on_events: Callable,       # async (List[RawEvent]) -> None
    webhook_secret: Optional[str] = None,
    skip_signature_check: bool = False,
) -> FastAPI:
    """
    Factory that returns a configured FastAPI app.

    Args:
        normalizer:            Normalizer instance
        on_events:             Async callback to receive List[RawEvent]
        webhook_secret:        HMAC secret (from GitHub webhook settings)
        skip_signature_check:  Set True for local dev without a secret
    """
    app = FastAPI(title="OrgLens Webhook Listener", version="0.1.0")

    # ── Signature validation ──────────────────────────────────────────────────

    def _verify_signature(body: bytes, signature_header: str) -> bool:
        if not webhook_secret:
            return True
        if not signature_header or not signature_header.startswith("sha256="):
            return False
        expected = "sha256=" + hmac.new(
            webhook_secret.encode(), body, hashlib.sha256
        ).hexdigest()
        return hmac.compare_digest(expected, signature_header)

    # ── Webhook endpoint ──────────────────────────────────────────────────────

    @app.post("/webhook", status_code=status.HTTP_200_OK)
    async def receive_webhook(
        request: Request,
        x_github_event: str = Header(default=""),
        x_hub_signature_256: str = Header(default=""),
    ):
        body = await request.body()

        if not skip_signature_check:
            if not _verify_signature(body, x_hub_signature_256):
                log.warning("Webhook signature mismatch — rejected")
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Invalid webhook signature",
                )

        try:
            payload = await request.json()
        except Exception:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid JSON"
            )

        repo_name = (payload.get("repository") or {}).get("full_name", "unknown")
        log.info("Webhook received: event=%s repo=%s", x_github_event, repo_name)

        events = normalizer.normalize_webhook(payload, x_github_event)
        if events:
            await on_events(events)
            log.debug("Queued %d event(s) from webhook %s", len(events), x_github_event)

        return {"queued": len(events)}

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    return app
