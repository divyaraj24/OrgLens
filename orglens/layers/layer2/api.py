from __future__ import annotations

from contextlib import asynccontextmanager
import hashlib
import hmac
import json
import time
from typing import Any

from fastapi import FastAPI, Header, HTTPException, Request, status


def _extract_bearer_token(authorization: str | None) -> str | None:
    if not authorization:
        return None
    prefix = "bearer "
    auth_lower = authorization.lower()
    if not auth_lower.startswith(prefix):
        return None
    return authorization[len(prefix):].strip()


def _extract_api_key(authorization: str | None, x_api_key: str | None) -> str | None:
    bearer = _extract_bearer_token(authorization)
    if bearer:
        return bearer
    if x_api_key:
        return x_api_key.strip()
    return None


def _extract_signature(signature: str | None) -> str | None:
    if not signature:
        return None
    candidate = signature.strip()
    if candidate.startswith("sha256="):
        candidate = candidate[len("sha256=") :]
    return candidate or None


def _verify_signature(
    *,
    body: bytes,
    timestamp: str | None,
    signature: str | None,
    secret: str,
    tolerance_seconds: int,
) -> bool:
    if not timestamp or not signature:
        return False

    try:
        ts_int = int(timestamp)
    except ValueError:
        return False

    if abs(int(time.time()) - ts_int) > tolerance_seconds:
        return False

    provided = _extract_signature(signature)
    if not provided:
        return False

    message = timestamp.encode("utf-8") + b"." + body
    expected = hmac.new(secret.encode("utf-8"), message, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, provided)


def create_ingestion_app(runtime: Any) -> FastAPI:
    @asynccontextmanager
    async def _lifespan(_: FastAPI):
        await runtime.start()
        try:
            yield
        finally:
            await runtime.stop()

    app = FastAPI(title="OrgLens Layer 2 Ingestion", version="0.1.0", lifespan=_lifespan)

    @app.post("/api/ingest", status_code=status.HTTP_202_ACCEPTED)
    async def ingest(
        request: Request,
        authorization: str | None = Header(default=None),
        x_api_key: str | None = Header(default=None, alias="X-Api-Key"),
        x_orglens_timestamp: str | None = Header(default=None, alias="X-Orglens-Timestamp"),
        x_orglens_signature: str | None = Header(default=None, alias="X-Orglens-Signature"),
    ) -> dict[str, int]:
        token = _extract_api_key(authorization, x_api_key)
        if runtime.api_key and token != runtime.api_key:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized")

        raw_body = await request.body()
        signature_secret = getattr(runtime, "signature_secret", "")
        signature_tolerance_seconds = int(getattr(runtime, "signature_tolerance_seconds", 300))
        if signature_secret and not _verify_signature(
            body=raw_body,
            timestamp=x_orglens_timestamp,
            signature=x_orglens_signature,
            secret=signature_secret,
            tolerance_seconds=signature_tolerance_seconds,
        ):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid ingest signature",
            )

        try:
            body = json.loads(raw_body)
        except json.JSONDecodeError as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid JSON payload",
            ) from exc

        if not isinstance(body, list):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Batch must be an array")

        source = "unknown"
        if body:
            first_source = body[0].get("source")
            if isinstance(first_source, str):
                source = first_source

        await runtime.queue.add_batch(body, source=source)
        await runtime.status.mark_received(len(body))
        return {"queued": len(body)}

    @app.get("/health")
    async def health() -> dict[str, Any]:
        redis_ok = await runtime.queue.ping()
        pg_ok = await runtime.store.ping()
        minio_ok = True
        if runtime.archive is not None:
            try:
                minio_ok = await runtime.archive.ping()
            except Exception:
                minio_ok = False

        healthy = redis_ok and pg_ok
        return {
            "status": "ok" if healthy else "degraded",
            "redis": redis_ok,
            "postgres": pg_ok,
            "minio": minio_ok,
        }

    @app.get("/api/ingest/status")
    async def ingest_status() -> dict[str, Any]:
        depth = await runtime.queue.depth()
        snapshot = await runtime.status.snapshot(queue_depth=depth)
        return snapshot.model_dump()

    return app
