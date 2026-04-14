from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


@dataclass
class StreamEntry:
    stream_id: str
    payload: list[dict[str, Any]]
    received_at: str
    source: str


class RedisStreamQueue:
    """Redis Stream adapter used by Layer 2 API and worker."""

    def __init__(
        self,
        redis_url: str,
        stream_name: str = "orglens:ingest",
        group_name: str = "orglens:workers",
        consumer_name: str = "worker-1",
    ) -> None:
        import redis.asyncio as redis

        self._redis = redis.from_url(redis_url, decode_responses=True)
        self._stream = stream_name
        self._group = group_name
        self._consumer = consumer_name

    async def ensure_group(self) -> None:
        try:
            await self._redis.xgroup_create(self._stream, self._group, id="0", mkstream=True)
        except Exception as exc:
            # BUSYGROUP means the group already exists.
            if "BUSYGROUP" not in str(exc):
                raise

    async def add_batch(self, payload: list[dict[str, Any]], source: str) -> str:
        fields = {
            "payload": json.dumps(payload),
            "received_at": datetime.now(timezone.utc).isoformat(),
            "source": source,
        }
        entry_id = await self._redis.xadd(self._stream, fields)
        return str(entry_id)

    async def read_group(self, count: int = 1, block_ms: int = 5000) -> list[StreamEntry]:
        results = await self._redis.xreadgroup(
            groupname=self._group,
            consumername=self._consumer,
            streams={self._stream: ">"},
            count=count,
            block=block_ms,
        )
        entries: list[StreamEntry] = []
        for _, messages in results:
            for stream_id, fields in messages:
                payload = json.loads(fields.get("payload", "[]"))
                entries.append(
                    StreamEntry(
                        stream_id=stream_id,
                        payload=payload,
                        received_at=fields.get("received_at", ""),
                        source=fields.get("source", "unknown"),
                    )
                )
        return entries

    async def ack(self, stream_id: str) -> None:
        await self._redis.xack(self._stream, self._group, stream_id)

    async def depth(self) -> int:
        return int(await self._redis.xlen(self._stream))

    async def ping(self) -> bool:
        return bool(await self._redis.ping())

    async def close(self) -> None:
        await self._redis.aclose()
