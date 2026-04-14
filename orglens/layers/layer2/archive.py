from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from io import BytesIO
from typing import Any

import aioboto3
import pyarrow as pa
import pyarrow.parquet as pq

from orglens.layers.layer2.models import RawEventIn


def _hour_key(ts: datetime) -> datetime:
    ts_utc = ts.astimezone(timezone.utc)
    return ts_utc.replace(minute=0, second=0, microsecond=0)


class MinioArchive:
    """Non-blocking MinIO parquet archival for replay and long-term storage."""

    def __init__(
        self,
        endpoint_url: str,
        access_key: str,
        secret_key: str,
        bucket: str = "raw-events",
        secure: bool = False,
    ) -> None:
        self._session = aioboto3.Session()
        self._endpoint_url = endpoint_url
        self._access_key = access_key
        self._secret_key = secret_key
        self._bucket = bucket
        self._secure = secure

    async def ping(self) -> bool:
        async with self._client() as s3:
            await s3.head_bucket(Bucket=self._bucket)
            return True

    async def append_events(self, events: list[RawEventIn]) -> None:
        grouped: dict[tuple[str, datetime], list[RawEventIn]] = defaultdict(list)
        for event in events:
            grouped[(event.repo, _hour_key(event.timestamp))].append(event)

        async with self._client() as s3:
            for (repo, hour), grouped_events in grouped.items():
                key = self._build_key(repo, hour)
                rows = [self._event_to_row(e) for e in grouped_events]

                existing_rows = await self._read_existing_rows(s3, key)
                all_rows = existing_rows + rows

                table = pa.Table.from_pylist(all_rows)
                buffer = BytesIO()
                pq.write_table(table, buffer)
                buffer.seek(0)

                await s3.put_object(Bucket=self._bucket, Key=key, Body=buffer.getvalue())

    async def list_keys(self, repo: str, from_date: datetime) -> list[str]:
        prefix = f"{repo}/{from_date.year:04d}/"
        keys: list[str] = []
        async with self._client() as s3:
            paginator = s3.get_paginator("list_objects_v2")
            async for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix):
                for item in page.get("Contents", []):
                    key = item.get("Key")
                    if key and key.endswith(".parquet"):
                        keys.append(key)
        return sorted(keys)

    async def read_events(self, key: str) -> list[dict[str, Any]]:
        async with self._client() as s3:
            obj = await s3.get_object(Bucket=self._bucket, Key=key)
            body = await obj["Body"].read()
        table = pq.read_table(BytesIO(body))
        return table.to_pylist()

    def _build_key(self, repo: str, hour: datetime) -> str:
        return (
            f"{repo}/"
            f"{hour.year:04d}/{hour.month:02d}/{hour.day:02d}/"
            f"{hour.hour:02d}:00.parquet"
        )

    def _event_to_row(self, event: RawEventIn) -> dict[str, Any]:
        return {
            "event_id": event.event_id,
            "source": event.source.value,
            "repo": event.repo,
            "actor": event.actor,
            "event_type": event.event_type.value,
            "target": event.target,
            "module": event.module,
            "timestamp": event.timestamp.isoformat(),
            "metadata": event.metadata,
        }

    async def _read_existing_rows(self, s3: Any, key: str) -> list[dict[str, Any]]:
        try:
            obj = await s3.get_object(Bucket=self._bucket, Key=key)
        except Exception:
            return []
        body = await obj["Body"].read()
        table = pq.read_table(BytesIO(body))
        return table.to_pylist()

    def _client(self):
        protocol = "https" if self._secure else "http"
        endpoint = self._endpoint_url
        if not endpoint.startswith("http://") and not endpoint.startswith("https://"):
            endpoint = f"{protocol}://{endpoint}"
        return self._session.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=self._access_key,
            aws_secret_access_key=self._secret_key,
        )
