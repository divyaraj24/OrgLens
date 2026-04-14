from __future__ import annotations

from datetime import datetime, timezone

import pytest

from orglens.layers.layer2.processor import EventProcessor


class FakeStore:
    def __init__(self) -> None:
        self.dead_letters = []
        self.events = []
        self.fail_writes = False

    async def write_dead_letter(self, letter):
        self.dead_letters.append(letter)

    async def write_events(self, events):
        if self.fail_writes:
            raise RuntimeError("db down")
        written = 0
        seen = set()
        for e in events:
            if e.event_id in seen:
                continue
            seen.add(e.event_id)
            written += 1
            self.events.append(e)
        return written, len(list(events)) - written


class FakeArchive:
    def __init__(self) -> None:
        self.calls = 0

    async def append_events(self, events):
        self.calls += 1


VALID_EVENT = {
    "event_id": "commit_repo_abc",
    "source": "webhook",
    "repo": "django/django",
    "actor": "alice",
    "event_type": "commit",
    "target": "django/db/models.py",
    "module": "orm",
    "timestamp": datetime.now(timezone.utc).isoformat(),
    "metadata": {"lines_changed": 10},
}


@pytest.mark.asyncio
async def test_processor_validates_and_dead_letters_invalid() -> None:
    store = FakeStore()
    processor = EventProcessor(store=store, archive=None)

    payload = [VALID_EVENT, {"event_id": "bad"}]
    result = await processor.process_batch(payload, source="webhook")

    assert result.events_received == 2
    assert result.events_written == 1
    assert result.dead_letters == 1
    assert len(store.dead_letters) == 1


@pytest.mark.asyncio
async def test_processor_retries_then_dead_letters_on_db_failure() -> None:
    store = FakeStore()
    store.fail_writes = True
    processor = EventProcessor(store=store, archive=None, max_retries=1)

    with pytest.raises(RuntimeError, match="db_write_failed"):
        await processor.process_batch([VALID_EVENT], source="webhook")

    assert len(store.dead_letters) == 0
