"""
BatchBuffer async tests.
"""
import asyncio

import pytest

from orglens.layers.layer1.buffer.batch_buffer import BatchBuffer
from orglens.layers.layer1.models.raw_event import EventType, RawEvent
from datetime import datetime, timezone


def _make_event(n: int) -> RawEvent:
    return RawEvent(
        event_id=f"evt-{n}",
        event_type=EventType.COMMIT,
        source="test",
        repo="test/repo",
        timestamp=datetime(2024, 1, 1, tzinfo=timezone.utc),
        target="test/target",
        actor=f"user-{n}",
    )


class TestBatchBuffer:
    @pytest.mark.asyncio
    async def test_flush_on_count_threshold(self):
        """Buffer should flush when max_events is reached."""
        received = []

        async def sink(events):
            received.extend(events)

        buf = BatchBuffer(sink_fn=sink, max_events=5, flush_interval=9999)
        buf.start()

        # Add 5 events — should trigger flush
        await buf.add([_make_event(i) for i in range(5)])
        await asyncio.sleep(0.05)  # let flush complete

        assert len(received) == 5
        await buf.stop()

    @pytest.mark.asyncio
    async def test_flush_on_stop(self):
        """Remaining events should be flushed on stop()."""
        received = []

        async def sink(events):
            received.extend(events)

        buf = BatchBuffer(sink_fn=sink, max_events=100, flush_interval=9999)
        buf.start()
        await buf.add([_make_event(i) for i in range(3)])
        await buf.stop()

        assert len(received) == 3

    @pytest.mark.asyncio
    async def test_flush_on_timer(self):
        """Buffer should auto-flush when the timer fires."""
        received = []

        async def sink(events):
            received.extend(events)

        buf = BatchBuffer(sink_fn=sink, max_events=100, flush_interval=0.1)
        buf.start()
        await buf.add([_make_event(0)])

        # Wait slightly longer than the flush interval
        await asyncio.sleep(0.25)
        assert len(received) == 1
        await buf.stop()

    @pytest.mark.asyncio
    async def test_sink_error_does_not_crash(self):
        """If the sink raises, the buffer should log and continue."""
        call_count = 0

        async def bad_sink(events):
            nonlocal call_count
            call_count += 1
            raise RuntimeError("Sink failure!")

        buf = BatchBuffer(sink_fn=bad_sink, max_events=1, flush_interval=9999)
        buf.start()

        # Should not raise
        await buf.add([_make_event(0)])
        await asyncio.sleep(0.05)
        assert call_count == 1
        await buf.stop()

    @pytest.mark.asyncio
    async def test_no_flush_when_empty(self):
        """Sink should NOT be called when the buffer is empty."""
        call_count = 0

        async def sink(events):
            nonlocal call_count
            call_count += 1

        buf = BatchBuffer(sink_fn=sink, max_events=100, flush_interval=0.05)
        buf.start()
        await asyncio.sleep(0.15)
        await buf.stop()

        assert call_count == 0
