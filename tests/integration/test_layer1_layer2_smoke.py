from __future__ import annotations

import json
from pathlib import Path

import pytest

from orglens.layers.layer1.normalizer.normalizer import Normalizer
from orglens.layers.layer2.processor import EventProcessor


class FakeStore:
    def __init__(self) -> None:
        self.written = []
        self.dead_letters = []

    async def write_events(self, events):
        self.written.extend(events)
        return len(events), 0

    async def write_dead_letter(self, letter):
        self.dead_letters.append(letter)


@pytest.mark.asyncio
async def test_layer1_to_layer2_webhook_push_flow() -> None:
    payload_path = Path("tests/fixtures/push_payload.json")
    payload = json.loads(payload_path.read_text())

    normalizer = Normalizer(module_map_path="module_map.yaml")
    layer1_events = normalizer.normalize_webhook(payload, "push")
    assert layer1_events, "Layer 1 should produce events from push payload"

    processor = EventProcessor(store=FakeStore(), archive=None)
    layer2_result = await processor.process_batch(
        payload=[e.model_dump(mode="json") for e in layer1_events],
        source="webhook",
    )

    assert layer2_result.success is True
    assert layer2_result.events_received == len(layer1_events)
    assert layer2_result.events_written == len(layer1_events)
    assert layer2_result.dead_letters == 0
