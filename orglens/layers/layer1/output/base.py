"""Abstract OutputSink base class."""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import List

from orglens.layers.layer1.models.raw_event import RawEvent


class OutputSink(ABC):
    @abstractmethod
    async def send(self, events: List[RawEvent]) -> None:
        """Send a batch of events to the output destination."""
        ...

    async def close(self) -> None:
        """Optional teardown (e.g., close DB connections)."""
