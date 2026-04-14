"""
FileOutput — appends events to a newline-delimited JSON (JSONL) file.
Useful for local development and debugging.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import List

from orglens.layers.layer1.models.raw_event import RawEvent
from orglens.layers.layer1.output.base import OutputSink

log = logging.getLogger(__name__)


class FileOutput(OutputSink):
    def __init__(self, path: str = "events.jsonl"):
        self._path = Path(path)

    async def send(self, events: List[RawEvent]) -> None:
        with self._path.open("a") as f:
            for e in events:
                f.write(e.model_dump_json() + "\n")
        log.info("Wrote %d events to %s", len(events), self._path)
