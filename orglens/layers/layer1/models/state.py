"""
StateManager — persists the last-fetch timestamps for each repo × event-type so
that Perceval always resumes from the right checkpoint.

State file format  (state.json):
{
  "django/django": {
    "commits":       "2024-03-26T18:00:00Z",
    "pull_requests": "2024-03-26T18:00:00Z",
    "issues":        "2024-03-26T18:00:00Z"
  }
}
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional

log = logging.getLogger(__name__)

# Keys used in the state file
EVENT_KEYS = ("commits", "pull_requests", "issues")

# ISO 8601 — the format Perceval accepts via --from-date
_FMT = "%Y-%m-%dT%H:%M:%SZ"


class StateManager:
    def __init__(self, state_file: str = "state.json"):
        self._path = Path(state_file)
        self._state: Dict[str, Dict[str, str]] = {}
        self._load()

    # ── Persistence ───────────────────────────────────────────────────────────

    def _load(self) -> None:
        if self._path.exists():
            try:
                with self._path.open() as f:
                    self._state = json.load(f)
                log.info("Loaded state from %s", self._path)
            except (json.JSONDecodeError, OSError) as exc:
                log.warning("Could not read state file (%s); starting fresh", exc)
                self._state = {}

    def save(self) -> None:
        try:
            with self._path.open("w") as f:
                json.dump(self._state, f, indent=2)
            log.debug("State saved to %s", self._path)
        except OSError as exc:
            log.error("Failed to save state: %s", exc)

    # ── Accessors ─────────────────────────────────────────────────────────────

    def get_last_fetch(self, repo: str, event_key: str) -> Optional[str]:
        """Return the last-fetch timestamp string for repo × event_key, or None."""
        return self._state.get(repo, {}).get(event_key)

    def set_last_fetch(self, repo: str, event_key: str, ts: datetime) -> None:
        """Record a successful fetch timestamp and immediately persist."""
        if repo not in self._state:
            self._state[repo] = {}
        self._state[repo][event_key] = ts.strftime(_FMT)
        self.save()

    def now_utc(self) -> datetime:
        return datetime.now(tz=timezone.utc)
