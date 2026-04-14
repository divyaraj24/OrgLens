"""
OutputRouter — builds the correct OutputSink based on config / CLI flag.
"""
from __future__ import annotations

import logging
from typing import Any, Dict

from orglens.layers.layer1.output.base import OutputSink

log = logging.getLogger(__name__)


def build_output_sink(output_cfg: Dict[str, Any], override: str | None = None) -> OutputSink:
    """
    Factory function.

    Args:
        output_cfg:  The `output` section of config.yaml as a dict.
        override:    CLI --output flag value (overrides config if provided).

    Returns:
        Configured OutputSink instance.
    """
    target = override or output_cfg.get("target", "api")
    log.info("Output target: %s", target)

    if target == "file":
        from orglens.layers.layer1.output.file_output import FileOutput
        path = output_cfg.get("file", {}).get("path", "events.jsonl")
        return FileOutput(path=path)

    if target == "api":
        from orglens.layers.layer1.output.api_output import ApiOutput
        api_cfg = output_cfg.get("api", {})
        return ApiOutput(
            url=api_cfg.get("url", ""),
            api_key=api_cfg.get("api_key", ""),
            auth_scheme=api_cfg.get("auth_scheme", "x-api-key"),
            signing_secret=api_cfg.get("signing_secret", ""),
            timeout=float(api_cfg.get("timeout_seconds", 10)),
            max_retries=int(api_cfg.get("max_retries", 3)),
        )

    if target == "pg":
        from orglens.layers.layer1.output.pg_output import PgOutput
        dsn = output_cfg.get("pg", {}).get("dsn", "")
        if not dsn:
            raise ValueError("output.pg.dsn is required when output.target = 'pg'")
        return PgOutput(dsn=dsn)

    raise ValueError(f"Unknown output target '{target}'. Valid: file | api | pg")
