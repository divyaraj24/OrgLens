from __future__ import annotations

from dataclasses import dataclass

from orglens.config import load_config


@dataclass
class InferenceSettings:
    pg_dsn: str = "postgresql://orglens:password@localhost:5432/orglens"
    schedule: str = "30 */6 * * *"
    window_days: int = 30
    baseline_offset_days: int = 60


def load_inference_settings(config_path: str | None = None) -> InferenceSettings:
    cfg = load_config(config_path)
    l2 = cfg.get("layer2", {})
    pg_cfg = l2.get("postgres", {})

    analytics = cfg.get("layer2_analytics", {})
    inference = analytics.get("inference", {})

    return InferenceSettings(
        pg_dsn=pg_cfg.get("dsn", "postgresql://orglens:password@localhost:5432/orglens"),
        schedule=inference.get("schedule", "30 */6 * * *"),
        window_days=int(inference.get("window_days", 30)),
        baseline_offset_days=int(inference.get("baseline_offset_days", 60)),
    )


# Backward-compatible alias for pre-simplification imports.
load_layer4_settings = load_inference_settings
