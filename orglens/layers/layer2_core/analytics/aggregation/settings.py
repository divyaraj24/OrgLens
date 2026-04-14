from __future__ import annotations

from dataclasses import dataclass

from orglens.config import load_config


@dataclass
class AggregationSettings:
    pg_dsn: str = "postgresql://orglens:password@localhost:5432/orglens"
    schedule: str = "0 */6 * * *"

    commit_weight: float = 3.0
    lines_weight: float = 0.01
    pr_weight: float = 2.0
    review_weight: float = 1.5
    issue_weight: float = 1.0

    windows: tuple[int, ...] = (7, 30, 90)


def load_aggregation_settings(config_path: str | None = None) -> AggregationSettings:
    cfg = load_config(config_path)

    l2 = cfg.get("layer2", {})
    pg_cfg = l2.get("postgres", {})

    analytics = cfg.get("layer2_analytics", {})
    aggregation = analytics.get("aggregation", {})
    weights = aggregation.get("weights", {})

    windows = tuple(int(v) for v in aggregation.get("windows", [7, 30, 90]))
    if not windows:
        windows = (7, 30, 90)

    return AggregationSettings(
        pg_dsn=pg_cfg.get("dsn", "postgresql://orglens:password@localhost:5432/orglens"),
        schedule=aggregation.get("schedule", "0 */6 * * *"),
        commit_weight=float(weights.get("commit", 3.0)),
        lines_weight=float(weights.get("lines", 0.01)),
        pr_weight=float(weights.get("pr", 2.0)),
        review_weight=float(weights.get("review", 1.5)),
        issue_weight=float(weights.get("issue", 1.0)),
        windows=windows,
    )


# Backward-compatible alias for pre-simplification imports.
load_layer3_settings = load_aggregation_settings
