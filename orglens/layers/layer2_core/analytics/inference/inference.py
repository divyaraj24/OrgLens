from __future__ import annotations

import logging
import math
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

from orglens.layers.layer2_core.analytics.inference.settings import InferenceSettings
from orglens.layers.layer2_core.analytics.inference.store import InferenceStore

log = logging.getLogger(__name__)


def _group_snapshot(rows: list[dict[str, Any]]) -> dict[tuple[str, str], list[dict[str, Any]]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[(row["repo"], row["module"])].append(row)
    for key in grouped:
        grouped[key] = sorted(grouped[key], key=lambda r: float(r["owner_share"]), reverse=True)
    return grouped


def _bus_factor(distribution: list[dict[str, Any]]) -> int:
    cumulative = 0.0
    for idx, row in enumerate(distribution, start=1):
        cumulative += float(row["owner_share"])
        if cumulative >= 0.5:
            return idx
    return len(distribution)


def _bus_factor_risk(bf: int) -> str:
    if bf <= 1:
        return "CRITICAL"
    if bf == 2:
        return "HIGH"
    if bf == 3:
        return "MEDIUM"
    return "LOW"


def _drift_risk(score: float) -> str:
    if score > 70:
        return "CRITICAL"
    if score > 40:
        return "HIGH"
    if score > 20:
        return "MEDIUM"
    return "STABLE"


def _succession_risk(score: float) -> str:
    if score > 80:
        return "CRITICAL"
    if score > 50:
        return "HIGH"
    if score > 25:
        return "MEDIUM"
    return "LOW"


def _jsd(p: dict[str, float], q: dict[str, float]) -> float:
    actors = set(p) | set(q)
    if not actors:
        return 0.0

    m = {a: (p.get(a, 0.0) + q.get(a, 0.0)) / 2.0 for a in actors}

    def _kld(a: dict[str, float], b: dict[str, float]) -> float:
        value = 0.0
        for actor in actors:
            pa = a.get(actor, 0.0)
            pb = b.get(actor, 0.0)
            if pa > 0 and pb > 0:
                value += pa * math.log2(pa / pb)
        return value

    return 0.5 * _kld(p, m) + 0.5 * _kld(q, m)


class InferenceEngine:
    def __init__(self, settings: InferenceSettings, store: InferenceStore) -> None:
        self._settings = settings
        self._store = store

    async def run_once(
        self,
        repos: list[str] | None = None,
        module: str = "all",
        now: datetime | None = None,
    ) -> dict[str, int]:
        now_utc = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        latest_rows = await self._store.fetch_latest_snapshot(
            window_days=self._settings.window_days,
            repos=repos,
        )
        baseline_rows = await self._store.fetch_baseline_snapshot(
            window_days=self._settings.window_days,
            baseline_offset_days=self._settings.baseline_offset_days,
            repos=repos,
        )

        latest = _group_snapshot(latest_rows)
        baseline = _group_snapshot(baseline_rows)

        await self._store.reset_run(computed_at=now_utc)

        results = {"bus_factor": 0, "drift": 0, "succession": 0}

        if module in {"all", "bus_factor"}:
            rows = self._build_bus_factor_rows(latest, now_utc)
            results["bus_factor"] = await self._store.insert_bus_factor_rows(rows)

        if module in {"all", "drift"}:
            rows = self._build_drift_rows(latest, baseline, now_utc)
            results["drift"] = await self._store.insert_drift_rows(rows)

        if module in {"all", "succession"}:
            rows = self._build_succession_rows(latest, baseline, now_utc)
            results["succession"] = await self._store.insert_succession_rows(rows)

        log.info(
            "Inference complete bus_factor=%d drift=%d succession=%d",
            results["bus_factor"],
            results["drift"],
            results["succession"],
        )
        return results

    async def run_history(
        self,
        repos: list[str] | None = None,
        module: str = "all",
        step_days: int = 30,
        max_points: int = 240,
    ) -> dict[str, int]:
        step_days = max(1, int(step_days))
        max_points = max(1, int(max_points))

        window_ends = await self._store.fetch_window_ends(
            window_days=self._settings.window_days,
            repos=repos,
        )
        window_ends = self._sample_window_ends(
            window_ends=window_ends,
            step_days=step_days,
            max_points=max_points,
        )

        results = {"bus_factor": 0, "drift": 0, "succession": 0, "points": len(window_ends)}
        for window_end in window_ends:
            window_end_utc = window_end.astimezone(timezone.utc)
            latest_rows = await self._store.fetch_snapshot_at(
                window_days=self._settings.window_days,
                window_end=window_end_utc,
                repos=repos,
            )
            baseline_rows = await self._store.fetch_baseline_snapshot_at(
                window_days=self._settings.window_days,
                window_end=window_end_utc,
                baseline_offset_days=self._settings.baseline_offset_days,
                repos=repos,
            )

            latest = _group_snapshot(latest_rows)
            baseline = _group_snapshot(baseline_rows)

            await self._store.reset_run(computed_at=window_end_utc)

            if module in {"all", "bus_factor"}:
                rows = self._build_bus_factor_rows(latest, window_end_utc)
                results["bus_factor"] += await self._store.insert_bus_factor_rows(rows)

            if module in {"all", "drift"}:
                rows = self._build_drift_rows(latest, baseline, window_end_utc)
                results["drift"] += await self._store.insert_drift_rows(rows)

            if module in {"all", "succession"}:
                rows = self._build_succession_rows(latest, baseline, window_end_utc)
                results["succession"] += await self._store.insert_succession_rows(rows)

        log.info(
            "Inference history complete points=%d bus_factor=%d drift=%d succession=%d",
            results["points"],
            results["bus_factor"],
            results["drift"],
            results["succession"],
        )
        return results

    def _sample_window_ends(
        self,
        *,
        window_ends: list[datetime],
        step_days: int,
        max_points: int,
    ) -> list[datetime]:
        if not window_ends:
            return []

        sampled: list[datetime] = [window_ends[0]]
        min_step = timedelta(days=step_days)
        for ts in window_ends[1:]:
            if ts - sampled[-1] >= min_step:
                sampled.append(ts)

        if sampled[-1] != window_ends[-1]:
            sampled.append(window_ends[-1])

        if len(sampled) <= max_points:
            return sampled

        stride = max(1, len(sampled) // max_points)
        out = sampled[::stride]
        if out[-1] != sampled[-1]:
            out.append(sampled[-1])
        return out[-max_points:]

    def _build_bus_factor_rows(
        self,
        latest: dict[tuple[str, str], list[dict[str, Any]]],
        computed_at: datetime,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for (repo, module), distribution in latest.items():
            bf = _bus_factor(distribution)
            top_owners = [
                {
                    "actor": d["actor"],
                    "owner_share": float(d["owner_share"]),
                }
                for d in distribution[:5]
            ]
            rows.append(
                {
                    "repo": repo,
                    "module": module,
                    "window_days": self._settings.window_days,
                    "computed_at": computed_at,
                    "bus_factor": bf,
                    "risk_level": _bus_factor_risk(bf),
                    "top_owners": top_owners,
                    "explanation": f"{bf} contributors needed to cross 50% ownership share",
                }
            )
        return rows

    def _build_drift_rows(
        self,
        latest: dict[tuple[str, str], list[dict[str, Any]]],
        baseline: dict[tuple[str, str], list[dict[str, Any]]],
        computed_at: datetime,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []

        for key, current_dist in latest.items():
            prev_dist = baseline.get(key)
            if not prev_dist:
                continue

            current_map = {r["actor"]: float(r["owner_share"]) for r in current_dist}
            prev_map = {r["actor"]: float(r["owner_share"]) for r in prev_dist}
            jsd_value = _jsd(current_map, prev_map)
            drift_score = jsd_value * 100.0

            prev_top_owner = prev_dist[0]["actor"] if prev_dist else None
            curr_top_owner = current_dist[0]["actor"] if current_dist else None
            owner_changed = bool(prev_top_owner and curr_top_owner and prev_top_owner != curr_top_owner)

            rows.append(
                {
                    "repo": key[0],
                    "module": key[1],
                    "computed_at": computed_at,
                    "drift_score": drift_score,
                    "risk_level": _drift_risk(drift_score),
                    "jsd_value": jsd_value,
                    "prev_top_owner": prev_top_owner,
                    "curr_top_owner": curr_top_owner,
                    "owner_changed": owner_changed,
                    "explanation": "Ownership drift computed via Jensen-Shannon divergence",
                }
            )
        return rows

    def _build_succession_rows(
        self,
        latest: dict[tuple[str, str], list[dict[str, Any]]],
        baseline: dict[tuple[str, str], list[dict[str, Any]]],
        computed_at: datetime,
    ) -> list[dict[str, Any]]:
        criticality: dict[tuple[str, str], float] = defaultdict(float)
        gaps: dict[tuple[str, str], list[float]] = defaultdict(list)
        owned_modules: dict[tuple[str, str], list[str]] = defaultdict(list)

        curr_weight: dict[tuple[str, str], float] = defaultdict(float)
        prev_weight: dict[tuple[str, str], float] = defaultdict(float)

        for (repo, module), dist in latest.items():
            if not dist:
                continue

            bf = max(_bus_factor(dist), 1)
            top = dist[0]
            second_share = float(dist[1]["owner_share"]) if len(dist) > 1 else 0.0
            top_share = float(top["owner_share"])
            gap = max(top_share - second_share, 0.0)

            actor_key = (repo, top["actor"])
            criticality[actor_key] += top_share * (1.0 / bf)
            gaps[actor_key].append(gap)
            owned_modules[actor_key].append(module)

            for row in dist:
                curr_weight[(repo, row["actor"])] += float(row.get("weight", 0.0))

        for (repo, _module), dist in baseline.items():
            for row in dist:
                prev_weight[(repo, row["actor"])] += float(row.get("weight", 0.0))

        rows: list[dict[str, Any]] = []
        all_keys = set(curr_weight) | set(prev_weight) | set(criticality)
        for repo, actor in sorted(all_keys):
            crit = float(criticality.get((repo, actor), 0.0))
            actor_gaps = gaps.get((repo, actor), [])
            avg_gap = (sum(actor_gaps) / len(actor_gaps)) if actor_gaps else 1.0
            replaceability = max(0.0, min(1.0, 1.0 - avg_gap))

            prev = float(prev_weight.get((repo, actor), 0.0))
            curr = float(curr_weight.get((repo, actor), 0.0))
            continuity = (curr / prev) if prev > 0 else 1.0
            continuity = max(continuity, 0.1)

            score = crit * (1.0 - replaceability) * (1.0 / continuity) * 100.0
            risk_level = _succession_risk(score)

            rows.append(
                {
                    "repo": repo,
                    "actor": actor,
                    "computed_at": computed_at,
                    "risk_score": score,
                    "risk_level": risk_level,
                    "criticality": crit,
                    "replaceability": replaceability,
                    "continuity": continuity,
                    "owned_modules": owned_modules.get((repo, actor), []),
                    "explanation": "Risk = criticality x (1-replaceability) x (1/continuity)",
                }
            )

        return rows
