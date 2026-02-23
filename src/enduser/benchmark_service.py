from __future__ import annotations

import os
from collections.abc import Mapping
from datetime import date, datetime

BENCHMARK_COMPONENTS: tuple[str, ...] = ("QQQ", "KOSPI200", "BTC", "SGOV")
DEFAULT_BENCHMARK_WEIGHTS: dict[str, float] = {
    "QQQ": 0.45,
    "KOSPI200": 0.25,
    "BTC": 0.20,
    "SGOV": 0.10,
}
WEIGHT_ENV_MAP: dict[str, str] = {
    "QQQ": "BENCHMARK_WEIGHT_QQQ",
    "KOSPI200": "BENCHMARK_WEIGHT_KOSPI200",
    "BTC": "BENCHMARK_WEIGHT_BTC",
    "SGOV": "BENCHMARK_WEIGHT_SGOV",
}


def _parse_as_of(raw: object) -> date | None:
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return raw.date()
    if isinstance(raw, date):
        return raw
    text = str(raw).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text).date()
    except ValueError:
        return None


def _load_weights() -> dict[str, float]:
    weights = dict(DEFAULT_BENCHMARK_WEIGHTS)
    for key, env_name in WEIGHT_ENV_MAP.items():
        raw = os.getenv(env_name)
        if raw is None or raw.strip() == "":
            continue
        try:
            parsed = float(raw)
        except ValueError:
            continue
        if parsed < 0:
            continue
        weights[key] = parsed

    total_weight = sum(weights.values())
    if total_weight <= 0:
        return dict(DEFAULT_BENCHMARK_WEIGHTS)

    # Keep benchmark return scale stable even when env overrides are partially set.
    return {key: value / total_weight for key, value in weights.items()}


def _build_daily_levels(repository: object, metric_key: str) -> dict[date, float]:
    rows = repository.read_macro_series_points(metric_key, limit=10_000)
    levels: dict[date, float] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        day = _parse_as_of(row.get("as_of"))
        if day is None or day in levels:
            continue
        value = row.get("value")
        try:
            levels[day] = float(value)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            continue
    return levels


def _build_daily_returns(levels: dict[date, float]) -> dict[date, float]:
    returns: dict[date, float] = {}
    ordered_days = sorted(levels.keys())
    for idx in range(1, len(ordered_days)):
        prev_day = ordered_days[idx - 1]
        day = ordered_days[idx]
        prev_value = levels[prev_day]
        current_value = levels[day]
        if prev_value == 0:
            continue
        returns[day] = (current_value / prev_value) - 1.0
    return returns


def compute_benchmark_series(repository: object, start_date: object, end_date: object) -> list[dict[str, object]]:
    """Compute weighted daily return / indexed NAV series for policy benchmark.

    Returns [{as_of, benchmark_return, benchmark_nav}, ...] for dates where all
    benchmark components have an available daily return.
    """

    start = _parse_as_of(start_date)
    end = _parse_as_of(end_date)
    if start is None or end is None or start > end:
        return []

    weights = _load_weights()
    per_component_returns: dict[str, dict[date, float]] = {}
    for metric_key in BENCHMARK_COMPONENTS:
        levels = _build_daily_levels(repository, metric_key)
        per_component_returns[metric_key] = _build_daily_returns(levels)

    if not per_component_returns:
        return []

    common_dates: set[date] | None = None
    for metric_key in BENCHMARK_COMPONENTS:
        metric_dates = set(per_component_returns[metric_key].keys())
        common_dates = metric_dates if common_dates is None else common_dates & metric_dates

    if not common_dates:
        return []

    nav = 1.0
    series: list[dict[str, object]] = []
    for day in sorted(common_dates):
        if day < start or day > end:
            continue
        benchmark_return = 0.0
        for metric_key in BENCHMARK_COMPONENTS:
            benchmark_return += weights.get(metric_key, 0.0) * per_component_returns[metric_key][day]
        nav *= 1.0 + benchmark_return
        series.append(
            {
                "as_of": day.isoformat(),
                "benchmark_return": benchmark_return,
                "benchmark_nav": nav,
            }
        )

    return series
