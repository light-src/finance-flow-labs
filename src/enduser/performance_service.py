from __future__ import annotations

import math
import os
from datetime import date, datetime
from statistics import stdev
from typing import Any

from src.enduser.benchmark_service import compute_benchmark_series


DEFAULT_MDD_ALERT_THRESHOLD = -0.20


def _parse_as_of(raw: object) -> date | None:
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


def _to_float(raw: object) -> float | None:
    try:
        if raw is None:
            return None
        value = float(raw)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(value):
        return None
    return value


def _load_mdd_alert_threshold() -> float:
    raw = os.getenv("MDD_ALERT_THRESHOLD")
    if raw is None or raw.strip() == "":
        return DEFAULT_MDD_ALERT_THRESHOLD

    try:
        parsed = float(raw)
    except ValueError:
        return DEFAULT_MDD_ALERT_THRESHOLD

    if parsed > 0:
        parsed = -parsed

    if parsed < -1.0:
        parsed = parsed / 100.0

    if parsed > 0 or parsed < -1.0:
        return DEFAULT_MDD_ALERT_THRESHOLD

    return parsed


def _compute_mdd(nav_points: list[tuple[date, float]]) -> float | None:
    if not nav_points:
        return None

    peak = nav_points[0][1]
    if peak <= 0:
        return None

    mdd = 0.0
    for _, nav in nav_points:
        if nav <= 0:
            continue
        if nav > peak:
            peak = nav
            continue
        drawdown = (nav / peak) - 1.0
        if drawdown < mdd:
            mdd = drawdown
    return mdd


def _compute_daily_returns(nav_points: list[tuple[date, float]]) -> list[float]:
    returns: list[float] = []
    for idx in range(1, len(nav_points)):
        prev_nav = nav_points[idx - 1][1]
        nav = nav_points[idx][1]
        if prev_nav <= 0:
            continue
        returns.append((nav / prev_nav) - 1.0)
    return returns


def _compute_sharpe_ratio(daily_returns: list[float]) -> float | None:
    if len(daily_returns) < 2:
        return None

    mean_return = sum(daily_returns) / len(daily_returns)
    volatility = stdev(daily_returns)
    if volatility == 0:
        return None

    return (mean_return / volatility) * math.sqrt(252)


def build_performance_view(repository: object, limit: int = 365) -> dict[str, Any]:
    rows = repository.read_portfolio_snapshots(limit=limit) if hasattr(repository, "read_portfolio_snapshots") else []

    nav_points: list[tuple[date, float]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        as_of = _parse_as_of(row.get("as_of"))
        nav = _to_float(row.get("nav"))
        if as_of is None or nav is None or nav <= 0:
            continue
        nav_points.append((as_of, nav))

    nav_points = sorted(nav_points, key=lambda item: item[0])

    nav_series: list[dict[str, Any]] = []
    for idx, (as_of, nav) in enumerate(nav_points):
        if idx == 0:
            daily_return = None
        else:
            prev_nav = nav_points[idx - 1][1]
            daily_return = (nav / prev_nav) - 1.0 if prev_nav > 0 else None
        nav_series.append(
            {
                "as_of": as_of.isoformat(),
                "nav": nav,
                "daily_return": daily_return,
            }
        )

    total_return: float | None = None
    if len(nav_points) >= 2 and nav_points[0][1] > 0:
        total_return = (nav_points[-1][1] / nav_points[0][1]) - 1.0

    mdd = _compute_mdd(nav_points)
    sharpe = _compute_sharpe_ratio(_compute_daily_returns(nav_points))

    benchmark_series: list[dict[str, Any]] = []
    benchmark_total_return: float | None = None
    if nav_points:
        benchmark_series = compute_benchmark_series(
            repository,
            start_date=nav_points[0][0].isoformat(),
            end_date=nav_points[-1][0].isoformat(),
        )
        if benchmark_series:
            last_nav = _to_float(benchmark_series[-1].get("benchmark_nav"))
            if last_nav is not None:
                benchmark_total_return = last_nav - 1.0

    alpha: float | None = None
    if total_return is not None and benchmark_total_return is not None:
        alpha = total_return - benchmark_total_return

    mdd_alert_threshold = _load_mdd_alert_threshold()
    # Floating-point safety for threshold edge values (e.g. -0.2 exactly).
    mdd_alert = bool(mdd is not None and mdd <= (mdd_alert_threshold + 1e-9))

    return {
        "total_return_pct": total_return * 100 if total_return is not None else None,
        "mdd_pct": mdd * 100 if mdd is not None else None,
        "sharpe_ratio": sharpe,
        "alpha_pct": alpha * 100 if alpha is not None else None,
        "mdd_alert": mdd_alert,
        "mdd_alert_threshold_pct": mdd_alert_threshold * 100,
        "nav_series": nav_series,
        "benchmark_series": benchmark_series,
    }
