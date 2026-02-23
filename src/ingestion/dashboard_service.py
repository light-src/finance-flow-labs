import json
import os
from collections import Counter
from datetime import datetime, timezone
from typing import Protocol


REQUIRED_LEARNING_HORIZONS: tuple[str, ...] = ("1W", "1M", "3M")
DEFAULT_MIN_REALIZED_BY_HORIZON: dict[str, int] = {"1W": 8, "1M": 12, "3M": 6}
DEFAULT_COVERAGE_FLOOR: float = 0.4
DEFAULT_BENCHMARK_MAX_STALE_DAYS: int = 7
DEFAULT_DEPLOYED_ACCESS_MAX_STALE_HOURS: int = 24
DEFAULT_DEPLOYED_ACCESS_FUTURE_SKEW_MINUTES: int = 5
DEFAULT_DEPLOYED_ACCESS_STATUS: dict[str, object] = {
    "status": "unknown",
    "reason": "access_check_unavailable",
    "checked_at": None,
    "remediation_hint": None,
}
POLICY_CHECK_BENCHMARK_KEYS: tuple[str, ...] = ("QQQ", "KOSPI200", "BTC", "SGOV")
POLICY_UNIVERSE_REGION_SENTINELS: dict[str, tuple[str, ...]] = {
    "US": ("QQQ",),
    "KR": ("KOSPI200",),
    "CRYPTO": ("BTC",),
}
POLICY_LOCK_REFERENCE = "docs/POLICY_LOCK_V1.md"


class DashboardRepositoryProtocol(Protocol):
    def read_latest_runs(self, limit: int = 20) -> list[dict[str, object]]: ...

    def read_status_counters(self) -> dict[str, int]: ...

    def read_learning_metrics(self, horizon: str = "1M") -> dict[str, object]: ...

    def read_forecast_error_category_stats(
        self, horizon: str = "1M", limit: int = 5
    ) -> list[dict[str, object]]: ...

    def read_forecast_error_attribution_detail(
        self,
        attribution_id: int,
        max_preview_chars: int = 240,
    ) -> dict[str, object] | None: ...

    def read_latest_canonical_metric(self, metric_name: str) -> dict[str, object] | None: ...


def _count_non_empty_evidence(value: object) -> bool:
    if isinstance(value, (list, str, dict)):
        return len(_parse_evidence_items(value)) > 0
    return value is not None


def _parse_evidence_items(value: object) -> list[object]:
    if isinstance(value, list):
        return value
    if isinstance(value, dict):
        if not value:
            return []
        items = value.get("items")
        if isinstance(items, list):
            return items
        return [value]
    if isinstance(value, str):
        trimmed = value.strip()
        if trimmed in {"", "[]", "null", "None", "{}"}:
            return []
        try:
            parsed = json.loads(trimmed)
            if isinstance(parsed, list):
                return parsed
            if isinstance(parsed, dict):
                items = parsed.get("items")
                if isinstance(items, list):
                    return items
                return [parsed]
        except json.JSONDecodeError:
            return []
    return []


def _has_traceable_hard_evidence(value: object) -> bool:
    for item in _parse_evidence_items(value):
        if not isinstance(item, dict):
            continue

        source = str(item.get("source", "")).strip()
        metric = str(item.get("metric", "") or item.get("metric_key", "")).strip()
        entity_id = str(item.get("entity_id", "")).strip()
        as_of = str(item.get("as_of", "")).strip()
        available_at = str(item.get("available_at", "")).strip()
        lineage_id = str(item.get("lineage_id", "")).strip()

        has_reference = bool(metric or entity_id or as_of or available_at or lineage_id)
        if source and has_reference:
            return True
    return False


def _preview_trace_fields(value: object) -> dict[str, str]:
    for item in _parse_evidence_items(value):
        if not isinstance(item, dict):
            continue
        source = str(item.get("source", "")).strip()
        metric = str(item.get("metric", "") or item.get("metric_key", "")).strip()
        as_of = str(item.get("as_of", "")).strip()
        lineage_id = str(item.get("lineage_id", "")).strip()
        return {
            "trace_source": source or "",
            "trace_metric": metric or "",
            "trace_as_of": as_of or "",
            "trace_lineage_id": lineage_id or "",
        }
    return {
        "trace_source": "",
        "trace_metric": "",
        "trace_as_of": "",
        "trace_lineage_id": "",
    }


def _classify_evidence_gap(evidence_hard: object, evidence_soft: object) -> str:
    hard_items = _parse_evidence_items(evidence_hard)
    soft_items = _parse_evidence_items(evidence_soft)
    has_hard = len(hard_items) > 0
    has_soft = len(soft_items) > 0
    has_traceable_hard = _has_traceable_hard_evidence(evidence_hard)

    if not has_hard and not has_soft:
        return "missing_hard_and_soft"
    if has_hard and not has_traceable_hard:
        return "hard_untraceable"
    return "none"


def _to_reason_codes(evidence_gap_reason: str) -> list[str]:
    if evidence_gap_reason == "missing_hard_and_soft":
        return ["missing_hard_evidence", "missing_soft_evidence"]
    if evidence_gap_reason == "hard_untraceable":
        return ["hard_evidence_untraceable"]
    return []


def _recommended_action(evidence_gap_reason: str) -> str:
    if evidence_gap_reason == "missing_hard_and_soft":
        return "data_lag_or_pipeline_failure"
    if evidence_gap_reason == "hard_untraceable":
        return "mapping_fix_required"
    return "no_action"


def _safe_repo_call(default: object, fn: object, *args: object, **kwargs: object) -> object:
    try:
        if not callable(fn):
            return default
        return fn(*args, **kwargs)
    except Exception:
        return default


def _parse_iso_utc(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    trimmed = value.strip()
    if not trimmed:
        return None
    try:
        parsed = datetime.fromisoformat(trimmed.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _default_learning_metrics(horizon: str) -> dict[str, object]:
    return {
        "horizon": horizon,
        "forecast_count": 0,
        "realized_count": 0,
        "realization_coverage": None,
        "hit_rate": None,
        "mean_abs_forecast_error": None,
        "mean_signed_forecast_error": None,
    }


def _int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return max(1, int(raw.strip()))
    except ValueError:
        return default


def _float_env(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return float(raw.strip())
    except ValueError:
        return default




def _normalize_deployed_access_status(payload: object) -> dict[str, object]:
    if not isinstance(payload, dict):
        return dict(DEFAULT_DEPLOYED_ACCESS_STATUS)

    status_raw = str(payload.get("status", "")).strip().upper()
    if status_raw in {"OK", "HEALTHY"}:
        status = "ok"
    elif status_raw in {"DEGRADED", "AUTH_WALL", "CRITICAL"}:
        status = "degraded"
    elif status_raw:
        status = status_raw.lower()
    else:
        ok_flag = payload.get("ok")
        auth_wall = payload.get("auth_wall_redirect")
        if ok_flag is True:
            status = "ok"
        elif auth_wall is True:
            status = "degraded"
        else:
            status = "unknown"

    checked_at = payload.get("checked_at") or payload.get("as_of")
    reason = payload.get("reason")
    remediation_hint = payload.get("remediation_hint")

    checked_at_str = str(checked_at) if checked_at is not None else None
    checked_at_dt = _parse_iso_utc(checked_at_str)
    now_utc = datetime.now(timezone.utc)
    max_stale_hours = _int_env("STREAMLIT_ACCESS_MAX_STALE_HOURS", DEFAULT_DEPLOYED_ACCESS_MAX_STALE_HOURS)
    future_skew_minutes = _int_env(
        "STREAMLIT_ACCESS_FUTURE_SKEW_MINUTES", DEFAULT_DEPLOYED_ACCESS_FUTURE_SKEW_MINUTES
    )
    stale_age_hours: float | None = None
    is_stale = False
    is_future_skew = False
    if checked_at_dt is not None:
        delta_seconds = (now_utc - checked_at_dt).total_seconds()
        is_future_skew = delta_seconds < -(future_skew_minutes * 60)
        stale_age_hours = max(0.0, delta_seconds / 3600.0)
        is_stale = stale_age_hours >= float(max_stale_hours)

    normalized_reason = str(reason) if reason is not None else "access_check_unavailable"
    normalized_remediation_hint = str(remediation_hint) if remediation_hint is not None else None

    if status == "ok":
        if checked_at_dt is None:
            status = "unknown"
            normalized_reason = "access_check_missing_checked_at"
            if normalized_remediation_hint is None:
                normalized_remediation_hint = "Provide checked_at/as_of in STREAMLIT_ACCESS_CHECK_JSON for freshness validation."
        elif is_future_skew:
            status = "unknown"
            normalized_reason = f"access_check_future_timestamp:>{future_skew_minutes}m"
            if normalized_remediation_hint is None:
                normalized_remediation_hint = "Verify system clock and regenerate STREAMLIT_ACCESS_CHECK_JSON with UTC checked_at."
        elif is_stale:
            status = "degraded"
            normalized_reason = f"access_check_stale:{stale_age_hours:.1f}h>={max_stale_hours}h"
            if normalized_remediation_hint is None:
                normalized_remediation_hint = "Rerun streamlit-access-check and refresh STREAMLIT_ACCESS_CHECK_JSON."

    return {
        "status": status,
        "reason": normalized_reason,
        "checked_at": checked_at_str,
        "remediation_hint": normalized_remediation_hint,
        "is_stale": is_stale,
        "is_future_skew": is_future_skew,
        "stale_age_hours": stale_age_hours,
    }


def _load_deployed_access_status() -> dict[str, object]:
    access_mode = os.getenv("DEPLOY_ACCESS_MODE", "public").strip().lower() or "public"
    restricted_login_path = os.getenv("DEPLOY_RESTRICTED_LOGIN_PATH")

    raw_json = os.getenv("STREAMLIT_ACCESS_CHECK_JSON")
    if raw_json is None or raw_json.strip() == "":
        base = dict(DEFAULT_DEPLOYED_ACCESS_STATUS)
    else:
        try:
            payload = json.loads(raw_json)
        except json.JSONDecodeError:
            base = {
                "status": "unknown",
                "reason": "invalid_access_check_json",
                "checked_at": None,
                "remediation_hint": "Set STREAMLIT_ACCESS_CHECK_JSON to valid JSON from streamlit-access-check output.",
            }
        else:
            base = _normalize_deployed_access_status(payload)

    base["deploy_access_mode"] = access_mode
    base["restricted_login_path"] = restricted_login_path
    return base

def _reliability_thresholds() -> dict[str, object]:
    min_realized_by_horizon = {
        horizon: _int_env(
            f"LEARNING_RELIABILITY_MIN_REALIZED_{horizon}",
            DEFAULT_MIN_REALIZED_BY_HORIZON[horizon],
        )
        for horizon in REQUIRED_LEARNING_HORIZONS
    }
    return {
        "min_realized_by_horizon": min_realized_by_horizon,
        "coverage_floor": _float_env("LEARNING_RELIABILITY_COVERAGE_FLOOR", DEFAULT_COVERAGE_FLOOR),
    }


def _classify_learning_reliability(
    row: dict[str, object],
    min_realized_required: int,
    coverage_floor: float,
) -> dict[str, object]:
    realized_count_raw = row.get("realized_count", 0)
    try:
        realized_count = int(realized_count_raw)
    except (TypeError, ValueError):
        realized_count = 0

    coverage_raw = row.get("realization_coverage")
    try:
        coverage = None if coverage_raw is None else float(coverage_raw)
    except (TypeError, ValueError):
        coverage = None

    low_sample_threshold = max(min_realized_required + 1, min_realized_required * 2)
    if realized_count < min_realized_required:
        state = "insufficient"
        reason = f"realized_count_below_min:{realized_count}<{min_realized_required}"
    elif realized_count < low_sample_threshold:
        state = "low_sample"
        reason = f"realized_count_low_sample:{realized_count}<{low_sample_threshold}"
    elif coverage is not None and coverage < coverage_floor:
        state = "low_sample"
        reason = f"coverage_below_floor:{coverage:.3f}<{coverage_floor:.3f}"
    else:
        state = "reliable"
        reason = "sample_and_coverage_ok"

    return {
        "reliability_state": state,
        "reliability_reason": reason,
        "min_realized_required": min_realized_required,
        "observed_realized_count": realized_count,
        "coverage_floor": coverage_floor,
    }


def _build_policy_compliance(
    repository: DashboardRepositoryProtocol,
    counters: dict[str, object],
    learning_metrics_by_horizon: dict[str, dict[str, object]],
    latest_run_time: str | None,
) -> dict[str, object]:
    checks: list[dict[str, object]] = []

    raw_events = int(counters.get("raw_events", 0) or 0)
    canonical_events = int(counters.get("canonical_events", 0) or 0)

    universe_regions_present: dict[str, bool] = {
        region: False for region in POLICY_UNIVERSE_REGION_SENTINELS
    }
    universe_region_evidence: dict[str, list[dict[str, object]]] = {
        region: [] for region in POLICY_UNIVERSE_REGION_SENTINELS
    }

    if hasattr(repository, "read_macro_series_points"):
        for region, metric_keys in POLICY_UNIVERSE_REGION_SENTINELS.items():
            for metric_key in metric_keys:
                points = _safe_repo_call([], repository.read_macro_series_points, metric_key=metric_key, limit=1)
                if isinstance(points, list) and points:
                    top = points[0] if isinstance(points[0], dict) else {}
                    universe_regions_present[region] = True
                    universe_region_evidence[region].append(
                        {
                            "metric_key": metric_key,
                            "as_of": top.get("as_of"),
                            "source": top.get("source"),
                        }
                    )
                    break

    required_region_count = len(POLICY_UNIVERSE_REGION_SENTINELS)
    present_region_count = sum(1 for is_present in universe_regions_present.values() if is_present)
    missing_regions = [
        region for region, is_present in universe_regions_present.items() if not is_present
    ]
    region_coverage_counts = {
        region: len(universe_region_evidence.get(region, []))
        for region in POLICY_UNIVERSE_REGION_SENTINELS
    }
    region_metadata_completeness = (
        present_region_count / required_region_count if required_region_count > 0 else 0.0
    )

    if all(universe_regions_present.values()):
        universe_status = "PASS"
        universe_reason = "Region-aware HARD evidence confirms US/KR/Crypto coverage."
    elif raw_events > 0 and canonical_events > 0:
        universe_status = "WARN"
        universe_reason = (
            "Ingest data exists but region-aware coverage evidence is incomplete: "
            + ", ".join(missing_regions)
        )
    else:
        universe_status = "WARN"
        universe_reason = "No ingest evidence; universe coverage cannot be validated."

    checks.append(
        {
            "check": "Universe coverage (US/KR/Crypto)",
            "status": universe_status,
            "reason": universe_reason,
            "as_of": latest_run_time,
            "evidence": {
                "raw_events": raw_events,
                "canonical_events": canonical_events,
                "regions_present": universe_regions_present,
                "missing_regions": missing_regions,
                "region_metric_evidence": universe_region_evidence,
                "region_coverage_counts": region_coverage_counts,
                "required_region_count": required_region_count,
                "present_region_count": present_region_count,
                "region_metadata_completeness": region_metadata_completeness,
                "region_dimension_ready": all(universe_regions_present.values()),
            },
        }
    )

    checks.extend(
        [
            {
                "check": "KR asset scope lock (Index/ETF/single-stock)",
                "status": "PASS",
                "reason": "Policy lock declares KR index+ETF+single-stock scope.",
                "as_of": latest_run_time,
                "evidence": {"policy_lock": POLICY_LOCK_REFERENCE},
            },
            {
                "check": "Max MDD guardrail lock (-30%)",
                "status": "PASS",
                "reason": "Policy lock declares max drawdown guardrail at -30%.",
                "as_of": latest_run_time,
                "evidence": {"policy_lock": POLICY_LOCK_REFERENCE},
            },
        ]
    )

    checks.append(
        {
            "check": "Crypto scope lock (BTC/ETH core + top alts 일부)",
            "status": "PASS",
            "reason": "Policy lock allows BTC/ETH core with capped top-alt sleeve.",
            "as_of": latest_run_time,
            "evidence": {"policy_lock": POLICY_LOCK_REFERENCE},
        }
    )

    crypto_btc_eth_metric = os.getenv("PORTFOLIO_EXPOSURE_CRYPTO_BTC_ETH_METRIC", "portfolio_exposure_crypto_btc_eth_share")
    crypto_alt_metric = os.getenv("PORTFOLIO_EXPOSURE_CRYPTO_ALT_METRIC", "portfolio_exposure_crypto_alt_share")
    leverage_metric = os.getenv("PORTFOLIO_EXPOSURE_LEVERAGE_METRIC", "portfolio_exposure_leverage_share")

    def _to_share(value: object) -> float | None:
        try:
            if value is None:
                return None
            parsed = float(value)
            if parsed < 0:
                return None
            if parsed > 1.0:
                return parsed / 100.0 if parsed <= 100.0 else None
            return parsed
        except (TypeError, ValueError):
            return None

    crypto_btc_eth_row = None
    crypto_alt_row = None
    leverage_row = None
    if hasattr(repository, "read_latest_canonical_metric"):
        crypto_btc_eth_row = _safe_repo_call(None, repository.read_latest_canonical_metric, metric_name=crypto_btc_eth_metric)
        crypto_alt_row = _safe_repo_call(None, repository.read_latest_canonical_metric, metric_name=crypto_alt_metric)
        leverage_row = _safe_repo_call(None, repository.read_latest_canonical_metric, metric_name=leverage_metric)

    btc_eth_share = _to_share((crypto_btc_eth_row or {}).get("metric_value") if isinstance(crypto_btc_eth_row, dict) else None)
    alt_share = _to_share((crypto_alt_row or {}).get("metric_value") if isinstance(crypto_alt_row, dict) else None)
    if btc_eth_share is not None and alt_share is None:
        alt_share = max(0.0, 1.0 - btc_eth_share)
    if alt_share is not None and btc_eth_share is None:
        btc_eth_share = max(0.0, 1.0 - alt_share)

    if btc_eth_share is None or alt_share is None:
        checks.append(
            {
                "check": "Crypto sleeve composition (BTC/ETH >=70%, alts <=30%)",
                "status": "UNKNOWN",
                "reason": "Portfolio crypto sleeve exposure feed missing latest btc/eth or alt share.",
                "as_of": latest_run_time,
                "evidence": {
                    "dependency": "portfolio_exposure_crypto_sleeve",
                    "metric_names": [crypto_btc_eth_metric, crypto_alt_metric],
                    "btc_eth_row": crypto_btc_eth_row,
                    "alt_row": crypto_alt_row,
                },
            }
        )
    else:
        crypto_pass = btc_eth_share >= 0.70 and alt_share <= 0.30
        checks.append(
            {
                "check": "Crypto sleeve composition (BTC/ETH >=70%, alts <=30%)",
                "status": "PASS" if crypto_pass else "FAIL",
                "reason": (
                    "Crypto sleeve is within policy threshold."
                    if crypto_pass
                    else "Crypto sleeve violates policy threshold (btc_eth>=70% and alt<=30%)."
                ),
                "as_of": max(
                    [
                        v
                        for v in [
                            (crypto_btc_eth_row or {}).get("as_of") if isinstance(crypto_btc_eth_row, dict) else None,
                            (crypto_alt_row or {}).get("as_of") if isinstance(crypto_alt_row, dict) else None,
                        ]
                        if v is not None
                    ],
                    default=latest_run_time,
                ),
                "evidence": {
                    "btc_eth_share": btc_eth_share,
                    "alt_share": alt_share,
                    "metric_names": [crypto_btc_eth_metric, crypto_alt_metric],
                    "btc_eth_row": crypto_btc_eth_row,
                    "alt_row": crypto_alt_row,
                },
            }
        )

    leverage_share = _to_share((leverage_row or {}).get("metric_value") if isinstance(leverage_row, dict) else None)
    if leverage_share is None:
        checks.append(
            {
                "check": "Leverage sleeve cap (<=20%)",
                "status": "UNKNOWN",
                "reason": "Portfolio leverage exposure feed missing latest leverage share.",
                "as_of": latest_run_time,
                "evidence": {
                    "dependency": "portfolio_exposure_leverage_sleeve",
                    "metric_name": leverage_metric,
                    "row": leverage_row,
                },
            }
        )
    else:
        leverage_pass = leverage_share <= 0.20
        checks.append(
            {
                "check": "Leverage sleeve cap (<=20%)",
                "status": "PASS" if leverage_pass else "FAIL",
                "reason": (
                    "Leverage sleeve is within policy cap."
                    if leverage_pass
                    else "Leverage sleeve exceeds policy cap of 20%."
                ),
                "as_of": (leverage_row or {}).get("as_of") if isinstance(leverage_row, dict) else latest_run_time,
                "evidence": {
                    "leverage_share": leverage_share,
                    "metric_name": leverage_metric,
                    "row": leverage_row,
                },
            }
        )

    primary = learning_metrics_by_horizon.get("1M", {})
    reliability = str(primary.get("reliability_state", "insufficient"))
    if reliability == "reliable":
        primary_status = "PASS"
    elif reliability == "low_sample":
        primary_status = "WARN"
    else:
        primary_status = "FAIL"
    checks.append(
        {
            "check": "Primary horizon readiness (1M)",
            "status": primary_status,
            "reason": str(primary.get("reliability_reason", "missing_reliability_metadata")),
            "as_of": latest_run_time,
            "evidence": {
                "reliability_state": reliability,
                "realized_count": primary.get("realized_count", 0),
                "min_realized_required": primary.get("min_realized_required"),
                "realization_coverage": primary.get("realization_coverage"),
            },
        }
    )

    benchmark_points: dict[str, int] = {}
    benchmark_as_of: dict[str, object] = {}
    if hasattr(repository, "read_macro_series_points"):
        for key in POLICY_CHECK_BENCHMARK_KEYS:
            points = _safe_repo_call([], repository.read_macro_series_points, metric_key=key, limit=1)
            if isinstance(points, list) and points:
                benchmark_points[key] = len(points)
                top = points[0] if isinstance(points[0], dict) else {}
                benchmark_as_of[key] = top.get("as_of")
            else:
                benchmark_points[key] = 0
                benchmark_as_of[key] = None
    else:
        benchmark_points = {key: 0 for key in POLICY_CHECK_BENCHMARK_KEYS}
        benchmark_as_of = {key: None for key in POLICY_CHECK_BENCHMARK_KEYS}

    missing_keys = [key for key, count in benchmark_points.items() if count == 0]
    max_stale_days = _int_env("POLICY_CHECK_BENCHMARK_MAX_STALE_DAYS", DEFAULT_BENCHMARK_MAX_STALE_DAYS)
    latest_run_dt = _parse_iso_utc(latest_run_time)
    stale_keys: list[str] = []
    stale_age_days: dict[str, int] = {}
    if latest_run_dt is not None:
        for key, as_of in benchmark_as_of.items():
            as_of_dt = _parse_iso_utc(as_of)
            if as_of_dt is None:
                continue
            age_days = max(0, int((latest_run_dt - as_of_dt).total_seconds() // 86400))
            stale_age_days[key] = age_days
            if age_days >= max_stale_days:
                stale_keys.append(key)

    if missing_keys:
        benchmark_status = "WARN"
        benchmark_reason = f"Missing benchmark series: {', '.join(missing_keys)}"
    elif stale_keys:
        benchmark_status = "WARN"
        benchmark_reason = (
            f"Stale benchmark series (>={max_stale_days}d): {', '.join(sorted(stale_keys))}"
        )
    else:
        benchmark_status = "PASS"
        benchmark_reason = "All benchmark components have recent points."

    checks.append(
        {
            "check": "Benchmark readiness (QQQ/KOSPI200/BTC/SGOV)",
            "status": benchmark_status,
            "reason": benchmark_reason,
            "as_of": max([v for v in benchmark_as_of.values() if v is not None], default=latest_run_time),
            "evidence": {
                "series_point_count": benchmark_points,
                "series_latest_as_of": benchmark_as_of,
                "series_age_days": stale_age_days,
                "max_stale_days": max_stale_days,
            },
        }
    )

    checks.extend(
        [
            {
                "check": "Rebalancing cadence lock (Quarterly)",
                "status": "PASS",
                "reason": "Policy lock sets rebalancing cadence to quarterly.",
                "as_of": latest_run_time,
                "evidence": {"policy_lock": POLICY_LOCK_REFERENCE},
            },
            {
                "check": "Execution mode lock (paper auto / real manual)",
                "status": "PASS",
                "reason": "Policy lock enforces manual approval for real trades.",
                "as_of": latest_run_time,
                "evidence": {"policy_lock": POLICY_LOCK_REFERENCE},
            },
            {
                "check": "Reporting SLA lock (daily summary + critical alerts)",
                "status": "PASS",
                "reason": "Policy lock defines daily detailed summary and immediate critical alerts.",
                "as_of": latest_run_time,
                "evidence": {"policy_lock": POLICY_LOCK_REFERENCE},
            },
        ]
    )

    summary = {
        "total": len(checks),
        "pass": sum(1 for row in checks if row["status"] == "PASS"),
        "warn": sum(1 for row in checks if row["status"] == "WARN"),
        "fail": sum(1 for row in checks if row["status"] == "FAIL"),
        "unknown": sum(1 for row in checks if row["status"] == "UNKNOWN"),
    }
    return {"checks": checks, "summary": summary}


def build_dashboard_view(
    repository: DashboardRepositoryProtocol,
    limit: int = 20,
) -> dict[str, object]:
    recent_runs = _safe_repo_call([], repository.read_latest_runs, limit=limit)
    counters = _safe_repo_call(
        {"raw_events": 0, "canonical_events": 0, "quarantine_events": 0},
        repository.read_status_counters,
    )
    tracked_horizons = REQUIRED_LEARNING_HORIZONS
    thresholds = _reliability_thresholds()
    min_realized_by_horizon = thresholds["min_realized_by_horizon"]
    coverage_floor = float(thresholds["coverage_floor"])
    learning_metrics_by_horizon = {
        horizon: _safe_repo_call(
            _default_learning_metrics(horizon),
            repository.read_learning_metrics,
            horizon=horizon,
        )
        for horizon in tracked_horizons
    }
    learning_reliability_by_horizon = {
        horizon: _classify_learning_reliability(
            row if isinstance(row, dict) else {},
            int(min_realized_by_horizon[horizon]),
            coverage_floor,
        )
        for horizon, row in learning_metrics_by_horizon.items()
    }
    for horizon, row in learning_metrics_by_horizon.items():
        if isinstance(row, dict):
            row.update(learning_reliability_by_horizon[horizon])
    learning_metrics = learning_metrics_by_horizon["1M"]

    attribution_summary = {
        "total": 0,
        "top_category": "n/a",
        "top_count": 0,
        "top_categories": [],
        "hard_evidence_coverage": None,
        "hard_evidence_traceability_coverage": None,
        "soft_evidence_coverage": None,
        "evidence_gap_count": 0,
        "evidence_gap_coverage": None,
    }
    if hasattr(repository, "read_forecast_error_category_stats"):
        category_stats = _safe_repo_call(
            [],
            repository.read_forecast_error_category_stats,
            horizon="1M",
            limit=5,
        )
        if isinstance(category_stats, list) and category_stats:
            top = category_stats[0]
            attribution_summary = {
                "total": int(sum(int(row.get("attribution_count", 0)) for row in category_stats)),
                "top_category": str(top.get("category", "n/a")),
                "top_count": int(top.get("attribution_count", 0)),
                "top_categories": category_stats,
                "hard_evidence_coverage": None,
                "hard_evidence_traceability_coverage": None,
                "soft_evidence_coverage": None,
                "evidence_gap_count": 0,
                "evidence_gap_coverage": None,
            }

    attribution_gap_rows: list[dict[str, object]] = []
    attribution_gap_details: dict[int, dict[str, object]] = {}
    if hasattr(repository, "read_forecast_error_attributions"):
        attribution_rows = _safe_repo_call(
            [],
            repository.read_forecast_error_attributions,
            horizon="1M",
            limit=200,
        )
        if isinstance(attribution_rows, list) and attribution_rows:
            categories = [
                str(row.get("category", "unknown"))
                for row in attribution_rows
                if isinstance(row, dict) and row.get("category")
            ]
            category_counts = Counter(categories)
            if category_counts and attribution_summary["top_category"] == "n/a":
                top_category, top_count = category_counts.most_common(1)[0]
                attribution_summary.update(
                    {
                        "total": len(attribution_rows),
                        "top_category": top_category,
                        "top_count": int(top_count),
                        "top_categories": [
                            {"category": key, "attribution_count": int(value)}
                            for key, value in category_counts.most_common(5)
                        ],
                    }
                )

            hard_count = 0
            traceable_hard_count = 0
            soft_count = 0
            evidence_gap_count = 0
            valid_rows = 0
            for row in attribution_rows:
                if not isinstance(row, dict):
                    continue
                valid_rows += 1
                has_hard = _count_non_empty_evidence(row.get("evidence_hard"))
                has_traceable_hard = _has_traceable_hard_evidence(row.get("evidence_hard"))
                has_soft = _count_non_empty_evidence(row.get("evidence_soft"))
                evidence_gap_reason = _classify_evidence_gap(
                    row.get("evidence_hard"), row.get("evidence_soft")
                )
                trace_fields = _preview_trace_fields(row.get("evidence_hard"))
                attribution_gap_rows.append(
                    {
                        "attribution_id": row.get("attribution_id"),
                        "thesis_id": row.get("thesis_id"),
                        "forecast_id": row.get("forecast_id"),
                        "horizon": row.get("horizon", "1M"),
                        "category": row.get("category", "unknown"),
                        "created_at": row.get("created_at"),
                        "has_hard_evidence": has_hard,
                        "has_traceable_hard_evidence": has_traceable_hard,
                        "has_soft_evidence": has_soft,
                        "evidence_gap_reason": evidence_gap_reason,
                        "reason_codes": _to_reason_codes(evidence_gap_reason),
                        "recommended_action": _recommended_action(evidence_gap_reason),
                        **trace_fields,
                    }
                )
                if has_hard:
                    hard_count += 1
                if has_traceable_hard:
                    traceable_hard_count += 1
                if has_soft:
                    soft_count += 1
                if evidence_gap_reason == "missing_hard_and_soft":
                    evidence_gap_count += 1

            if hasattr(repository, "read_forecast_error_attribution_detail"):
                for row in attribution_gap_rows:
                    if row.get("evidence_gap_reason") == "none":
                        continue
                    attribution_id = row.get("attribution_id")
                    if not isinstance(attribution_id, int):
                        continue
                    detail = _safe_repo_call(
                        None,
                        repository.read_forecast_error_attribution_detail,
                        attribution_id=attribution_id,
                        max_preview_chars=240,
                    )
                    if isinstance(detail, dict):
                        attribution_gap_details[attribution_id] = detail

            if valid_rows > 0:
                attribution_summary["total"] = valid_rows
                attribution_summary["hard_evidence_coverage"] = hard_count / valid_rows
                attribution_summary["hard_evidence_traceability_coverage"] = (
                    traceable_hard_count / valid_rows
                )
                attribution_summary["soft_evidence_coverage"] = soft_count / valid_rows
                attribution_summary["evidence_gap_count"] = evidence_gap_count
                attribution_summary["evidence_gap_coverage"] = evidence_gap_count / valid_rows

    pending_refresh_requests: list[dict[str, object]] = []
    if hasattr(repository, "read_pending_refresh_requests"):
        pending_rows = _safe_repo_call([], repository.read_pending_refresh_requests, limit=50)
        if isinstance(pending_rows, list):
            pending_refresh_requests = [row for row in pending_rows if isinstance(row, dict)]

    if isinstance(recent_runs, list) and recent_runs:
        latest = recent_runs[0]
        if isinstance(latest, dict):
            last_run_status = str(latest.get("status", "unknown"))
            last_run_time = str(latest.get("finished_at", ""))
        else:
            last_run_status = "unknown"
            last_run_time = ""
    else:
        last_run_status = "no-data"
        last_run_time = ""

    return {
        "last_run_status": last_run_status,
        "last_run_time": last_run_time,
        "counters": counters,
        "learning_metrics": learning_metrics,
        "learning_metrics_by_horizon": learning_metrics_by_horizon,
        "learning_reliability_by_horizon": learning_reliability_by_horizon,
        "learning_reliability_thresholds": {
            "min_realized_by_horizon": min_realized_by_horizon,
            "coverage_floor": coverage_floor,
        },
        "attribution_summary": attribution_summary,
        "attribution_gap_rows": attribution_gap_rows,
        "attribution_gap_details": attribution_gap_details,
        "policy_compliance": _build_policy_compliance(
            repository=repository,
            counters=counters if isinstance(counters, dict) else {},
            learning_metrics_by_horizon=learning_metrics_by_horizon,
            latest_run_time=last_run_time or None,
        ),
        "deployed_access": _load_deployed_access_status(),
        "pending_refresh_requests": pending_refresh_requests,
        "recent_runs": recent_runs,
    }
