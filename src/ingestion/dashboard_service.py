import json
from collections import Counter
from typing import Protocol


class DashboardRepositoryProtocol(Protocol):
    def read_latest_runs(self, limit: int = 20) -> list[dict[str, object]]: ...

    def read_status_counters(self) -> dict[str, int]: ...

    def read_learning_metrics(self, horizon: str = "1M") -> dict[str, object]: ...

    def read_forecast_error_category_stats(
        self, horizon: str = "1M", limit: int = 5
    ) -> list[dict[str, object]]: ...


def _count_non_empty_evidence(value: object) -> bool:
    if isinstance(value, list):
        return len(value) > 0
    if isinstance(value, str):
        return len(_parse_evidence_items(value)) > 0
    return value is not None


def _parse_evidence_items(value: object) -> list[object]:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        trimmed = value.strip()
        if trimmed in {"", "[]", "null", "None"}:
            return []
        try:
            parsed = json.loads(trimmed)
            if isinstance(parsed, list):
                return parsed
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


def _classify_evidence_gap_reason(evidence_hard: object, evidence_soft: object) -> str:
    hard_items = _parse_evidence_items(evidence_hard)
    soft_items = _parse_evidence_items(evidence_soft)

    has_hard = len(hard_items) > 0
    has_traceable_hard = _has_traceable_hard_evidence(evidence_hard)
    has_soft = len(soft_items) > 0

    if has_hard and has_traceable_hard:
        return "none"
    if has_hard and not has_traceable_hard:
        return "hard_untraceable" if has_soft else "hard_untraceable_no_soft"
    if not has_hard and has_soft:
        return "missing_hard"
    return "missing_hard_and_soft"


def _safe_repo_call(default: object, fn: object, *args: object, **kwargs: object) -> object:
    try:
        if not callable(fn):
            return default
        return fn(*args, **kwargs)
    except Exception:
        return default


def build_dashboard_view(
    repository: DashboardRepositoryProtocol,
    limit: int = 20,
) -> dict[str, object]:
    recent_runs = _safe_repo_call([], repository.read_latest_runs, limit=limit)
    counters = _safe_repo_call(
        {"raw_events": 0, "canonical_events": 0, "quarantine_events": 0},
        repository.read_status_counters,
    )
    learning_metrics = _safe_repo_call(
        {
            "horizon": "1M",
            "forecast_count": 0,
            "realized_count": 0,
            "realization_coverage": None,
            "hit_rate": None,
            "mean_abs_forecast_error": None,
            "mean_signed_forecast_error": None,
        },
        repository.read_learning_metrics,
        horizon="1M",
    )

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
    attribution_gap_rows: list[dict[str, object]] = []
    attribution_gap_rows_status = "unknown"
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

    if hasattr(repository, "read_forecast_error_attributions"):
        try:
            attribution_rows = repository.read_forecast_error_attributions(horizon="1M", limit=200)
            attribution_gap_rows_status = "ok"
        except Exception:
            attribution_rows = []
            attribution_gap_rows_status = "unknown"
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
                reason = _classify_evidence_gap_reason(
                    row.get("evidence_hard"),
                    row.get("evidence_soft"),
                )
                if has_hard:
                    hard_count += 1
                if has_traceable_hard:
                    traceable_hard_count += 1
                if has_soft:
                    soft_count += 1
                if reason != "none":
                    evidence_gap_count += 1

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
                        "evidence_gap_reason": reason,
                        "source": row.get("source"),
                        "metric": row.get("metric"),
                        "as_of": row.get("as_of"),
                        "lineage_id": row.get("lineage_id"),
                    }
                )

            if valid_rows > 0:
                attribution_summary["total"] = valid_rows
                attribution_summary["hard_evidence_coverage"] = hard_count / valid_rows
                attribution_summary["hard_evidence_traceability_coverage"] = (
                    traceable_hard_count / valid_rows
                )
                attribution_summary["soft_evidence_coverage"] = soft_count / valid_rows
                attribution_summary["evidence_gap_count"] = evidence_gap_count
                attribution_summary["evidence_gap_coverage"] = evidence_gap_count / valid_rows
    else:
        attribution_gap_rows_status = "unknown"

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
        "attribution_summary": attribution_summary,
        "attribution_gap_rows": attribution_gap_rows[:50],
        "attribution_gap_rows_status": attribution_gap_rows_status,
        "recent_runs": recent_runs,
    }
