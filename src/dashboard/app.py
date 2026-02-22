import os
import importlib
from collections.abc import Mapping


UNKNOWN_PLACEHOLDERS = {"", "n/a", "na", "none", "null", "unknown", "-", "--"}
CRITICAL_METRICS = {
    "raw_events",
    "canonical_events",
    "quarantine_events",
    "forecast_count",
    "realized_count",
    "coverage_pct",
    "attribution_total",
    "hard_evidence_pct",
    "hard_evidence_traceability_pct",
    "evidence_gap_count",
}


def build_operator_cards(view: Mapping[str, object]) -> dict[str, object]:
    def parse_int_metric(value: object) -> tuple[int | None, str]:
        if value is None:
            return None, "unknown"
        if isinstance(value, bool):
            return int(value), "ok"
        if isinstance(value, int):
            return value, "ok"
        if isinstance(value, float):
            return int(value), "ok"
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in UNKNOWN_PLACEHOLDERS:
                return None, "unknown"
            try:
                return int(value), "ok"
            except ValueError:
                try:
                    return int(float(value)), "ok"
                except ValueError:
                    return None, "error"
        return None, "error"

    def parse_float_metric(value: object) -> tuple[float | None, str]:
        if value is None:
            return None, "unknown"
        if isinstance(value, bool):
            return float(int(value)), "ok"
        if isinstance(value, (int, float)):
            return float(value), "ok"
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in UNKNOWN_PLACEHOLDERS:
                return None, "unknown"
            try:
                return float(value), "ok"
            except ValueError:
                return None, "error"
        return None, "error"

    metric_status: dict[str, str] = {}

    counters = view.get("counters", {})
    if isinstance(counters, Mapping):
        raw_events, metric_status["raw_events"] = parse_int_metric(counters.get("raw_events"))
        canonical_events, metric_status["canonical_events"] = parse_int_metric(counters.get("canonical_events"))
        quarantine_events, metric_status["quarantine_events"] = parse_int_metric(counters.get("quarantine_events"))
    else:
        raw_events, metric_status["raw_events"] = None, "unknown"
        canonical_events, metric_status["canonical_events"] = None, "unknown"
        quarantine_events, metric_status["quarantine_events"] = None, "unknown"

    learning = view.get("learning_metrics", {})
    if isinstance(learning, Mapping):
        forecast_count, metric_status["forecast_count"] = parse_int_metric(learning.get("forecast_count"))
        realized_count, metric_status["realized_count"] = parse_int_metric(learning.get("realized_count"))
        realization_coverage = learning.get("realization_coverage")
        hit_rate = learning.get("hit_rate")
        mae = learning.get("mean_abs_forecast_error")
        signed_error = learning.get("mean_signed_forecast_error")
    else:
        forecast_count, metric_status["forecast_count"] = None, "unknown"
        realized_count, metric_status["realized_count"] = None, "unknown"
        realization_coverage = None
        hit_rate = None
        mae = None
        signed_error = None

    attribution_summary = view.get("attribution_summary", {})
    if isinstance(attribution_summary, Mapping):
        attribution_total, metric_status["attribution_total"] = parse_int_metric(attribution_summary.get("total"))
        top_category = str(attribution_summary.get("top_category", "n/a"))
        top_count, metric_status["attribution_top_count"] = parse_int_metric(attribution_summary.get("top_count"))
        hard_evidence_coverage = attribution_summary.get("hard_evidence_coverage")
        hard_evidence_traceability_coverage = attribution_summary.get(
            "hard_evidence_traceability_coverage"
        )
        soft_evidence_coverage = attribution_summary.get("soft_evidence_coverage")
        evidence_gap_count, metric_status["evidence_gap_count"] = parse_int_metric(
            attribution_summary.get("evidence_gap_count")
        )
        evidence_gap_coverage = attribution_summary.get("evidence_gap_coverage")
    else:
        attribution_total, metric_status["attribution_total"] = None, "unknown"
        top_category = "n/a"
        top_count, metric_status["attribution_top_count"] = None, "unknown"
        hard_evidence_coverage = None
        hard_evidence_traceability_coverage = None
        soft_evidence_coverage = None
        evidence_gap_count, metric_status["evidence_gap_count"] = None, "unknown"
        evidence_gap_coverage = None

    realization_coverage_value, metric_status["coverage_pct"] = parse_float_metric(realization_coverage)
    hit_rate_value, metric_status["hit_rate_pct"] = parse_float_metric(hit_rate)
    mae_value, metric_status["mae_pct"] = parse_float_metric(mae)
    signed_error_value, metric_status["signed_error_pct"] = parse_float_metric(signed_error)
    hard_evidence_value, metric_status["hard_evidence_pct"] = parse_float_metric(hard_evidence_coverage)
    hard_evidence_traceability_value, metric_status["hard_evidence_traceability_pct"] = parse_float_metric(
        hard_evidence_traceability_coverage
    )
    soft_evidence_value, metric_status["soft_evidence_pct"] = parse_float_metric(soft_evidence_coverage)
    evidence_gap_value, metric_status["evidence_gap_pct"] = parse_float_metric(evidence_gap_coverage)

    coverage_pct = "n/a" if realization_coverage_value is None else f"{realization_coverage_value * 100:.1f}%"
    hit_rate_pct = "n/a" if hit_rate_value is None else f"{hit_rate_value * 100:.1f}%"
    mae_pct = "n/a" if mae_value is None else f"{mae_value * 100:.2f}%"
    signed_error_pct = "n/a" if signed_error_value is None else f"{signed_error_value * 100:.2f}%"
    hard_evidence_pct = "n/a" if hard_evidence_value is None else f"{hard_evidence_value * 100:.1f}%"
    hard_evidence_traceability_pct = (
        "n/a" if hard_evidence_traceability_value is None else f"{hard_evidence_traceability_value * 100:.1f}%"
    )
    soft_evidence_pct = "n/a" if soft_evidence_value is None else f"{soft_evidence_value * 100:.1f}%"
    evidence_gap_pct = "n/a" if evidence_gap_value is None else f"{evidence_gap_value * 100:.1f}%"

    critical_unknown_or_error = [
        metric_name
        for metric_name in CRITICAL_METRICS
        if metric_status.get(metric_name) in {"unknown", "error"}
    ]

    return {
        "last_run_status": str(view.get("last_run_status", "no-data")),
        "last_run_time": str(view.get("last_run_time", "")),
        "raw_events": "n/a" if raw_events is None else raw_events,
        "canonical_events": "n/a" if canonical_events is None else canonical_events,
        "quarantine_events": "n/a" if quarantine_events is None else quarantine_events,
        "forecast_count": "n/a" if forecast_count is None else forecast_count,
        "realized_count": "n/a" if realized_count is None else realized_count,
        "coverage_pct": coverage_pct,
        "hit_rate_pct": hit_rate_pct,
        "mae_pct": mae_pct,
        "signed_error_pct": signed_error_pct,
        "attribution_total": "n/a" if attribution_total is None else attribution_total,
        "attribution_top_category": top_category,
        "attribution_top_count": "n/a" if top_count is None else top_count,
        "hard_evidence_pct": hard_evidence_pct,
        "hard_evidence_traceability_pct": hard_evidence_traceability_pct,
        "soft_evidence_pct": soft_evidence_pct,
        "evidence_gap_count": "n/a" if evidence_gap_count is None else evidence_gap_count,
        "evidence_gap_pct": evidence_gap_pct,
        "metric_status": metric_status,
        "critical_unknown_or_error": critical_unknown_or_error,
    }


def load_dashboard_view(dsn: str) -> dict[str, object]:
    dashboard_service = importlib.import_module("src.ingestion.dashboard_service")
    postgres_repository = importlib.import_module("src.ingestion.postgres_repository")
    repository = postgres_repository.PostgresRepository(dsn=dsn)
    build_dashboard_view = dashboard_service.build_dashboard_view
    return build_dashboard_view(repository)


def run_streamlit_app(dsn: str) -> None:
    st = importlib.import_module("streamlit")

    view = load_dashboard_view(dsn)
    cards = build_operator_cards(view)

    st.set_page_config(page_title="Ingestion Operator Dashboard", layout="wide")
    st.title("Ingestion Operator Dashboard")
    st.caption("Manual update monitoring (cron separated)")

    if cards["critical_unknown_or_error"]:
        st.warning(
            "Some dashboard metrics are unavailable or malformed; verify the data pipeline before acting. "
            f"Critical metrics affected: {', '.join(cards['critical_unknown_or_error'])}"
        )

    c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16 = st.columns(16)
    c1.metric("Last Status", cards["last_run_status"], cards["last_run_time"])
    c2.metric("Raw", cards["raw_events"])
    c3.metric("Canonical", cards["canonical_events"])
    c4.metric("Quarantine", cards["quarantine_events"])
    c5.metric("1M Forecasts", cards["forecast_count"])
    c6.metric("1M Realized", cards["realized_count"])
    c7.metric("1M Coverage", cards["coverage_pct"])
    c8.metric("1M Hit Rate", cards["hit_rate_pct"])
    c9.metric("1M MAE", cards["mae_pct"])
    c10.metric("1M Bias", cards["signed_error_pct"])
    c11.metric("1M Attr", cards["attribution_total"])
    c12.metric("Top Attr", cards["attribution_top_category"], cards["attribution_top_count"])
    c13.metric("HARD Evd", cards["hard_evidence_pct"])
    c14.metric("HARD Trace", cards["hard_evidence_traceability_pct"])
    c15.metric("SOFT Evd", cards["soft_evidence_pct"])
    c16.metric("No-Evd Attr", cards["evidence_gap_count"], cards["evidence_gap_pct"])

    recent_runs = view.get("recent_runs", [])
    if isinstance(recent_runs, list) and recent_runs:
        st.subheader("Recent Runs")
        st.dataframe(recent_runs, use_container_width=True)
    else:
        st.info("No run history found.")


def main() -> None:
    dsn = os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL")
    if not dsn:
        raise ValueError("SUPABASE_DB_URL or DATABASE_URL is required")
    run_streamlit_app(dsn)


if __name__ == "__main__":
    main()
