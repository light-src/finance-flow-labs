import os
import math
import importlib
from collections.abc import Mapping

from src.ingestion.dashboard_service import REQUIRED_LEARNING_HORIZONS

PLACEHOLDER_STRINGS = {"", "-", "n/a", "na", "none", "null", "unknown"}
CRITICAL_METRIC_KEYS = {
    "raw_events",
    "canonical_events",
    "quarantine_events",
    "forecast_count",
    "realized_count",
    "coverage_pct",
    "hit_rate_pct",
    "attribution_total",
    "hard_evidence_pct",
    "hard_evidence_traceability_pct",
    "evidence_gap_count",
}

RELIABILITY_BADGES = {
    "reliable": "🟢 reliable",
    "low_sample": "🟠 low_sample",
    "insufficient": "🔴 insufficient",
}

LOCKED_POLICY_ROWS: tuple[tuple[str, str], ...] = (
    ("Universe", "US + KR + Crypto"),
    ("KR asset scope", "Index + ETF + single stocks"),
    ("Crypto scope", "BTC/ETH core + top alts 일부"),
    ("Max MDD guardrail", "-30%"),
    ("Leverage sleeve cap", "20%"),
    ("Benchmark composite", "45% QQQ + 25% KOSPI200 proxy + 20% BTC + 10% SGOV"),
    ("Primary evaluation horizon", "1M (aux: 1W, 3M)"),
    ("Rebalancing cadence", "Quarterly"),
    ("Execution mode", "Paper-trade auto, real-trade manual approval"),
    ("Reporting", "Daily summary + immediate critical-event alerts"),
)

KPI_LAYOUT_TIERS: tuple[tuple[str, tuple[str, ...]], ...] = (
    (
        "Tier 1 · Critical",
        (
            "last_status",
            "primary_reliability",
            "policy_summary",
            "evidence_gap_count",
        ),
    ),
    (
        "Tier 2 · Core Pipeline",
        (
            "raw_events",
            "canonical_events",
            "quarantine_events",
            "forecast_count",
            "realized_count",
            "coverage_pct",
        ),
    ),
    (
        "Tier 2 · Core Pipeline (cont.)",
        (
            "hit_rate_pct",
        ),
    ),
    (
        "Tier 3 · Diagnostics",
        (
            "mae_pct",
            "signed_error_pct",
            "attribution_total",
            "attribution_top_category",
            "hard_evidence_traceability_pct",
            "soft_evidence_pct",
        ),
    ),
    (
        "Tier 3 · Diagnostics (cont.)",
        (
            "hard_evidence_pct",
            "evidence_gap_pct",
        ),
    ),
)


def get_kpi_layout_tiers() -> tuple[tuple[str, tuple[str, ...]], ...]:
    return KPI_LAYOUT_TIERS


def _is_placeholder(value: object) -> bool:
    return isinstance(value, str) and value.strip().lower() in PLACEHOLDER_STRINGS


def _metric(value: object, status: str = "ok", reason: str | None = None) -> dict[str, object]:
    return {"value": value, "status": status, "reason": reason}


def _humanize_reliability_reason(reason: str) -> str:
    if reason.startswith("realized_count_below_min:"):
        detail = reason.split(":", 1)[1] if ":" in reason else ""
        return f"Realized sample below minimum ({detail})."
    if reason.startswith("realized_count_low_sample:"):
        detail = reason.split(":", 1)[1] if ":" in reason else ""
        return f"Realized sample is still low ({detail})."
    if reason.startswith("coverage_below_floor:"):
        detail = reason.split(":", 1)[1] if ":" in reason else ""
        return f"Realization coverage below floor ({detail})."
    if reason == "sample_and_coverage_ok":
        return "Sample size and coverage meet reliability threshold."
    if reason == "missing_reliability_metadata":
        return "Reliability metadata missing from backend response."
    return reason


def _build_policy_panel_rows(policy_checks: list[object]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for check in policy_checks:
        if not isinstance(check, Mapping):
            continue
        row = dict(check)
        if row.get("check") == "Universe coverage (US/KR/Crypto)":
            evidence = row.get("evidence") if isinstance(row.get("evidence"), Mapping) else {}
            coverage_counts = evidence.get("region_coverage_counts") if isinstance(evidence, Mapping) else None
            if not isinstance(coverage_counts, Mapping):
                coverage_counts = {}
            row["us_coverage_count"] = int(coverage_counts.get("US", 0) or 0)
            row["kr_coverage_count"] = int(coverage_counts.get("KR", 0) or 0)
            row["crypto_coverage_count"] = int(coverage_counts.get("CRYPTO", 0) or 0)
            completeness = evidence.get("region_metadata_completeness") if isinstance(evidence, Mapping) else None
            if isinstance(completeness, (int, float)) and math.isfinite(float(completeness)):
                row["region_metadata_completeness"] = f"{float(completeness) * 100:.1f}%"
            else:
                row["region_metadata_completeness"] = "n/a"
            row["evaluation_window_as_of"] = row.get("as_of") or "n/a"
        rows.append(row)
    return rows


def build_operator_cards(view: Mapping[str, object]) -> dict[str, object]:
    def to_int_metric(value: object) -> dict[str, object]:
        if value is None or _is_placeholder(value):
            return _metric("n/a", status="unknown", reason="missing_or_placeholder")
        if isinstance(value, bool):
            return _metric(int(value))
        if isinstance(value, int):
            return _metric(value)
        if isinstance(value, float):
            if not math.isfinite(value):
                return _metric("n/a", status="error", reason="non_finite_numeric")
            return _metric(int(value))
        if isinstance(value, str):
            normalized = value.strip().replace(",", "")
            try:
                return _metric(int(normalized))
            except ValueError:
                try:
                    parsed = float(normalized)
                except ValueError:
                    return _metric("n/a", status="error", reason="invalid_numeric")
                if not math.isfinite(parsed):
                    return _metric("n/a", status="error", reason="non_finite_numeric")
                return _metric(int(parsed))
        return _metric("n/a", status="error", reason="unsupported_type")

    def to_pct_metric(value: object, precision: int) -> dict[str, object]:
        if value is None or _is_placeholder(value):
            return _metric("n/a", status="unknown", reason="missing_or_placeholder")
        if isinstance(value, bool):
            raw = float(int(value))
        elif isinstance(value, (int, float)):
            raw = float(value)
        elif isinstance(value, str):
            normalized = value.strip().replace(",", "")
            scale = 1.0
            if normalized.endswith("%"):
                normalized = normalized[:-1].strip()
                scale = 0.01
            try:
                raw = float(normalized) * scale
            except ValueError:
                return _metric("n/a", status="error", reason="invalid_numeric")
        else:
            return _metric("n/a", status="error", reason="unsupported_type")
        if not math.isfinite(raw):
            return _metric("n/a", status="error", reason="non_finite_numeric")
        return _metric(f"{raw * 100:.{precision}f}%")

    counters = view.get("counters", {})
    if isinstance(counters, Mapping):
        raw_events = to_int_metric(counters.get("raw_events"))
        canonical_events = to_int_metric(counters.get("canonical_events"))
        quarantine_events = to_int_metric(counters.get("quarantine_events"))
    else:
        raw_events = _metric("n/a", status="unknown", reason="missing_block")
        canonical_events = _metric("n/a", status="unknown", reason="missing_block")
        quarantine_events = _metric("n/a", status="unknown", reason="missing_block")

    learning = view.get("learning_metrics", {})
    if isinstance(learning, Mapping):
        forecast_count = to_int_metric(learning.get("forecast_count"))
        realized_count = to_int_metric(learning.get("realized_count"))
        coverage_pct = to_pct_metric(learning.get("realization_coverage"), precision=1)
        hit_rate_pct = to_pct_metric(learning.get("hit_rate"), precision=1)
        mae_pct = to_pct_metric(learning.get("mean_abs_forecast_error"), precision=2)
        signed_error_pct = to_pct_metric(learning.get("mean_signed_forecast_error"), precision=2)
    else:
        forecast_count = _metric("n/a", status="unknown", reason="missing_block")
        realized_count = _metric("n/a", status="unknown", reason="missing_block")
        coverage_pct = _metric("n/a", status="unknown", reason="missing_block")
        hit_rate_pct = _metric("n/a", status="unknown", reason="missing_block")
        mae_pct = _metric("n/a", status="unknown", reason="missing_block")
        signed_error_pct = _metric("n/a", status="unknown", reason="missing_block")

    attribution_summary = view.get("attribution_summary", {})
    if isinstance(attribution_summary, Mapping):
        attribution_total = to_int_metric(attribution_summary.get("total"))
        top_category = _metric(str(attribution_summary.get("top_category", "n/a")))
        top_count = to_int_metric(attribution_summary.get("top_count"))
        hard_evidence_pct = to_pct_metric(attribution_summary.get("hard_evidence_coverage"), precision=1)
        hard_evidence_traceability_pct = to_pct_metric(
            attribution_summary.get("hard_evidence_traceability_coverage"), precision=1
        )
        soft_evidence_pct = to_pct_metric(attribution_summary.get("soft_evidence_coverage"), precision=1)
        evidence_gap_count = to_int_metric(attribution_summary.get("evidence_gap_count"))
        evidence_gap_pct = to_pct_metric(attribution_summary.get("evidence_gap_coverage"), precision=1)
    else:
        attribution_total = _metric("n/a", status="unknown", reason="missing_block")
        top_category = _metric("n/a", status="unknown", reason="missing_block")
        top_count = _metric("n/a", status="unknown", reason="missing_block")
        hard_evidence_pct = _metric("n/a", status="unknown", reason="missing_block")
        hard_evidence_traceability_pct = _metric("n/a", status="unknown", reason="missing_block")
        soft_evidence_pct = _metric("n/a", status="unknown", reason="missing_block")
        evidence_gap_count = _metric("n/a", status="unknown", reason="missing_block")
        evidence_gap_pct = _metric("n/a", status="unknown", reason="missing_block")

    metrics = {
        "raw_events": raw_events,
        "canonical_events": canonical_events,
        "quarantine_events": quarantine_events,
        "forecast_count": forecast_count,
        "realized_count": realized_count,
        "coverage_pct": coverage_pct,
        "hit_rate_pct": hit_rate_pct,
        "mae_pct": mae_pct,
        "signed_error_pct": signed_error_pct,
        "attribution_total": attribution_total,
        "attribution_top_category": top_category,
        "attribution_top_count": top_count,
        "hard_evidence_pct": hard_evidence_pct,
        "hard_evidence_traceability_pct": hard_evidence_traceability_pct,
        "soft_evidence_pct": soft_evidence_pct,
        "evidence_gap_count": evidence_gap_count,
        "evidence_gap_pct": evidence_gap_pct,
    }

    critical_unknown_or_error = any(
        metrics[key]["status"] in {"unknown", "error"} for key in CRITICAL_METRIC_KEYS
    )

    horizon_rows: list[dict[str, object]] = []
    learning_by_horizon_raw = view.get("learning_metrics_by_horizon", {})
    learning_by_horizon = learning_by_horizon_raw if isinstance(learning_by_horizon_raw, Mapping) else {}
    for horizon in REQUIRED_LEARNING_HORIZONS:
        row = learning_by_horizon.get(horizon)
        if not isinstance(row, Mapping):
            row = {}
        row_forecast = to_int_metric(row.get("forecast_count"))
        row_realized = to_int_metric(row.get("realized_count"))
        row_coverage = to_pct_metric(row.get("realization_coverage"), precision=1)
        row_hit_rate = to_pct_metric(row.get("hit_rate"), precision=1)
        row_mae = to_pct_metric(row.get("mean_abs_forecast_error"), precision=2)
        reliability_state = str(row.get("reliability_state", "insufficient"))
        reliability_reason = str(row.get("reliability_reason", "missing_reliability_metadata"))
        row_metrics = (row_forecast, row_realized, row_coverage, row_hit_rate, row_mae)
        if any(metric["status"] == "error" for metric in row_metrics):
            row_status = "error"
        elif any(metric["status"] == "unknown" for metric in row_metrics):
            row_status = "unknown"
        elif reliability_state == "reliable":
            row_status = "ok"
        else:
            row_status = "warn"
        horizon_rows.append(
            {
                "horizon": horizon,
                "forecast_count": row_forecast["value"],
                "realized_count": row_realized["value"],
                "coverage_pct": row_coverage["value"],
                "hit_rate_pct": row_hit_rate["value"],
                "mae_pct": row_mae["value"],
                "reliability": reliability_state,
                "reliability_badge": RELIABILITY_BADGES.get(
                    reliability_state, f"⚪ {reliability_state}"
                ),
                "reliability_reason": reliability_reason,
                "reliability_reason_text": _humanize_reliability_reason(reliability_reason),
                "min_realized_required": row.get("min_realized_required"),
                "status": row_status,
            }
        )

    has_horizon_alert = any(row.get("status") != "ok" for row in horizon_rows)
    primary_horizon_row = next((row for row in horizon_rows if row.get("horizon") == "1M"), None)
    primary_horizon_reliability_alert = bool(
        primary_horizon_row
        and primary_horizon_row.get("reliability") in {"insufficient", "low_sample"}
    )

    policy_payload = view.get("policy_compliance", {})
    policy_checks = policy_payload.get("checks") if isinstance(policy_payload, Mapping) else None
    policy_summary = policy_payload.get("summary") if isinstance(policy_payload, Mapping) else None
    if isinstance(policy_checks, list) and policy_checks:
        policy_compliance_panel = _build_policy_panel_rows(policy_checks)
    else:
        policy_compliance_panel = [
            {
                "check": item,
                "status": "UNKNOWN",
                "reason": "Compliance computation unavailable; showing locked policy rows only.",
                "as_of": None,
                "evidence": {"locked_value": locked_value},
            }
            for item, locked_value in LOCKED_POLICY_ROWS
        ]
    if not isinstance(policy_summary, Mapping):
        policy_summary = {
            "total": len(policy_compliance_panel),
            "pass": 0,
            "warn": 0,
            "fail": 0,
            "unknown": len(policy_compliance_panel),
        }

    return {
        "last_run_status": str(view.get("last_run_status", "no-data")),
        "last_run_time": str(view.get("last_run_time", "")),
        **{k: v["value"] for k, v in metrics.items()},
        "metric_status": {k: {"status": v["status"], "reason": v["reason"]} for k, v in metrics.items()},
        "has_critical_metric_alert": critical_unknown_or_error or has_horizon_alert,
        "has_primary_horizon_reliability_alert": primary_horizon_reliability_alert,
        "learning_metrics_panel": horizon_rows,
        "policy_compliance_panel": policy_compliance_panel,
        "policy_compliance_summary": dict(policy_summary),
        "deployed_access": view.get("deployed_access", {}),
        "has_deployed_access_alert": (
            isinstance(view.get("deployed_access"), Mapping)
            and str(view.get("deployed_access", {}).get("status", "unknown")).lower()
            in {"degraded", "auth_wall", "critical"}
        ),
    }


def _metric_delta(cards: Mapping[str, object], key: str) -> str | None:
    status = cards.get("metric_status", {}).get(key, {}).get("status")
    return status if status in {"unknown", "error"} else None


def _policy_summary_label(cards: Mapping[str, object]) -> str:
    summary = cards.get("policy_compliance_summary", {})
    if not isinstance(summary, Mapping):
        return "n/a"
    fail = int(summary.get("fail", 0) or 0)
    warn = int(summary.get("warn", 0) or 0)
    unknown = int(summary.get("unknown", 0) or 0)
    return f"F:{fail} W:{warn} U:{unknown}"


def _policy_summary_delta(cards: Mapping[str, object]) -> str:
    summary = cards.get("policy_compliance_summary", {})
    if not isinstance(summary, Mapping):
        return "unknown"
    fail = int(summary.get("fail", 0) or 0)
    warn = int(summary.get("warn", 0) or 0)
    unknown = int(summary.get("unknown", 0) or 0)
    if fail > 0:
        return "FAIL"
    if warn > 0:
        return "WARN"
    if unknown > 0:
        return "UNKNOWN"
    return "PASS"


def _primary_reliability(cards: Mapping[str, object]) -> tuple[str, str | None]:
    panel = cards.get("learning_metrics_panel", [])
    if not isinstance(panel, list):
        return ("n/a", "unknown")
    row = next((item for item in panel if isinstance(item, Mapping) and item.get("horizon") == "1M"), None)
    if not isinstance(row, Mapping):
        return ("n/a", "unknown")
    return (
        str(row.get("reliability_badge", "n/a")),
        str(row.get("status")) if row.get("status") in {"warn", "unknown", "error"} else None,
    )


def load_dashboard_view(dsn: str) -> dict[str, object]:
    dashboard_service = importlib.import_module("src.ingestion.dashboard_service")
    postgres_repository = importlib.import_module("src.ingestion.postgres_repository")
    repository = postgres_repository.PostgresRepository(dsn=dsn)
    build_dashboard_view = dashboard_service.build_dashboard_view
    return build_dashboard_view(repository)


def update_refresh_request_status(
    dsn: str,
    *,
    request_id: int,
    status: str,
    handler: str | None = None,
    result_message: str | None = None,
    ingestion_run_id: str | None = None,
) -> dict[str, object] | None:
    postgres_repository = importlib.import_module("src.ingestion.postgres_repository")
    repository = postgres_repository.PostgresRepository(dsn=dsn)
    return repository.update_refresh_request_status(
        request_id=request_id,
        status=status,
        handler=handler,
        result_message=result_message,
        ingestion_run_id=ingestion_run_id,
    )


def run_streamlit_app(dsn: str, *, configure_page: bool = True) -> None:
    st = importlib.import_module("streamlit")

    view = load_dashboard_view(dsn)
    cards = build_operator_cards(view)

    if configure_page:
        st.set_page_config(page_title="Ingestion Operator Dashboard", layout="wide")
    st.title("Ingestion Operator Dashboard")
    st.caption("Manual update monitoring (cron separated)")

    if cards["has_critical_metric_alert"]:
        st.warning(
            "Some dashboard metrics are unavailable or malformed; verify data pipeline before acting."
        )
    if cards.get("has_primary_horizon_reliability_alert"):
        st.warning(
            "Primary horizon (1M) learning metrics are not yet statistically reliable; treat KPI changes as directional only."
        )

    deployed_access = cards.get("deployed_access", {})
    if isinstance(deployed_access, Mapping):
        deploy_access_mode = str(deployed_access.get("deploy_access_mode", "public")).lower()
        restricted_login_path = deployed_access.get("restricted_login_path")
        if deploy_access_mode == "restricted":
            login_hint = restricted_login_path or "(not configured)"
            st.info(
                "Restricted access mode is active for this deployment. "
                f"Operator login path: {login_hint}"
            )

    if cards.get("has_deployed_access_alert") and isinstance(deployed_access, Mapping):
        status = str(deployed_access.get("status", "degraded")).upper()
        reason = str(deployed_access.get("reason", "unknown"))
        checked_at = deployed_access.get("checked_at") or "n/a"
        remediation_hint = deployed_access.get("remediation_hint") or "Check Streamlit public visibility and rerun access smoke check."
        st.error(
            f"Deployed accessibility incident ({status}) · reason={reason} · checked_at={checked_at}\n"
            f"Next step: {remediation_hint}"
        )
        st.info(
            "Degraded mode: dashboard insights may be incomplete. Execution policy remains paper-trade auto / real-trade manual approval."
        )

    if cards.get("forecast_count") == 0:
        st.info(
            "No forecast records yet. Seed the learning loop with `python3 -m src.ingestion.cli forecast-record-create ...` (see docs/ingestion-runbook.md)."
        )

    tier_specs = {
        "last_status": ("Last Status", cards["last_run_status"], cards["last_run_time"]),
        "primary_reliability": ("1M Reliability", *_primary_reliability(cards)),
        "policy_summary": (
            "Policy Summary",
            _policy_summary_label(cards),
            _policy_summary_delta(cards),
        ),
        "evidence_gap_count": ("No-Evd Attr", cards["evidence_gap_count"], cards["evidence_gap_pct"]),
        "raw_events": ("Raw", cards["raw_events"], _metric_delta(cards, "raw_events")),
        "canonical_events": (
            "Canonical",
            cards["canonical_events"],
            _metric_delta(cards, "canonical_events"),
        ),
        "quarantine_events": (
            "Quarantine",
            cards["quarantine_events"],
            _metric_delta(cards, "quarantine_events"),
        ),
        "forecast_count": (
            "1M Forecasts",
            cards["forecast_count"],
            _metric_delta(cards, "forecast_count"),
        ),
        "realized_count": (
            "1M Realized",
            cards["realized_count"],
            _metric_delta(cards, "realized_count"),
        ),
        "coverage_pct": ("1M Coverage", cards["coverage_pct"], _metric_delta(cards, "coverage_pct")),
        "hit_rate_pct": ("1M Hit Rate", cards["hit_rate_pct"], _metric_delta(cards, "hit_rate_pct")),
        "mae_pct": ("1M MAE", cards["mae_pct"], _metric_delta(cards, "mae_pct")),
        "signed_error_pct": (
            "1M Bias",
            cards["signed_error_pct"],
            _metric_delta(cards, "signed_error_pct"),
        ),
        "attribution_total": (
            "1M Attr",
            cards["attribution_total"],
            _metric_delta(cards, "attribution_total"),
        ),
        "attribution_top_category": (
            "Top Attr",
            cards["attribution_top_category"],
            cards["attribution_top_count"],
        ),
        "hard_evidence_pct": (
            "HARD Evd",
            cards["hard_evidence_pct"],
            _metric_delta(cards, "hard_evidence_pct"),
        ),
        "hard_evidence_traceability_pct": (
            "HARD Trace",
            cards["hard_evidence_traceability_pct"],
            _metric_delta(cards, "hard_evidence_traceability_pct"),
        ),
        "soft_evidence_pct": (
            "SOFT Evd",
            cards["soft_evidence_pct"],
            _metric_delta(cards, "soft_evidence_pct"),
        ),
        "evidence_gap_pct": (
            "No-Evd %",
            cards["evidence_gap_pct"],
            _metric_delta(cards, "evidence_gap_pct"),
        ),
    }

    for tier_label, keys in KPI_LAYOUT_TIERS:
        st.caption(tier_label)
        cols = st.columns(len(keys))
        for col, key in zip(cols, keys):
            label, value, delta = tier_specs[key]
            col.metric(label, value, delta)

    learning_panel = cards.get("learning_metrics_panel", [])
    if isinstance(learning_panel, list) and learning_panel:
        st.subheader("Multi-horizon Learning Metrics")
        st.dataframe(learning_panel, use_container_width=True)

    policy_panel = cards.get("policy_compliance_panel", [])
    if isinstance(policy_panel, list) and policy_panel:
        st.subheader("Policy Compliance (Locked Constraints)")
        summary = cards.get("policy_compliance_summary", {})
        if isinstance(summary, Mapping):
            p1, p2, p3, p4, p5 = st.columns(5)
            p1.metric("Total", summary.get("total", 0))
            p2.metric("PASS", summary.get("pass", 0))
            p3.metric("WARN", summary.get("warn", 0))
            p4.metric("FAIL", summary.get("fail", 0))
            p5.metric("UNKNOWN", summary.get("unknown", 0))
        st.dataframe(policy_panel, use_container_width=True)

    attribution_gap_rows = view.get("attribution_gap_rows", [])
    if isinstance(attribution_gap_rows, list):
        st.subheader("Attribution Evidence Gaps (1M)")
        only_problematic = st.checkbox(
            "Show only problematic rows", value=True, key="show_only_problematic_attribution_rows"
        )
        rows_to_show = attribution_gap_rows
        if only_problematic:
            rows_to_show = [
                row
                for row in attribution_gap_rows
                if isinstance(row, dict) and row.get("evidence_gap_reason") != "none"
            ]
        if rows_to_show:
            st.dataframe(rows_to_show, use_container_width=True)
            selectable_rows = [
                row
                for row in rows_to_show
                if isinstance(row, Mapping) and row.get("evidence_gap_reason") != "none"
            ]
            attribution_ids = [
                int(row["attribution_id"])
                for row in selectable_rows
                if isinstance(row.get("attribution_id"), int)
            ]
            if attribution_ids:
                selected_id = st.selectbox(
                    "Drill-through attribution_id",
                    attribution_ids,
                    key="attribution_gap_drillthrough_id",
                )
                detail_map = view.get("attribution_gap_details", {})
                detail = detail_map.get(selected_id) if isinstance(detail_map, Mapping) else None
                selected_row = next(
                    (
                        row
                        for row in selectable_rows
                        if isinstance(row, Mapping) and row.get("attribution_id") == selected_id
                    ),
                    None,
                )
                with st.expander("Evidence Drill-through", expanded=True):
                    if isinstance(selected_row, Mapping):
                        st.write(
                            {
                                "reason_codes": selected_row.get("reason_codes", []),
                                "recommended_action": selected_row.get("recommended_action", "no_action"),
                                "trace_source": selected_row.get("trace_source", ""),
                                "trace_metric": selected_row.get("trace_metric", ""),
                                "trace_as_of": selected_row.get("trace_as_of", ""),
                                "trace_lineage_id": selected_row.get("trace_lineage_id", ""),
                            }
                        )
                    if isinstance(detail, Mapping):
                        st.write(detail)
                    else:
                        st.info("Detail payload unavailable for selected row.")
        else:
            st.info("No attribution evidence gaps detected in recent 1M rows.")

    pending_refresh_requests = view.get("pending_refresh_requests", [])
    if isinstance(pending_refresh_requests, list) and pending_refresh_requests:
        st.subheader("Pending Refresh Requests")
        st.dataframe(pending_refresh_requests, use_container_width=True)

        request_ids = [
            int(row["id"])
            for row in pending_refresh_requests
            if isinstance(row, Mapping) and isinstance(row.get("id"), int)
        ]
        if request_ids:
            with st.expander("Handle refresh request", expanded=False):
                selected_request_id = st.selectbox(
                    "request_id",
                    request_ids,
                    key="refresh_request_selected_id",
                )
                next_status = st.selectbox(
                    "next_status",
                    ["accepted", "running", "completed", "failed", "dismissed"],
                    key="refresh_request_next_status",
                )
                handler = st.text_input("handler", value="operator", key="refresh_request_handler")
                result_message = st.text_input(
                    "result_message",
                    value="",
                    key="refresh_request_result_message",
                    help="Optional outcome note for audit trail.",
                )
                ingestion_run_id = st.text_input(
                    "ingestion_run_id",
                    value="",
                    key="refresh_request_ingestion_run_id",
                    help="Optional run reference when linked ingestion execution exists.",
                )

                if st.button("Update refresh request status", key="refresh_request_update_button"):
                    updated = update_refresh_request_status(
                        dsn,
                        request_id=int(selected_request_id),
                        status=str(next_status),
                        handler=(handler.strip() or None),
                        result_message=(result_message.strip() or None),
                        ingestion_run_id=(ingestion_run_id.strip() or None),
                    )
                    if isinstance(updated, Mapping):
                        st.success(
                            "Refresh request updated "
                            f"(request_id={updated.get('id')}, status={updated.get('status')})."
                        )
                    else:
                        st.error("Refresh request update failed. Verify request_id and database connectivity.")
    else:
        st.info("No pending refresh requests.")

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
