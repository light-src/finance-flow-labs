from __future__ import annotations

from datetime import date, datetime
from typing import Any
import importlib
import os

from src.enduser.refresh_requests import submit_macro_refresh_request

_REGIME_META: dict[str, tuple[str, str]] = {
    "risk_on": ("🟢", "Risk-On"),
    "risk_off": ("🔴", "Risk-Off"),
    "neutral": ("⚪", "Neutral"),
}

_READINESS_META: dict[str, tuple[str, str]] = {
    "ready": ("✅", "READY"),
    "stale": ("⚠️", "STALE"),
    "missing": ("🟨", "MISSING"),
    "error": ("🛑", "ERROR"),
}


def _normalize_as_of(value: object) -> str:
    if isinstance(value, datetime):
        return value.isoformat(timespec="seconds")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, str) and value.strip():
        return value
    return "N/A"


def _normalize_readiness(status: object) -> str:
    normalized = str(status or "ok").strip().lower()
    if normalized in {"ok", "ready"}:
        return "ready"
    if normalized in {"stale", "missing", "error"}:
        return normalized
    return "error"


def _emit(st: Any, level: str, text: str) -> None:
    fn = getattr(st, level, None)
    if callable(fn):
        fn(text)
        return
    fallback = getattr(st, "write", None)
    if callable(fallback):
        fallback(text)


def _render_decision_guidance(st: Any, readiness: str) -> None:
    if readiness == "ready":
        _emit(st, "success", "Decision guidance: Macro signal is decision-grade for thesis review (still apply portfolio risk limits).")
        st.write("Data update path: scheduled macro ingestion pipeline.")
        return

    if readiness == "stale":
        _emit(st, "warning", "Decision guidance: Do not execute new thesis entries from this signal until fresh data is available.")
        st.write("Data update path: wait for next scheduled ingestion or ask operator to trigger manual refresh.")
        return

    if readiness == "missing":
        _emit(st, "warning", "Decision guidance: Block signal-based investment action. No macro regime record is available yet.")
        st.write("Data update path: ingestion must create at least one macro regime snapshot.")
        return

    _emit(st, "error", "Decision guidance: Block signal-based investment action until data integrity issue is resolved.")
    st.write("Data update path: operator should inspect ingestion/DB integrity and rerun pipeline.")


def _render_freshness_lineage(st: Any, regime_signal: dict[str, Any]) -> None:
    st.write("Data freshness & lineage")
    st.caption(f"as_of: {_normalize_as_of(regime_signal.get('as_of'))}")

    freshness_days = regime_signal.get("freshness_days")
    if freshness_days is not None:
        st.caption(f"freshness_threshold: {freshness_days}d")

    lineage_id = regime_signal.get("lineage_id")
    if lineage_id:
        st.caption(f"lineage_id: {lineage_id}")

    source_tags = regime_signal.get("source_tags")
    if source_tags:
        tags = ", ".join(str(tag) for tag in source_tags if str(tag).strip())
        if tags:
            st.caption(f"source_tags: {tags}")


def render_macro_regime_card(
    regime_signal: dict[str, Any] | None,
    *,
    dsn: str | None = None,
) -> None:
    st = importlib.import_module("streamlit")

    st.subheader("Macro regime signal")

    payload = regime_signal or {"status": "missing", "message": "No macro regime signal yet. Analysis pipeline data is pending."}
    readiness = _normalize_readiness(payload.get("status"))
    badge_emoji, badge_label = _READINESS_META[readiness]
    st.markdown(f"### {badge_emoji} {badge_label}")

    message = str(payload.get("message") or "").strip()
    if message:
        if readiness == "error":
            st.error(message)
        elif readiness in {"stale", "missing"}:
            st.warning(message)
        else:
            st.info(message)

    _render_decision_guidance(st, readiness)

    if readiness in {"ready", "stale"}:
        regime_key = str(payload.get("regime", "neutral")).strip().lower().replace("-", "_")
        emoji, regime_label = _REGIME_META.get(regime_key, _REGIME_META["neutral"])

        confidence_raw = payload.get("confidence", 0.0)
        try:
            confidence = max(0.0, min(float(confidence_raw), 1.0))
        except (TypeError, ValueError):
            confidence = 0.0

        drivers = [str(item) for item in payload.get("drivers", []) if str(item).strip()][:3]

        st.markdown(f"#### Regime: {emoji} {regime_label}")
        st.write(f"신뢰도: {confidence * 100:.0f}%")
        st.progress(confidence)

        st.write("핵심 드라이버 (Top 3):")
        if drivers:
            for driver in drivers:
                st.write(f"• {driver}")
        else:
            st.write("• Driver data unavailable")

        evidence_hard = [str(item) for item in payload.get("evidence_hard", []) if str(item).strip()]
        evidence_soft = [str(item) for item in payload.get("evidence_soft", []) if str(item).strip()]

        st.write("HARD evidence:")
        if evidence_hard:
            for item in evidence_hard[:3]:
                st.write(f"• {item}")
        else:
            st.write("• 없음")

        st.write("SOFT evidence:")
        if evidence_soft:
            for item in evidence_soft[:3]:
                st.write(f"• {item}")
        else:
            st.write("• 없음")

    _render_freshness_lineage(st, payload)

    if hasattr(st, "button"):
        requested = st.button("Request data refresh", key="macro_refresh_request")
        if requested:
            if not dsn:
                st.warning("Refresh request could not be persisted (missing DB connection).")
                return

            requested_by = str(
                os.getenv("ENDUSER_REQUESTER_ID")
                or getattr(st, "session_state", {}).get("session_id")
                or "enduser_session"
            )
            try:
                result = submit_macro_refresh_request(dsn, requested_by=requested_by)
            except Exception as exc:  # pragma: no cover - UI boundary
                st.error(f"Refresh request failed: {exc}")
                return

            request_id = str(result.get("id", ""))
            status = str(result.get("status", "pending"))
            if bool(result.get("deduplicated")):
                st.warning(
                    f"Refresh request already pending within cooldown window (request_id={request_id}, status={status})."
                )
            else:
                st.success(
                    f"Refresh request submitted (request_id={request_id}, status={status}). Operator queue updated."
                )
