from __future__ import annotations

from datetime import date, datetime
from typing import Any
import importlib

_REGIME_META: dict[str, tuple[str, str]] = {
    "risk_on": ("🟢", "Risk-On"),
    "risk_off": ("🔴", "Risk-Off"),
    "neutral": ("⚪", "Neutral"),
}

_READINESS_META: dict[str, tuple[str, str, str, str]] = {
    "ready": (
        "✅",
        "READY",
        "success",
        "Signal is decision-grade. You can use this signal in thesis review with normal risk checks.",
    ),
    "stale": (
        "🟠",
        "STALE",
        "warning",
        "Signal is outdated. Defer new thesis execution until macro data is refreshed.",
    ),
    "missing": (
        "⚪",
        "MISSING",
        "warning",
        "No macro signal record is available. Do not execute thesis from this signal yet.",
    ),
    "error": (
        "⛔",
        "ERROR",
        "error",
        "Signal read failed or malformed. Block thesis execution until data integrity is restored.",
    ),
}


def _normalize_as_of(value: object) -> str:
    if isinstance(value, datetime):
        return value.isoformat(timespec="seconds")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, str) and value.strip():
        return value
    return "N/A"


def _to_readiness(status: str) -> str:
    normalized = status.strip().lower()
    if normalized in {"ok", "ready"}:
        return "ready"
    if normalized in {"stale", "missing", "error"}:
        return normalized
    return "error"


def _emit_state(st: Any, readiness: str, *, message: str | None) -> None:
    icon, label, level, implication = _READINESS_META[readiness]
    content = [
        f"{icon} **Readiness: {label}**",
        implication,
        "",
        f"What happened: {message or 'No additional details provided.'}",
    ]
    text = "\n".join(content)
    if level == "success":
        st.success(text)
    elif level == "warning":
        st.warning(text)
    else:
        st.error(text)


def _render_freshness_and_lineage(st: Any, regime_signal: dict[str, Any]) -> None:
    st.write("Data freshness & lineage")
    st.caption(f"as_of: {_normalize_as_of(regime_signal.get('as_of'))}")

    freshness_days = regime_signal.get("freshness_days")
    if freshness_days is not None:
        st.caption(f"freshness_threshold_days: {freshness_days}")

    lineage_id = regime_signal.get("lineage_id")
    if lineage_id:
        st.caption(f"lineage_id: {lineage_id}")

    source_tags = regime_signal.get("source_tags")
    if source_tags:
        tags = ", ".join(str(tag) for tag in source_tags if str(tag).strip())
        if tags:
            st.caption(f"source_tags: {tags}")


def _render_refresh_cta(st: Any) -> None:
    if st.button("Request data refresh", key="macro_regime_refresh_request"):
        st.info("Refresh request recorded. Data operator/pipeline will pick this up in the next cycle.")


def _render_signal_body(st: Any, regime_signal: dict[str, Any]) -> None:
    regime_key = str(regime_signal.get("regime", "neutral")).strip().lower().replace("-", "_")
    emoji, regime_label = _REGIME_META.get(regime_key, _REGIME_META["neutral"])

    confidence_raw = regime_signal.get("confidence", 0.0)
    try:
        confidence = max(0.0, min(float(confidence_raw), 1.0))
    except (TypeError, ValueError):
        confidence = 0.0

    drivers = [str(item) for item in regime_signal.get("drivers", []) if str(item).strip()][:3]

    st.markdown(f"### {emoji} {regime_label}")
    st.write(f"신뢰도: {confidence * 100:.0f}%")
    st.progress(confidence)

    st.write("핵심 드라이버 (Top 3):")
    if drivers:
        for driver in drivers:
            st.write(f"• {driver}")
    else:
        st.write("• Driver data unavailable")

    evidence_hard = [str(item) for item in regime_signal.get("evidence_hard", []) if str(item).strip()]
    evidence_soft = [str(item) for item in regime_signal.get("evidence_soft", []) if str(item).strip()]

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


def render_macro_regime_card(regime_signal: dict[str, Any] | None) -> None:
    st = importlib.import_module("streamlit")

    st.subheader("Macro regime signal")

    if not regime_signal:
        regime_signal = {
            "status": "missing",
            "message": "No macro regime signal yet. Analysis pipeline data is pending.",
        }

    readiness = _to_readiness(str(regime_signal.get("status", "error")))
    _emit_state(st, readiness, message=str(regime_signal.get("message") or "").strip() or None)
    _render_freshness_and_lineage(st, regime_signal)

    if readiness != "ready":
        _render_refresh_cta(st)
        return

    _render_signal_body(st, regime_signal)
