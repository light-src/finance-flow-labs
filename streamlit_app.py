from __future__ import annotations

import logging
import os
from collections.abc import Callable, Mapping

import streamlit as st

from src.dashboard.app import run_streamlit_app
from src.enduser.app import run_enduser_app
from src.ingestion.streamlit_access import check_streamlit_access

LOGGER = logging.getLogger(__name__)
VALID_VIEWS = {"enduser", "operator"}
DEFAULT_VIEW = "enduser"
SESSION_KEY = "ffl_view"


def _query_param_to_text(raw_value: object) -> str:
    if isinstance(raw_value, (list, tuple)):
        raw_value = raw_value[0] if raw_value else None
    return str(raw_value or "").strip().lower()


def _normalize_view(raw_value: object) -> str:
    normalized = _query_param_to_text(raw_value)
    if normalized in VALID_VIEWS:
        return normalized
    return DEFAULT_VIEW


def resolve_view(
    query_params: Mapping[str, object],
    session_state: Mapping[str, object],
) -> tuple[str, str | None]:
    requested = query_params.get("view")
    if requested is not None:
        requested_text = _query_param_to_text(requested)
        normalized = _normalize_view(requested)
        if requested_text and requested_text not in VALID_VIEWS:
            return normalized, f"Unknown view '{requested_text}'. Falling back to '{DEFAULT_VIEW}'."
        return normalized, None

    current = session_state.get(SESSION_KEY)
    if current in VALID_VIEWS:
        return str(current), None

    return DEFAULT_VIEW, None


def _should_render_access_status_banner(active_view: str) -> bool:
    return active_view in VALID_VIEWS


def _render_access_status_banner() -> None:
    base_url = os.getenv("STREAMLIT_PUBLIC_URL")
    if not base_url:
        return

    result = check_streamlit_access(base_url).to_dict()
    status = str(result.get("reason", "unknown"))

    if bool(result.get("ok")):
        st.success(f"Access status: OK ({status})")
        return

    checked_url = str(result.get("final_url") or base_url)
    remediation = result.get("remediation_hint") or "Check deployment visibility and health."
    st.error(f"Access status: DEGRADED ({status}) · url={checked_url}")
    st.info(f"Remediation: {remediation}")


def _render_view_toggle(active_view: str) -> str:
    labels = ["End-user", "Operator"]
    index = 0 if active_view == "enduser" else 1
    selected = st.radio(
        "Workspace",
        labels,
        index=index,
        horizontal=True,
        key="ffl_workspace_toggle",
        help="Default landing is End-user. Operator is available for ingestion and policy diagnostics.",
    )
    return "enduser" if selected == "End-user" else "operator"


def _run_view_app(app_fn: Callable[..., None], dsn: str) -> None:
    """Invoke view app without duplicate page config, with legacy compatibility.

    Some deployments can end up with version-skewed modules where app_fn does not
    accept ``configure_page`` yet. Fall back to the legacy signature instead of
    crashing the whole Streamlit shell.
    """

    try:
        app_fn(dsn, configure_page=False)
    except TypeError as exc:
        if "configure_page" not in str(exc):
            raise
        app_fn(dsn)


def main() -> None:
    dsn = os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL")
    if not dsn:
        raise ValueError("SUPABASE_DB_URL or DATABASE_URL is required")

    view, warning_message = resolve_view(st.query_params, st.session_state)

    st.set_page_config(page_title="finance-flow-labs", layout="wide")

    if warning_message:
        LOGGER.warning(warning_message)
        st.warning(warning_message)

    selected_view = _render_view_toggle(view)
    st.session_state[SESSION_KEY] = selected_view
    st.query_params["view"] = selected_view

    if _should_render_access_status_banner(selected_view):
        _render_access_status_banner()

    if selected_view == "operator":
        _run_view_app(run_streamlit_app, dsn)
    else:
        _run_view_app(run_enduser_app, dsn)


if __name__ == "__main__":
    main()
