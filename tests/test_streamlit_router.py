from __future__ import annotations

import importlib


router = importlib.import_module("streamlit_app")


def test_resolve_view_defaults_to_enduser_when_no_query_or_session():
    view, warning = router.resolve_view({}, {})

    assert view == "enduser"
    assert warning is None


def test_resolve_view_uses_query_param_when_valid():
    view, warning = router.resolve_view({"view": "operator"}, {})

    assert view == "operator"
    assert warning is None


def test_resolve_view_falls_back_and_warns_when_invalid_query_param():
    view, warning = router.resolve_view({"view": "admin"}, {})

    assert view == "enduser"
    assert warning is not None
    assert "Unknown view 'admin'" in warning


def test_resolve_view_uses_session_state_when_query_missing():
    view, warning = router.resolve_view({}, {"ffl_view": "operator"})

    assert view == "operator"
    assert warning is None


def test_access_banner_visibility_is_operator_only():
    assert router._should_render_access_status_banner("operator") is True
    assert router._should_render_access_status_banner("enduser") is False
