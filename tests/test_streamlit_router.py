from __future__ import annotations

import importlib


router = importlib.import_module("streamlit_app")


def test_resolve_view_defaults_to_enduser_when_no_query_or_session():
    view, warning = router.resolve_view({}, {}, operator_enabled=False)

    assert view == "enduser"
    assert warning is None


def test_resolve_view_uses_query_param_when_valid_and_operator_enabled():
    view, warning = router.resolve_view({"view": "operator"}, {}, operator_enabled=True)

    assert view == "operator"
    assert warning is None


def test_resolve_view_accepts_tuple_query_param_shape():
    view, warning = router.resolve_view({"view": ("operator",)}, {}, operator_enabled=True)

    assert view == "operator"
    assert warning is None


def test_resolve_view_falls_back_and_warns_when_invalid_query_param():
    view, warning = router.resolve_view({"view": "admin"}, {}, operator_enabled=False)

    assert view == "enduser"
    assert warning is not None
    assert "Unknown view 'admin'" in warning


def test_resolve_view_uses_session_state_when_query_missing():
    view, warning = router.resolve_view({}, {"ffl_view": "operator"}, operator_enabled=True)

    assert view == "operator"
    assert warning is None


def test_resolve_view_blocks_operator_query_when_operator_disabled():
    view, warning = router.resolve_view({"view": "operator"}, {}, operator_enabled=False)

    assert view == "enduser"
    assert warning is not None
    assert "Operator view is disabled" in warning


def test_resolve_view_blocks_operator_session_when_operator_disabled():
    view, warning = router.resolve_view({}, {"ffl_view": "operator"}, operator_enabled=False)

    assert view == "enduser"
    assert warning is not None
    assert "Operator view is disabled" in warning


def test_access_banner_visibility_is_enabled_for_both_views():
    assert router._should_render_access_status_banner("operator") is True
    assert router._should_render_access_status_banner("enduser") is True
    assert router._should_render_access_status_banner("unknown") is False


def test_run_view_app_passes_configure_page_when_supported():
    called: dict[str, object] = {}

    def supported(dsn: str, *, configure_page: bool = True) -> None:
        called["dsn"] = dsn
        called["configure_page"] = configure_page

    router._run_view_app(supported, "postgres://dsn")

    assert called == {"dsn": "postgres://dsn", "configure_page": False}


def test_run_view_app_falls_back_when_configure_page_unsupported():
    called: dict[str, object] = {}

    def legacy(dsn: str) -> None:
        called["dsn"] = dsn

    router._run_view_app(legacy, "postgres://legacy")

    assert called == {"dsn": "postgres://legacy"}
