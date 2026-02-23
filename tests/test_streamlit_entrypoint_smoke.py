from __future__ import annotations

import importlib


router = importlib.import_module("streamlit_app")


class FakeStreamlit:
    def __init__(self, query_params: dict[str, object] | None = None, session_state: dict[str, object] | None = None):
        self.query_params = query_params or {}
        self.session_state = session_state or {}
        self.warnings: list[str] = []
        self.page_config_calls: list[dict[str, object]] = []

    def set_page_config(self, **kwargs):
        self.page_config_calls.append(kwargs)

    def warning(self, message: str):
        self.warnings.append(message)

    def radio(self, *args, **kwargs):
        raise AssertionError("radio should be mocked via _render_view_toggle")


def test_main_defaults_to_enduser_view(monkeypatch):
    fake_st = FakeStreamlit(query_params={}, session_state={})
    calls: list[tuple[str, bool]] = []
    banner_calls: list[str] = []

    monkeypatch.setenv("DATABASE_URL", "postgres://example")
    monkeypatch.setattr(router, "st", fake_st)
    monkeypatch.setattr(router, "_render_access_status_banner", lambda: banner_calls.append("called"))
    monkeypatch.setattr(router, "run_enduser_app", lambda dsn, configure_page=False: calls.append(("enduser", configure_page)))
    monkeypatch.setattr(router, "run_streamlit_app", lambda dsn, configure_page=False: calls.append(("operator", configure_page)))

    router.main()

    assert calls == [("enduser", False)]
    assert "view" not in fake_st.query_params
    assert banner_calls == ["called"]


def test_main_operator_deep_link_falls_back_when_operator_disabled(monkeypatch):
    fake_st = FakeStreamlit(query_params={"view": "operator"}, session_state={})
    calls: list[tuple[str, bool]] = []
    banner_calls: list[str] = []

    monkeypatch.setenv("DATABASE_URL", "postgres://example")
    monkeypatch.delenv("ENABLE_OPERATOR_VIEW", raising=False)
    monkeypatch.setattr(router, "st", fake_st)
    monkeypatch.setattr(router, "_render_access_status_banner", lambda: banner_calls.append("called"))
    monkeypatch.setattr(router, "run_enduser_app", lambda dsn, configure_page=False: calls.append(("enduser", configure_page)))
    monkeypatch.setattr(router, "run_streamlit_app", lambda dsn, configure_page=False: calls.append(("operator", configure_page)))

    router.main()

    assert calls == [("enduser", False)]
    assert "view" not in fake_st.query_params
    assert fake_st.warnings
    assert "Operator view is disabled" in fake_st.warnings[0]
    assert banner_calls == ["called"]


def test_main_supports_operator_deep_link_when_enabled(monkeypatch):
    fake_st = FakeStreamlit(query_params={"view": "operator"}, session_state={})
    calls: list[tuple[str, bool]] = []
    banner_calls: list[str] = []

    monkeypatch.setenv("DATABASE_URL", "postgres://example")
    monkeypatch.setenv("ENABLE_OPERATOR_VIEW", "true")
    monkeypatch.setattr(router, "st", fake_st)
    monkeypatch.setattr(router, "_render_access_status_banner", lambda: banner_calls.append("called"))
    monkeypatch.setattr(router, "run_enduser_app", lambda dsn, configure_page=False: calls.append(("enduser", configure_page)))
    monkeypatch.setattr(router, "run_streamlit_app", lambda dsn, configure_page=False: calls.append(("operator", configure_page)))

    router.main()

    assert calls == [("operator", False)]
    assert fake_st.query_params["view"] == "operator"
    assert banner_calls == ["called"]


def test_main_falls_back_to_enduser_on_unknown_view(monkeypatch):
    fake_st = FakeStreamlit(query_params={"view": "admin"}, session_state={})
    calls: list[tuple[str, bool]] = []

    monkeypatch.setenv("DATABASE_URL", "postgres://example")
    monkeypatch.setattr(router, "st", fake_st)
    monkeypatch.setattr(router, "_render_access_status_banner", lambda: None)
    monkeypatch.setattr(router, "run_enduser_app", lambda dsn, configure_page=False: calls.append(("enduser", configure_page)))
    monkeypatch.setattr(router, "run_streamlit_app", lambda dsn, configure_page=False: calls.append(("operator", configure_page)))

    router.main()

    assert calls == [("enduser", False)]
    assert fake_st.warnings
    assert "Unknown view 'admin'" in fake_st.warnings[0]
