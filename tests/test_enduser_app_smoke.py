from __future__ import annotations

import importlib
import types

import pytest


class _TabContext:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def test_run_enduser_app_renders_portfolio_and_signals_tabs(monkeypatch):
    calls: dict[str, object] = {"tabs": None, "info": [], "subheader": []}

    def set_page_config(**kwargs):
        calls["page_config"] = kwargs

    def title(text: str):
        calls["title"] = text

    def tabs(labels: list[str]):
        calls["tabs"] = labels
        return [_TabContext(), _TabContext()]

    def subheader(text: str):
        calls["subheader"].append(text)

    def info(text: str):
        calls["info"].append(text)

    def markdown(text: str):
        calls.setdefault("markdown", []).append(text)

    def caption(text: str):
        calls.setdefault("captions", []).append(text)

    def write(text: str):
        calls.setdefault("write", []).append(text)

    def progress(value: float):
        calls.setdefault("progress", []).append(value)

    fake_streamlit = types.SimpleNamespace(
        set_page_config=set_page_config,
        title=title,
        caption=caption,
        tabs=tabs,
        info=info,
        subheader=subheader,
        markdown=markdown,
        write=write,
        progress=progress,
        warning=lambda text: calls.setdefault("warning", []).append(text),
        success=lambda text: calls.setdefault("success", []).append(text),
        error=lambda text: calls.setdefault("error", []).append(text),
        button=lambda *_args, **_kwargs: False,
    )

    monkeypatch.setitem(__import__("sys").modules, "streamlit", fake_streamlit)
    app = importlib.import_module("src.enduser.app")
    monkeypatch.setattr(
        app,
        "read_latest_macro_regime_signal",
        lambda _dsn: {
            "status": "ok",
            "regime": "risk_on",
            "confidence": 0.7,
            "drivers": ["cpi_cooling"],
            "as_of": "2026-02-22T19:00:00Z",
            "lineage_id": "run-1",
        },
    )

    app.run_enduser_app("postgres://example")

    assert calls["tabs"] == ["Portfolio", "Signals"]
    assert calls["subheader"] == ["Macro regime signal"]
    assert calls["info"] == ["Coming soon", "More signal cards coming soon"]


def test_run_enduser_app_wires_reader_payload_into_signal_card(monkeypatch):
    calls: dict[str, object] = {"reader_dsn": None, "render_payload": None}

    class _TabContext:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    fake_streamlit = types.SimpleNamespace(
        set_page_config=lambda **_: None,
        title=lambda *_: None,
        caption=lambda *_: None,
        tabs=lambda _labels: [_TabContext(), _TabContext()],
        info=lambda *_: None,
        subheader=lambda *_: None,
        success=lambda *_: None,
        warning=lambda *_: None,
        error=lambda *_: None,
        write=lambda *_: None,
        markdown=lambda *_: None,
        progress=lambda *_: None,
        button=lambda *_args, **_kwargs: False,
    )

    monkeypatch.setitem(__import__("sys").modules, "streamlit", fake_streamlit)
    app = importlib.import_module("src.enduser.app")

    expected_payload = {
        "status": "ok",
        "regime": "risk_off",
        "confidence": 0.61,
        "as_of": "2026-02-22T21:30:00Z",
    }

    def _fake_reader(dsn: str):
        calls["reader_dsn"] = dsn
        return expected_payload

    def _fake_render(*, regime_signal):
        calls["render_payload"] = regime_signal

    monkeypatch.setattr(app, "read_latest_macro_regime_signal", _fake_reader)
    monkeypatch.setattr(app, "render_macro_regime_card", _fake_render)

    app.run_enduser_app("postgres://macro-signal")

    assert calls["reader_dsn"] == "postgres://macro-signal"
    assert calls["render_payload"] == expected_payload


def test_enduser_entrypoint_requires_database_url(monkeypatch):
    monkeypatch.delenv("SUPABASE_DB_URL", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)

    enduser_app = importlib.import_module("enduser_app")
    with pytest.raises(ValueError, match="DATABASE_URL"):
        enduser_app.main()
