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
    )

    monkeypatch.setitem(__import__("sys").modules, "streamlit", fake_streamlit)
    app = importlib.import_module("src.enduser.app")

    app.run_enduser_app("postgres://example")

    assert calls["tabs"] == ["Portfolio", "Signals"]
    assert calls["subheader"] == ["Macro regime signal"]
    assert calls["info"] == [
        "Coming soon",
        "macro signal read failed",
        "More signal cards coming soon",
    ]


def test_enduser_entrypoint_requires_database_url(monkeypatch):
    monkeypatch.delenv("SUPABASE_DB_URL", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)

    enduser_app = importlib.import_module("enduser_app")
    with pytest.raises(ValueError, match="DATABASE_URL"):
        enduser_app.main()
