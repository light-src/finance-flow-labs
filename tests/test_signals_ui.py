from __future__ import annotations

import importlib
import sys
import types


def _load_signals(monkeypatch, fake_streamlit):
    monkeypatch.setitem(sys.modules, "streamlit", fake_streamlit)
    sys.modules.pop("src.enduser.signals", None)
    return importlib.import_module("src.enduser.signals")


def test_render_macro_regime_card_ready_path(monkeypatch):
    calls: dict[str, list] = {
        "subheader": [],
        "success": [],
        "warning": [],
        "error": [],
        "markdown": [],
        "caption": [],
        "write": [],
        "progress": [],
        "button": [],
    }

    fake_streamlit = types.SimpleNamespace(
        subheader=lambda text: calls["subheader"].append(text),
        success=lambda text: calls["success"].append(text),
        warning=lambda text: calls["warning"].append(text),
        error=lambda text: calls["error"].append(text),
        markdown=lambda text: calls["markdown"].append(text),
        caption=lambda text: calls["caption"].append(text),
        write=lambda text: calls["write"].append(text),
        progress=lambda value: calls["progress"].append(value),
        button=lambda label, key=None: calls["button"].append((label, key)) or False,
        info=lambda *_: None,
    )

    signals = _load_signals(monkeypatch, fake_streamlit)

    signals.render_macro_regime_card(
        {
            "status": "ok",
            "regime": "risk_on",
            "confidence": 0.8,
            "drivers": ["금리 하락 방향", "실업률 안정", "CPI 둔화", "ignored"],
            "as_of": "2026-02-22T18:30:00Z",
            "lineage_id": "run-1",
            "source_tags": ["macro_analysis_results"],
            "freshness_days": 7,
            "evidence_hard": ["FRED:CPIAUCSL 3m down", "FRED:UNRATE stable"],
            "evidence_soft": ["Fed commentary softer"],
        }
    )

    assert calls["subheader"] == ["Macro regime signal"]
    assert len(calls["success"]) == 1
    assert "Readiness: READY" in calls["success"][0]
    assert calls["warning"] == []
    assert calls["error"] == []
    assert calls["markdown"] == ["### 🟢 Risk-On"]
    assert "as_of: 2026-02-22T18:30:00Z" in calls["caption"]
    assert "freshness_threshold_days: 7" in calls["caption"]
    assert "lineage_id: run-1" in calls["caption"]
    assert "source_tags: macro_analysis_results" in calls["caption"]
    assert calls["progress"] == [0.8]
    assert calls["button"] == []


def test_render_macro_regime_card_missing_state_guidance(monkeypatch):
    calls: dict[str, list] = {"warning": [], "button": [], "caption": []}

    fake_streamlit = types.SimpleNamespace(
        subheader=lambda *_: None,
        success=lambda *_: None,
        warning=lambda text: calls["warning"].append(text),
        error=lambda *_: None,
        markdown=lambda *_: None,
        caption=lambda text: calls["caption"].append(text),
        write=lambda *_: None,
        progress=lambda *_: None,
        button=lambda label, key=None: calls["button"].append((label, key)) or False,
        info=lambda *_: None,
    )

    signals = _load_signals(monkeypatch, fake_streamlit)
    signals.render_macro_regime_card(None)

    assert len(calls["warning"]) == 1
    assert "Readiness: MISSING" in calls["warning"][0]
    assert "Do not execute thesis" in calls["warning"][0]
    assert calls["button"] == [("Request data refresh", "macro_regime_refresh_request")]
    assert "as_of: N/A" in calls["caption"]


def test_render_macro_regime_card_stale_state_guidance(monkeypatch):
    calls: dict[str, list] = {"warning": [], "button": []}

    fake_streamlit = types.SimpleNamespace(
        subheader=lambda *_: None,
        success=lambda *_: None,
        warning=lambda text: calls["warning"].append(text),
        error=lambda *_: None,
        markdown=lambda *_: None,
        caption=lambda *_: None,
        write=lambda *_: None,
        progress=lambda *_: None,
        button=lambda label, key=None: calls["button"].append((label, key)) or False,
        info=lambda *_: None,
    )

    signals = _load_signals(monkeypatch, fake_streamlit)
    signals.render_macro_regime_card(
        {
            "status": "stale",
            "message": "Latest macro regime signal is stale (> 7 days).",
            "as_of": "2026-02-01T00:00:00Z",
            "freshness_days": 7,
        }
    )

    assert len(calls["warning"]) == 1
    assert "Readiness: STALE" in calls["warning"][0]
    assert "Defer new thesis execution" in calls["warning"][0]
    assert calls["button"] == [("Request data refresh", "macro_regime_refresh_request")]


def test_render_macro_regime_card_error_state_guidance(monkeypatch):
    calls: dict[str, list] = {"error": [], "button": []}

    fake_streamlit = types.SimpleNamespace(
        subheader=lambda *_: None,
        success=lambda *_: None,
        warning=lambda *_: None,
        error=lambda text: calls["error"].append(text),
        markdown=lambda *_: None,
        caption=lambda *_: None,
        write=lambda *_: None,
        progress=lambda *_: None,
        button=lambda label, key=None: calls["button"].append((label, key)) or False,
        info=lambda *_: None,
    )

    signals = _load_signals(monkeypatch, fake_streamlit)
    signals.render_macro_regime_card({"status": "error", "message": "db timeout"})

    assert len(calls["error"]) == 1
    assert "Readiness: ERROR" in calls["error"][0]
    assert "Block thesis execution" in calls["error"][0]
    assert calls["button"] == [("Request data refresh", "macro_regime_refresh_request")]
