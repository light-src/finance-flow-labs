from __future__ import annotations

import importlib
import sys
import types


def _load_signals_with_fake_streamlit(monkeypatch, *, button_return: bool = False):
    calls: dict[str, list] = {
        "subheader": [],
        "markdown": [],
        "caption": [],
        "write": [],
        "progress": [],
        "info": [],
        "warning": [],
        "error": [],
        "success": [],
        "button": [],
    }

    def _button(label, key=None):
        calls["button"].append((label, key))
        return button_return

    fake_streamlit = types.SimpleNamespace(
        subheader=lambda text: calls["subheader"].append(text),
        markdown=lambda text: calls["markdown"].append(text),
        caption=lambda text: calls["caption"].append(text),
        write=lambda text: calls["write"].append(text),
        progress=lambda value: calls["progress"].append(value),
        info=lambda text: calls["info"].append(text),
        warning=lambda text: calls["warning"].append(text),
        error=lambda text: calls["error"].append(text),
        success=lambda text: calls["success"].append(text),
        button=_button,
    )

    monkeypatch.setitem(sys.modules, "streamlit", fake_streamlit)
    sys.modules.pop("src.enduser.signals", None)
    signals = importlib.import_module("src.enduser.signals")
    return signals, calls


def test_render_macro_regime_card_ready_state(monkeypatch):
    signals, calls = _load_signals_with_fake_streamlit(monkeypatch)

    signals.render_macro_regime_card(
        {
            "status": "ok",
            "regime": "risk_on",
            "confidence": 0.8,
            "drivers": ["금리 하락 방향", "실업률 안정", "CPI 둔화", "ignored"],
            "as_of": "2026-02-22T18:30:00Z",
            "lineage_id": "run-123",
            "source_tags": ["macro_analysis_results"],
            "freshness_days": 7,
            "evidence_hard": ["FRED:CPIAUCSL 3m down"],
            "evidence_soft": ["Fed commentary softer"],
        }
    )

    assert calls["subheader"] == ["Macro regime signal"]
    assert "### ✅ READY" in calls["markdown"]
    assert "#### Regime: 🟢 Risk-On" in calls["markdown"]
    assert calls["progress"] == [0.8]
    assert "Data freshness & lineage" in calls["write"]
    assert "as_of: 2026-02-22T18:30:00Z" in calls["caption"]
    assert "freshness_threshold: 7d" in calls["caption"]
    assert "lineage_id: run-123" in calls["caption"]
    assert "source_tags: macro_analysis_results" in calls["caption"]


def test_render_macro_regime_card_missing_state(monkeypatch):
    signals, calls = _load_signals_with_fake_streamlit(monkeypatch)

    signals.render_macro_regime_card(None)

    assert "### 🟨 MISSING" in calls["markdown"]
    assert calls["warning"]
    assert any("Block signal-based investment action" in text for text in calls["warning"])
    assert calls["button"] == [("Request data refresh", "macro_refresh_request")]


def test_render_macro_regime_card_stale_state(monkeypatch):
    signals, calls = _load_signals_with_fake_streamlit(monkeypatch)

    signals.render_macro_regime_card(
        {
            "status": "stale",
            "message": "Latest macro regime signal is stale (> 7 days).",
            "regime": "neutral",
            "confidence": 0.4,
            "as_of": "2026-02-01T00:00:00Z",
            "freshness_days": 7,
        }
    )

    assert "### ⚠️ STALE" in calls["markdown"]
    assert "Latest macro regime signal is stale (> 7 days)." in calls["warning"]
    assert any("Do not execute new thesis entries" in text for text in calls["warning"])
    assert "as_of: 2026-02-01T00:00:00Z" in calls["caption"]


def test_render_macro_regime_card_error_state_and_refresh_hook(monkeypatch):
    signals, calls = _load_signals_with_fake_streamlit(monkeypatch, button_return=True)

    signals.render_macro_regime_card(
        {
            "status": "error",
            "message": "Latest macro regime row is malformed.",
            "freshness_days": 7,
        }
    )

    assert "### 🛑 ERROR" in calls["markdown"]
    assert "Latest macro regime row is malformed." in calls["error"]
    assert any("Block signal-based investment action" in text for text in calls["error"])
    assert any("Refresh request recorded." in text for text in calls["info"])
