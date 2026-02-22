from __future__ import annotations

from datetime import datetime, timedelta, timezone

from src.enduser.macro_signal_reader import load_latest_macro_regime_signal


class _Repo:
    def __init__(self, rows=None, should_fail: bool = False):
        self._rows = rows or []
        self._should_fail = should_fail

    def read_latest_macro_analysis(self, limit: int = 1):
        if self._should_fail:
            raise RuntimeError("boom")
        return self._rows[:limit]


def test_load_latest_macro_regime_signal_ok():
    now = datetime.now(timezone.utc)
    repo = _Repo(
        rows=[
            {
                "as_of": now.isoformat(),
                "regime": "risk_on",
                "confidence": 0.7,
                "reason_codes": ["disinflation", "growth_stable"],
                "run_id": "run-1",
            }
        ]
    )

    signal = load_latest_macro_regime_signal(repo)

    assert signal["status"] == "ok"
    assert signal["regime"] == "risk_on"
    assert signal["drivers"] == ["disinflation", "growth_stable"]
    assert signal["lineage_id"] == "run-1"


def test_load_latest_macro_regime_signal_missing_and_error_paths():
    assert load_latest_macro_regime_signal(_Repo()) == {
        "status": "missing",
        "reason": "no macro regime analysis yet",
    }

    failed = load_latest_macro_regime_signal(_Repo(should_fail=True))
    assert failed["status"] == "error"


def test_load_latest_macro_regime_signal_stale():
    stale_as_of = (datetime.now(timezone.utc) - timedelta(days=10)).isoformat()
    repo = _Repo(rows=[{"as_of": stale_as_of, "regime": "neutral", "confidence": 0.5}])

    signal = load_latest_macro_regime_signal(repo, stale_after_days=7)

    assert signal["status"] == "stale"
    assert signal["as_of"] == stale_as_of
