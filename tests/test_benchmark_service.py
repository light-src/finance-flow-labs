import math

from src.enduser.benchmark_service import compute_benchmark_series


class FakeBenchmarkRepository:
    def __init__(self, metric_rows):
        self.metric_rows = metric_rows

    def read_macro_series_points(self, metric_key, limit=10000):
        rows = self.metric_rows.get(metric_key, [])
        return rows[:limit]


def test_compute_benchmark_series_returns_weighted_daily_return_and_nav(monkeypatch):
    monkeypatch.delenv("BENCHMARK_WEIGHT_QQQ", raising=False)
    monkeypatch.delenv("BENCHMARK_WEIGHT_KOSPI200", raising=False)
    monkeypatch.delenv("BENCHMARK_WEIGHT_BTC", raising=False)
    monkeypatch.delenv("BENCHMARK_WEIGHT_SGOV", raising=False)

    repo = FakeBenchmarkRepository(
        {
            "QQQ": [
                {"as_of": "2026-02-20", "value": 101.0},
                {"as_of": "2026-02-19", "value": 100.0},
            ],
            "KOSPI200": [
                {"as_of": "2026-02-20", "value": 201.0},
                {"as_of": "2026-02-19", "value": 200.0},
            ],
            "BTC": [
                {"as_of": "2026-02-20", "value": 51000.0},
                {"as_of": "2026-02-19", "value": 50000.0},
            ],
            "SGOV": [
                {"as_of": "2026-02-20", "value": 100.2},
                {"as_of": "2026-02-19", "value": 100.0},
            ],
        }
    )

    rows = compute_benchmark_series(repo, "2026-02-19", "2026-02-20")

    assert len(rows) == 1
    row = rows[0]
    assert row["as_of"] == "2026-02-20"

    expected_return = (
        0.45 * (101.0 / 100.0 - 1.0)
        + 0.25 * (201.0 / 200.0 - 1.0)
        + 0.20 * (51000.0 / 50000.0 - 1.0)
        + 0.10 * (100.2 / 100.0 - 1.0)
    )
    assert math.isclose(row["benchmark_return"], expected_return, rel_tol=1e-12)
    assert math.isclose(row["benchmark_nav"], 1.0 + expected_return, rel_tol=1e-12)


def test_compute_benchmark_series_honors_env_weight_override(monkeypatch):
    monkeypatch.setenv("BENCHMARK_WEIGHT_QQQ", "1.0")
    monkeypatch.setenv("BENCHMARK_WEIGHT_KOSPI200", "0")
    monkeypatch.setenv("BENCHMARK_WEIGHT_BTC", "0")
    monkeypatch.setenv("BENCHMARK_WEIGHT_SGOV", "0")

    repo = FakeBenchmarkRepository(
        {
            "QQQ": [
                {"as_of": "2026-02-20", "value": 110.0},
                {"as_of": "2026-02-19", "value": 100.0},
            ],
            "KOSPI200": [
                {"as_of": "2026-02-20", "value": 500.0},
                {"as_of": "2026-02-19", "value": 1.0},
            ],
            "BTC": [
                {"as_of": "2026-02-20", "value": 500.0},
                {"as_of": "2026-02-19", "value": 1.0},
            ],
            "SGOV": [
                {"as_of": "2026-02-20", "value": 500.0},
                {"as_of": "2026-02-19", "value": 1.0},
            ],
        }
    )

    rows = compute_benchmark_series(repo, "2026-02-19", "2026-02-20")

    assert len(rows) == 1
    assert math.isclose(rows[0]["benchmark_return"], 0.10, rel_tol=1e-12)
    assert math.isclose(rows[0]["benchmark_nav"], 1.10, rel_tol=1e-12)


def test_compute_benchmark_series_skips_when_component_series_missing():
    repo = FakeBenchmarkRepository(
        {
            "QQQ": [
                {"as_of": "2026-02-20", "value": 101.0},
                {"as_of": "2026-02-19", "value": 100.0},
            ],
            "KOSPI200": [
                {"as_of": "2026-02-20", "value": 201.0},
                {"as_of": "2026-02-19", "value": 200.0},
            ],
            "BTC": [
                {"as_of": "2026-02-20", "value": 51000.0},
                {"as_of": "2026-02-19", "value": 50000.0},
            ],
            "SGOV": [{"as_of": "2026-02-20", "value": 100.2}],
        }
    )

    rows = compute_benchmark_series(repo, "2026-02-19", "2026-02-20")
    assert rows == []


def test_compute_benchmark_series_normalizes_partial_env_weights(monkeypatch):
    monkeypatch.setenv("BENCHMARK_WEIGHT_QQQ", "0.45")
    monkeypatch.setenv("BENCHMARK_WEIGHT_KOSPI200", "0.25")
    monkeypatch.setenv("BENCHMARK_WEIGHT_BTC", "0.20")
    monkeypatch.setenv("BENCHMARK_WEIGHT_SGOV", "0")

    repo = FakeBenchmarkRepository(
        {
            "QQQ": [
                {"as_of": "2026-02-20", "value": 110.0},
                {"as_of": "2026-02-19", "value": 100.0},
            ],
            "KOSPI200": [
                {"as_of": "2026-02-20", "value": 110.0},
                {"as_of": "2026-02-19", "value": 100.0},
            ],
            "BTC": [
                {"as_of": "2026-02-20", "value": 110.0},
                {"as_of": "2026-02-19", "value": 100.0},
            ],
            "SGOV": [
                {"as_of": "2026-02-20", "value": 200.0},
                {"as_of": "2026-02-19", "value": 100.0},
            ],
        }
    )

    rows = compute_benchmark_series(repo, "2026-02-19", "2026-02-20")
    assert len(rows) == 1
    # QQQ/KOSPI/BTC all +10%, SGOV zero weight after normalization.
    assert math.isclose(rows[0]["benchmark_return"], 0.10, rel_tol=1e-12)


def test_compute_benchmark_series_ignores_negative_weight_overrides(monkeypatch):
    monkeypatch.setenv("BENCHMARK_WEIGHT_QQQ", "-1")

    repo = FakeBenchmarkRepository(
        {
            "QQQ": [
                {"as_of": "2026-02-20", "value": 101.0},
                {"as_of": "2026-02-19", "value": 100.0},
            ],
            "KOSPI200": [
                {"as_of": "2026-02-20", "value": 201.0},
                {"as_of": "2026-02-19", "value": 200.0},
            ],
            "BTC": [
                {"as_of": "2026-02-20", "value": 51000.0},
                {"as_of": "2026-02-19", "value": 50000.0},
            ],
            "SGOV": [
                {"as_of": "2026-02-20", "value": 100.2},
                {"as_of": "2026-02-19", "value": 100.0},
            ],
        }
    )

    rows = compute_benchmark_series(repo, "2026-02-19", "2026-02-20")
    expected_return = (
        0.45 * (101.0 / 100.0 - 1.0)
        + 0.25 * (201.0 / 200.0 - 1.0)
        + 0.20 * (51000.0 / 50000.0 - 1.0)
        + 0.10 * (100.2 / 100.0 - 1.0)
    )
    assert len(rows) == 1
    assert math.isclose(rows[0]["benchmark_return"], expected_return, rel_tol=1e-12)
