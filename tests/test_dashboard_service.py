import importlib

import pytest


dashboard_service = importlib.import_module("src.ingestion.dashboard_service")
build_dashboard_view = dashboard_service.build_dashboard_view
REQUIRED_LEARNING_HORIZONS = dashboard_service.REQUIRED_LEARNING_HORIZONS


class FakeDashboardRepo:
    def read_macro_series_points(self, metric_key, limit=1):
        return [{"metric_key": metric_key, "as_of": "2026-02-22T08:00:00Z", "value": 1.0}]

    def read_latest_runs(self, limit=20):
        return [
            {"run_id": "run-2", "status": "success", "finished_at": "2026-02-18T01:00:00Z"},
            {"run_id": "run-1", "status": "quarantine", "finished_at": "2026-02-18T00:00:00Z"},
        ]

    def read_status_counters(self):
        return {"raw_events": 100, "canonical_events": 90, "quarantine_events": 10}

    def read_learning_metrics(self, horizon="1M"):
        return {
            "horizon": horizon,
            "forecast_count": 20,
            "realized_count": 12,
            "realization_coverage": 0.6,
            "hit_rate": 0.58,
            "mean_abs_forecast_error": 0.031,
        }

    def read_forecast_error_category_stats(self, horizon="1M", limit=5):
        return [
            {
                "category": "macro_miss",
                "attribution_count": 2,
                "mean_contribution": -0.021,
                "mean_abs_contribution": 0.021,
            },
            {
                "category": "valuation_miss",
                "attribution_count": 1,
                "mean_contribution": -0.008,
                "mean_abs_contribution": 0.008,
            },
        ]

    def read_forecast_error_attributions(self, horizon="1M", limit=200):
        return [
            {"category": "macro_miss", "evidence_hard": [{"source": "FRED"}], "evidence_soft": [{"note": "regime"}]},
            {"category": "macro_miss", "evidence_hard": [{"metric": "CPI"}], "evidence_soft": []},
            {"category": "valuation_miss", "evidence_hard": [], "evidence_soft": [{"note": "narrative"}]},
            {"category": "unknown", "evidence_hard": [], "evidence_soft": []},
        ]


def test_dashboard_service_uses_policy_locked_horizon_list():
    view = build_dashboard_view(FakeDashboardRepo())

    assert tuple(view["learning_metrics_by_horizon"].keys()) == REQUIRED_LEARNING_HORIZONS


def test_dashboard_service_builds_operator_view_model():
    view = build_dashboard_view(FakeDashboardRepo())

    assert view["last_run_status"] == "success"
    assert view["last_run_time"] == "2026-02-18T01:00:00Z"
    assert view["counters"]["raw_events"] == 100
    assert view["learning_metrics"]["horizon"] == "1M"
    assert view["learning_metrics"]["hit_rate"] == 0.58
    assert view["learning_metrics_by_horizon"]["1W"]["horizon"] == "1W"
    assert view["learning_metrics_by_horizon"]["1M"]["horizon"] == "1M"
    assert view["learning_metrics_by_horizon"]["3M"]["horizon"] == "3M"
    assert view["learning_metrics_by_horizon"]["1M"]["reliability_state"] == "low_sample"
    assert "realized_count_low_sample" in view["learning_metrics_by_horizon"]["1M"]["reliability_reason"]
    assert view["learning_reliability_by_horizon"]["1W"]["min_realized_required"] == 8
    assert view["attribution_summary"]["total"] == 4
    assert view["attribution_summary"]["top_category"] == "macro_miss"
    assert view["attribution_summary"]["top_count"] == 2
    assert len(view["attribution_summary"]["top_categories"]) == 2
    assert view["attribution_summary"]["top_categories"][0]["mean_abs_contribution"] == 0.021
    assert view["attribution_summary"]["hard_evidence_coverage"] == 0.5
    assert view["attribution_summary"]["hard_evidence_traceability_coverage"] == 0.0
    assert view["attribution_summary"]["soft_evidence_coverage"] == 0.5
    assert view["attribution_summary"]["evidence_gap_count"] == 1
    assert view["attribution_summary"]["evidence_gap_coverage"] == 0.25
    assert len(view["attribution_gap_rows"]) == 4
    assert view["attribution_gap_rows"][0]["evidence_gap_reason"] == "hard_untraceable"
    assert view["attribution_gap_rows"][-1]["evidence_gap_reason"] == "missing_hard_and_soft"
    assert view["policy_compliance"]["summary"]["total"] == 11
    assert view["policy_compliance"]["summary"]["unknown"] == 2
    assert view["policy_compliance"]["checks"][6]["status"] == "WARN"
    assert view["policy_compliance"]["checks"][7]["status"] == "PASS"
    assert len(view["recent_runs"]) == 2


class FailingLearningRepo(FakeDashboardRepo):
    def read_learning_metrics(self, horizon="1M"):
        raise RuntimeError("psycopg2.errors.UndefinedTable")

    def read_forecast_error_category_stats(self, horizon="1M", limit=5):
        raise RuntimeError("psycopg2.errors.UndefinedTable")

    def read_forecast_error_attributions(self, horizon="1M", limit=200):
        raise RuntimeError("psycopg2.errors.UndefinedTable")


def test_dashboard_service_falls_back_when_learning_tables_missing():
    view = build_dashboard_view(FailingLearningRepo())

    assert view["last_run_status"] == "success"
    assert view["learning_metrics"]["horizon"] == "1M"
    assert view["learning_metrics"]["forecast_count"] == 0
    assert view["learning_metrics"]["realized_count"] == 0
    assert view["learning_metrics"]["hit_rate"] is None
    assert view["learning_metrics_by_horizon"]["1W"]["forecast_count"] == 0
    assert view["learning_metrics_by_horizon"]["3M"]["forecast_count"] == 0
    assert view["attribution_summary"]["total"] == 0
    assert view["attribution_summary"]["top_category"] == "n/a"


class BrokenDashboardRepo:
    def read_latest_runs(self, limit=20):
        raise RuntimeError("db unavailable")

    def read_status_counters(self):
        raise RuntimeError("db unavailable")

    def read_learning_metrics(self, horizon="1M"):
        raise RuntimeError("db unavailable")


def test_dashboard_service_falls_back_when_core_dashboard_queries_fail():
    view = build_dashboard_view(BrokenDashboardRepo())

    assert view["last_run_status"] == "no-data"
    assert view["counters"] == {
        "raw_events": 0,
        "canonical_events": 0,
        "quarantine_events": 0,
    }
    assert view["learning_metrics"]["forecast_count"] == 0
    assert view["learning_metrics"]["realized_count"] == 0
    assert view["learning_metrics_by_horizon"]["1M"]["forecast_count"] == 0


class StringEvidenceRepo(FakeDashboardRepo):
    def read_forecast_error_category_stats(self, horizon="1M", limit=5):
        return []

    def read_forecast_error_attributions(self, horizon="1M", limit=200):
        return [
            {
                "category": "macro_miss",
                "evidence_hard": '[{"source":"FRED","metric":"CPI"}]',
                "evidence_soft": '[{"note":"narrative"}]',
            },
            {
                "category": "unknown",
                "evidence_hard": '{bad-json',
                "evidence_soft": 'not-json',
            },
        ]


def test_dashboard_service_ignores_invalid_string_evidence_payloads():
    view = build_dashboard_view(StringEvidenceRepo())

    summary = view["attribution_summary"]
    assert summary["total"] == 2
    assert summary["hard_evidence_coverage"] == 0.5
    assert summary["hard_evidence_traceability_coverage"] == 0.5
    assert summary["soft_evidence_coverage"] == 0.5
    assert summary["evidence_gap_count"] == 1
    assert summary["evidence_gap_coverage"] == 0.5


class ObjectEvidenceRepo(FakeDashboardRepo):
    def read_forecast_error_category_stats(self, horizon="1M", limit=5):
        return []

    def read_forecast_error_attributions(self, horizon="1M", limit=200):
        return [
            {
                "category": "macro_miss",
                "evidence_hard": '{"source":"FRED","metric":"CPI"}',
                "evidence_soft": '{"items":[{"note":"single-payload"}]}',
            },
            {
                "category": "valuation_miss",
                "evidence_hard": {"items": [{"source": "ECOS", "metric": "CLI"}]},
                "evidence_soft": {},
            },
        ]


def test_dashboard_service_parses_object_evidence_payloads():
    view = build_dashboard_view(ObjectEvidenceRepo())

    summary = view["attribution_summary"]
    assert summary["total"] == 2
    assert summary["hard_evidence_coverage"] == 1.0
    assert summary["hard_evidence_traceability_coverage"] == 1.0
    assert summary["soft_evidence_coverage"] == 0.5
    assert summary["evidence_gap_count"] == 0
    assert summary["evidence_gap_coverage"] == 0.0


def test_dashboard_service_policy_compliance_marks_missing_benchmark_dependencies_unknown():
    class MissingBenchmarkRepo(FakeDashboardRepo):
        def read_macro_series_points(self, metric_key, limit=1):
            return []

    view = build_dashboard_view(MissingBenchmarkRepo())
    benchmark_check = view["policy_compliance"]["checks"][7]
    assert benchmark_check["status"] == "WARN"
    assert "Missing benchmark series" in benchmark_check["reason"]


def test_dashboard_service_policy_checks_include_as_of_from_latest_run_when_direct_evidence_missing():
    view = build_dashboard_view(FakeDashboardRepo())
    checks = view["policy_compliance"]["checks"]

    assert checks[0]["as_of"] == "2026-02-18T01:00:00Z"
    assert checks[1]["as_of"] == "2026-02-18T01:00:00Z"
    assert checks[2]["as_of"] == "2026-02-18T01:00:00Z"
    assert checks[3]["as_of"] == "2026-02-18T01:00:00Z"
    assert checks[4]["as_of"] == "2026-02-18T01:00:00Z"
    assert checks[5]["as_of"] == "2026-02-18T01:00:00Z"
    assert checks[6]["as_of"] == "2026-02-18T01:00:00Z"


def test_dashboard_service_policy_compliance_warns_on_stale_benchmark_series(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("POLICY_BENCHMARK_MAX_STALE_DAYS", "7")

    class StaleBenchmarkRepo(FakeDashboardRepo):
        def read_macro_series_points(self, metric_key, limit=1):
            return [{"metric_key": metric_key, "as_of": "2026-02-01T00:00:00Z", "value": 1.0}]

    view = build_dashboard_view(StaleBenchmarkRepo())
    benchmark_check = view["policy_compliance"]["checks"][7]

    assert benchmark_check["status"] == "WARN"
    assert "Stale benchmark series" in benchmark_check["reason"]
    assert set(benchmark_check["evidence"]["stale_series"]) == {"QQQ", "KOSPI200", "BTC", "SGOV"}


def test_dashboard_service_learning_reliability_threshold_env_override(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("LEARNING_RELIABILITY_MIN_REALIZED_1M", "10")
    monkeypatch.setenv("LEARNING_RELIABILITY_COVERAGE_FLOOR", "0.65")

    class CoverageWeakRepo(FakeDashboardRepo):
        def read_learning_metrics(self, horizon="1M"):
            return {
                "horizon": horizon,
                "forecast_count": 30,
                "realized_count": 20,
                "realization_coverage": 0.6,
                "hit_rate": 0.57,
                "mean_abs_forecast_error": 0.028,
            }

    view = build_dashboard_view(CoverageWeakRepo())
    rel = view["learning_metrics_by_horizon"]["1M"]
    assert rel["reliability_state"] == "low_sample"
    assert "coverage_below_floor" in rel["reliability_reason"]
    assert rel["min_realized_required"] == 10
