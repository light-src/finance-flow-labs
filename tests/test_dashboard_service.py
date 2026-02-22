import importlib


dashboard_service = importlib.import_module("src.ingestion.dashboard_service")
build_dashboard_view = dashboard_service.build_dashboard_view


class FakeDashboardRepo:
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


def test_dashboard_service_builds_operator_view_model():
    view = build_dashboard_view(FakeDashboardRepo())

    assert view["last_run_status"] == "success"
    assert view["last_run_time"] == "2026-02-18T01:00:00Z"
    assert view["counters"]["raw_events"] == 100
    assert view["learning_metrics"]["horizon"] == "1M"
    assert view["learning_metrics"]["hit_rate"] == 0.58
    assert view["attribution_summary"]["total"] == 4
    assert view["attribution_summary"]["top_category"] == "macro_miss"
    assert view["attribution_summary"]["top_count"] == 2
    assert len(view["attribution_summary"]["top_categories"]) == 2
    assert view["attribution_summary"]["top_categories"][0]["mean_abs_contribution"] == 0.021
    assert view["attribution_summary"]["hard_evidence_coverage"] == 0.5
    assert view["attribution_summary"]["hard_evidence_traceability_coverage"] == 0.0
    assert view["attribution_summary"]["soft_evidence_coverage"] == 0.5
    assert view["attribution_summary"]["evidence_gap_count"] == 4
    assert view["attribution_summary"]["evidence_gap_coverage"] == 1.0
    assert view["attribution_gap_rows_status"] == "ok"
    assert len(view["attribution_gap_rows"]) == 4
    reasons = {row["evidence_gap_reason"] for row in view["attribution_gap_rows"]}
    assert "hard_untraceable" in reasons
    assert "hard_untraceable_no_soft" in reasons
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
    assert view["attribution_summary"]["total"] == 0
    assert view["attribution_summary"]["top_category"] == "n/a"
    assert view["attribution_gap_rows_status"] == "unknown"


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
    rows = view["attribution_gap_rows"]
    assert rows[0]["evidence_gap_reason"] == "none"
    assert rows[1]["evidence_gap_reason"] == "missing_hard_and_soft"


def test_dashboard_service_classifies_attribution_gap_reasons():
    classify = dashboard_service._classify_evidence_gap_reason
    assert classify('[{"source":"FRED","metric":"CPI"}]', '[]') == "none"
    assert classify('[{"source":"FRED"}]', '[]') == "hard_untraceable_no_soft"
    assert classify('[]', '[{"note":"narrative"}]') == "missing_hard"
    assert classify('[]', '[]') == "missing_hard_and_soft"
