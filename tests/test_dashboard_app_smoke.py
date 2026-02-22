import importlib


dashboard_app = importlib.import_module("src.dashboard.app")


def test_dashboard_app_module_loads():
    assert hasattr(dashboard_app, "build_operator_cards")


def test_dashboard_app_builds_cards_from_view_model():
    cards = dashboard_app.build_operator_cards(
        {
            "last_run_status": "success",
            "last_run_time": "2026-02-18T01:00:00Z",
            "counters": {
                "raw_events": 100,
                "canonical_events": 90,
                "quarantine_events": 10,
            },
            "learning_metrics": {
                "horizon": "1M",
                "forecast_count": 25,
                "realized_count": 10,
                "realization_coverage": 0.4,
                "hit_rate": 0.6,
                "mean_abs_forecast_error": 0.025,
                "mean_signed_forecast_error": -0.007,
            },
            "attribution_summary": {
                "total": 7,
                "top_category": "macro_miss",
                "top_count": 3,
                "hard_evidence_coverage": 0.86,
                "hard_evidence_traceability_coverage": 0.71,
                "soft_evidence_coverage": 0.57,
                "evidence_gap_count": 1,
                "evidence_gap_coverage": 0.14,
            },
            "recent_runs": [],
        }
    )

    assert cards["last_run_status"] == "success"
    assert cards["raw_events"] == 100
    assert cards["quarantine_events"] == 10
    assert cards["forecast_count"] == 25
    assert cards["realized_count"] == 10
    assert cards["coverage_pct"] == "40.0%"
    assert cards["hit_rate_pct"] == "60.0%"
    assert cards["mae_pct"] == "2.50%"
    assert cards["signed_error_pct"] == "-0.70%"
    assert cards["attribution_total"] == 7
    assert cards["attribution_top_category"] == "macro_miss"
    assert cards["attribution_top_count"] == 3
    assert cards["hard_evidence_pct"] == "86.0%"
    assert cards["hard_evidence_traceability_pct"] == "71.0%"
    assert cards["soft_evidence_pct"] == "57.0%"
    assert cards["evidence_gap_count"] == 1
    assert cards["evidence_gap_pct"] == "14.0%"


def test_dashboard_app_treats_malformed_count_metrics_as_unknown():
    cards = dashboard_app.build_operator_cards(
        {
            "counters": {
                "raw_events": "n/a",
                "canonical_events": "",
                "quarantine_events": "unknown",
            },
            "learning_metrics": {
                "forecast_count": "-",
                "realized_count": "none",
            },
            "attribution_summary": {
                "total": "not-a-number",
                "top_count": "?",
                "evidence_gap_count": "",
            },
        }
    )

    assert cards["raw_events"] == "n/a"
    assert cards["canonical_events"] == "n/a"
    assert cards["quarantine_events"] == "n/a"
    assert cards["forecast_count"] == "n/a"
    assert cards["realized_count"] == "n/a"
    assert cards["attribution_total"] == "n/a"
    assert cards["attribution_top_count"] == "n/a"
    assert cards["evidence_gap_count"] == "n/a"


def test_dashboard_app_parses_numeric_strings_for_percent_metrics():
    cards = dashboard_app.build_operator_cards(
        {
            "learning_metrics": {
                "realization_coverage": "0.4",
                "hit_rate": "0.6",
                "mean_abs_forecast_error": "0.025",
                "mean_signed_forecast_error": "-0.007",
            },
            "attribution_summary": {
                "hard_evidence_coverage": "0.86",
                "hard_evidence_traceability_coverage": "0.71",
                "soft_evidence_coverage": "0.57",
                "evidence_gap_coverage": "0.14",
            },
        }
    )

    assert cards["coverage_pct"] == "40.0%"
    assert cards["hit_rate_pct"] == "60.0%"
    assert cards["mae_pct"] == "2.50%"
    assert cards["signed_error_pct"] == "-0.70%"
    assert cards["hard_evidence_pct"] == "86.0%"
    assert cards["hard_evidence_traceability_pct"] == "71.0%"
    assert cards["soft_evidence_pct"] == "57.0%"
    assert cards["evidence_gap_pct"] == "14.0%"


def test_dashboard_app_builds_attribution_gap_table_default_only_gaps():
    view = {
        "attribution_gap_rows": [
            {"attribution_id": 1, "evidence_gap_reason": "none"},
            {"attribution_id": 2, "evidence_gap_reason": "missing_hard"},
        ]
    }
    rows = dashboard_app._build_attribution_gap_table(view)
    assert len(rows) == 1
    assert rows[0]["attribution_id"] == 2


def test_dashboard_app_builds_attribution_gap_table_with_all_rows_toggle():
    view = {
        "attribution_gap_rows": [
            {"attribution_id": 1, "evidence_gap_reason": "none"},
            {"attribution_id": 2, "evidence_gap_reason": "missing_hard"},
        ]
    }
    rows = dashboard_app._build_attribution_gap_table(view, only_gaps=False)
    assert len(rows) == 2
