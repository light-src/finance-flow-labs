import importlib
import math


dashboard_app = importlib.import_module("src.dashboard.app")


def test_dashboard_app_module_loads():
    assert hasattr(dashboard_app, "build_operator_cards")
    assert hasattr(dashboard_app, "update_refresh_request_status")


def test_dashboard_app_update_refresh_request_status_delegates_to_repository(monkeypatch):
    captured: dict[str, object] = {}

    class FakeRepository:
        def __init__(self, dsn: str):
            captured["dsn"] = dsn

        def update_refresh_request_status(self, **kwargs):
            captured["kwargs"] = kwargs
            return {"id": kwargs["request_id"], "status": kwargs["status"]}

    class FakeModule:
        PostgresRepository = FakeRepository

    original_import_module = dashboard_app.importlib.import_module

    def fake_import_module(name: str):
        if name == "src.ingestion.postgres_repository":
            return FakeModule()
        return original_import_module(name)

    monkeypatch.setattr(dashboard_app.importlib, "import_module", fake_import_module)

    row = dashboard_app.update_refresh_request_status(
        "postgresql://demo",
        request_id=123,
        status="completed",
        handler="operator-a",
        result_message="manual run complete",
        ingestion_run_id="run-20260223-01",
    )

    assert row == {"id": 123, "status": "completed"}
    assert captured["dsn"] == "postgresql://demo"
    assert captured["kwargs"] == {
        "request_id": 123,
        "status": "completed",
        "handler": "operator-a",
        "result_message": "manual run complete",
        "ingestion_run_id": "run-20260223-01",
    }


def test_dashboard_app_kpi_layout_is_tiered_and_bounded():
    tiers = dashboard_app.get_kpi_layout_tiers()
    assert all(len(keys) <= 6 for _, keys in tiers)

    rendered_keys = {key for _, keys in tiers for key in keys}
    assert rendered_keys == {
        "last_status",
        "primary_reliability",
        "policy_summary",
        "evidence_gap_count",
        "raw_events",
        "canonical_events",
        "quarantine_events",
        "forecast_count",
        "realized_count",
        "coverage_pct",
        "hit_rate_pct",
        "mae_pct",
        "signed_error_pct",
        "attribution_total",
        "attribution_top_category",
        "hard_evidence_pct",
        "hard_evidence_traceability_pct",
        "soft_evidence_pct",
        "evidence_gap_pct",
    }


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
                "forecast_count": 40,
                "realized_count": 25,
                "realization_coverage": 0.63,
                "hit_rate": 0.6,
                "mean_abs_forecast_error": 0.025,
                "mean_signed_forecast_error": -0.007,
            },
            "learning_metrics_by_horizon": {
                "1W": {
                    "horizon": "1W",
                    "forecast_count": 20,
                    "realized_count": 16,
                    "realization_coverage": 0.8,
                    "hit_rate": 0.5,
                    "mean_abs_forecast_error": 0.01,
                    "reliability_state": "reliable",
                    "reliability_reason": "sample_and_coverage_ok",
                    "min_realized_required": 8,
                },
                "1M": {
                    "horizon": "1M",
                    "forecast_count": 40,
                    "realized_count": 25,
                    "realization_coverage": 0.63,
                    "hit_rate": 0.6,
                    "mean_abs_forecast_error": 0.025,
                    "reliability_state": "reliable",
                    "reliability_reason": "sample_and_coverage_ok",
                    "min_realized_required": 12,
                },
                "3M": {
                    "horizon": "3M",
                    "forecast_count": 16,
                    "realized_count": 12,
                    "realization_coverage": 0.75,
                    "hit_rate": 0.58,
                    "mean_abs_forecast_error": 0.03,
                    "reliability_state": "reliable",
                    "reliability_reason": "sample_and_coverage_ok",
                    "min_realized_required": 6,
                },
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
    assert cards["forecast_count"] == 40
    assert cards["realized_count"] == 25
    assert cards["coverage_pct"] == "63.0%"
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
    assert cards["has_critical_metric_alert"] is False
    assert len(cards["learning_metrics_panel"]) == 3
    assert cards["learning_metrics_panel"][0]["horizon"] == "1W"
    assert len(cards["policy_compliance_panel"]) >= 10
    assert cards["policy_compliance_panel"][0]["check"] == "Universe"
    assert cards["policy_compliance_panel"][0]["status"] == "UNKNOWN"
    assert cards["policy_compliance_summary"]["unknown"] >= 10
    assert cards["learning_metrics_panel"][0]["reliability_badge"] == "🟢 reliable"
    assert (
        cards["learning_metrics_panel"][0]["reliability_reason_text"]
        == "Sample size and coverage meet reliability threshold."
    )


def test_dashboard_app_treats_malformed_count_metrics_as_unknown_or_error():
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
    assert cards["metric_status"]["raw_events"]["status"] == "unknown"
    assert cards["metric_status"]["attribution_total"]["status"] == "error"
    assert cards["has_critical_metric_alert"] is True


def test_dashboard_app_parses_numeric_strings_for_percent_metrics():
    cards = dashboard_app.build_operator_cards(
        {
            "learning_metrics": {
                "realization_coverage": "0.4",
                "hit_rate": "60%",
                "mean_abs_forecast_error": "2.5%",
                "mean_signed_forecast_error": "-0.007",
            },
            "attribution_summary": {
                "hard_evidence_coverage": "0.86",
                "hard_evidence_traceability_coverage": "71%",
                "soft_evidence_coverage": "0.57",
                "evidence_gap_coverage": "14%",
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


def test_dashboard_app_parses_comma_grouped_integer_strings_for_count_metrics():
    cards = dashboard_app.build_operator_cards(
        {
            "counters": {
                "raw_events": "1,234",
                "canonical_events": "2,000",
                "quarantine_events": "10",
            },
            "learning_metrics": {
                "forecast_count": "12,345",
                "realized_count": "6,789",
            },
            "attribution_summary": {
                "total": "1,111",
                "top_count": "222",
                "evidence_gap_count": "3",
            },
        }
    )

    assert cards["raw_events"] == 1234
    assert cards["canonical_events"] == 2000
    assert cards["forecast_count"] == 12345
    assert cards["realized_count"] == 6789
    assert cards["attribution_total"] == 1111
    assert cards["attribution_top_count"] == 222
    assert cards["metric_status"]["raw_events"]["status"] == "ok"


def test_dashboard_app_flags_missing_required_horizon_metrics():
    cards = dashboard_app.build_operator_cards(
        {
            "learning_metrics_by_horizon": {
                "1M": {
                    "horizon": "1M",
                    "forecast_count": 25,
                    "realized_count": 10,
                    "realization_coverage": 0.4,
                    "hit_rate": 0.6,
                    "mean_abs_forecast_error": 0.025,
                }
            }
        }
    )

    assert len(cards["learning_metrics_panel"]) == 3
    assert cards["learning_metrics_panel"][0]["horizon"] == "1W"
    assert cards["learning_metrics_panel"][0]["status"] == "unknown"
    assert cards["learning_metrics_panel"][2]["horizon"] == "3M"
    assert cards["learning_metrics_panel"][2]["status"] == "unknown"
    assert cards["has_critical_metric_alert"] is True


def test_dashboard_app_keeps_true_zero_values_as_ok_not_unknown():
    cards = dashboard_app.build_operator_cards(
        {
            "counters": {
                "raw_events": "0",
                "canonical_events": 0,
                "quarantine_events": 0.0,
            },
            "learning_metrics": {
                "forecast_count": "0",
                "realized_count": 0,
                "realization_coverage": "0",
                "hit_rate": 0,
            },
            "attribution_summary": {
                "total": "0",
                "hard_evidence_coverage": "0",
                "hard_evidence_traceability_coverage": "0",
                "evidence_gap_count": 0,
            },
        }
    )

    assert cards["raw_events"] == 0
    assert cards["coverage_pct"] == "0.0%"
    assert cards["metric_status"]["raw_events"]["status"] == "ok"
    assert cards["metric_status"]["coverage_pct"]["status"] == "ok"


def test_dashboard_app_flags_missing_horizon_block_as_critical_alert():
    cards = dashboard_app.build_operator_cards({"learning_metrics_by_horizon": None})

    assert len(cards["learning_metrics_panel"]) == 3
    assert all(row["status"] == "unknown" for row in cards["learning_metrics_panel"])
    assert cards["has_critical_metric_alert"] is True


def test_dashboard_app_flags_primary_horizon_reliability_guardrail():
    cards = dashboard_app.build_operator_cards(
        {
            "learning_metrics_by_horizon": {
                "1W": {
                    "horizon": "1W",
                    "forecast_count": 10,
                    "realized_count": 4,
                    "realization_coverage": 0.4,
                    "hit_rate": 0.5,
                    "mean_abs_forecast_error": 0.02,
                    "reliability_state": "insufficient",
                    "reliability_reason": "realized_count_below_min:4<8",
                    "min_realized_required": 8,
                },
                "1M": {
                    "horizon": "1M",
                    "forecast_count": 12,
                    "realized_count": 9,
                    "realization_coverage": 0.75,
                    "hit_rate": 0.55,
                    "mean_abs_forecast_error": 0.03,
                    "reliability_state": "insufficient",
                    "reliability_reason": "realized_count_below_min:9<12",
                    "min_realized_required": 12,
                },
                "3M": {
                    "horizon": "3M",
                    "forecast_count": 6,
                    "realized_count": 5,
                    "realization_coverage": 0.83,
                    "hit_rate": 0.5,
                    "mean_abs_forecast_error": 0.04,
                    "reliability_state": "insufficient",
                    "reliability_reason": "realized_count_below_min:5<6",
                    "min_realized_required": 6,
                },
            }
        }
    )

    assert cards["has_primary_horizon_reliability_alert"] is True
    assert cards["learning_metrics_panel"][1]["reliability"] == "insufficient"
    assert cards["learning_metrics_panel"][1]["reliability_badge"] == "🔴 insufficient"
    assert "Realized sample below minimum" in cards["learning_metrics_panel"][1]["reliability_reason_text"]
    assert cards["learning_metrics_panel"][1]["status"] == "warn"


def test_dashboard_app_uses_policy_compliance_payload_when_present():
    cards = dashboard_app.build_operator_cards(
        {
            "policy_compliance": {
                "checks": [
                    {
                        "check": "Primary horizon readiness (1M)",
                        "status": "FAIL",
                        "reason": "realized_count_below_min:4<12",
                        "as_of": None,
                        "evidence": {"realized_count": 4},
                    }
                ],
                "summary": {"total": 1, "pass": 0, "warn": 0, "fail": 1, "unknown": 0},
            }
        }
    )

    assert cards["policy_compliance_panel"][0]["status"] == "FAIL"
    assert cards["policy_compliance_summary"]["fail"] == 1


def test_dashboard_app_policy_panel_surfaces_universe_region_evidence_columns():
    cards = dashboard_app.build_operator_cards(
        {
            "policy_compliance": {
                "checks": [
                    {
                        "check": "Universe coverage (US/KR/Crypto)",
                        "status": "WARN",
                        "reason": "Ingest data exists but region-aware coverage evidence is incomplete: KR",
                        "as_of": "2026-02-22T11:20:00Z",
                        "evidence": {
                            "region_coverage_counts": {"US": 1, "KR": 0, "CRYPTO": 1},
                            "region_metadata_completeness": 2 / 3,
                        },
                    }
                ],
                "summary": {"total": 1, "pass": 0, "warn": 1, "fail": 0, "unknown": 0},
            }
        }
    )

    row = cards["policy_compliance_panel"][0]
    assert row["us_coverage_count"] == 1
    assert row["kr_coverage_count"] == 0
    assert row["crypto_coverage_count"] == 1
    assert row["region_metadata_completeness"] == "66.7%"
    assert row["evaluation_window_as_of"] == "2026-02-22T11:20:00Z"


def test_dashboard_app_marks_non_finite_numbers_as_error_not_crash():
    cards = dashboard_app.build_operator_cards(
        {
            "counters": {
                "raw_events": math.nan,
            },
            "learning_metrics": {
                "realization_coverage": "NaN",
                "hit_rate": math.inf,
            },
            "attribution_summary": {
                "hard_evidence_coverage": "inf",
            },
        }
    )

    assert cards["raw_events"] == "n/a"
    assert cards["coverage_pct"] == "n/a"
    assert cards["hit_rate_pct"] == "n/a"
    assert cards["hard_evidence_pct"] == "n/a"
    assert cards["metric_status"]["raw_events"]["status"] == "error"
    assert cards["metric_status"]["coverage_pct"]["reason"] == "non_finite_numeric"


def test_dashboard_app_flags_deployed_access_incident_when_degraded():
    cards = dashboard_app.build_operator_cards(
        {
            "deployed_access": {
                "status": "degraded",
                "reason": "auth_wall_redirect_detected",
                "checked_at": "2026-02-22T15:00:00Z",
                "remediation_hint": "set app public",
            }
        }
    )

    assert cards["has_deployed_access_alert"] is True
    assert cards["deployed_access"]["reason"] == "auth_wall_redirect_detected"


def test_dashboard_app_keeps_deployed_access_alert_off_when_status_ok():
    cards = dashboard_app.build_operator_cards({"deployed_access": {"status": "ok"}})

    assert cards["has_deployed_access_alert"] is False
