import importlib


def test_forecast_capture_cli_to_dashboard_projection(monkeypatch):
    cli = importlib.import_module("src.ingestion.cli")
    dashboard_service = importlib.import_module("src.ingestion.dashboard_service")

    class FakePostgresRepository:
        records_by_key: dict[tuple[str, str, str], dict[str, object]] = {}
        next_id = 1

        def __init__(self, dsn: str):
            self.dsn = dsn

        def write_forecast_record_idempotent(self, record: dict[str, object]) -> tuple[int, bool]:
            key = (
                str(record["thesis_id"]),
                str(record["horizon"]),
                record["as_of"].isoformat(),
            )
            if key in self.records_by_key:
                return int(self.records_by_key[key]["id"]), True

            forecast_id = self.next_id
            type(self).next_id += 1
            self.records_by_key[key] = {
                "id": forecast_id,
                "record": record,
            }
            return forecast_id, False

        def read_latest_runs(self, limit: int = 20) -> list[dict[str, object]]:
            return []

        def read_status_counters(self) -> dict[str, int]:
            return {"raw_events": 0, "canonical_events": 0, "quarantine_events": 0}

        def read_learning_metrics(self, horizon: str = "1M") -> dict[str, object]:
            count = sum(
                1
                for row in self.records_by_key.values()
                if row["record"]["horizon"] == horizon
            )
            return {
                "horizon": horizon,
                "forecast_count": count,
                "realized_count": 0,
                "realization_coverage": None,
                "hit_rate": None,
                "mean_abs_forecast_error": None,
                "mean_signed_forecast_error": None,
            }

        def read_forecast_error_category_stats(
            self, horizon: str = "1M", limit: int = 5
        ) -> list[dict[str, object]]:
            return []

    monkeypatch.setenv("DATABASE_URL", "postgresql://example")
    monkeypatch.setattr(cli, "PostgresRepository", FakePostgresRepository)

    first = cli.create_forecast_record_command(
        thesis_id="thesis-seed-1",
        horizon="1M",
        expected_return_low=0.01,
        expected_return_high=0.03,
        confidence=0.6,
        as_of="2026-02-22T00:00:00+00:00",
        key_drivers_json='["macro:disinflation"]',
        evidence_hard_json='[{"source":"fred","metric":"CPI","as_of":"2026-02-21"}]',
    )
    second = cli.create_forecast_record_command(
        thesis_id="thesis-seed-1",
        horizon="1M",
        expected_return_low=0.01,
        expected_return_high=0.03,
        confidence=0.6,
        as_of="2026-02-22T00:00:00+00:00",
        key_drivers_json='["macro:disinflation"]',
        evidence_hard_json='[{"source":"fred","metric":"CPI","as_of":"2026-02-21"}]',
    )

    assert first["deduplicated"] is False
    assert second["deduplicated"] is True
    assert first["forecast_id"] == second["forecast_id"]

    view = dashboard_service.build_dashboard_view(FakePostgresRepository("postgresql://example"))
    assert view["learning_metrics"]["forecast_count"] == 1
    assert view["learning_metrics_by_horizon"]["1M"]["forecast_count"] == 1
