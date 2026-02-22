import importlib

import pytest


cli = importlib.import_module("src.ingestion.cli")


def test_cli_exposes_manual_update_command():
    parser = cli.build_parser()
    args = parser.parse_args(["run-update", "--source", "sec_edgar"])

    assert args.command == "run-update"
    assert args.source == "sec_edgar"


def test_cli_exposes_expected_vs_realized_command_with_defaults():
    parser = cli.build_parser()
    args = parser.parse_args(["expected-vs-realized"])

    assert args.command == "expected-vs-realized"
    assert args.horizon == "1M"
    assert args.limit == 50


def test_read_expected_vs_realized_command_requires_database_url(monkeypatch):
    monkeypatch.delenv("SUPABASE_DB_URL", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)

    with pytest.raises(ValueError):
        cli.read_expected_vs_realized_command()


def test_read_expected_vs_realized_command_uses_postgres_repository(monkeypatch):
    class FakeRepository:
        def __init__(self, dsn: str) -> None:
            self.dsn = dsn

        def read_expected_vs_realized(self, horizon: str, limit: int):
            assert horizon == "3M"
            assert limit == 10
            return [{"horizon": horizon, "limit": limit, "dsn": self.dsn}]

    monkeypatch.setenv("DATABASE_URL", "postgres://example")
    monkeypatch.setattr(cli, "PostgresRepository", FakeRepository)

    rows = cli.read_expected_vs_realized_command(horizon="3M", limit=10)

    assert rows == [{"horizon": "3M", "limit": 10, "dsn": "postgres://example"}]


def test_cli_exposes_forecast_error_attributions_command_with_defaults():
    parser = cli.build_parser()
    args = parser.parse_args(["forecast-error-attributions"])

    assert args.command == "forecast-error-attributions"
    assert args.horizon == "1M"
    assert args.limit == 50


def test_read_forecast_error_attributions_command_requires_database_url(monkeypatch):
    monkeypatch.delenv("SUPABASE_DB_URL", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)

    with pytest.raises(ValueError):
        cli.read_forecast_error_attributions_command()


def test_read_forecast_error_attributions_command_uses_postgres_repository(monkeypatch):
    class FakeRepository:
        def __init__(self, dsn: str) -> None:
            self.dsn = dsn

        def read_forecast_error_attributions(self, horizon: str, limit: int):
            assert horizon == "1W"
            assert limit == 7
            return [{"horizon": horizon, "limit": limit, "dsn": self.dsn}]

    monkeypatch.setenv("DATABASE_URL", "postgres://example")
    monkeypatch.setattr(cli, "PostgresRepository", FakeRepository)

    rows = cli.read_forecast_error_attributions_command(horizon="1W", limit=7)

    assert rows == [{"horizon": "1W", "limit": 7, "dsn": "postgres://example"}]


def test_cli_exposes_forecast_record_create_command():
    parser = cli.build_parser()
    args = parser.parse_args(
        [
            "forecast-record-create",
            "--thesis-id",
            "thesis-1",
            "--horizon",
            "1M",
            "--expected-return-low",
            "0.01",
            "--expected-return-high",
            "0.05",
            "--confidence",
            "0.7",
            "--evidence-hard-json",
            "[]",
            "--as-of",
            "2026-02-22T00:00:00+00:00",
        ]
    )

    assert args.command == "forecast-record-create"
    assert args.thesis_id == "thesis-1"


def test_create_forecast_record_command_uses_idempotent_repository(monkeypatch):
    class FakeRepository:
        def __init__(self, dsn: str) -> None:
            self.dsn = dsn

        def write_forecast_record_idempotent(self, payload):
            assert payload["thesis_id"] == "thesis-1"
            assert payload["horizon"] == "1M"
            assert payload["expected_return_low"] == 0.02
            assert payload["expected_return_high"] == 0.08
            assert payload["confidence"] == 0.6
            assert payload["key_drivers"] == ["macro:disinflation"]
            assert payload["evidence_hard"][0]["source"] == "fred"
            return (77, True)

    monkeypatch.setenv("DATABASE_URL", "postgres://example")
    monkeypatch.setattr(cli, "PostgresRepository", FakeRepository)

    row = cli.create_forecast_record_command(
        thesis_id="thesis-1",
        horizon="1M",
        expected_return_low=0.02,
        expected_return_high=0.08,
        confidence=0.6,
        key_drivers_json='["macro:disinflation"]',
        evidence_hard_json='[{"source":"fred","metric":"CPI"}]',
        evidence_soft_json='[{"source":"news","note":"tone"}]',
        as_of="2026-02-22T00:00:00+00:00",
    )

    assert row == {"forecast_id": 77, "deduplicated": True, "thesis_id": "thesis-1", "horizon": "1M"}


def test_create_forecast_record_command_rejects_invalid_range(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgres://example")

    with pytest.raises(ValueError):
        cli.create_forecast_record_command(
            thesis_id="thesis-1",
            horizon="1M",
            expected_return_low=0.09,
            expected_return_high=0.08,
            confidence=0.6,
            key_drivers_json="[]",
            evidence_hard_json='[{"source":"fred"}]',
            as_of="2026-02-22T00:00:00+00:00",
        )


def test_create_forecast_record_command_rejects_empty_hard_evidence(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgres://example")

    with pytest.raises(ValueError, match="non-empty"):
        cli.create_forecast_record_command(
            thesis_id="thesis-1",
            horizon="1M",
            expected_return_low=0.01,
            expected_return_high=0.03,
            confidence=0.6,
            key_drivers_json="[]",
            evidence_hard_json="[]",
            as_of="2026-02-22T00:00:00+00:00",
        )


def test_cli_exposes_streamlit_access_probe_command_with_defaults():
    parser = cli.build_parser()
    args = parser.parse_args(["streamlit-access-probe"])

    assert args.command == "streamlit-access-probe"
    assert args.url == "https://finance-flow-labs.streamlit.app/"
    assert args.timeout_seconds == 10.0


def test_probe_streamlit_access_command_detects_auth_wall_redirect(monkeypatch):
    class FakeHTTPError(cli.urllib.error.HTTPError):
        def __init__(self):
            super().__init__(
                url="https://finance-flow-labs.streamlit.app/",
                code=303,
                msg="See Other",
                hdrs={"Location": "https://share.streamlit.io/-/auth/app?redirect_uri=https://finance-flow-labs.streamlit.app/"},
                fp=None,
            )

    class FakeOpener:
        def open(self, request, timeout=10):
            raise FakeHTTPError()

    monkeypatch.setattr(cli.urllib.request, "build_opener", lambda *_: FakeOpener())

    probe = cli.probe_streamlit_access_command()

    assert probe["status"] == "auth_wall_redirect"
    assert probe["status_code"] == 303


def test_probe_streamlit_access_command_reports_ok(monkeypatch):
    class FakeResponse:
        status = 200

        class headers:
            @staticmethod
            def items():
                return []

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class FakeOpener:
        def open(self, request, timeout=10):
            return FakeResponse()

    monkeypatch.setattr(cli.urllib.request, "build_opener", lambda *_: FakeOpener())

    probe = cli.probe_streamlit_access_command()

    assert probe == {
        "url": "https://finance-flow-labs.streamlit.app/",
        "status": "ok",
        "status_code": 200,
        "location": None,
        "reason": "landing_url_accessible",
    }
