import importlib

import pytest


cli = importlib.import_module("src.ingestion.cli")


def test_cli_exposes_manual_update_command():
    parser = cli.build_parser()
    args = parser.parse_args(["run-update", "--source", "sec_edgar"])

    assert args.command == "run-update"
    assert args.source == "sec_edgar"


def test_cli_exposes_portfolio_snapshot_create_command():
    parser = cli.build_parser()
    args = parser.parse_args(
        [
            "portfolio-snapshot-create",
            "--as-of",
            "2026-02-23",
            "--nav",
            "1000",
            "--us-weight",
            "0.4",
            "--kr-weight",
            "0.2",
            "--crypto-weight",
            "0.3",
            "--leverage-weight",
            "0.1",
        ]
    )

    assert args.command == "portfolio-snapshot-create"
    assert args.as_of == "2026-02-23"
    assert args.nav == 1000.0
    assert args.us_weight == 0.4


def test_create_portfolio_snapshot_command_requires_database_url(monkeypatch):
    monkeypatch.delenv("SUPABASE_DB_URL", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)

    with pytest.raises(ValueError):
        cli.create_portfolio_snapshot_command(as_of="2026-02-23", nav=1000)


def test_create_portfolio_snapshot_command_uses_postgres_repository(monkeypatch):
    class FakeRepository:
        def __init__(self, dsn: str) -> None:
            self.dsn = dsn

        def write_portfolio_snapshot(self, payload):
            assert payload["as_of"] == "2026-02-23"
            assert payload["nav"] == 1000
            assert payload["us_weight"] == 0.4
            assert payload["kr_weight"] == 0.2
            assert payload["crypto_weight"] == 0.3
            assert payload["leverage_weight"] == 0.1
            return 301

    monkeypatch.setenv("DATABASE_URL", "postgres://example")
    monkeypatch.setattr(cli, "PostgresRepository", FakeRepository)

    row = cli.create_portfolio_snapshot_command(
        as_of="2026-02-23",
        nav=1000,
        us_weight=0.4,
        kr_weight=0.2,
        crypto_weight=0.3,
        leverage_weight=0.1,
    )

    assert row == {"id": 301, "as_of": "2026-02-23", "nav": 1000}


def test_create_portfolio_snapshot_command_rejects_weight_out_of_range(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgres://example")

    with pytest.raises(ValueError, match="us_weight"):
        cli.create_portfolio_snapshot_command(
            as_of="2026-02-23",
            nav=1000,
            us_weight=1.1,
        )


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


def test_cli_exposes_streamlit_access_check_command_with_defaults():
    parser = cli.build_parser()
    args = parser.parse_args(["streamlit-access-check", "--url", "https://finance-flow-labs.streamlit.app/"])

    assert args.command == "streamlit-access-check"
    assert args.url == "https://finance-flow-labs.streamlit.app/"
    assert args.timeout_seconds == 15
    assert args.attempts == 3
    assert args.backoff_seconds == 0.5


def test_run_streamlit_access_check_command_returns_serializable_dict(monkeypatch):
    class FakeResult:
        def to_dict(self):
            return {
                "ok": False,
                "status_code": 303,
                "final_url": "https://finance-flow-labs.streamlit.app/",
                "auth_wall_redirect": True,
                "reason": "auth_wall_redirect_detected",
            }

    class FakeModule:
        @staticmethod
        def check_streamlit_access(
            url: str,
            timeout_seconds: float,
            attempts: int,
            backoff_seconds: float,
        ):
            assert url == "https://finance-flow-labs.streamlit.app/"
            assert timeout_seconds == 9
            assert attempts == 4
            assert backoff_seconds == 0.2
            return FakeResult()

    monkeypatch.setattr(cli.importlib, "import_module", lambda _: FakeModule())

    result = cli.run_streamlit_access_check_command(
        "https://finance-flow-labs.streamlit.app/",
        timeout_seconds=9,
        attempts=4,
        backoff_seconds=0.2,
    )

    assert result["auth_wall_redirect"] is True
    assert result["reason"] == "auth_wall_redirect_detected"


def test_cli_exposes_deploy_access_gate_command_with_defaults():
    parser = cli.build_parser()
    args = parser.parse_args(["deploy-access-gate", "--url", "https://finance-flow-labs.streamlit.app/"])

    assert args.command == "deploy-access-gate"
    assert args.mode is None
    assert args.restricted_login_path is None


def test_run_deploy_access_gate_command_combines_access_and_policy(monkeypatch):
    monkeypatch.setenv("DEPLOY_ACCESS_MODE", "public")

    def fake_access_check(**kwargs):
        assert kwargs["url"] == "https://finance-flow-labs.streamlit.app/"
        return {
            "ok": False,
            "auth_wall_redirect": True,
            "reason": "auth_wall_redirect_detected",
            "alert_severity": "critical",
            "remediation_hint": "hint",
        }

    class FakeDecision:
        def to_dict(self):
            return {"ok": False, "release_blocker": True, "reason": "auth_wall_redirect_detected"}

    class FakePolicyModule:
        @staticmethod
        def normalize_access_mode(mode: str):
            return mode

        @staticmethod
        def evaluate_deploy_access(access_result, *, mode: str, restricted_login_path: str | None):
            assert access_result["reason"] == "auth_wall_redirect_detected"
            assert mode == "public"
            assert restricted_login_path is None
            return FakeDecision()

    monkeypatch.setattr(cli, "run_streamlit_access_check_command", fake_access_check)
    monkeypatch.setattr(cli.importlib, "import_module", lambda _: FakePolicyModule())

    result = cli.run_deploy_access_gate_command("https://finance-flow-labs.streamlit.app/")

    assert result["deploy_access_mode"] == "public"
    assert result["gate"]["release_blocker"] is True


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


def test_cli_exposes_learning_bootstrap_command_with_defaults():
    parser = cli.build_parser()
    args = parser.parse_args(["learning-bootstrap", "--as-of", "2026-02-22T00:00:00+00:00"])

    assert args.command == "learning-bootstrap"
    assert args.horizons == "1W,1M,3M"
    assert args.min_samples == "8,12,6"
    assert args.dry_run is False


def test_run_learning_bootstrap_command_dry_run_has_deterministic_plan():
    result = cli.run_learning_bootstrap_command(
        as_of="2026-02-22T00:00:00+00:00",
        horizons="1W,1M",
        min_samples="2,1",
        dry_run=True,
    )

    assert result["dry_run"] is True
    assert result["planned_rows"] == 3
    assert result["plan"][0]["thesis_id"].startswith("bootstrap-1w-")


def test_run_learning_bootstrap_command_requires_database_url_for_non_dry_run(monkeypatch):
    monkeypatch.delenv("SUPABASE_DB_URL", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)

    with pytest.raises(ValueError, match="DATABASE_URL"):
        cli.run_learning_bootstrap_command(
            as_of="2026-02-22T00:00:00+00:00",
            dry_run=False,
        )
