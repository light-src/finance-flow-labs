import argparse
import importlib
import json
import os
import urllib.request
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from typing import Optional

from .adapters.ecos import EcosAdapter
from .adapters.fred import FredAdapter
from .adapters.opendart import OpenDartAdapter
from .adapters.sec_edgar import SecEdgarAdapter
from .http_client import HttpResponse, SimpleHttpClient
from .postgres_repository import PostgresRepository
from .quality_gate import BatchMetrics
from .repository import InMemoryRepository
from .source_registry import SourceDescriptor


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ingestion")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_update = subparsers.add_parser("run-update")
    _ = run_update.add_argument("--source", required=True)
    _ = run_update.add_argument("--entity")

    portfolio_snapshot_create = subparsers.add_parser("portfolio-snapshot-create")
    _ = portfolio_snapshot_create.add_argument("--as-of", required=True)
    _ = portfolio_snapshot_create.add_argument("--nav", required=True, type=float)
    _ = portfolio_snapshot_create.add_argument("--us-weight", type=float)
    _ = portfolio_snapshot_create.add_argument("--kr-weight", type=float)
    _ = portfolio_snapshot_create.add_argument("--crypto-weight", type=float)
    _ = portfolio_snapshot_create.add_argument("--leverage-weight", type=float)

    expected_vs_realized = subparsers.add_parser("expected-vs-realized")
    _ = expected_vs_realized.add_argument("--horizon", default="1M")
    _ = expected_vs_realized.add_argument("--limit", type=int, default=50)

    forecast_error_attributions = subparsers.add_parser("forecast-error-attributions")
    _ = forecast_error_attributions.add_argument("--horizon", default="1M")
    _ = forecast_error_attributions.add_argument("--limit", type=int, default=50)

    forecast_error_category_stats = subparsers.add_parser("forecast-error-category-stats")
    _ = forecast_error_category_stats.add_argument("--horizon", default="1M")
    _ = forecast_error_category_stats.add_argument("--limit", type=int, default=20)

    forecast_record_create = subparsers.add_parser("forecast-record-create")
    _ = forecast_record_create.add_argument("--thesis-id", required=True)
    _ = forecast_record_create.add_argument("--horizon", required=True, choices=["1W", "1M", "3M"])
    _ = forecast_record_create.add_argument("--expected-return-low", required=True, type=float)
    _ = forecast_record_create.add_argument("--expected-return-high", required=True, type=float)
    _ = forecast_record_create.add_argument("--expected-volatility", type=float)
    _ = forecast_record_create.add_argument("--expected-drawdown", type=float)
    _ = forecast_record_create.add_argument("--confidence", required=True, type=float)
    _ = forecast_record_create.add_argument("--key-drivers-json", default="[]")
    _ = forecast_record_create.add_argument("--evidence-hard-json", required=True)
    _ = forecast_record_create.add_argument("--evidence-soft-json", default="[]")
    _ = forecast_record_create.add_argument("--as-of", required=True)

    streamlit_access_check = subparsers.add_parser("streamlit-access-check")
    _ = streamlit_access_check.add_argument("--url", required=True)
    _ = streamlit_access_check.add_argument("--timeout-seconds", type=float, default=15)
    _ = streamlit_access_check.add_argument("--attempts", type=int, default=3)
    _ = streamlit_access_check.add_argument("--backoff-seconds", type=float, default=0.5)

    deploy_access_gate = subparsers.add_parser("deploy-access-gate")
    _ = deploy_access_gate.add_argument("--url", required=True)
    _ = deploy_access_gate.add_argument("--mode", choices=["public", "restricted"])
    _ = deploy_access_gate.add_argument("--restricted-login-path")
    _ = deploy_access_gate.add_argument("--timeout-seconds", type=float, default=15)
    _ = deploy_access_gate.add_argument("--attempts", type=int, default=3)
    _ = deploy_access_gate.add_argument("--backoff-seconds", type=float, default=0.5)

    learning_bootstrap = subparsers.add_parser("learning-bootstrap")
    _ = learning_bootstrap.add_argument("--as-of", required=True)
    _ = learning_bootstrap.add_argument("--horizons", default="1W,1M,3M")
    _ = learning_bootstrap.add_argument("--min-samples", default="8,12,6")
    _ = learning_bootstrap.add_argument("--dry-run", action="store_true")

    return parser


def _urllib_transport(method: str, url: str, headers: Mapping[str, str]) -> HttpResponse:
    request = urllib.request.Request(url=url, method=method, headers=dict(headers))
    with urllib.request.urlopen(request, timeout=30) as response:
        return HttpResponse(
            status_code=response.status,
            body=response.read(),
            headers=dict(response.headers.items()),
        )


def _collect_payload(source: str, entity: Optional[str]) -> tuple[str, dict[str, object]]:
    client = SimpleHttpClient(transport=_urllib_transport, max_retries=2)

    if source == "sec_edgar":
        cik = entity or os.getenv("SEC_CIK", "0000320193")
        user_agent = os.getenv("SEC_USER_AGENT", "finanace-flow-labs/0.1 ops@example.com")
        payload = SecEdgarAdapter(client=client, user_agent=user_agent).fetch_company_facts(cik)
        return cik, payload

    if source == "fred":
        series_id = entity or os.getenv("FRED_SERIES_ID", "CPIAUCSL")
        api_key = os.getenv("FRED_API_KEY", "")
        payload = FredAdapter(client=client, api_key=api_key).fetch_series_observations(series_id)
        return series_id, payload

    if source == "opendart":
        corp_code = entity or os.getenv("DART_CORP_CODE", "00126380")
        api_key = os.getenv("DART_API_KEY", os.getenv("DART_CRTFC_KEY", ""))
        payload = OpenDartAdapter(client=client, api_key=api_key).fetch_company(corp_code)
        return corp_code, payload

    if source == "ecos":
        stat_code = entity or os.getenv("ECOS_STAT_CODE", "722Y001")
        api_key = os.getenv("ECOS_API_KEY", "")
        payload = EcosAdapter(client=client, api_key=api_key).fetch_statistic(stat_code)
        return stat_code, payload

    raise ValueError(f"unsupported source: {source}")


def _parse_json_array(raw: str, field_name: str) -> list[object]:
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{field_name} must be valid JSON: {exc.msg}") from exc
    if not isinstance(value, list):
        raise ValueError(f"{field_name} must be a JSON array")
    return value


def _parse_iso_datetime(raw: str, field_name: str) -> datetime:
    normalized = raw.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be ISO-8601 datetime") from exc
    if dt.tzinfo is None:
        raise ValueError(f"{field_name} must include timezone offset (e.g. +00:00)")
    return dt


def _parse_iso_date(raw: str, field_name: str) -> str:
    normalized = raw.strip()
    try:
        parsed = datetime.fromisoformat(normalized).date()
    except ValueError as exc:
        raise ValueError(f"{field_name} must be ISO-8601 date (YYYY-MM-DD)") from exc
    return parsed.isoformat()


def run_update_command(source: str, entity: Optional[str] = None) -> dict[str, object]:
    manual_runner = importlib.import_module("src.ingestion.manual_runner")
    run_manual_update = manual_runner.run_manual_update
    entity_id, payload = _collect_payload(source, entity)

    dsn = os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL")
    run_history_repository = PostgresRepository(dsn=dsn) if dsn else None
    data_repository = PostgresRepository(dsn=dsn) if dsn else InMemoryRepository()

    now = datetime.now(timezone.utc)
    summary = run_manual_update(
        source=SourceDescriptor(
            name=source,
            utility=5,
            reliability=4,
            legal=4,
            cost=3,
            maintenance=3,
        ),
        metrics=BatchMetrics(
            freshness=True,
            completeness=True,
            schema_drift=False,
            license_ok=True,
        ),
        idempotency_key=f"{source}|{entity_id}|{now.date().isoformat()}|manual",
        payload=payload,
        rows=[{"entity_id": entity_id, "available_at": now}],
        decision_time=now,
        repository=data_repository,
        run_history_repository=run_history_repository,
    )
    return summary


def create_portfolio_snapshot_command(
    as_of: str,
    nav: float,
    us_weight: Optional[float] = None,
    kr_weight: Optional[float] = None,
    crypto_weight: Optional[float] = None,
    leverage_weight: Optional[float] = None,
) -> dict[str, object]:
    dsn = os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL")
    if not dsn:
        raise ValueError("SUPABASE_DB_URL or DATABASE_URL is required")
    if nav <= 0:
        raise ValueError("nav must be > 0")

    for field_name, value in [
        ("us_weight", us_weight),
        ("kr_weight", kr_weight),
        ("crypto_weight", crypto_weight),
        ("leverage_weight", leverage_weight),
    ]:
        if value is None:
            continue
        if not (0 <= value <= 1):
            raise ValueError(f"{field_name} must be between 0 and 1")

    repository = PostgresRepository(dsn=dsn)
    snapshot_id = repository.write_portfolio_snapshot(
        {
            "as_of": _parse_iso_date(as_of, "as_of"),
            "nav": nav,
            "us_weight": us_weight,
            "kr_weight": kr_weight,
            "crypto_weight": crypto_weight,
            "leverage_weight": leverage_weight,
        }
    )

    return {
        "id": snapshot_id,
        "as_of": _parse_iso_date(as_of, "as_of"),
        "nav": nav,
    }


def read_expected_vs_realized_command(horizon: str = "1M", limit: int = 50) -> list[dict[str, object]]:
    dsn = os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL")
    if not dsn:
        raise ValueError("SUPABASE_DB_URL or DATABASE_URL is required")

    repository = PostgresRepository(dsn=dsn)
    return repository.read_expected_vs_realized(horizon=horizon, limit=limit)


def read_forecast_error_attributions_command(
    horizon: str = "1M", limit: int = 50
) -> list[dict[str, object]]:
    dsn = os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL")
    if not dsn:
        raise ValueError("SUPABASE_DB_URL or DATABASE_URL is required")

    repository = PostgresRepository(dsn=dsn)
    return repository.read_forecast_error_attributions(horizon=horizon, limit=limit)


def read_forecast_error_category_stats_command(
    horizon: str = "1M", limit: int = 20
) -> list[dict[str, object]]:
    dsn = os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL")
    if not dsn:
        raise ValueError("SUPABASE_DB_URL or DATABASE_URL is required")

    repository = PostgresRepository(dsn=dsn)
    return repository.read_forecast_error_category_stats(horizon=horizon, limit=limit)


def run_streamlit_access_check_command(
    url: str,
    timeout_seconds: float = 15,
    attempts: int = 3,
    backoff_seconds: float = 0.5,
) -> dict[str, object]:
    streamlit_access = importlib.import_module("src.ingestion.streamlit_access")
    check_streamlit_access = streamlit_access.check_streamlit_access
    result = check_streamlit_access(
        url=url,
        timeout_seconds=timeout_seconds,
        attempts=attempts,
        backoff_seconds=backoff_seconds,
    )
    return result.to_dict()


def run_deploy_access_gate_command(
    url: str,
    mode: str | None = None,
    restricted_login_path: str | None = None,
    timeout_seconds: float = 15,
    attempts: int = 3,
    backoff_seconds: float = 0.5,
) -> dict[str, object]:
    deploy_policy = importlib.import_module("src.ingestion.deploy_access_policy")

    effective_mode = mode or os.getenv("DEPLOY_ACCESS_MODE", "public")
    effective_restricted_login_path = restricted_login_path or os.getenv("DEPLOY_RESTRICTED_LOGIN_PATH")

    access_result = run_streamlit_access_check_command(
        url=url,
        timeout_seconds=timeout_seconds,
        attempts=attempts,
        backoff_seconds=backoff_seconds,
    )
    decision = deploy_policy.evaluate_deploy_access(
        access_result,
        mode=effective_mode,
        restricted_login_path=effective_restricted_login_path,
    )

    return {
        "url": url,
        "deploy_access_mode": deploy_policy.normalize_access_mode(effective_mode),
        "restricted_login_path": effective_restricted_login_path,
        "access_check": access_result,
        "gate": decision.to_dict(),
    }


def create_forecast_record_command(
    thesis_id: str,
    horizon: str,
    expected_return_low: float,
    expected_return_high: float,
    confidence: float,
    as_of: str,
    key_drivers_json: str,
    evidence_hard_json: str,
    evidence_soft_json: str = "[]",
    expected_volatility: Optional[float] = None,
    expected_drawdown: Optional[float] = None,
) -> dict[str, object]:
    dsn = os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL")
    if not dsn:
        raise ValueError("SUPABASE_DB_URL or DATABASE_URL is required")

    if expected_return_low > expected_return_high:
        raise ValueError("expected_return_low must be <= expected_return_high")
    if not (0 <= confidence <= 1):
        raise ValueError("confidence must be between 0 and 1")

    key_drivers = _parse_json_array(key_drivers_json, "key_drivers_json")
    evidence_hard = _parse_json_array(evidence_hard_json, "evidence_hard_json")
    evidence_soft = _parse_json_array(evidence_soft_json, "evidence_soft_json")

    if not evidence_hard:
        raise ValueError("evidence_hard_json must be a non-empty JSON array")

    payload = {
        "thesis_id": thesis_id,
        "horizon": horizon,
        "expected_return_low": expected_return_low,
        "expected_return_high": expected_return_high,
        "expected_volatility": expected_volatility,
        "expected_drawdown": expected_drawdown,
        "confidence": confidence,
        "key_drivers": key_drivers,
        "evidence_hard": evidence_hard,
        "evidence_soft": evidence_soft,
        "as_of": _parse_iso_datetime(as_of, "as_of"),
    }

    repository = PostgresRepository(dsn=dsn)
    forecast_id, deduplicated = repository.write_forecast_record_idempotent(payload)
    return {
        "forecast_id": forecast_id,
        "deduplicated": deduplicated,
        "thesis_id": thesis_id,
        "horizon": horizon,
    }


def run_learning_bootstrap_command(
    as_of: str,
    horizons: str = "1W,1M,3M",
    min_samples: str = "8,12,6",
    dry_run: bool = False,
) -> dict[str, object]:
    as_of_dt = _parse_iso_datetime(as_of, "as_of")
    horizon_list = [h.strip() for h in horizons.split(",") if h.strip()]
    sample_list = [int(s.strip()) for s in min_samples.split(",") if s.strip()]
    if len(horizon_list) != len(sample_list):
        raise ValueError("horizons and min-samples must have the same number of items")

    dsn = os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL")
    if not dsn and not dry_run:
        raise ValueError("SUPABASE_DB_URL or DATABASE_URL is required")

    plan: list[dict[str, object]] = []
    horizon_days = {"1W": 7, "1M": 30, "3M": 90}
    for horizon, sample_count in zip(horizon_list, sample_list):
        if horizon not in horizon_days:
            raise ValueError(f"unsupported horizon: {horizon}")
        for idx in range(sample_count):
            point_as_of = as_of_dt - timedelta(days=(idx + 1) * horizon_days[horizon])
            plan.append(
                {
                    "horizon": horizon,
                    "thesis_id": f"bootstrap-{horizon.lower()}-{idx + 1:02d}",
                    "as_of": point_as_of,
                    "evaluated_at": point_as_of + timedelta(days=horizon_days[horizon]),
                    "expected_return_low": 0.01,
                    "expected_return_high": 0.04,
                    "realized_return": 0.025,
                    "category": "unknown",
                }
            )

    if dry_run:
        return {"dry_run": True, "as_of": as_of_dt.isoformat(), "planned_rows": len(plan), "plan": plan}

    repository = PostgresRepository(dsn=dsn)
    upserted = 0
    realized = 0
    attributed = 0
    for row in plan:
        repository.write_investment_thesis(
            {
                "thesis_id": row["thesis_id"],
                "scope_level": "portfolio",
                "target_id": row["horizon"],
                "title": f"Bootstrap {row['horizon']} sample",
                "summary": "Deterministic bootstrap sample for learning-loop readiness",
                "evidence_hard": [{"source": "bootstrap", "metric": "seed"}],
                "evidence_soft": [],
                "as_of": row["as_of"],
                "lineage_id": "learning-bootstrap-v1",
            }
        )
        forecast_id, _ = repository.write_forecast_record_idempotent(
            {
                "thesis_id": row["thesis_id"],
                "horizon": row["horizon"],
                "expected_return_low": row["expected_return_low"],
                "expected_return_high": row["expected_return_high"],
                "expected_volatility": 0.12,
                "expected_drawdown": -0.08,
                "confidence": 0.55,
                "key_drivers": ["bootstrap:readiness"],
                "evidence_hard": [{"source": "bootstrap", "metric": "seed"}],
                "evidence_soft": [],
                "as_of": row["as_of"],
            }
        )
        upserted += 1

        existing = repository.read_expected_vs_realized(horizon=str(row["horizon"]), limit=500)
        if any(r.get("forecast_id") == forecast_id and r.get("realization_id") is not None for r in existing):
            continue

        realization_id = repository.write_realization_from_outcome(
            forecast_id=forecast_id,
            realized_return=float(row["realized_return"]),
            realized_volatility=0.11,
            max_drawdown=-0.06,
            evaluated_at=row["evaluated_at"],
        )
        realized += 1
        repository.write_forecast_error_attribution(
            {
                "realization_id": realization_id,
                "category": row["category"],
                "contribution": 0.0,
                "note": "bootstrap seed",
                "evidence_hard": [{"source": "bootstrap", "metric": "seed"}],
                "evidence_soft": [],
            }
        )
        attributed += 1

    return {
        "dry_run": False,
        "as_of": as_of_dt.isoformat(),
        "planned_rows": len(plan),
        "forecast_upserts": upserted,
        "realization_inserts": realized,
        "attribution_inserts": attributed,
    }


def main(argv: Optional[list[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "run-update":
        summary = run_update_command(args.source, args.entity)
        print(json.dumps(summary))
        return 0

    if args.command == "portfolio-snapshot-create":
        summary = create_portfolio_snapshot_command(
            as_of=args.as_of,
            nav=args.nav,
            us_weight=args.us_weight,
            kr_weight=args.kr_weight,
            crypto_weight=args.crypto_weight,
            leverage_weight=args.leverage_weight,
        )
        print(json.dumps(summary, default=str))
        return 0

    if args.command == "expected-vs-realized":
        rows = read_expected_vs_realized_command(horizon=args.horizon, limit=args.limit)
        print(json.dumps(rows, default=str))
        return 0

    if args.command == "forecast-error-attributions":
        rows = read_forecast_error_attributions_command(horizon=args.horizon, limit=args.limit)
        print(json.dumps(rows, default=str))
        return 0

    if args.command == "forecast-error-category-stats":
        rows = read_forecast_error_category_stats_command(horizon=args.horizon, limit=args.limit)
        print(json.dumps(rows, default=str))
        return 0

    if args.command == "forecast-record-create":
        row = create_forecast_record_command(
            thesis_id=args.thesis_id,
            horizon=args.horizon,
            expected_return_low=args.expected_return_low,
            expected_return_high=args.expected_return_high,
            expected_volatility=args.expected_volatility,
            expected_drawdown=args.expected_drawdown,
            confidence=args.confidence,
            key_drivers_json=args.key_drivers_json,
            evidence_hard_json=args.evidence_hard_json,
            evidence_soft_json=args.evidence_soft_json,
            as_of=args.as_of,
        )
        print(json.dumps(row, default=str))
        return 0

    if args.command == "streamlit-access-check":
        result = run_streamlit_access_check_command(
            url=args.url,
            timeout_seconds=args.timeout_seconds,
            attempts=args.attempts,
            backoff_seconds=args.backoff_seconds,
        )
        print(json.dumps(result, default=str))
        return 0 if bool(result.get("ok")) else 2

    if args.command == "deploy-access-gate":
        result = run_deploy_access_gate_command(
            url=args.url,
            mode=args.mode,
            restricted_login_path=args.restricted_login_path,
            timeout_seconds=args.timeout_seconds,
            attempts=args.attempts,
            backoff_seconds=args.backoff_seconds,
        )
        print(json.dumps(result, default=str))
        gate = result.get("gate") if isinstance(result, dict) else None
        gate_ok = bool(gate.get("ok")) if isinstance(gate, dict) else False
        return 0 if gate_ok else 2

    if args.command == "learning-bootstrap":
        result = run_learning_bootstrap_command(
            as_of=args.as_of,
            horizons=args.horizons,
            min_samples=args.min_samples,
            dry_run=args.dry_run,
        )
        print(json.dumps(result, default=str))
        return 0

    parser.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
