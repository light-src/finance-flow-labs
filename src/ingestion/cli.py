import argparse
import importlib
import json
import os
import urllib.error
import urllib.request
from collections.abc import Mapping
from datetime import datetime, timezone
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

    streamlit_access_probe = subparsers.add_parser("streamlit-access-probe")
    _ = streamlit_access_probe.add_argument(
        "--url", default="https://finance-flow-labs.streamlit.app/"
    )
    _ = streamlit_access_probe.add_argument("--timeout-seconds", type=float, default=10.0)

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


def probe_streamlit_access_command(
    url: str = "https://finance-flow-labs.streamlit.app/",
    timeout_seconds: float = 10.0,
) -> dict[str, object]:
    class NoRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self, req, fp, code, msg, hdrs, newurl):  # type: ignore[override]
            return None

    request = urllib.request.Request(url=url, method="GET")
    opener = urllib.request.build_opener(NoRedirect)

    try:
        with opener.open(request, timeout=timeout_seconds) as response:
            status_code = response.status
            headers = dict(response.headers.items())
    except urllib.error.HTTPError as exc:
        status_code = exc.code
        headers = dict(exc.headers.items()) if exc.headers else {}
    except urllib.error.URLError as exc:
        return {
            "url": url,
            "status": "network_error",
            "status_code": None,
            "location": None,
            "reason": str(exc.reason),
        }

    location = headers.get("Location") or headers.get("location")
    if status_code in {301, 302, 303, 307, 308} and location and "share.streamlit.io/-/auth/app" in location:
        return {
            "url": url,
            "status": "auth_wall_redirect",
            "status_code": status_code,
            "location": location,
            "reason": "landing_url_redirects_to_streamlit_auth",
        }

    if status_code >= 400:
        return {
            "url": url,
            "status": "http_error",
            "status_code": status_code,
            "location": location,
            "reason": "landing_url_returned_error_status",
        }

    return {
        "url": url,
        "status": "ok",
        "status_code": status_code,
        "location": location,
        "reason": "landing_url_accessible",
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


def main(argv: Optional[list[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "run-update":
        summary = run_update_command(args.source, args.entity)
        print(json.dumps(summary))
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

    if args.command == "streamlit-access-probe":
        probe = probe_streamlit_access_command(
            url=args.url,
            timeout_seconds=args.timeout_seconds,
        )
        print(json.dumps(probe, default=str))
        return 0 if probe.get("status") == "ok" else 2

    parser.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
