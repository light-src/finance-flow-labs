from __future__ import annotations

from collections.abc import Callable
from typing import Any

from src.ingestion.postgres_repository import PostgresRepository

RepositoryFactory = Callable[[str], Any]


REQUEST_TYPE_MACRO_SIGNAL = "macro_signal"
SOURCE_VIEW_ENDUSER_SIGNALS = "enduser/signals"


def submit_macro_refresh_request(
    dsn: str,
    *,
    requested_by: str,
    cooldown_minutes: int = 10,
    repository_factory: RepositoryFactory = PostgresRepository,
) -> dict[str, object]:
    repository = repository_factory(dsn)
    return repository.create_refresh_request(
        request_type=REQUEST_TYPE_MACRO_SIGNAL,
        source_view=SOURCE_VIEW_ENDUSER_SIGNALS,
        requested_by=requested_by,
        note=None,
        cooldown_minutes=cooldown_minutes,
    )
