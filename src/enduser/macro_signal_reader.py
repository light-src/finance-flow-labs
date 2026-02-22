from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from typing import Any


def _parse_as_of(raw: object) -> datetime | None:
    if isinstance(raw, datetime):
        return raw if raw.tzinfo is not None else raw.replace(tzinfo=timezone.utc)
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def _normalize_drivers(raw: object) -> list[str]:
    if isinstance(raw, list):
        return [str(item).strip() for item in raw if str(item).strip()]
    return []


def load_latest_macro_regime_signal(repository: object, *, stale_after_days: int = 7) -> dict[str, Any]:
    """Load latest macro regime payload for end-user card rendering.

    Returns shape:
    - {status: "ok", regime, confidence, drivers, as_of, lineage_id}
    - {status: "missing"|"stale"|"error", reason, as_of?}
    """

    try:
        rows = repository.read_latest_macro_analysis(limit=1)
    except Exception:
        return {"status": "error", "reason": "macro signal read failed"}

    if not rows:
        return {"status": "missing", "reason": "no macro regime analysis yet"}

    row = rows[0]
    if not isinstance(row, Mapping):
        return {"status": "error", "reason": "invalid macro signal payload"}

    as_of = row.get("as_of")
    as_of_dt = _parse_as_of(as_of)
    if as_of_dt is None:
        return {"status": "error", "reason": "macro signal missing valid as_of"}

    if datetime.now(timezone.utc) - as_of_dt > timedelta(days=max(stale_after_days, 1)):
        return {
            "status": "stale",
            "reason": f"macro signal stale (> {max(stale_after_days, 1)}d)",
            "as_of": as_of,
        }

    return {
        "status": "ok",
        "regime": row.get("regime", "neutral"),
        "confidence": row.get("confidence", 0.0),
        "drivers": _normalize_drivers(row.get("reason_codes")),
        "as_of": as_of,
        "lineage_id": row.get("run_id") or row.get("lineage_id"),
    }
