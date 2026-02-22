from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from urllib.error import URLError
from urllib.parse import urljoin
import time

import requests


AUTH_WALL_HOST = "share.streamlit.io"
AUTH_WALL_PATH_PREFIX = "/-/auth/app"
STREAMLIT_LOGIN_PATH_PREFIX = "/-/login"


@dataclass(frozen=True)
class AccessCheckResult:
    ok: bool
    status_code: int | None
    final_url: str
    auth_wall_redirect: bool
    reason: str
    redirect_chain: tuple[str, ...] = ()

    @property
    def alert(self) -> bool:
        return not self.ok

    @property
    def alert_severity(self) -> str:
        if self.ok:
            return "none"
        if self.auth_wall_redirect:
            return "critical"
        return "warning"

    @property
    def remediation_hint(self) -> str | None:
        if self.ok:
            return None
        if self.auth_wall_redirect:
            return (
                "auth_wall_detected: verify Streamlit Community Cloud visibility is set to Public "
                "(or document restricted-mode operator login path), then redeploy and rerun streamlit-access-check"
            )
        if self.reason.startswith("network_error:"):
            return "network_error: rerun with retries/backoff and confirm endpoint/network health"
        return "unexpected_response: verify dashboard shell is reachable and Streamlit app is serving expected HTML"

    def to_dict(self) -> dict[str, object]:
        return {
            "ok": self.ok,
            "status_code": self.status_code,
            "final_url": self.final_url,
            "auth_wall_redirect": self.auth_wall_redirect,
            "reason": self.reason,
            "redirect_chain": list(self.redirect_chain),
            "alert": self.alert,
            "alert_severity": self.alert_severity,
            "remediation_hint": self.remediation_hint,
        }


def _default_fetch(url: str, timeout_seconds: float) -> tuple[int | None, str, Mapping[str, str], str, Sequence[str]]:
    try:
        response = requests.get(url, timeout=timeout_seconds, allow_redirects=True)
        redirect_chain = [hop.url for hop in response.history]
        return response.status_code, response.url, dict(response.headers.items()), response.text[:4096], redirect_chain
    except requests.RequestException as exc:
        raise URLError(str(exc)) from exc


def _normalize_fetch_result(
    payload: tuple[int | None, str, Mapping[str, str], str]
    | tuple[int | None, str, Mapping[str, str], str, Sequence[str]],
) -> tuple[int | None, str, Mapping[str, str], str, Sequence[str]]:
    if len(payload) == 4:
        status_code, final_url, headers, body = payload
        return status_code, final_url, headers, body, ()

    status_code, final_url, headers, body, redirect_chain = payload
    return status_code, final_url, headers, body, tuple(redirect_chain)


def _is_auth_wall_url(url: str) -> bool:
    try:
        from urllib.parse import parse_qs, urlparse

        parsed = urlparse(url)
    except ValueError:
        return False

    if parsed.netloc == AUTH_WALL_HOST and parsed.path.startswith(AUTH_WALL_PATH_PREFIX):
        return True

    # Streamlit can bounce through app-local /-/login before redirecting back to auth wall.
    # Treat this as auth wall as well to avoid false "unexpected_response" on redirect loops.
    if parsed.path.startswith(STREAMLIT_LOGIN_PATH_PREFIX):
        query = parse_qs(parsed.query)
        payload_values = query.get("payload")
        if payload_values:
            return True

    return False


def _check_streamlit_access_once(
    url: str,
    *,
    timeout_seconds: float,
    fetch_fn: Callable[
        [str, float],
        tuple[int | None, str, Mapping[str, str], str]
        | tuple[int | None, str, Mapping[str, str], str, Sequence[str]],
    ],
) -> AccessCheckResult:
    try:
        status_code, final_url, headers, body, redirect_chain = _normalize_fetch_result(fetch_fn(url, timeout_seconds))
    except URLError as exc:
        return AccessCheckResult(
            ok=False,
            status_code=None,
            final_url=url,
            auth_wall_redirect=False,
            reason=f"network_error:{exc.reason}",
            redirect_chain=(),
        )

    location = headers.get("Location") or headers.get("location")
    auth_wall_redirect = _is_auth_wall_url(final_url) or any(_is_auth_wall_url(hop_url) for hop_url in redirect_chain)
    if not auth_wall_redirect and isinstance(location, str):
        next_url = urljoin(url, location)
        auth_wall_redirect = _is_auth_wall_url(next_url)

    if auth_wall_redirect:
        return AccessCheckResult(
            ok=False,
            status_code=status_code,
            final_url=final_url,
            auth_wall_redirect=True,
            reason="auth_wall_redirect_detected",
            redirect_chain=tuple(redirect_chain),
        )

    normalized_body = body.lower()
    has_streamlit_shell_hint = "streamlit" in normalized_body or "__next" in normalized_body
    if status_code in {200, 304} and has_streamlit_shell_hint:
        return AccessCheckResult(
            ok=True,
            status_code=status_code,
            final_url=final_url,
            auth_wall_redirect=False,
            reason="ok",
            redirect_chain=tuple(redirect_chain),
        )

    return AccessCheckResult(
        ok=False,
        status_code=status_code,
        final_url=final_url,
        auth_wall_redirect=False,
        reason="unexpected_response",
        redirect_chain=tuple(redirect_chain),
    )


def check_streamlit_access(
    url: str,
    *,
    timeout_seconds: float = 15,
    fetch: Callable[
        [str, float],
        tuple[int | None, str, Mapping[str, str], str]
        | tuple[int | None, str, Mapping[str, str], str, Sequence[str]],
    ]
    | None = None,
    attempts: int = 3,
    backoff_seconds: float = 0.5,
) -> AccessCheckResult:
    fetch_fn = fetch or _default_fetch
    max_attempts = max(1, attempts)

    for attempt in range(1, max_attempts + 1):
        result = _check_streamlit_access_once(url, timeout_seconds=timeout_seconds, fetch_fn=fetch_fn)
        should_retry = result.reason.startswith("network_error:") and attempt < max_attempts
        if not should_retry:
            return result

        sleep_seconds = max(0.0, backoff_seconds) * attempt
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)

    return result
