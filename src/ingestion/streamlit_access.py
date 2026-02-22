from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin
from urllib.request import HTTPRedirectHandler, Request, build_opener


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

    def to_dict(self) -> dict[str, object]:
        return {
            "ok": self.ok,
            "status_code": self.status_code,
            "final_url": self.final_url,
            "auth_wall_redirect": self.auth_wall_redirect,
            "reason": self.reason,
        }


class _NoRedirectHandler(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # type: ignore[override]
        return None


def _default_fetch(url: str, timeout_seconds: float) -> tuple[int | None, str, Mapping[str, str], str]:
    request = Request(url=url, method="GET")
    opener = build_opener(_NoRedirectHandler())
    try:
        with opener.open(request, timeout=timeout_seconds) as response:
            status_code = getattr(response, "status", None)
            final_url = response.geturl()
            headers = dict(response.headers.items())
            body = response.read(4096).decode("utf-8", errors="replace")
            return status_code, final_url, headers, body
    except HTTPError as exc:
        body = exc.read(4096).decode("utf-8", errors="replace")
        return exc.code, exc.geturl() or url, dict(exc.headers.items()), body


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


def check_streamlit_access(
    url: str,
    *,
    timeout_seconds: float = 15,
    fetch: Callable[[str, float], tuple[int | None, str, Mapping[str, str], str]] | None = None,
) -> AccessCheckResult:
    fetch_fn = fetch or _default_fetch

    try:
        status_code, final_url, headers, body = fetch_fn(url, timeout_seconds)
    except URLError as exc:
        return AccessCheckResult(
            ok=False,
            status_code=None,
            final_url=url,
            auth_wall_redirect=False,
            reason=f"network_error:{exc.reason}",
        )

    location = headers.get("Location") or headers.get("location")
    auth_wall_redirect = _is_auth_wall_url(final_url)
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
        )

    return AccessCheckResult(
        ok=False,
        status_code=status_code,
        final_url=final_url,
        auth_wall_redirect=False,
        reason="unexpected_response",
    )
