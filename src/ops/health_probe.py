from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from urllib.parse import urljoin

import requests


@dataclass(frozen=True)
class EndpointCheck:
    name: str
    path: str
    ok: bool
    status_code: int | None
    reason: str


@dataclass(frozen=True)
class HealthProbeResult:
    ok: bool
    checks: tuple[EndpointCheck, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "ok": self.ok,
            "checks": [
                {
                    "name": item.name,
                    "path": item.path,
                    "ok": item.ok,
                    "status_code": item.status_code,
                    "reason": item.reason,
                }
                for item in self.checks
            ],
        }


FetchFn = Callable[[str, float], tuple[int | None, Mapping[str, str], str]]


def _default_fetch(url: str, timeout_seconds: float) -> tuple[int | None, Mapping[str, str], str]:
    response = requests.get(url, timeout=timeout_seconds, allow_redirects=True)
    return response.status_code, dict(response.headers.items()), response.text[:4096]


def _is_shell_fallback(body: str) -> bool:
    normalized = body.lower()
    return "<!doctype html" in normalized and (
        "streamlit" in normalized or "<div id=\"root\"></div>" in normalized or "/-/build/assets/index" in normalized
    )


def run_health_probe(base_url: str, *, timeout_seconds: float = 15, fetch: FetchFn | None = None) -> HealthProbeResult:
    fetch_fn = fetch or _default_fetch

    checks: list[EndpointCheck] = []

    def run(name: str, path: str, *, expect_json: bool, allow_shell_fallback: bool = False) -> None:
        url = urljoin(base_url.rstrip("/") + "/", path.lstrip("/"))
        try:
            status_code, headers, body = fetch_fn(url, timeout_seconds)
        except requests.RequestException:
            checks.append(EndpointCheck(name=name, path=path, ok=False, status_code=None, reason="network_error"))
            return

        lowered_headers = {str(k).lower(): str(v) for k, v in headers.items()}
        content_type = lowered_headers.get("content-type", "").lower()
        shell_fallback = _is_shell_fallback(body)

        if status_code != 200:
            checks.append(EndpointCheck(name=name, path=path, ok=False, status_code=status_code, reason="non_200"))
            return

        if shell_fallback and not allow_shell_fallback:
            checks.append(
                EndpointCheck(
                    name=name,
                    path=path,
                    ok=False,
                    status_code=status_code,
                    reason="shell_fallback_detected",
                )
            )
            return

        if expect_json and "application/json" not in content_type:
            checks.append(
                EndpointCheck(
                    name=name,
                    path=path,
                    ok=False,
                    status_code=status_code,
                    reason="unexpected_content_type",
                )
            )
            return

        checks.append(EndpointCheck(name=name, path=path, ok=True, status_code=status_code, reason="ok"))

    run("liveness", "/healthz", expect_json=True)
    run("readiness", "/readyz", expect_json=True)
    run("manifest", "/manifest.json", expect_json=False)
    run("robots", "/robots.txt", expect_json=False)

    return HealthProbeResult(ok=all(item.ok for item in checks), checks=tuple(checks))
