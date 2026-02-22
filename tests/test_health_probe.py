import importlib


health_probe = importlib.import_module("src.ops.health_probe")


def test_run_health_probe_passes_with_expected_payloads():
    payloads = {
        "https://example.com/healthz": (200, {"content-type": "application/json"}, '{"status":"ok"}'),
        "https://example.com/readyz": (200, {"content-type": "application/json"}, '{"status":"ready"}'),
        "https://example.com/manifest.json": (200, {"content-type": "application/json"}, '{"name":"app"}'),
        "https://example.com/robots.txt": (200, {"content-type": "text/plain"}, "User-agent: *"),
    }

    def fake_fetch(url: str, timeout_seconds: float):
        return payloads[url]

    result = health_probe.run_health_probe("https://example.com", fetch=fake_fetch)

    assert result.ok is True
    assert all(item.ok for item in result.checks)


def test_run_health_probe_flags_shell_fallback_for_ready_and_static_routes():
    shell = "<!doctype html><html><title>Streamlit</title></html>"
    payloads = {
        "https://example.com/healthz": (200, {"content-type": "application/json"}, '{"status":"ok"}'),
        "https://example.com/readyz": (200, {"content-type": "text/html"}, shell),
        "https://example.com/manifest.json": (200, {"content-type": "text/html"}, shell),
        "https://example.com/robots.txt": (200, {"content-type": "text/html"}, shell),
    }

    def fake_fetch(url: str, timeout_seconds: float):
        return payloads[url]

    result = health_probe.run_health_probe("https://example.com", fetch=fake_fetch)

    assert result.ok is False
    reasons = {item.path: item.reason for item in result.checks}
    assert reasons["/readyz"] == "shell_fallback_detected"
    assert reasons["/manifest.json"] == "shell_fallback_detected"
    assert reasons["/robots.txt"] == "shell_fallback_detected"


def test_run_health_probe_flags_readiness_wrong_content_type_even_without_shell_fallback():
    payloads = {
        "https://example.com/healthz": (200, {"content-type": "application/json"}, '{"status":"ok"}'),
        "https://example.com/readyz": (200, {"content-type": "text/plain"}, "ready"),
        "https://example.com/manifest.json": (200, {"content-type": "application/json"}, '{}'),
        "https://example.com/robots.txt": (200, {"content-type": "text/plain"}, "User-agent: *"),
    }

    def fake_fetch(url: str, timeout_seconds: float):
        return payloads[url]

    result = health_probe.run_health_probe("https://example.com", fetch=fake_fetch)

    readiness = next(item for item in result.checks if item.path == "/readyz")
    assert readiness.ok is False
    assert readiness.reason == "unexpected_content_type"
