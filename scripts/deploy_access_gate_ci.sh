#!/usr/bin/env bash
set -euo pipefail

URL="${1:-https://finance-flow-labs.streamlit.app/}"
MODE="${DEPLOY_ACCESS_MODE:-public}"
LOGIN_PATH="${DEPLOY_RESTRICTED_LOGIN_PATH:-}"

TMP_JSON="$(mktemp)"
cleanup() {
  rm -f "$TMP_JSON"
}
trap cleanup EXIT

python3 - "$URL" "$MODE" "$LOGIN_PATH" "$TMP_JSON" <<'PY'
import json
import subprocess
import sys
from pathlib import Path

url = sys.argv[1]
mode = sys.argv[2]
login_path = sys.argv[3]
out_path = Path(sys.argv[4])

routes = [
    ("default", url),
    ("enduser", f"{url}?view=enduser"),
    ("operator", f"{url}?view=operator"),
]

results: list[dict[str, object]] = []
release_blocker = False
first_blocker: dict[str, object] | None = None


def _parse_payload(raw: str) -> dict[str, object] | None:
    text = raw.strip()
    if not text:
        return None
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return None
    if not isinstance(parsed, dict):
        return None
    return parsed


for label, route_url in routes:
    cmd = [
        "python3",
        "-m",
        "src.ingestion.cli",
        "deploy-access-gate",
        "--url",
        route_url,
        "--mode",
        mode,
    ]
    if login_path:
        cmd += ["--restricted-login-path", login_path]

    proc = subprocess.run(cmd, check=False, capture_output=True, text=True)

    payload = _parse_payload(proc.stdout)
    if payload is None:
        fallback_reason = f"deploy_access_gate_command_failed:{proc.returncode}"
        payload = {
            "url": route_url,
            "deploy_access_mode": mode,
            "restricted_login_path": login_path or None,
            "access_check": {
                "ok": False,
                "status_code": None,
                "final_url": route_url,
                "auth_wall_redirect": False,
                "reason": fallback_reason,
                "alert": True,
                "alert_severity": "critical",
                "remediation_hint": (
                    "deploy-access-gate returned non-JSON output; inspect stderr and "
                    "CLI/runtime environment."
                ),
            },
            "gate": {
                "ok": False,
                "mode": mode,
                "release_blocker": True,
                "reason": fallback_reason,
                "severity": "critical",
                "operator_message": proc.stderr.strip()
                or "deploy-access-gate command failed without structured output.",
            },
        }

    access = payload.get("access_check", {})
    gate = payload.get("gate", {})
    reason = str(gate.get("reason") or access.get("reason") or "unknown")
    severity = str(gate.get("severity") or access.get("alert_severity") or "unknown")
    hint = str(gate.get("remediation_hint") or access.get("remediation_hint") or "")
    operator_message = str(gate.get("operator_message") or "")

    is_blocker = bool(gate.get("release_blocker"))
    if proc.returncode not in {0, 2}:
        is_blocker = True
        if reason == "unknown":
            reason = f"deploy_access_gate_exit_{proc.returncode}"
        if severity == "unknown":
            severity = "critical"
        if not operator_message:
            operator_message = (
                proc.stderr.strip() or f"deploy-access-gate exited with code {proc.returncode}."
            )

    release_blocker = release_blocker or is_blocker
    if is_blocker and first_blocker is None:
        first_blocker = {
            "route": label,
            "reason": reason,
            "severity": severity,
            "operator_message": operator_message,
            "hint": hint,
            "cli_exit_code": proc.returncode,
        }

    print(
        f"[deploy-access-gate:{label}] mode={mode} exit_code={proc.returncode} "
        f"release_blocker={is_blocker} reason={reason} severity={severity}"
    )
    if operator_message:
        print(f"[deploy-access-gate:{label}] operator_message={operator_message}")
    if hint:
        print(f"[deploy-access-gate:{label}] hint={hint}")

    results.append(
        {
            "route": label,
            "url": route_url,
            "cli_exit_code": proc.returncode,
            "release_blocker": is_blocker,
            "reason": reason,
            "severity": severity,
            "operator_message": operator_message,
            "hint": hint,
        }
    )

summary_lines = ["## Deploy Access Gate", f"- mode: `{mode}`", "- routes:"]
for item in results:
    summary_lines.append(
        "  - "
        + f"{item['route']}: exit_code=`{item['cli_exit_code']}` blocker=`{str(item['release_blocker']).lower()}` "
        + f"reason=`{item['reason']}` severity=`{item['severity']}`"
    )
    if item["operator_message"]:
        summary_lines.append(f"    - operator_message: {item['operator_message']}")
    if item["hint"]:
        summary_lines.append(f"    - hint: {item['hint']}")

if first_blocker:
    summary_lines += [
        "- first_blocker:",
        f"  - route: `{first_blocker['route']}`",
        f"  - reason: `{first_blocker['reason']}`",
        f"  - severity: `{first_blocker['severity']}`",
        f"  - cli_exit_code: `{first_blocker['cli_exit_code']}`",
    ]
    if first_blocker["operator_message"]:
        summary_lines.append(f"  - operator_message: {first_blocker['operator_message']}")
    if first_blocker["hint"]:
        summary_lines.append(f"  - hint: {first_blocker['hint']}")

Path("deploy_access_gate_summary.md").write_text("\n".join(summary_lines) + "\n", encoding="utf-8")
out_path.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")

if release_blocker:
    raise SystemExit(2)
PY

cat "$TMP_JSON"
