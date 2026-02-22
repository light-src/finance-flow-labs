#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.ops.health_probe import run_health_probe


def main() -> int:
    parser = argparse.ArgumentParser(description="Probe deployed Streamlit operational endpoints")
    parser.add_argument("--url", default="https://finance-flow-labs.streamlit.app/", help="Base URL")
    parser.add_argument("--timeout", type=float, default=15.0, help="HTTP timeout seconds")
    args = parser.parse_args()

    result = run_health_probe(args.url, timeout_seconds=args.timeout)
    print(json.dumps(result.to_dict(), ensure_ascii=False, indent=2))
    return 0 if result.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
