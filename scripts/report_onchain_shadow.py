#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from polymarket_bot.onchain_measurement import coverage_report


def main() -> None:
    parser = argparse.ArgumentParser(description="Report Stage One on-chain/data-API measurement results")
    parser.add_argument("--log", type=Path, default=Path("runs/onchain_shadow/shadow_onchain.jsonl"))
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    report = coverage_report(args.log)
    payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(payload)
    print(payload, end="")


if __name__ == "__main__":
    main()
