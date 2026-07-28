"""Paper trading controls — pause / resume / daily loss cap.

Uses a simple kill-switch file the paper follower checks on each cycle.
Located at /root/flip/projects/polymarket-copybot/optsig-paper.disabled
(we reuse the polybot-disabled pattern but with a separate file).

CLI:
  python -m polymarket_bot.paper_control pause
  python -m polymarket_bot.paper_control resume
  python -m polymarket_bot.paper_control status
  python -m polymarket_bot.paper_control daily-cap --max-usd 50
  python -m polymarket_bot.paper_control today   # show today's PnL

The paper follower is also wired to auto-pause if realized_pnl_today
falls below -max_usd (default $50). Set via env var
POLYBOT_DAILY_LOSS_CAP_USD.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, UTC
from pathlib import Path
from typing import Any

logger = logging.getLogger("polymarket_bot.paper_control")

KILL_SWITCH = Path("/root/flip/projects/polymarket-copybot/optsig-paper.disabled")
DAILY_CAP_FILE = Path("/root/flip/projects/polymarket-copybot/runs/paper/.daily_cap")


def _get_json(url: str, timeout: float = 5.0) -> dict[str, Any] | None:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except (urllib.error.URLError, OSError, json.JSONDecodeError):
        return None


def cmd_pause(_args, no_color: bool = False) -> int:
    KILL_SWITCH.touch()
    print(f"⏸️  Paused paper trading. Kill switch: {KILL_SWITCH}")
    print("  Resume with: python -m polymarket_bot.paper_control resume")
    return 0


def cmd_resume(_args, no_color: bool = False) -> int:
    if KILL_SWITCH.exists():
        KILL_SWITCH.unlink()
        print(f"▶️  Resumed paper trading. Kill switch removed.")
    else:
        print("Already running (no kill switch).")
    return 0


def cmd_status(_args, no_color: bool = False) -> int:
    paper = _get_json("http://localhost:8710/api/paper") or {}
    state = "PAUSED ⏸️" if KILL_SWITCH.exists() else "ACTIVE ▶️"
    print(f"Paper trading: {state}")
    if KILL_SWITCH.exists():
        print(f"  Kill switch: {KILL_SWITCH}")
    if DAILY_CAP_FILE.exists():
        try:
            cap_data = json.loads(DAILY_CAP_FILE.read_text())
            print(f"  Daily cap: ${cap_data.get('max_usd', '?')}")
        except Exception:
            pass
    pnl_today = paper.get("realized_pnl_today", 0)
    print(f"  PnL today:    ${pnl_today:+,.2f}")
    print(f"  Account:      ${paper.get('account_value', 0):,.2f}")
    return 0


def cmd_daily_cap(args, no_color: bool = False) -> int:
    cap_usd = float(args.max_usd)
    DAILY_CAP_FILE.parent.mkdir(parents=True, exist_ok=True)
    DAILY_CAP_FILE.write_text(json.dumps({
        "max_usd": cap_usd,
        "set_at": datetime.now(UTC).isoformat(timespec="seconds"),
    }))
    print(f"Daily loss cap set to ${cap_usd:.2f}. Bot will auto-pause if realized_pnl_today < -${cap_usd:.2f}")
    return 0


def cmd_today(_args, no_color: bool = False) -> int:
    paper = _get_json("http://localhost:8710/api/paper") or {}
    pnl_today = paper.get("realized_pnl_today", 0)
    sigs = paper.get("signals_today", 0)
    accepts = paper.get("accepts_today", 0)
    print(f"Today ({datetime.now(UTC).strftime('%Y-%m-%d %H:%M UTC')}):")
    print(f"  PnL:         ${pnl_today:+,.2f}")
    print(f"  Signals:     {sigs}")
    print(f"  Accepts:     {accepts}")
    rate = (accepts / sigs * 100) if sigs else 0
    print(f"  Accept rate: {rate:.1f}%")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Paper trading controls")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("pause", help="Pause paper trading (creates kill switch)")
    sub.add_parser("resume", help="Resume paper trading (removes kill switch)")
    sub.add_parser("status", help="Show current state")
    sub.add_parser("today", help="Show today's PnL summary")
    cap_p = sub.add_parser("daily-cap", help="Set daily loss cap (auto-pause)")
    cap_p.add_argument("--max-usd", type=float, required=True,
                       help="Auto-pause if realized_pnl_today < -max_usd")
    args = parser.parse_args()
    logging.basicConfig(level="INFO", format="%(asctime)s %(message)s")
    cmds = {
        "pause": cmd_pause,
        "resume": cmd_resume,
        "status": cmd_status,
        "today": cmd_today,
        "daily-cap": cmd_daily_cap,
    }
    return cmds[args.cmd](args)


if __name__ == "__main__":
    sys.exit(main())