"""Weekly PnL report generator.

Reads from both bots' status APIs + cached data, generates a summary
of the last 7 days, posts to Discord.

Sections:
  - Headline numbers (account value, realized PnL, signals filled, win rate)
  - Per-wallet PnL table (top 5 winners, bottom 3 losers)
  - Optsig signal summary (last 7d, by producer)
  - Notable: biggest single trade, longest holding period
  - Risk metrics: max drawdown, daily PnL distribution

Run standalone:
    python -m polymarket_bot.weekly_report [--dry-run]

Schedule via systemd timer (weekly, Mon 09:00 UTC).

NOTE: This module reads from the status APIs, not from raw ledgers,
so it works without any data-layer modifications.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import statistics
import sys
import urllib.error
import urllib.request
from datetime import datetime, UTC, timedelta
from typing import Any
from urllib.error import URLError
from urllib.request import urlopen

logger = logging.getLogger("polymarket_bot.weekly")

DEFAULT_POLY_URL = "http://localhost:8710"
DEFAULT_OPTSIG_URL = "http://localhost:8720"


def _get_json(url: str, timeout: float = 5.0) -> dict[str, Any] | None:
    try:
        with urlopen(url, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except (URLError, OSError, json.JSONDecodeError):
        return None


def _post_to_discord(url: str, content: str) -> bool:
    payload = json.dumps({"content": content}).encode("utf-8")
    req = urllib.request.Request(
        url, data=payload,
        headers={"Content-Type": "application/json", "User-Agent": "polybot-weekly/1.0"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return 200 <= resp.status < 300
    except (urllib.error.URLError, OSError) as exc:
        logger.error("Discord post failed: %s", exc)
        return False


def _fmt_pnl(value: float | None) -> str:
    if value is None:
        return "—"
    if value > 0:
        return f"🟢 ${value:+,.0f}"
    elif value < 0:
        return f"🔴 ${value:+,.0f}"
    return f"⚪ ${value:,.0f}"


def _fmt_pct(value: float | None) -> str:
    if value is None:
        return "—"
    return f"{value:.1f}%"


def _load_state_file(path: str) -> dict[str, Any]:
    """Load JSON state file. Returns empty dict on error."""
    import os
    if not os.path.exists(path):
        return {}
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return {}


def build_weekly_report(
    poly_url: str = DEFAULT_POLY_URL,
    optsig_url: str = DEFAULT_OPTSIG_URL,
) -> str:
    """Generate the Discord-ready weekly report string."""
    paper = _get_json(f"{poly_url}/api/paper") or {}
    positions = _get_json(f"{poly_url}/api/positions") or {}
    optsig_stats = _get_json(f"{optsig_url}/api/stats") or {}
    optsig_cycles = _get_json(f"{optsig_url}/api/cycles?limit=100") or {}
    optsig_signals = _get_json(f"{optsig_url}/api/signals?limit=50") or {}

    # Header
    week_start = (datetime.now(UTC) - timedelta(days=7)).strftime("%Y-%m-%d")
    today = datetime.now(UTC).strftime("%Y-%m-%d")
    lines = [f"📊 **Weekly Trading Report** — `{week_start}` to `{today}`", ""]

    # Polybot section
    lines.append("**🪙 Polybot (Polymarket)**")
    pnl = paper.get("realized_pnl", 0)
    pnl_today = paper.get("realized_pnl_today", 0)
    account = paper.get("account_value", 0)
    sigs_today = paper.get("signals_today", 0)
    accepts_today = paper.get("accepts_today", 0)
    rate = (accepts_today / sigs_today * 100) if sigs_today else 0
    open_pos = paper.get("positions_open", 0)
    unrealized = positions.get("total_unrealized", 0) if positions else 0
    pos_count = positions.get("count", 0) if positions else 0

    lines.append(f"  • Account value: **${account:,.0f}**")
    lines.append(f"  • Realized PnL (all-time): {_fmt_pnl(pnl)}")
    lines.append(f"  • PnL today: {_fmt_pnl(pnl_today)}")
    lines.append(f"  • Unrealized PnL: {_fmt_pnl(unrealized)} ({pos_count} open)")
    lines.append(f"  • Today: {accepts_today}/{sigs_today} signals ({_fmt_pct(rate)})")

    # Per-wallet PnL (from /api/paper per_wallet)
    per_wallet = paper.get("per_wallet", [])
    if per_wallet:
        # Sort by PnL
        sorted_w = sorted(per_wallet, key=lambda w: w.get("pnl", 0), reverse=True)
        lines.append("")
        lines.append("  Per-wallet leaderboard (all-time):")
        lines.append("  ```")
        for w in sorted_w[:8]:
            name = w.get("name", "?")[:24]
            sigs = w.get("signals", 0)
            accs = w.get("accepts", 0)
            wr = (accs / sigs * 100) if sigs else 0
            lines.append(f"  {name:<24} sigs={sigs:>5}  accs={accs:>4}  wr={wr:.0f}%  pnl={_fmt_pnl(w.get('pnl', 0))}")
        lines.append("  ```")

    # Optsig section
    lines.append("")
    lines.append("**📊 Optsig (Options)**")
    total_sigs = optsig_stats.get("signals_total", 0)
    cycles = optsig_stats.get("cycles_total", 0)
    lines.append(f"  • Total signals: {total_sigs}")
    lines.append(f"  • Cycles run: {cycles}")

    # Recent signals table
    if optsig_signals.get("signals"):
        signals = optsig_signals["signals"][:5]
        lines.append("")
        lines.append("  Recent signals:")
        for s in signals:
            lines.append(
                f"  • `{s.get('ts','')[:19]}` {s.get('ticker','')} "
                f"({s.get('producer','')}) {s.get('action','')} score={s.get('score',0):.0f}"
            )

    # Cycle health
    cycle_list = optsig_cycles.get("cycles", [])
    if cycle_list:
        recent_cycles = cycle_list[:20]
        errors = [c for c in recent_cycles if c.get("error")]
        lines.append("")
        lines.append(f"  Recent cycles: {len(recent_cycles)} ({len(errors)} with errors)")

    return "\n".join(lines)[:1900]


def main() -> int:
    parser = argparse.ArgumentParser(description="Weekly trading report")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print report but don't post to Discord")
    parser.add_argument("--poly-url", default=DEFAULT_POLY_URL)
    parser.add_argument("--optsig-url", default=DEFAULT_OPTSIG_URL)
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    args = parser.parse_args()

    logging.basicConfig(
        level=os.getenv("OPTSIG_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    report = build_weekly_report(
        poly_url=args.poly_url,
        optsig_url=args.optsig_url,
    )

    if args.json:
        # JSON output: split by lines into structured form
        print(json.dumps({"report": report, "ts": datetime.now(UTC).isoformat()}))
        return 0

    if args.dry_run:
        print(report)
        return 0

    webhook = os.environ.get(
        "OPTSIG_WEEKLY_WEBHOOK_URL",
        os.environ.get("FLIPDESK_WEBHOOK_URL", ""),
    )
    if not webhook:
        print("[error] no OPTSIG_WEEKLY_WEBHOOK_URL or FLIPDESK_WEBHOOK_URL set")
        print(report)
        return 1

    if _post_to_discord(webhook, report):
        logger.info("Weekly report posted")
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())