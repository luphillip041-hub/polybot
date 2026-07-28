"""CLI: terminal browser for polybot + optsig status.

Subcommands:
  polybot status   - Quick account/PnL summary
  polybot positions - List open positions with mark
  polybot rejects  - Top reject reasons today
  polybot signals  - Recent accept/reject breakdown
  optsig status    - Cycle count + last cycle
  optsig signals   - Recent signal list
  optsig cycles    - Recent cycle heartbeats
  optsig universe  - Current top-N most-active tickers
  watch            - Live refresh dashboard (Ctrl-C to exit)

Connects to the local status APIs. No auth needed (localhost only).

Usage:
    python -m polymarket_bot.cli polybot status
    python -m polymarket_bot.cli polybot positions
    python -m polymarket_bot.cli optsig signals --limit 5
    python -m polymarket_bot.cli watch
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, UTC
from typing import Any
from urllib.error import URLError
from urllib.request import urlopen

DEFAULT_POLY_URL = "http://localhost:8710"
DEFAULT_OPTSIG_URL = "http://localhost:8720"

# ANSI colors
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
BLUE = "\033[94m"
GREY = "\033[90m"
BOLD = "\033[1m"
RESET = "\033[0m"


def _get_json(url: str, timeout: float = 5.0) -> dict[str, Any] | None:
    try:
        with urlopen(url, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except (URLError, OSError, json.JSONDecodeError):
        return None


def _color(text: str, color: str, no_color: bool = False) -> str:
    if no_color or not sys.stdout.isatty():
        return text
    return f"{color}{text}{RESET}"


def _format_pnl(value: float, no_color: bool = False) -> str:
    if value is None:
        return "—"
    s = f"${value:+,.0f}"
    if value > 0:
        return _color(s, GREEN, no_color)
    elif value < 0:
        return _color(s, RED, no_color)
    return s


def _format_pct(value: float, no_color: bool = False) -> str:
    if value is None:
        return "—"
    s = f"{value:.1f}%"
    if value >= 50:
        return _color(s, GREEN, no_color)
    elif value < 25:
        return _color(s, RED, no_color)
    return _color(s, YELLOW, no_color)


# ── polybot subcommands ────────────────────────────────────────────────────


def cmd_polybot_status(args, no_color: bool = False) -> int:
    paper = _get_json(f"{args.poly_url}/api/paper")
    if paper is None:
        print(f"{_color('Error: cannot reach polybot status API', RED, no_color)}")
        return 1
    pnl = paper.get("realized_pnl", 0)
    pnl_today = paper.get("realized_pnl_today", 0)
    account = paper.get("account_value", 0)
    sigs = paper.get("signals_today", 0)
    accepts = paper.get("accepts_today", 0)
    rate = (accepts / sigs * 100) if sigs else 0
    open_pos = paper.get("positions_open", 0)
    coverage = paper.get("signal_coverage_pct", 0)
    latency = paper.get("avg_detection_latency_s", 0)
    pnl_p50 = paper.get("detection_latency_p50", 0)
    pnl_p90 = paper.get("detection_latency_p90", 0)

    print(f"{_color('🪙 Polybot Status', BOLD, no_color)}")
    print(f"  Account value:    {_color(f'${account:,.0f}', BLUE, no_color)}")
    print(f"  Realized PnL:     {_format_pnl(pnl, no_color)}")
    print(f"  PnL today:       {_format_pnl(pnl_today, no_color)}")
    print(f"  Open positions:  {open_pos}")
    print(f"  Signals today:   {sigs}")
    print(f"  Accepts today:   {accepts} ({_format_pct(rate, no_color)})")
    print(f"  BBO coverage:    {coverage}%")
    print(f"  Latency avg:     {latency:.0f}s (p50={pnl_p50:.0f}s, p90={pnl_p90:.0f}s)")
    return 0


def cmd_polybot_positions(args, no_color: bool = False) -> int:
    payload = _get_json(f"{args.poly_url}/api/positions")
    if payload is None or payload.get("count", 0) == 0:
        print(f"{_color('No open positions', GREY, no_color)}")
        return 0
    pos_count = payload.get("count", 0)
    print(f"{_color(f'📊 Open Positions ({pos_count})', BOLD, no_color)}")
    print(f"  Total cost:      ${payload.get('total_cost', 0):.2f}")
    print(f"  Total unrealized: {_format_pnl(payload.get('total_unrealized', 0), no_color)}")
    print()
    print(f"  {'WALLET':<14} {'COST':>8} {'CUR':>8} {'UPNL':>10} {'%':>7}  {'TOKEN':<24}")
    for p in payload.get("positions", []):
        wallet = p.get("wallet", "")[:12] + "…"
        cost = p.get("cost_usd", 0)
        cur = p.get("current_price", 0)
        upnl = p.get("unrealized_pnl", 0)
        pct = p.get("unrealized_pct", 0)
        token = p.get("token", "")[:22] + "…"
        upnl_str = _format_pnl(upnl, no_color)
        print(f"  {wallet:<14} ${cost:>7.2f} {cur:>8.3f} {upnl_str:>10} {pct:>6.1f}%  {token:<24}")
    return 0


def cmd_polybot_rejects(args, no_color: bool = False) -> int:
    paper = _get_json(f"{args.poly_url}/api/paper")
    if paper is None:
        print(_color("Error: cannot reach polybot status API", RED, no_color))
        return 1
    reasons = paper.get("rejects_by_reason", {})
    if not reasons:
        print(_color("No rejects today", GREY, no_color))
        return 0
    print(_color(f"❌ Reject Reasons Today (total: {paper.get('rejects_today', 0)})", BOLD, no_color))
    total = sum(reasons.values())
    for reason, count in sorted(reasons.items(), key=lambda x: -x[1]):
        pct = count / total * 100 if total else 0
        print(f"  {reason:<30} {count:>5}  ({pct:.0f}%)")
    return 0


def cmd_polybot_signals(args, no_color: bool = False) -> int:
    paper = _get_json(f"{args.poly_url}/api/paper")
    if paper is None:
        print(_color("Error: cannot reach polybot status API", RED, no_color))
        return 1
    sigs = paper.get("signals_today", 0)
    accepts = paper.get("accepts_today", 0)
    rate = (accepts / sigs * 100) if sigs else 0
    print(_color(f"📡 Signal Stats Today", BOLD, no_color))
    print(f"  Total signals:   {sigs}")
    print(f"  Accepts:         {accepts} ({_format_pct(rate, no_color)})")
    print(f"  Rejects:         {paper.get('rejects_today', 0)}")
    by_lat = paper.get("accepts_by_latency", {})
    if by_lat:
        print(f"  By latency:")
        for k, v in by_lat.items():
            print(f"    {k:<12} {v}")
    # Per-wallet
    per_wallet = paper.get("per_wallet", [])
    if per_wallet:
        print(f"\n  Per-wallet (today):")
        print(f"  {'WALLET':<24} {'SIGS':>5} {'ACCEPTS':>8} {'PNL':>10}")
        for w in per_wallet:
            name = w.get("name", "")[:22]
            print(f"  {name:<24} {w.get('signals', 0):>5} {w.get('accepts', 0):>8} {_format_pnl(w.get('pnl', 0), no_color):>10}")
    return 0


# ── optsig subcommands ─────────────────────────────────────────────────────


def cmd_optsig_status(args, no_color: bool = False) -> int:
    stats = _get_json(f"{args.optsig_url}/api/stats")
    if stats is None:
        print(_color("Error: cannot reach optsig status API", RED, no_color))
        return 1
    last = stats.get("last_heartbeat") or {}
    print(_color("📊 Optsig Status", BOLD, no_color))
    print(f"  Signals total:   {stats.get('signals_total', 0)}")
    print(f"  Open signals:    {stats.get('open_signals', 0)}")
    print(f"  Cycles run:      {stats.get('cycles_total', 0)}")
    if last:
        print(f"  Last cycle:      {last.get('cycle_ts', '?')}")
        print(f"  Last status:     {last.get('status', '?')}")
        print(f"  Last emitted:    {last.get('signals_emitted', 0)} signals")
        if last.get("error"):
            err = str(last.get("error"))[:100]
            print(f"  Last error:      {_color(err, RED, no_color)}")
    return 0


def cmd_optsig_signals(args, no_color: bool = False) -> int:
    payload = _get_json(f"{args.optsig_url}/api/signals?limit={args.limit}")
    if payload is None:
        print(_color("Error: cannot reach optsig status API", RED, no_color))
        return 1
    signals = payload.get("signals", [])
    if not signals:
        print(_color("No signals yet", GREY, no_color))
        return 0
    print(_color(f"🎯 Recent Signals (n={len(signals)})", BOLD, no_color))
    print(f"  {'TS':<19} {'TICKER':<6} {'PRODUCER':<14} {'ACTION':<5} {'SCORE':>5} {'CREDIT':>7}")
    for s in signals:
        ts = (s.get("ts") or "")[:19]
        ticker = s.get("ticker", "")
        producer = s.get("producer", "")
        action = s.get("action", "")
        score = s.get("score", 0)
        credit = s.get("est_credit", 0) or 0
        print(f"  {ts:<19} {ticker:<6} {producer:<14} {action:<5} {score:>5.0f} ${credit:>6.2f}")
    return 0


def cmd_optsig_cycles(args, no_color: bool = False) -> int:
    payload = _get_json(f"{args.optsig_url}/api/cycles?limit={args.limit}")
    if payload is None:
        print(_color("Error: cannot reach optsig status API", RED, no_color))
        return 1
    cycles = payload.get("cycles", [])
    if not cycles:
        print(_color("No cycles yet", GREY, no_color))
        return 0
    print(_color(f"🔄 Recent Cycles (n={len(cycles)})", BOLD, no_color))
    for c in cycles:
        ts = (c.get("cycle_ts") or "")[:19]
        status = c.get("status", "?")
        sigs = c.get("signals_emitted", 0)
        err = c.get("error", "")
        line = f"  {ts}  status={status}  signals={sigs}"
        if err:
            line += _color(f"  ERROR: {str(err)[:60]}", RED, no_color)
        print(line)
    return 0


def cmd_optsig_universe(args, no_color: bool = False) -> int:
    payload = _get_json(f"{args.optsig_url}/api/universe")
    if payload is None:
        print(_color("Error: cannot reach optsig status API", RED, no_color))
        return 1
    members = payload.get("members", [])
    if not members:
        print(_color("Universe empty", GREY, no_color))
        return 0
    print(_color(f"🌐 Universe ({len(members)} tickers, built {payload.get('built_at', '?')})", BOLD, no_color))
    print(f"  {'TICKER':<6} {'TRADES':>12} {'VOL':>16} {'RV20':>8}")
    for m in members[:20]:
        sym = m.get("symbol", "")
        trades = m.get("trade_count", 0)
        vol = m.get("volume", 0)
        rv = m.get("rv20")
        rv_str = f"{rv:.1%}" if rv is not None else "—"
        vol_str = f"${vol:>15,d}" if vol else "—"
        print(f"  {sym:<6} {trades:>12,d} {vol_str} {rv_str:>8}")
    return 0


# ── watch command ──────────────────────────────────────────────────────────


def cmd_watch(args, no_color: bool = False) -> int:
    """Live refresh dashboard every N seconds."""
    print(_color(f"👁  Watching (refresh every {args.interval}s, Ctrl-C to exit)...", BOLD, no_color))
    print()
    while True:
        try:
            # Clear screen
            sys.stdout.write("\033[2J\033[H")
            sys.stdout.flush()

            ts = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
            print(_color(f"=== {ts} ===", BOLD, no_color))
            print()
            cmd_polybot_status(args, no_color)
            print()
            cmd_optsig_status(args, no_color)
            print()
            poly_pos = _get_json(f"{args.poly_url}/api/positions")
            if poly_pos and poly_pos.get("count", 0) > 0:
                cmd_polybot_positions(args, no_color)
            time.sleep(args.interval)
        except KeyboardInterrupt:
            print()
            print(_color("👋 Stopped", GREY, no_color))
            return 0


# ── main ────────────────────────────────────────────────────────────────────


def main() -> int:
    parser = argparse.ArgumentParser(description="Polybot + Optsig terminal browser")
    parser.add_argument("--poly-url", default=DEFAULT_POLY_URL, help="Polybot status URL")
    parser.add_argument("--optsig-url", default=DEFAULT_OPTSIG_URL, help="Optsig status URL")
    parser.add_argument("--no-color", action="store_true", help="Disable ANSI colors")
    sub = parser.add_subparsers(dest="bot", required=True, help="Bot to query")

    # polybot subcommands
    poly = sub.add_parser("polybot", help="Polybot commands")
    poly_sub = poly.add_subparsers(dest="cmd", required=True)
    poly_sub.add_parser("status", help="Quick account/PnL summary")
    poly_sub.add_parser("positions", help="List open positions with mark")
    poly_sub.add_parser("rejects", help="Top reject reasons today")
    poly_sub.add_parser("signals", help="Recent accept/reject breakdown")

    # optsig subcommands
    optsig = sub.add_parser("optsig", help="Optsig commands")
    optsig_sub = optsig.add_subparsers(dest="cmd", required=True)
    optsig_sub.add_parser("status", help="Cycle count + last cycle")
    sig_p = optsig_sub.add_parser("signals", help="Recent signal list")
    sig_p.add_argument("--limit", type=int, default=10)
    cyc_p = optsig_sub.add_parser("cycles", help="Recent cycle heartbeats")
    cyc_p.add_argument("--limit", type=int, default=5)
    optsig_sub.add_parser("universe", help="Current top-N most-active tickers")

    # watch
    watch_p = sub.add_parser("watch", help="Live refresh dashboard")
    watch_p.add_argument("--interval", type=int, default=10, help="Refresh interval (s)")

    args = parser.parse_args()
    no_color = args.no_color or not sys.stdout.isatty()

    if args.bot == "watch":
        return cmd_watch(args, no_color)

    cmd_map = {
        ("polybot", "status"): cmd_polybot_status,
        ("polybot", "positions"): cmd_polybot_positions,
        ("polybot", "rejects"): cmd_polybot_rejects,
        ("polybot", "signals"): cmd_polybot_signals,
        ("optsig", "status"): cmd_optsig_status,
        ("optsig", "signals"): cmd_optsig_signals,
        ("optsig", "cycles"): cmd_optsig_cycles,
        ("optsig", "universe"): cmd_optsig_universe,
    }
    fn = cmd_map.get((args.bot, args.cmd))
    if fn is None:
        print(f"Unknown command: {args.bot} {args.cmd}")
        return 1
    return fn(args, no_color)


if __name__ == "__main__":
    sys.exit(main())