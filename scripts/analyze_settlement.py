#!/usr/bin/env python3
"""Settlement analysis: concentration, depth-adjusted fills, wallet breakdown.

Reads runs/paper/ledger.jsonl + runs/paper/state.json, joins entry rows
with resolution rows by token ID, and produces:

1. Per-wallet PnL distribution ex-outliers
2. Depth-adjusted fill simulation (using BBO at detection)
3. FerrariChampions2026 chronological breakdown

Usage:
    python scripts/analyze_settlement.py

Output:
    prints tables + writes runs/paper/analysis_settlement_1.json
"""

from __future__ import annotations

import json
import sys
from collections import defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
LEDGER = HERE.parent / "runs" / "paper" / "ledger.jsonl"
OUTPUT = HERE.parent / "runs" / "paper" / "analysis_settlement_1.json"

# Wallet address -> display name mapping
WALLET_NAMES = {
    "0xfe787d2da716d60e8acff57fb87eb13cd4d10319": "ferrariChampions2026",
    "0x224a89dbe0db0d6124b335edabd15b3f877da3d5": "wr0ngw4yb3tt0r",
    "0x4be1fa92e6ceaf886aac0bbec3be6c527133aa70": "Eztennis",
    "0xee00ba338c59557141789b127927a55f5cc5cea1": "S-Works",
    "0x4bff30af91642dc7d2b19a8664378fe55c45fc26": "Sassy-Bucket",
}

def addr_name(addr: str) -> str:
    return WALLET_NAMES.get(addr, addr[:10])


def load_ledger() -> dict:
    """Load ledger, return {token: entry_row} and {token: resolution_row}."""
    entries: dict[str, dict] = {}
    resolutions: dict[str, dict] = {}
    signals: dict[str, dict] = {}

    if not LEDGER.exists():
        print(f"ERROR: ledger not found at {LEDGER}")
        sys.exit(1)

    with open(LEDGER) as f:
        for line in f:
            row = json.loads(line)
            t = row.get("type")
            token = row.get("token", "")
            if t == "entry":
                entries[token] = row
            elif t == "resolution":
                resolutions[token] = row
            elif t == "signal":
                if token not in signals:
                    signals[token] = row

    print(f"Loaded: {len(entries)} entries, {len(resolutions)} resolutions, {len(signals)} signals")
    return {"entries": entries, "resolutions": resolutions, "signals": signals}


def price_band(price: float) -> str:
    if price < 0.10:
        return "<10¢"
    elif price < 0.30:
        return "10-30¢"
    elif price < 0.70:
        return "30-70¢"
    else:
        return ">70¢"


def part1_per_wallet_pnl(entries: dict, resolutions: dict) -> dict:
    """Per-wallet PnL breakdown with outlier removal."""
    wallet_trades: dict[str, list[dict]] = defaultdict(list)

    for token, entry in entries.items():
        res = resolutions.get(token)
        if not res:
            continue
        wallet = entry.get("wallet", "unknown")
        entry_price = entry.get("sim_fill_price", 0) or entry.get("wallet_fill_price", 0)
        pnl = res.get("pnl", 0)
        side = entry.get("side", "?")
        won = pnl > 0
        wallet_trades[wallet].append({
            "token": token,
            "entry_price": entry_price,
            "sim_size": entry.get("sim_size", 0),
            "pnl": pnl,
            "won": won,
            "side": side,
            "ts": entry.get("ts", ""),
            "band": price_band(entry_price),
        })

    rows = []
    for wallet_addr, trades in sorted(wallet_trades.items(), key=lambda x: -sum(t["pnl"] for t in x[1])):
        name = addr_name(wallet_addr)
        n = len(trades)
        pnls = [t["pnl"] for t in trades]
        total_pnl = sum(pnls)
        wins = sum(1 for t in trades if t["won"])
        win_rate = wins / n if n else 0

        # Top-1 and top-3 removal
        sorted_pnls = sorted(pnls, reverse=True)
        ex_top1 = sum(sorted_pnls[1:]) if len(sorted_pnls) > 1 else 0
        ex_top3 = sum(sorted_pnls[3:]) if len(sorted_pnls) > 3 else 0

        # Median PnL
        sorted_pnls_asc = sorted(pnls)
        med = sorted_pnls_asc[n // 2] if n else 0

        # Histogram by price band
        band_pnl: dict[str, list[float]] = defaultdict(list)
        band_wins: dict[str, int] = defaultdict(int)
        band_total: dict[str, int] = defaultdict(int)
        for t in trades:
            b = t["band"]
            band_pnl[b].append(t["pnl"])
            band_wins[b] += 1 if t["won"] else 0
            band_total[b] += 1

        band_summary = {}
        for b in ["<10¢", "10-30¢", "30-70¢", ">70¢"]:
            bp = band_pnl.get(b, [])
            bt = band_total.get(b, 0)
            band_summary[b] = {
                "n": bt,
                "total_pnl": round(sum(bp), 2),
                "win_rate": round(band_wins.get(b, 0) / bt, 3) if bt else 0,
            }

        rows.append({
            "wallet": name,
            "address": wallet_addr,
            "trades": n,
            "wins": wins,
            "win_rate": round(win_rate, 3),
            "total_pnl": round(total_pnl, 2),
            "pnl_ex_top1": round(ex_top1, 2),
            "pnl_ex_top3": round(ex_top3, 2),
            "median_pnl": round(med, 2),
            "price_band_breakdown": band_summary,
        })

    return rows


def part2_depth_adjusted(entries: dict, resolutions: dict) -> dict:
    """Depth-adjusted fill simulation using BBO from detection."""
    results = []
    insufficient_depth = []
    no_depth = []
    low_price_details = []

    for token, entry in entries.items():
        res = resolutions.get(token)
        if not res:
            continue

        entry_price = entry.get("sim_fill_price", 0) or entry.get("wallet_fill_price", 0)
        sim_size = entry.get("sim_size", 0)
        pnl = res.get("pnl", 0)
        book = entry.get("book_snapshot") or {}

        best_ask = book.get("best_ask")
        ask_size = book.get("ask_size")
        best_bid = book.get("best_bid")
        bid_size = book.get("bid_size")

        if best_ask is None or ask_size is None:
            no_depth.append({
                "token": token[:14],
                "entry_price": entry_price,
                "reason": "missing BBO",
            })
            results.append({
                "token": token[:14],
                "entry_price": entry_price,
                "size": sim_size,
                "bbo_ask": None,
                "bbo_ask_size": None,
                "1x_fillable": 0,
                "1x_fill_pct": 0,
                "2x_fillable": 0,
                "2x_fill_pct": 0,
                "5x_fillable": 0,
                "5x_fill_pct": 0,
                "pnl_1x": 0,
                "pnl_2x": 0,
                "pnl_5x": 0,
                "depth_data": "none",
            })
            continue

        # Actual fill matches BBO ask price within tolerance
        fill_price_match = abs(entry_price - best_ask) / best_ask < 0.15 if best_ask else True

        # Max fillable at BBO (no deeper levels available)
        bbo_fillable = ask_size

        # 1x = actual paper size
        size_1x = sim_size
        size_2x = sim_size * 2
        size_5x = sim_size * 5

        fill_1x = min(size_1x, bbo_fillable)
        fill_2x = min(size_2x, bbo_fillable)
        fill_5x = min(size_5x, bbo_fillable)

        pct_1x = fill_1x / size_1x if size_1x else 0
        pct_2x = fill_2x / size_2x if size_2x else 0
        pct_5x = fill_5x / size_5x if size_5x else 0

        # PnL proportional: same per-share PnL on filled shares
        per_share = pnl / sim_size if sim_size else 0
        pnl_1x = per_share * fill_1x
        pnl_2x = per_share * fill_2x
        pnl_5x = per_share * fill_5x

        fill_ok_1x = fill_1x >= size_1x
        if not fill_ok_1x:
            insufficient_depth.append({
                "token": token[:14],
                "entry_price": entry_price,
                "size": size_1x,
                "bbo_ask": best_ask,
                "bbo_ask_size": bbo_fillable,
                "fillable": fill_1x,
                "shortfall": size_1x - fill_1x,
            })

        if entry_price < 0.10:
            low_price_details.append({
                "token": token[:14],
                "entry_price": entry_price,
                "ask_size_at_entry": bbo_fillable,
                "sim_size": sim_size,
                "fillable_at_1x": fill_1x,
            })

        results.append({
            "token": token[:14],
            "entry_price": entry_price,
            "size": size_1x,
            "bbo_ask": best_ask,
            "bbo_ask_size": bbo_fillable,
            "fill_price_match": fill_price_match,
            "1x_fillable": round(fill_1x, 2),
            "1x_fill_pct": round(pct_1x, 4),
            "2x_fillable": round(fill_2x, 2),
            "2x_fill_pct": round(pct_2x, 4),
            "5x_fillable": round(fill_5x, 2),
            "5x_fill_pct": round(pct_5x, 4),
            "pnl_1x": round(pnl_1x, 2),
            "pnl_2x": round(pnl_2x, 2),
            "pnl_5x": round(pnl_5x, 2),
            "depth_data": "bbo_only",
        })

    # Summary
    bbo_only_positions = sum(1 for r in results if r.get("depth_data") == "bbo_only")
    no_depth_positions = sum(1 for r in results if r.get("depth_data") == "none")

    total_pnl_1x = sum(r["pnl_1x"] for r in results)
    total_pnl_2x = sum(r["pnl_2x"] for r in results)
    total_pnl_5x = sum(r["pnl_5x"] for r in results)

    total_size_1x = sum(r["size"] for r in results)
    total_fill_1x = sum(r["1x_fillable"] for r in results)
    total_fill_5x = sum(r["5x_fillable"] for r in results)

    summary = {
        "positions_with_bbo": bbo_only_positions,
        "positions_no_depth_data": no_depth_positions,
        "total_size_1x": round(total_size_1x, 2),
        "total_fill_1x": round(total_fill_1x, 2),
        "1x_fill_pct": round(total_fill_1x / total_size_1x, 4) if total_size_1x else 0,
        "total_pnl_1x": round(total_pnl_1x, 2),
        "total_pnl_2x": round(total_pnl_2x, 2),
        "total_pnl_5x": round(total_pnl_5x, 2),
        "insufficient_depth_count": len(insufficient_depth),
        "low_price_entries": len(low_price_details),
    }

    return {
        "summary": summary,
        "per_position": results,
        "insufficient_depth": insufficient_depth,
        "no_depth_data": no_depth,
        "low_price_details": low_price_details,
    }


def part3_ferrari_breakdown(entries: dict, resolutions: dict) -> dict:
    """Chronological trade list for ferrariChampions2026."""
    wallet = "0xfe787d2da716d60e8acff57fb87eb13cd4d10319"
    trades = []
    for token, entry in entries.items():
        if entry.get("wallet") != wallet:
            continue
        res = resolutions.get(token)
        if not res:
            continue
        entry_price = entry.get("sim_fill_price", 0) or entry.get("wallet_fill_price", 0)
        pnl = res.get("pnl", 0)
        side = entry.get("side", "?")
        sim_size = entry.get("sim_size", 0)

        # Try to get market info from condition_id
        condition_id = res.get("resolution_condition_id", entry.get("market", ""))

        trades.append({
            "ts": entry.get("ts", ""),
            "token": token[:14],
            "entry_price": entry_price,
            "size": sim_size,
            "side": side,
            "pnl": pnl,
            "won": pnl > 0,
            "band": price_band(entry_price),
            "condition_id": condition_id[:20] + "...",
            "resolution_side": res.get("resolution_side", "?"),
        })

    # Sort chronologically
    trades.sort(key=lambda t: t["ts"])

    # Category analysis: try to infer market category from condition_id prefix
    # (condition_id is a keccak256 hash — no inherent category info without a lookup table)
    # We'll report what we have
    band_wins = defaultdict(lambda: {"n": 0, "wins": 0, "total_pnl": 0.0, "total_loss": 0.0})
    for t in trades:
        b = t["band"]
        band_wins[b]["n"] += 1
        band_wins[b]["wins"] += 1 if t["won"] else 0
        if t["pnl"] > 0:
            band_wins[b]["total_pnl"] += t["pnl"]
        else:
            band_wins[b]["total_loss"] += t["pnl"]

    band_summary = {}
    for b in ["<10¢", "10-30¢", "30-70¢", ">70¢"]:
        bw = band_wins.get(b, {"n": 0, "wins": 0, "total_pnl": 0.0, "total_loss": 0.0})
        band_summary[b] = {
            "n": bw["n"],
            "win_rate": round(bw["wins"] / bw["n"], 3) if bw["n"] else 0,
            "total_win_pnl": round(bw["total_pnl"], 2),
            "total_loss_pnl": round(bw["total_loss"], 2),
        }

    n = len(trades)
    pnls = [t["pnl"] for t in trades]
    total_pnl = sum(pnls)
    wins = sum(1 for t in trades if t["won"])

    return {
        "wallet": "ferrariChampions2026",
        "address": wallet,
        "total_trades": n,
        "wins": wins,
        "win_rate": round(wins / n, 3) if n else 0,
        "total_pnl": round(total_pnl, 2),
        "median_pnl": round(sorted(pnls)[n // 2], 2) if n else 0,
        "biggest_win": round(max(pnls), 2) if pnls else 0,
        "biggest_loss": round(min(pnls), 2) if pnls else 0,
        "price_band_breakdown": band_summary,
        "trades": trades,
    }


def print_wallet_table(rows: list[dict]):
    """Print part 1 table."""
    print(f"{'Wallet':<22} {'Trades':>6} {'WR':>5} {'Tot PnL':>10} {'Ex-Top1':>10} {'Ex-Top3':>10} {'Median':>8}")
    print("-" * 75)
    for r in rows:
        print(f"{r['wallet']:<22} {r['trades']:>6} {r['win_rate']:.1%} {r['total_pnl']:>+8.2f}  "
              f"{r['pnl_ex_top1']:>+8.2f}  {r['pnl_ex_top3']:>+8.2f} {r['median_pnl']:>+7.2f}")
    print()

    # Price band breakdown
    all_bands = ["<10¢", "10-30¢", "30-70¢", ">70¢"]
    print(f"{'Wallet':<22}   ", end="")
    for b in all_bands:
        print(f"  {b:>8}", end="")
    print()
    for r in rows:
        print(f"{r['wallet']:<22} ", end="")
        for b in all_bands:
            bb = r["price_band_breakdown"].get(b, {"n": 0, "total_pnl": 0, "win_rate": 0})
            pct = f"{bb['win_rate']:.0%}" if bb["win_rate"] else "-"
            print(f"  {bb['n']:>2}t {bb['total_pnl']:>+5.0f} {pct}", end="")
        print()
    print()


def print_depth_summary(depth: dict):
    s = depth["summary"]
    print("Depth-Adjusted Fill Simulation")
    print(f"  Positions with BBO data:     {s['positions_with_bbo']}")
    print(f"  Positions with no depth:     {s['positions_no_depth_data']}")
    print(f"  Insufficient depth (1x):     {s['insufficient_depth_count']}")
    print(f"  Low-price entries (<10¢):    {s['low_price_entries']}")
    print(f"  Total intended size:         {s['total_size_1x']:,.2f} shares")
    print(f"  Total fillable at BBO (1x):  {s['total_fill_1x']:,.2f} ({s['1x_fill_pct']:.1%})")
    print()
    print(f"  {'Scenario':>12}  {'Total PnL':>12}  {'% Filled':>10}")
    print(f"  {'-'*12}  {'-'*12}  {'-'*10}")
    print(f"  {'1x (paper)':>12}  {s['total_pnl_1x']:>+10.2f}    {s['1x_fill_pct']:.1%}")
    print(f"  {'2x':>12}  {s['total_pnl_2x']:>+10.2f}")
    print(f"  {'5x':>12}  {s['total_pnl_5x']:>+10.2f}")

    if depth["insufficient_depth"]:
        print()
        print(f"  Positions with insufficient BBO depth for 1x ({len(depth['insufficient_depth'])}):")
        for d in depth["insufficient_depth"][:10]:
            print(f"    {d['token']} price={d['entry_price']:.4f} size={d['size']:.1f} ask_size={d['bbo_ask_size']:.1f} "
                  f"fill={d['fillable']:.1f} shortfall={d['shortfall']:.1f}")
        if len(depth["insufficient_depth"]) > 10:
            print(f"    ... and {len(depth['insufficient_depth']) - 10} more")

    if depth["low_price_details"]:
        print()
        print(f"  Low-price entries (<10¢) - ask sizes at entry:")
        for d in depth["low_price_details"][:15]:
            print(f"    {d['token']} price={d['entry_price']:.4f} ask_sz={d['ask_size_at_entry']:.1f} "
                  f"sim_sz={d['sim_size']:.1f} fillable={d['fillable_at_1x']:.1f}")
        if len(depth["low_price_details"]) > 15:
            print(f"    ... and {len(depth['low_price_details']) - 15} more")
    print()


def print_ferrari_table(ferrari: dict):
    print("FerrariChampions2026 — Chronological Breakdown")
    print(f"  Total trades: {ferrari['total_trades']}  Wins: {ferrari['wins']}  "
          f"Win rate: {ferrari['win_rate']:.1%}")
    print(f"  Total PnL: {ferrari['total_pnl']:+.2f}  "
          f"Biggest win: {ferrari['biggest_win']:+.2f}  "
          f"Biggest loss: {ferrari['biggest_loss']:+.2f}")
    print()

    # Price band breakdown
    bb = ferrari["price_band_breakdown"]
    print(f"  {'Band':>8}  {'Trades':>6}  {'WR':>5}  {'Win PnL':>10}  {'Loss PnL':>10}")
    print(f"  {'-'*8}  {'-'*6}  {'-'*5}  {'-'*10}  {'-'*10}")
    for b in ["<10¢", "10-30¢", "30-70¢", ">70¢"]:
        v = bb.get(b, {"n": 0, "win_rate": 0, "total_win_pnl": 0, "total_loss_pnl": 0})
        if v["n"]:
            print(f"  {b:>8}  {v['n']:>6}  {v['win_rate']:.0%}  {v['total_win_pnl']:>+9.0f}  {v['total_loss_pnl']:>+9.0f}")
    print()

    # Chronological trade list
    print(f"  {'Date':<20} {'Token':<14} {'Price':>7} {'Size':>8} {'PnL':>10} {'Band':>8}")
    print(f"  {'-'*20} {'-'*14} {'-'*7} {'-'*8} {'-'*10} {'-'*8}")
    for t in ferrari["trades"]:
        ts_short = t["ts"][:16] if t["ts"] else "?"
        print(f"  {ts_short:<20} {t['token']:<14} {t['entry_price']:>7.3f} {t['size']:>8.1f} "
              f"{t['pnl']:>+9.2f} {t['band']:>8}")
    print()

    # Summary: do wins cluster?
    bands_with_wins = [(b, v) for b, v in bb.items() if v["n"] > 0]
    print("  Edge clustering analysis:")
    for b, v in bands_with_wins:
        if v["n"] and v["win_rate"] > 0.5:
            print(f"    ✅ {b}: win rate {v['win_rate']:.0%} across {v['n']} trades "
                  f"(net ${v['total_win_pnl'] + v['total_loss_pnl']:+.0f})")
        elif v["n"]:
            print(f"    {'⚠️' if v['win_rate'] > 0.2 else '❌'} {b}: win rate {v['win_rate']:.0%} across {v['n']} trades "
                  f"(net ${v['total_win_pnl'] + v['total_loss_pnl']:+.0f})")
    print()


def main():
    print("=" * 80)
    print("SETTLEMENT ANALYSIS — Concentration, Depth, Ferrari")
    print("=" * 80)
    print()

    data = load_ledger()
    entries = data["entries"]
    resolutions = data["resolutions"]

    # Part 1: Per-wallet PnL
    print("-" * 80)
    print("PART 1: PER-WALLET PnL DISTRIBUTION (ex-outliers)")
    print("-" * 80)
    wallet_rows = part1_per_wallet_pnl(entries, resolutions)
    print_wallet_table(wallet_rows)

    # Part 2: Depth-adjusted fills
    print("-" * 80)
    print("PART 2: DEPTH-ADJUSTED FILL SIMULATION")
    print("-" * 80)
    depth = part2_depth_adjusted(entries, resolutions)
    print_depth_summary(depth)

    # Part 3: Ferrari breakdown
    print("-" * 80)
    print("PART 3: FERRARIChampions2026 TRADE BREAKDOWN")
    print("-" * 80)
    ferrari = part3_ferrari_breakdown(entries, resolutions)
    print_ferrari_table(ferrari)

    # Build output JSON
    output = {
        "part1_wallet_pnl": wallet_rows,
        "part2_depth_adjusted": depth,
        "part3_ferrari": ferrari,
    }
    OUTPUT.write_text(json.dumps(output, indent=2, default=str))
    print(f"Full JSON written to {OUTPUT}")


if __name__ == "__main__":
    main()
