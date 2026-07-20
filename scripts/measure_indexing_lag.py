#!/usr/bin/env python3
"""Measure Polymarket /trades indexing lag for 5 high-volume markets.

Polls /trades every 3s for 60min, records first-seen vs fill-timestamp lag.
Usage: python scripts/measure_indexing_lag.py
"""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from polymarket_bot.http import get_json
from polymarket_bot.config import CONFIG

DATA_BASE = "https://data-api.polymarket.com"

# 5 highest-volume markets from the top-50 archive
MARKETS = [
    {"slug": "spain-wc-winner",  "condition_id": "0x7976b8dbacf9077eb1fce2db08e34f78d36e9c13d5ba1271475c237464b75b58"},
    {"slug": "argentina-wc-winner", "condition_id": "0x0c4cd2055d6ea89354ffddc55d6dbcef9355748112ea952fc925f3db6a5c457f"},
    {"slug": "norway-wc-winner", "condition_id": "0x7b52405ad0e0d31bfe6e767eb7f41f508fecca6cddc9f0fdfbca30f9d20ef1cb"},
    {"slug": "france-wc-winner",  "condition_id": "0x9b6fef249040fd17e9eef857cd04a8ae66ecbd8057e3b52ac6683c8944896b00"},
    {"slug": "england-wc-winner", "condition_id": "0x375409bc5eeeff961e8ec849ebc1de0db45e1c7caa65ba9cb8b600ca29266cf1"},
]

OUTPUT = REPO_ROOT / "runs" / f"indexing_lag_{datetime.now(UTC).strftime('%Y-%m-%d')}.jsonl"
POLL_INTERVAL = 3  # seconds
DURATION_MINUTES = 60
DURATION_SECONDS = DURATION_MINUTES * 60


def iso_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def fetch_trades(condition_id: str, limit: int = 100) -> list[dict[str, Any]]:
    try:
        data = get_json(DATA_BASE, "/trades", {"market": condition_id, "limit": limit}, user_agent=CONFIG.user_agent)
        return data if isinstance(data, list) else []
    except Exception as exc:
        print(f"  [ERROR] {condition_id[:20]}: {exc}", flush=True)
        return []


def run() -> None:
    seen_trade_ids: dict[str, dict[str, Any]] = {}  # trade_id -> {trade_id, market, fill_ts, first_seen_ts}
    poll_count = 0
    start_ts = time.time()
    deadline = start_ts + DURATION_SECONDS

    print(f"INDEXING LAG MEASUREMENT", flush=True)
    print(f"  Markets: {[m['slug'] for m in MARKETS]}", flush=True)
    print(f"  Poll interval: {POLL_INTERVAL}s", flush=True)
    print(f"  Duration: {DURATION_MINUTES}min", flush=True)
    print(f"  Output: {OUTPUT}", flush=True)
    print(f"  Started: {iso_now()}", flush=True)
    print(flush=True)

    while time.time() < deadline:
        cycle_start = time.time()
        poll_count += 1

        for m in MARKETS:
            trades = fetch_trades(m["condition_id"])
            now = time.time()
            for trade in trades:
                tid = str(trade.get("transactionHash") or trade.get("id") or "")
                if not tid or tid in seen_trade_ids:
                    continue
                fill_ts_raw = trade.get("timestamp")
                fill_ts: float | None = None
                if fill_ts_raw is not None:
                    if isinstance(fill_ts_raw, (int, float)):
                        fill_ts = float(fill_ts_raw)
                    elif isinstance(fill_ts_raw, str):
                        try:
                            fill_ts = datetime.fromisoformat(fill_ts_raw.replace("Z", "+00:00")).timestamp()
                        except ValueError:
                            pass
                if fill_ts is None:
                    continue
                lag = now - fill_ts
                record = {
                    "trade_id": tid,
                    "market": m["slug"],
                    "condition_id": m["condition_id"],
                    "fill_ts": datetime.fromtimestamp(fill_ts, UTC).isoformat(timespec="seconds"),
                    "first_seen_ts": datetime.fromtimestamp(now, UTC).isoformat(timespec="seconds"),
                    "lag_s": round(lag, 2),
                }
                seen_trade_ids[tid] = record
                OUTPUT.parent.mkdir(parents=True, exist_ok=True)
                with OUTPUT.open("ab") as f:
                    f.write((json.dumps(record, separators=(",", ":"), sort_keys=True) + "\n").encode("utf-8"))
                    f.flush()
                    os.fsync(f.fileno())
                print(f"  NEW trade={tid[:12]} market={m['slug']} lag={lag:.1f}s fill={record['fill_ts']}", flush=True)

        elapsed = time.time() - cycle_start
        sleep_left = max(0.0, POLL_INTERVAL - elapsed)
        if sleep_left > 0:
            time.sleep(sleep_left)

        if poll_count % 20 == 0:
            running = time.time() - start_ts
            remaining = max(0, deadline - time.time())
            print(f"  [PROGRESS] poll={poll_count} trades_found={len(seen_trade_ids)} elapsed={running:.0f}s remain={remaining:.0f}s", flush=True)

    # Final report
    elapsed_total = time.time() - start_ts
    print(flush=True)
    print("=" * 60, flush=True)
    print(f"MEASUREMENT COMPLETE", flush=True)
    print(f"  Duration: {elapsed_total:.0f}s ({elapsed_total / 60:.1f}min)", flush=True)
    print(f"  Polls: {poll_count}", flush=True)
    print(f"  Unique trades observed: {len(seen_trade_ids)}", flush=True)

    lags = sorted(r["lag_s"] for r in seen_trade_ids.values())
    n = len(lags)
    if n > 0:
        print(f"  Lag p10:  {lags[n // 10]:.1f}s" if n >= 10 else "  Lag p10:  N/A (<10 samples)", flush=True)
        print(f"  Lag p50:  {lags[n // 2]:.1f}s", flush=True)
        print(f"  Lag p90:  {lags[int(n * 0.9)]:.1f}s", flush=True)
        print(f"  Lag min:  {lags[0]:.1f}s", flush=True)
        print(f"  Lag max:  {lags[-1]:.1f}s", flush=True)
        over_120 = sum(1 for l in lags if l > 120)
        print(f"  Trades lag >120s: {over_120} ({100 * over_120 / n:.1f}%)", flush=True)
        print(flush=True)
        print("  Per-market p50:", flush=True)
        for m in MARKETS:
            market_lags = sorted(r["lag_s"] for r in seen_trade_ids.values() if r["market"] == m["slug"])
            if market_lags:
                mp50 = market_lags[len(market_lags) // 2]
                print(f"    {m['slug']:<30} p50={mp50:.1f}s trades={len(market_lags)}", flush=True)
            else:
                print(f"    {m['slug']:<30} no trades observed", flush=True)

    print(f"\nResults saved to {OUTPUT}", flush=True)


if __name__ == "__main__":
    run()
