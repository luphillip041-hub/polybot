"""Compute PnL time series from the polybot ledger.

Reads runs/paper/ledger.jsonl and builds:
  - daily_pnl: {date_str: cumulative_pnl}
  - per_wallet_daily: {wallet: {date_str: cumulative_pnl}}

Used by the /api/pnl/timeseries endpoint for sparkline charts.
"""

from __future__ import annotations

import json
import os
import time
from collections import defaultdict
from datetime import datetime, UTC
from pathlib import Path
from typing import Any

LEDGER_PATH = Path("/root/flip/projects/polymarket-copybot/runs/paper/ledger.jsonl")

# Cache: {key: (ts, data)}
_cache: dict[str, tuple[float, Any]] = {}
CACHE_TTL = 60.0  # 1 minute


def _parse_ts(raw: str) -> datetime | None:
    if not raw:
        return None
    try:
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        return datetime.fromisoformat(raw)
    except (ValueError, TypeError):
        return None


def _wallet_name_map() -> dict[str, str]:
    """Map wallet address → display name (from wallet_scores or fall through)."""
    scores_path = Path("/root/flip/projects/polymarket-copybot/runs/wallet_scores_latest.json")
    if not scores_path.exists():
        return {}
    try:
        scores = json.loads(scores_path.read_text())
    except Exception:
        return {}
    out = {}
    if isinstance(scores, list):
        for entry in scores:
            wallet = (entry.get("wallet") or "").lower()
            name = entry.get("name") or entry.get("display_name") or wallet
            if wallet:
                out[wallet] = name
    elif isinstance(scores, dict):
        for wallet, entry in scores.items():
            name = entry.get("name") or entry.get("display_name") or wallet
            if isinstance(name, str):
                out[wallet.lower()] = name
    return out


def compute_daily_pnl(ledger_path: Path = LEDGER_PATH,
                       days: int = 30) -> list[dict[str, Any]]:
    """Return list of {date, daily_pnl, cumulative_pnl} for the last `days` days.

    Reads ledger.jsonl, sums realized PnL per day (UTC), builds cumulative.
    Cached for 60s.
    """
    cache_key = f"daily_pnl_{ledger_path}_{days}"
    now = time.time()
    if cache_key in _cache and (now - _cache[cache_key][0]) < CACHE_TTL:
        return _cache[cache_key][1]

    if not ledger_path.exists():
        return []

    cutoff = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    from datetime import timedelta as td
    cutoff = cutoff - td(days=days)
    # Initialize zero days
    days_map: dict[str, float] = {}
    today = datetime.now(UTC).date()
    cur_date = cutoff.date()
    while cur_date <= today:
        days_map[cur_date.isoformat()] = 0.0
        cur_date = cur_date + td(days=1)

    # Stream ledger
    try:
        with open(ledger_path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if row.get("type") not in ("exit", "resolution"):
                    continue
                ts_str = row.get("ts") or row.get("time") or ""
                dt = _parse_ts(ts_str)
                if dt is None or dt < cutoff:
                    continue
                pnl = row.get("pnl") or row.get("realized_pnl")
                if pnl is None:
                    continue
                try:
                    pnl = float(pnl)
                except (ValueError, TypeError):
                    continue
                date_str = dt.date().isoformat()
                days_map[date_str] = days_map.get(date_str, 0.0) + pnl
    except Exception as e:
        # Don't crash on read errors
        pass

    # Build cumulative list
    result: list[dict[str, Any]] = []
    cum = 0.0
    for date_str in sorted(days_map.keys()):
        cum += days_map[date_str]
        result.append({
            "date": date_str,
            "daily_pnl": round(days_map[date_str], 2),
            "cumulative_pnl": round(cum, 2),
        })

    _cache[cache_key] = (now, result)
    return result


def compute_per_wallet_daily(ledger_path: Path = LEDGER_PATH,
                              days: int = 30,
                              top_n: int = 5) -> dict[str, Any]:
    """Per-wallet cumulative PnL time series, top_n wallets by total PnL."""
    cache_key = f"per_wallet_{ledger_path}_{days}_{top_n}"
    now = time.time()
    if cache_key in _cache and (now - _cache[cache_key][0]) < CACHE_TTL:
        return _cache[cache_key][1]

    if not ledger_path.exists():
        return {"wallets": [], "series": {}}

    cutoff = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    from datetime import timedelta as td
    cutoff = cutoff - td(days=days)

    name_map = _wallet_name_map()
    # wallet -> date -> pnl
    wallet_daily: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))

    try:
        with open(ledger_path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if row.get("type") not in ("exit", "resolution"):
                    continue
                ts_str = row.get("ts") or row.get("time") or ""
                dt = _parse_ts(ts_str)
                if dt is None or dt < cutoff:
                    continue
                pnl = row.get("pnl") or row.get("realized_pnl")
                if pnl is None:
                    continue
                try:
                    pnl = float(pnl)
                except (ValueError, TypeError):
                    continue
                wallet = (row.get("source_wallet") or row.get("wallet") or "").lower()
                if not wallet:
                    continue
                date_str = dt.date().isoformat()
                wallet_daily[wallet][date_str] += pnl
    except Exception:
        pass

    # Compute totals and select top_n
    totals = {w: sum(daily.values()) for w, daily in wallet_daily.items()}
    sorted_wallets = sorted(totals.items(), key=lambda x: -x[1])[:top_n]
    top_wallet_names = [name_map.get(w, w[:10]) for w, _ in sorted_wallets]

    # Build series: list of {wallet_name, total, points: [{date, cum}]}
    series: dict[str, Any] = {}
    for w, total in sorted_wallets:
        name = name_map.get(w, w[:10])
        cum = 0.0
        points = []
        # Get all dates sorted
        all_dates = sorted(wallet_daily[w].keys())
        for d in all_dates:
            cum += wallet_daily[w][d]
            points.append({"date": d, "cum": round(cum, 2)})
        series[name] = {
            "wallet_address": w[:16] + "…",
            "total": round(total, 2),
            "points": points,
        }

    result = {
        "wallets": top_wallet_names,
        "series": series,
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
    }
    _cache[cache_key] = (now, result)
    return result


def clear_cache() -> None:
    _cache.clear()