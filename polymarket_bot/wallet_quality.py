"""Per-wallet quality scoring from polybot ledger.

Computes from runs/paper/ledger.jsonl:
  - total signals seen, accepted
  - win rate (exits with positive pnl)
  - realized PnL, average pnl per trade
  - avg holding period (entry → exit, hours)
  - last seen timestamp (for "active" classification)
  - current open position count

Scoring rubric (0-100):
  +30 if PnL > 0
  +25 if win rate >= 50%
  +20 if total PnL > 5x their average trade size
  +15 if active in last 7 days
  +10 if avg holding period < 48h (active trader, not stale)

Higher score = more reliable leader.

Used by /api/wallets/quality for ranking and decisions.
"""

from __future__ import annotations

import json
import logging
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, UTC, timedelta
from pathlib import Path
from typing import Any

logger = logging.getLogger("polymarket_bot.wallet_quality")

LEDGER_PATH = Path("/root/flip/projects/polymarket-copybot/runs/paper/ledger.jsonl")

_cache: dict[str, tuple[float, list[dict[str, Any]]]] = {}
CACHE_TTL = 60.0


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


@dataclass
class WalletStats:
    wallet: str
    name: str
    signals: int = 0
    accepts: int = 0
    exits: int = 0
    wins: int = 0  # exits with pnl > 0
    realized_pnl: float = 0.0
    total_cost: float = 0.0
    avg_holding_hours: float = 0.0
    last_seen_at: str = ""
    open_positions: int = 0
    quality_score: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "wallet": self.wallet,
            "name": self.name,
            "signals": self.signals,
            "accepts": self.accepts,
            "exits": self.exits,
            "wins": self.wins,
            "win_rate": round(self.wins / self.exits * 100, 1) if self.exits else 0.0,
            "realized_pnl": round(self.realized_pnl, 2),
            "total_cost": round(self.total_cost, 2),
            "avg_pnl_per_trade": round(self.realized_pnl / max(self.exits, 1), 2),
            "avg_holding_hours": round(self.avg_holding_hours, 2),
            "last_seen_at": self.last_seen_at,
            "open_positions": self.open_positions,
            "quality_score": round(self.quality_score, 1),
        }


def compute_wallet_quality(ledger_path: Path = LEDGER_PATH) -> list[dict[str, Any]]:
    """Compute quality scores for all wallets in the ledger.

    Returns list of wallet stats dicts, sorted by quality_score desc.
    """
    cache_key = f"quality_{ledger_path}"
    now = time.time()
    if cache_key in _cache and (now - _cache[cache_key][0]) < CACHE_TTL:
        return _cache[cache_key][1]

    if not ledger_path.exists():
        return []

    name_map = _wallet_name_map()
    stats_map: dict[str, WalletStats] = {}

    # Track entry→exit pairs for holding period
    open_entries: dict[tuple[str, str], datetime] = {}  # (wallet, token) -> entry_ts

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
                wallet = (row.get("source_wallet") or row.get("wallet") or "").lower()
                if not wallet:
                    continue
                if wallet not in stats_map:
                    stats_map[wallet] = WalletStats(
                        wallet=wallet,
                        name=name_map.get(wallet, wallet[:14] + "…"),
                    )
                s = stats_map[wallet]
                row_type = row.get("type", "")
                ts = _parse_ts(row.get("ts") or "")
                if ts:
                    s.last_seen_at = max(s.last_seen_at, ts.isoformat())
                if row_type == "signal":
                    s.signals += 1
                elif row_type == "entry":
                    s.accepts += 1
                    cost = float(row.get("sim_fill_price") or 0) * float(row.get("sim_size") or 0)
                    s.total_cost += cost
                    token = row.get("token", "")
                    if token and ts:
                        open_entries[(wallet, token)] = ts
                elif row_type in ("exit", "resolution"):
                    s.exits += 1
                    pnl = float(row.get("pnl") or 0)
                    s.realized_pnl += pnl
                    if pnl > 0:
                        s.wins += 1
                    token = row.get("token", "")
                    if token:
                        entry_ts = open_entries.pop((wallet, token), None)
                        if entry_ts and ts:
                            holding = (ts - entry_ts).total_seconds() / 3600
                            # Running avg
                            n = s.exits
                            s.avg_holding_hours = (s.avg_holding_hours * (n - 1) + holding) / n
    except Exception as e:
        logger.exception("ledger read error: %s", e)

    # Get current open positions from state.json
    state_path = Path("/root/flip/projects/polymarket-copybot/runs/paper/state.json")
    if state_path.exists():
        try:
            state = json.loads(state_path.read_text())
            for pos_id, pos in (state.get("positions") or {}).items():
                wallet = (pos.get("wallet") or "").lower()
                if wallet in stats_map:
                    stats_map[wallet].open_positions += 1
        except Exception:
            pass

    # Compute quality scores
    for s in stats_map.values():
        score = 0.0
        # +30 PnL positive
        if s.realized_pnl > 0:
            score += 30
        elif s.realized_pnl > 1000:
            score += 25
        # +25 win rate >= 50%
        if s.exits > 0:
            wr = s.wins / s.exits
            if wr >= 0.5:
                score += 25
            elif wr >= 0.4:
                score += 15
        # +20 PnL > 5x avg trade size (edge evidence)
        if s.exits > 0 and s.total_cost > 0:
            avg_trade = s.total_cost / s.exits
            if avg_trade > 0 and s.realized_pnl / s.exits > 5 * avg_trade:
                score += 20
        # +15 active in last 7 days
        if s.last_seen_at:
            last = _parse_ts(s.last_seen_at)
            if last and (datetime.now(UTC) - last) < timedelta(days=7):
                score += 15
        # +10 avg holding < 48h
        if 0 < s.avg_holding_hours < 48:
            score += 10
        s.quality_score = min(score, 100.0)

    result = [s.to_dict() for s in stats_map.values()]
    result.sort(key=lambda w: w["quality_score"], reverse=True)
    _cache[cache_key] = (now, result)
    return result


def clear_cache() -> None:
    _cache.clear()