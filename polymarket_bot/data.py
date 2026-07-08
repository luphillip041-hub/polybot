from __future__ import annotations

from collections import Counter
from typing import Any

from .config import CONFIG, BotConfig
from .http import get_json

DATA_BASE = "https://data-api.polymarket.com"


def leaderboard(limit: int = 25, config: BotConfig = CONFIG) -> list[dict[str, Any]]:
    data = get_json(DATA_BASE, "/v1/leaderboard", {"limit": limit}, user_agent=config.user_agent)
    return data if isinstance(data, list) else []


def user_trades(wallet: str, limit: int = 50, config: BotConfig = CONFIG) -> list[dict[str, Any]]:
    data = get_json(DATA_BASE, "/trades", {"user": wallet, "limit": limit}, user_agent=config.user_agent)
    return data if isinstance(data, list) else []


def score_wallet(row: dict[str, Any], trades: list[dict[str, Any]]) -> dict[str, Any]:
    vol = _num(row.get("vol"))
    pnl = _num(row.get("pnl"))
    pnl_ratio = pnl / vol if vol > 0 else 0.0
    market_count = len({t.get("conditionId") for t in trades if t.get("conditionId")})
    sides = Counter(t.get("side") for t in trades)
    avg_trade_usd = sum(_num(t.get("size")) * _num(t.get("price")) for t in trades) / max(1, len(trades))

    score = 0.0
    reasons: list[str] = []
    blocked: list[str] = []
    if vol > 10_000:
        score += 25; reasons.append("meaningful volume")
    else:
        blocked.append("low leaderboard volume")
    if pnl > 0:
        score += 25; reasons.append("positive pnl")
    else:
        blocked.append("non-positive pnl")
    if 0.01 <= pnl_ratio <= 0.60:
        score += 20; reasons.append("plausible pnl/volume")
    else:
        blocked.append("pnl/volume ratio suspicious or weak")
    if market_count >= 5:
        score += 15; reasons.append("diversified recent trades")
    else:
        blocked.append("too few recent markets")
    if avg_trade_usd <= 5_000:
        score += 15; reasons.append("copyable average trade size")
    else:
        blocked.append("average trade size too large")
    return {
        "wallet": row.get("proxyWallet"),
        "user_name": row.get("userName"),
        "rank": row.get("rank"),
        "vol": vol,
        "pnl": pnl,
        "pnl_ratio": round(pnl_ratio, 4),
        "recent_trades": len(trades),
        "recent_markets": market_count,
        "avg_trade_usd": round(avg_trade_usd, 2),
        "side_mix": dict(sides),
        "copy_score": round(min(score, 100), 2),
        "reasons": reasons,
        "blocked_reasons": blocked,
    }


def _num(value: Any) -> float:
    try:
        return float(value or 0)
    except Exception:
        return 0.0
