from __future__ import annotations

from dataclasses import dataclass
from math import log10
from typing import Any

from .config import CONFIG, BotConfig


@dataclass(frozen=True)
class ScoreResult:
    score: float
    side: str
    outcome: str | None
    token_id: str | None
    reasons: list[str]
    blocked_reasons: list[str]


def score_market(market: dict[str, Any], config: BotConfig = CONFIG) -> ScoreResult:
    reasons: list[str] = []
    blocked: list[str] = []
    volume_24h = float(market.get("volume_24h") or 0)
    liquidity = float(market.get("liquidity") or 0)
    prices = [p for p in market.get("outcome_prices", []) if isinstance(p, (int, float))]
    outcomes = market.get("outcomes") or []
    tokens = market.get("clob_token_ids") or []

    if not market.get("enable_order_book"):
        blocked.append("orderbook disabled")
    if volume_24h < config.min_volume_24h:
        blocked.append(f"volume_24h {volume_24h:.0f} < {config.min_volume_24h:.0f}")
    if liquidity < config.min_liquidity:
        blocked.append(f"liquidity {liquidity:.0f} < {config.min_liquidity:.0f}")
    if not prices or not outcomes or not tokens:
        blocked.append("missing outcomes/prices/token ids")

    score = 0.0
    if volume_24h > 0:
        score += min(30.0, log10(volume_24h + 1) * 8)
        reasons.append("volume")
    if liquidity > 0:
        score += min(25.0, log10(liquidity + 1) * 6)
        reasons.append("liquidity")

    # Prefer tradeable but not already-decided probabilities.
    chosen_idx = None
    if prices:
        middle = [(i, p) for i, p in enumerate(prices) if 0.15 <= p <= 0.85]
        if middle:
            chosen_idx, prob = sorted(middle, key=lambda x: abs(x[1] - 0.5))[0]
            score += 25.0 - min(20.0, abs(prob - 0.5) * 40)
            reasons.append("balanced probability")
        else:
            chosen_idx, prob = max(enumerate(prices), key=lambda x: x[1])
            score += 5.0
            blocked.append("probability too extreme")

    if market.get("active") is False or market.get("closed") is True:
        blocked.append("market inactive/closed")
    if market.get("enable_order_book") and not blocked:
        score += 20.0

    outcome = str(outcomes[chosen_idx]) if chosen_idx is not None and chosen_idx < len(outcomes) else None
    token = str(tokens[chosen_idx]) if chosen_idx is not None and chosen_idx < len(tokens) else None
    score = round(min(score, 100.0), 2)
    if score < config.min_score:
        blocked.append(f"score {score:.2f} < {config.min_score:.2f}")
    return ScoreResult(score=score, side="BUY", outcome=outcome, token_id=token, reasons=reasons, blocked_reasons=blocked)
