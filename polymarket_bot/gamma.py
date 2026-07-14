from __future__ import annotations

import ast
from typing import Any

from .config import CONFIG, BotConfig
from .http import get_json


def _parse_array(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if value in (None, ""):
        return []
    if isinstance(value, str):
        try:
            parsed = ast.literal_eval(value)
            return parsed if isinstance(parsed, list) else []
        except Exception:
            return []
    return []


def active_events(limit: int = 50, config: BotConfig = CONFIG) -> list[dict[str, Any]]:
    data = get_json(config.gamma_base, "/events", {
        "active": "true",
        "closed": "false",
        "order": "volume_24hr",
        "ascending": "false",
        "limit": limit,
    }, user_agent=config.user_agent)
    return data if isinstance(data, list) else data.get("events", [])


def flatten_markets(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for event in events:
        markets = event.get("markets") or []
        if not isinstance(markets, list):
            continue
        for market in markets:
            if not isinstance(market, dict):
                continue
            outcomes = _parse_array(market.get("outcomes"))
            prices = _parse_array(market.get("outcomePrices"))
            clob_ids = _parse_array(market.get("clobTokenIds"))
            rows.append({
                "event_id": event.get("id"),
                "event_slug": event.get("slug"),
                "event_title": event.get("title") or event.get("question"),
                "market_id": market.get("id"),
                "condition_id": market.get("conditionId") or market.get("condition_id"),
                "market_slug": market.get("slug"),
                "question": market.get("question") or event.get("title"),
                "active": market.get("active"),
                "closed": market.get("closed"),
                "enable_order_book": bool(market.get("enableOrderBook")),
                "volume": _num(market.get("volume") or event.get("volume")),
                "volume_24h": _num(market.get("volume24hr") or market.get("volume_24hr") or event.get("volume24hr") or event.get("volume_24hr")),
                "liquidity": _num(market.get("liquidity") or event.get("liquidity")),
                "outcomes": outcomes,
                "outcome_prices": [_num(x) for x in prices],
                "clob_token_ids": [str(x) for x in clob_ids],
                "end_date": market.get("endDate") or event.get("endDate"),
            })
    return rows


def _num(value: Any) -> float:
    try:
        if value in (None, ""):
            return 0.0
        return float(value)
    except Exception:
        return 0.0


def markets_by_token(token_id: str, config: BotConfig = CONFIG) -> list[dict[str, Any]]:
    """Look up the Gamma market entry for a given CLOB token id.

    Returns a list (most-Gamma endpoints return arrays even for one-shot queries).
    An empty list means no market row exists for this token.
    """
    if not token_id:
        return []
    try:
        data = get_json(config.gamma_base, "/markets", {"clob_token_ids": token_id}, user_agent=config.user_agent)
    except Exception:
        return []
    return data if isinstance(data, list) else []


def resolved_outcome_for_token(token_id: str, config: BotConfig = CONFIG) -> dict[str, Any] | None:
    """Look up resolution status for the market that contains this token.

    Returns a dict like:
      {"resolved": True/False, "side": "YES"|"NO"|None, "question": ..., "market_id": ..., "closed": bool}

    Returns None if lookup failed or token is unknown to Gamma.
    Side is the outcome *our* token represents (YES or NO) so caller can price a long correctly.
    """
    markets = markets_by_token(token_id, config=config)
    if not markets:
        return None
    market = markets[0] if isinstance(markets[0], dict) else {}
    closed = bool(market.get("closed"))
    resolution_status = market.get("umaResolutionStatus") or market.get("resolution")
    clob_ids = _parse_array(market.get("clobTokenIds"))
    token_lower = token_id.lower()
    side: str | None = None
    for idx, cid in enumerate(clob_ids):
        if str(cid).lower() == token_lower:
            outcomes = _parse_array(market.get("outcomes"))
            if idx < len(outcomes):
                outcome_label = str(outcomes[idx]).strip().upper()
                if outcome_label in {"YES", "NO", "TRUE", "FALSE"}:
                    side = "YES" if outcome_label in {"YES", "TRUE"} else "NO"
                else:
                    # Multi-outcome markets — caller may need to inspect manually
                    side = outcome_label
            break
    return {
        "resolved": bool(closed and resolution_status),
        "resolution_status": resolution_status,
        "side": side,
        "question": market.get("question"),
        "market_id": market.get("id"),
        "condition_id": market.get("conditionId") or market.get("condition_id"),
        "closed": closed,
        "active": market.get("active"),
        "raw": market,
    }
