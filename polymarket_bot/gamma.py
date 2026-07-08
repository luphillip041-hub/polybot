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
