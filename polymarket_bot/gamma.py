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


def clob_condition_id_for_token(token_id: str, config: BotConfig = CONFIG) -> str | None:
    """Resolve a CLOB token id to its parent condition id via the CLOB service.

    The Gamma `markets?clob_token_ids=` filter returns an empty list for many tokens
    (including the ones in our open book), even though the market exists. To work
    around this we round-trip through the CLOB service, which exposes the
    condition↔token mapping directly.
    """
    if not token_id:
        return None
    try:
        from .http import get_json
        data = get_json(config.clob_base, f"/markets-by-token/{token_id}", user_agent=config.user_agent)
    except Exception:
        return None
    if isinstance(data, dict):
        cid = data.get("condition_id")
        return str(cid) if cid else None
    return None


def event_by_clob_token(token_id: str, config: BotConfig = CONFIG) -> list[dict[str, Any]]:
    """Fetch events containing this token (the truest path — bypasses CLOB/Gamma cid mismatch)."""
    if not token_id:
        return []
    try:
        data = get_json(config.gamma_base, "/events", {"clob_token_ids": token_id}, user_agent=config.user_agent)
    except Exception:
        return []
    return data if isinstance(data, list) else []


def event_by_condition(condition_id: str, config: BotConfig = CONFIG) -> list[dict[str, Any]]:
    """Legacy lookup kept for compatibility — sometimes returns wildcards.

    Returns the raw events list — caller must scan markets for `conditionId` match.
    """
    if not condition_id:
        return []
    try:
        data = get_json(config.gamma_base, "/events", {"condition_id": condition_id}, user_agent=config.user_agent)
    except Exception:
        return []
    return data if isinstance(data, list) else []


def resolved_outcome_for_token(token_id: str, config: BotConfig = CONFIG) -> dict[str, Any] | None:
    """Look up resolution status for the market that contains this token.

    Returns a dict like:
      {"resolved": True/False, "side": "YES"|"NO"|None, "question": ..., "market_id": ..., "closed": bool}

    Returns None if lookup failed or token is unknown.

    Lookup order:
      1. Gamma /events?clob_token_ids={token} → returns the parent event(s). Walk
         event.markets[] for the market that owns our token (by clobTokenIds[]).
         This is the most reliable path — CLOB `condition_id` and Gamma
         `conditionId` aren't always the same identifier.
      2. Fallback to /markets?clob_token_ids={token} for the rare cases where
         events lookup returns nothing.
    """
    if not token_id:
        return None

    # Path 1: /events?clob_token_ids=
    events = event_by_clob_token(token_id, config=config)
    market: dict[str, Any] | None = None
    event_dict: dict[str, Any] | None = None
    for evt in events:
        if not isinstance(evt, dict):
            continue
        for m in (evt.get("markets") or []):
            if not isinstance(m, dict):
                continue
            ids = _parse_array(m.get("clobTokenIds"))
            if any(str(x).lower() == token_id.lower() for x in ids):
                market = m
                event_dict = evt
                break
        if market is not None:
            break

    # Path 2: /markets?clob_token_ids= fallback
    if market is None:
        rows = markets_by_token(token_id, config=config)
        for row in rows:
            if isinstance(row, dict):
                market = row
                break

    if market is None:
        return None

    closed = bool(market.get("closed"))
    resolution_status = market.get("umaResolutionStatus") or market.get("resolution")
    clob_ids = _parse_array(market.get("clobTokenIds"))
    token_lower = token_id.lower()
    side: str | None = None
    for idx, cid_val in enumerate(clob_ids):
        if str(cid_val).lower() == token_lower:
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
        "question": (market.get("question") if isinstance(market, dict) else None)
            or (event_dict.get("title") if isinstance(event_dict, dict) else None),
        "market_id": market.get("id") if isinstance(market, dict) else None,
        "condition_id": str(market.get("conditionId") or market.get("condition_id") or ""),
        "closed": closed,
        "active": market.get("active") if isinstance(market, dict) else None,
        "raw": market,
    }
