from __future__ import annotations

from typing import Any

from .config import CONFIG, BotConfig
from .http import ApiError, get_json


def order_book(token_id: str, config: BotConfig = CONFIG) -> dict[str, Any]:
    return get_json(config.clob_base, "/book", {"token_id": token_id}, user_agent=config.user_agent)


def best_bid_ask(token_id: str, config: BotConfig = CONFIG) -> dict[str, Any]:
    try:
        book = order_book(token_id, config)
        bids = book.get("bids") or []
        asks = book.get("asks") or []
        best_bid = float(bids[0]["price"]) if bids else None
        best_ask = float(asks[0]["price"]) if asks else None
        return {
            "ok": True,
            "best_bid": best_bid,
            "best_ask": best_ask,
            "spread": (best_ask - best_bid) if best_bid is not None and best_ask is not None else None,
            "tick_size": book.get("tick_size"),
            "min_order_size": book.get("min_order_size"),
            "raw_error": None,
        }
    except (ApiError, Exception) as exc:
        return {"ok": False, "best_bid": None, "best_ask": None, "spread": None, "tick_size": None, "min_order_size": None, "raw_error": str(exc)}
