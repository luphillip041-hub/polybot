"""Live-execution boundary for the Polymarket copy bot.

Phase 1 ships the *quote-only* executor: for every accepted paper entry it
derives the exact order we would submit live (limit price, size, idempotent
client order id) and journals it to ``live_quotes.jsonl``.  Nothing is signed,
nothing is sent — there is deliberately no network path in this module.

The live gate is double-keyed, matching the other bots on this desk:
``POLYMARKET_PHASE=live`` AND ``LIVETRADE_ENABLED=true`` must both be set,
and ``is_live()`` is re-checked at every submit attempt, not just at startup.
``LiveClobExecutor`` fails closed: without credentials AND both gates it
raises immediately.  It is not wired into the follower yet.
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Mapping

QUOTE_ONLY = "quote_only"


def is_live(env: Mapping[str, str] | None = None) -> bool:
    """Double-gated live check.  Re-evaluated on every call, never cached."""
    env = env if env is not None else os.environ
    return (
        env.get("POLYMARKET_PHASE", "").strip().lower() == "live"
        and env.get("LIVETRADE_ENABLED", "").strip().lower() == "true"
    )


def build_order(entry_row: dict[str, Any], stake_usd: float) -> dict[str, Any]:
    """Derive the live order we *would* submit for an accepted entry.

    Limit price = the simulated fill price (already includes the slippage
    haircut — it is the worst price we accept).  client_order_id is the
    durable trade id so a restart/retry can never double-submit.
    """
    limit_price = entry_row.get("sim_fill_price")
    if limit_price is None:
        raise ValueError("entry row has no sim_fill_price")
    return {
        "client_order_id": str(entry_row.get("trade_id") or ""),
        "token": str(entry_row.get("token") or ""),
        "side": "BUY",
        "limit_price": float(limit_price),
        "size_usd": float(stake_usd),
        "est_shares": entry_row.get("sim_size"),
        "wallet": entry_row.get("wallet"),
        "order_type": "limit",
        "tif": "GTC_with_60s_cancel",
    }


class QuoteOnlyExecutor:
    """Journals would-be live orders.  No auth, no network, ever."""

    def __init__(self, quotes_path: Path, *, now_fn=time.time) -> None:
        self.quotes_path = quotes_path
        self.now_fn = now_fn

    def on_entry(self, entry_row: dict[str, Any], stake_usd: float) -> dict[str, Any]:
        order = build_order(entry_row, stake_usd)
        row = {
            "type": "live_quote",
            "mode": QUOTE_ONLY,
            "ts_epoch": self.now_fn(),
            **order,
        }
        self.quotes_path.parent.mkdir(parents=True, exist_ok=True)
        with self.quotes_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(row, sort_keys=True) + "\n")
        return row


class LiveClobExecutor:
    """Fail-closed live executor.  Not wired into the follower in Phase 1."""

    def __init__(self, *, env: Mapping[str, str] | None = None) -> None:
        if not is_live(env):
            raise RuntimeError(
                "LiveClobExecutor requires POLYMARKET_PHASE=live AND LIVETRADE_ENABLED=true"
            )
        env = env if env is not None else os.environ
        if not env.get("POLYMARKET_CLOB_API_KEY"):
            raise RuntimeError("LiveClobExecutor requires POLYMARKET_CLOB_API_KEY (fail closed)")
        raise NotImplementedError("live CLOB submission lands in Phase 2")
