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
    """Live CLOB executor: limit-only entries, idempotent, self-canceling.

    Hard rules (paper-to-live scaffold):
      - double gate re-checked here at construction AND on every submit
      - one order per trade id, ever — a restart replays nothing
      - limit-only, canceled after ``cancel_after_seconds`` unfilled
      - every transition journaled before/after the network call so the
        ledger of live actions survives a crash at any point
    """

    def __init__(
        self,
        orders_path: Path,
        *,
        env: Mapping[str, str] | None = None,
        cancel_after_seconds: float = 60.0,
        min_order_usd: float = 5.0,
        now_fn=time.time,
        client: Any | None = None,
    ) -> None:
        if not is_live(env):
            raise RuntimeError(
                "LiveClobExecutor requires POLYMARKET_PHASE=live AND LIVETRADE_ENABLED=true"
            )
        self.env = env
        from .live_clob import LiveClobClient

        self._client = client or LiveClobClient.from_env(env)
        self.orders_path = orders_path
        self.cancel_after_seconds = cancel_after_seconds
        self.min_order_usd = min_order_usd
        self.now_fn = now_fn
        self._orders: dict[str, dict[str, Any]] = {}
        self._load()

    def _load(self) -> None:
        try:
            raw = json.loads(self.orders_path.read_text())
        except (OSError, ValueError):
            self._orders = {}
            return
        self._orders = {k: v for k, v in raw.items() if isinstance(v, dict)} if isinstance(raw, dict) else {}

    def _save(self) -> None:
        self.orders_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.orders_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self._orders, indent=1, sort_keys=True))
        tmp.replace(self.orders_path)

    def on_entry(self, entry_row: dict[str, Any], stake_usd: float) -> dict[str, Any]:
        if not is_live(self.env):
            raise RuntimeError("live gates closed at submit time — refusing order")
        order = build_order(entry_row, stake_usd)
        cid = order["client_order_id"]
        if not cid:
            raise ValueError("entry row missing trade_id for client_order_id")
        existing = self._orders.get(cid)
        if existing:
            # Idempotent: a repeated entry for the same trade never resubmits.
            return existing
        if float(stake_usd) < self.min_order_usd:
            record = {**order, "state": "skipped_below_min", "ts_epoch": self.now_fn()}
            self._orders[cid] = record
            self._save()
            return record
        price = float(order["limit_price"])
        size_shares = float(stake_usd) / price if price > 0 else 0.0
        record: dict[str, Any] = {
            **order,
            "size_shares": size_shares,
            "state": "submitting",
            "submitted_epoch": self.now_fn(),
        }
        self._orders[cid] = record  # journaled BEFORE the network call
        self._save()
        placed = self._client.place_limit_buy(
            token_id=order["token"], price=price, size_shares=size_shares
        )
        record.update(
            {
                "state": "open" if placed.ok else "submit_failed",
                "order_id": placed.order_id,
                "submit_status": placed.status,
                "submit_error": placed.error,
                "submit_response_epoch": self.now_fn(),
            }
        )
        self._save()
        return record

    def housekeep(self) -> list[dict[str, Any]]:
        """Poll open orders: journal fills, cancel anything past its window.

        Called once per follower cycle; returns rows for the live ledger.
        """
        rows: list[dict[str, Any]] = []
        now = self.now_fn()
        for cid, record in list(self._orders.items()):
            if record.get("state") != "open" or not record.get("order_id"):
                continue
            try:
                info = self._client.get_order(str(record["order_id"]))
            except Exception as exc:
                rows.append(
                    {"type": "live_order_poll_error", "client_order_id": cid, "error": type(exc).__name__}
                )
                continue
            status = str(info.get("status") or "").upper()
            filled_size = float(info.get("size_matched") or info.get("filled_size") or 0.0)
            if status in {"MATCHED", "FILLED"} or filled_size >= float(record.get("size_shares") or 0) > 0:
                record["state"] = "filled"
                record["filled_epoch"] = now
                record["filled_size"] = filled_size
                rows.append(
                    {
                        "type": "live_fill",
                        "client_order_id": cid,
                        "order_id": record.get("order_id"),
                        "token": record.get("token"),
                        "limit_price": record.get("limit_price"),
                        "filled_size": filled_size,
                        "sim_fill_price": record.get("limit_price"),
                    }
                )
                self._save()
            elif now - float(record.get("submitted_epoch") or now) > self.cancel_after_seconds:
                canceled = self._client.cancel(str(record["order_id"]))
                record["state"] = "canceled" if canceled else "cancel_failed"
                record["canceled_epoch"] = now
                rows.append(
                    {
                        "type": "live_cancel",
                        "client_order_id": cid,
                        "order_id": record.get("order_id"),
                        "canceled": canceled,
                        "filled_size": filled_size or None,
                    }
                )
                self._save()
        return rows
