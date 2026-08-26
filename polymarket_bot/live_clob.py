"""Thin, fail-closed wrapper around the Polymarket CLOB client.

Only ever constructed inside the double-gated live path
(``POLYMARKET_PHASE=live`` AND ``LIVETRADE_ENABLED=true``).  Every missing
credential is an immediate raise — the executor never degrades into a
half-authenticated state.

Credentials come from the environment only (a chmod-600 env file in
production); nothing secret is ever written to the repo or the ledger.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Mapping

DEFAULT_CLOB_HOST = "https://clob.polymarket.com"
POLYGON_CHAIN_ID = 137


class ClobAuthError(RuntimeError):
    pass


@dataclass
class PlacedOrder:
    ok: bool
    order_id: str | None
    status: str | None
    error: str | None = None
    raw: dict[str, Any] | None = None


class LiveClobClient:
    """Minimal surface: place a limit buy, query an order, cancel an order."""

    def __init__(self, client: Any) -> None:
        self._client = client

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> "LiveClobClient":
        env = env if env is not None else os.environ
        key = (env.get("POLYMARKET_PRIVATE_KEY") or "").strip()
        if not key:
            raise ClobAuthError("POLYMARKET_PRIVATE_KEY missing (fail closed)")
        host = (env.get("POLYMARKET_CLOB_HOST") or DEFAULT_CLOB_HOST).strip()
        signature_type = int((env.get("POLYMARKET_SIGNATURE_TYPE") or "0").strip())
        funder = (env.get("POLYMARKET_FUNDER") or "").strip() or None

        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import ApiCreds

        api_key = (env.get("POLYMARKET_CLOB_API_KEY") or "").strip()
        api_secret = (env.get("POLYMARKET_CLOB_SECRET") or "").strip()
        api_passphrase = (env.get("POLYMARKET_CLOB_PASSPHRASE") or "").strip()

        client = ClobClient(
            host,
            chain_id=POLYGON_CHAIN_ID,
            key=key,
            signature_type=signature_type,
            funder=funder,
        )
        if api_key and api_secret and api_passphrase:
            client.set_api_creds(
                ApiCreds(
                    api_key=api_key,
                    api_secret=api_secret,
                    api_passphrase=api_passphrase,
                )
            )
        else:
            # Derive L2 creds from the L1 key.  Network call, done once at
            # startup; a failure here must kill the live path, not paper it.
            client.set_api_creds(client.create_or_derive_api_creds())
        return cls(client)

    def place_limit_buy(self, *, token_id: str, price: float, size_shares: float) -> PlacedOrder:
        from py_clob_client.clob_types import OrderArgs, OrderType

        price = round(min(0.999, max(0.001, float(price))), 3)
        try:
            resp = self._client.create_and_post_order(
                OrderArgs(
                    token_id=str(token_id),
                    price=price,
                    size=float(size_shares),
                    side="BUY",
                ),
            )
        except Exception as exc:
            return PlacedOrder(ok=False, order_id=None, status=None, error=f"{type(exc).__name__}: {exc}")
        if not isinstance(resp, dict):
            return PlacedOrder(ok=False, order_id=None, status=None, error=f"unexpected_response:{type(resp).__name__}")
        order_id = resp.get("orderID") or resp.get("id")
        status = resp.get("status")
        ok = bool(resp.get("success", order_id)) and not resp.get("errorMsg")
        return PlacedOrder(
            ok=bool(ok and order_id),
            order_id=str(order_id) if order_id else None,
            status=str(status) if status else None,
            error=str(resp.get("errorMsg")) if resp.get("errorMsg") else None,
            raw=resp,
        )

    def get_order(self, order_id: str) -> dict[str, Any]:
        resp = self._client.get_order(order_id)
        return resp if isinstance(resp, dict) else {"raw": resp}

    def cancel(self, order_id: str) -> bool:
        try:
            self._client.cancel(order_id)
            return True
        except Exception:
            return False
