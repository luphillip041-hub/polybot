"""Live CLOB executor: gates, idempotency, housekeep (mocked client)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from polymarket_bot.live_clob import PlacedOrder
from polymarket_bot.live_executor import LiveClobExecutor

LIVE_ENV = {"POLYMARKET_PHASE": "live", "LIVETRADE_ENABLED": "true"}


class FakeClient:
    def __init__(self) -> None:
        self.placed: list[dict] = []
        self.canceled: list[str] = []
        self.statuses: dict[str, dict] = {}

    def place_limit_buy(self, *, token_id, price, size_shares) -> PlacedOrder:
        self.placed.append({"token_id": token_id, "price": price, "size": size_shares})
        oid = f"order-{len(self.placed)}"
        self.statuses[oid] = {"status": "LIVE", "size_matched": 0.0}
        return PlacedOrder(ok=True, order_id=oid, status="LIVE")

    def get_order(self, order_id):
        return self.statuses[order_id]

    def cancel(self, order_id):
        self.canceled.append(order_id)
        return True


def _entry(sim_price: float = 0.51) -> dict:
    return {
        "trade_id": "0xabc:17",
        "token": "token-1",
        "wallet": "0xw",
        "sim_fill_price": sim_price,
        "sim_size": 196.0,
    }


def _executor(tmp_path: Path, client: FakeClient, *, now: list[float]) -> LiveClobExecutor:
    return LiveClobExecutor(
        tmp_path / "orders.json",
        env=LIVE_ENV,
        client=client,
        cancel_after_seconds=60.0,
        now_fn=lambda: now[0],
    )


def test_gates_required_even_with_client(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError, match="POLYMARKET_PHASE"):
        LiveClobExecutor(tmp_path / "o.json", env={}, client=FakeClient())


def test_missing_private_key_fails_closed(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError, match="PRIVATE_KEY"):
        LiveClobExecutor(tmp_path / "o.json", env=LIVE_ENV)  # no client -> from_env


def test_submit_places_and_journals(tmp_path: Path) -> None:
    client = FakeClient()
    now = [1000.0]
    ex = _executor(tmp_path, client, now=now)
    record = ex.on_entry(_entry(), 10.0)
    assert record["state"] == "open"
    assert record["order_id"] == "order-1"
    assert client.placed[0]["price"] == 0.51
    assert client.placed[0]["size"] == pytest.approx(10.0 / 0.51)
    # journaled on disk
    on_disk = json.loads((tmp_path / "orders.json").read_text())
    assert on_disk["0xabc:17"]["state"] == "open"


def test_idempotent_across_calls_and_restart(tmp_path: Path) -> None:
    client = FakeClient()
    now = [1000.0]
    ex = _executor(tmp_path, client, now=now)
    ex.on_entry(_entry(), 10.0)
    again = ex.on_entry(_entry(), 10.0)  # same trade id
    assert len(client.placed) == 1
    assert again["order_id"] == "order-1"
    # simulate restart: new executor instance reads the journal
    ex2 = _executor(tmp_path, client, now=now)
    ex2.on_entry(_entry(), 10.0)
    assert len(client.placed) == 1


def test_below_min_stake_never_submits(tmp_path: Path) -> None:
    client = FakeClient()
    now = [1000.0]
    ex = _executor(tmp_path, client, now=now)
    record = ex.on_entry(_entry(), 2.0)
    assert record["state"] == "skipped_below_min"
    assert client.placed == []


def test_submit_rechecks_gates_every_time(tmp_path: Path) -> None:
    client = FakeClient()
    now = [1000.0]
    ex = _executor(tmp_path, client, now=now)
    ex.env = {"POLYMARKET_PHASE": "live"}  # one gate dropped
    with pytest.raises(RuntimeError, match="gates closed"):
        ex.on_entry(_entry(), 10.0)
    assert client.placed == []


def test_housekeep_journals_fill(tmp_path: Path) -> None:
    client = FakeClient()
    now = [1000.0]
    ex = _executor(tmp_path, client, now=now)
    ex.on_entry(_entry(), 10.0)
    client.statuses["order-1"] = {"status": "MATCHED", "size_matched": 19.6}
    rows = ex.housekeep()
    assert rows[0]["type"] == "live_fill"
    assert rows[0]["filled_size"] == 19.6
    assert ex._orders["0xabc:17"]["state"] == "filled"


def test_housekeep_cancels_after_window(tmp_path: Path) -> None:
    client = FakeClient()
    now = [1000.0]
    ex = _executor(tmp_path, client, now=now)
    ex.on_entry(_entry(), 10.0)
    now[0] += 61  # past cancel window, still unfilled
    rows = ex.housekeep()
    assert rows[0]["type"] == "live_cancel"
    assert client.canceled == ["order-1"]
    assert ex._orders["0xabc:17"]["state"] == "canceled"


def test_housekeep_touches_nothing_when_order_live_and_young(tmp_path: Path) -> None:
    client = FakeClient()
    now = [1000.0]
    ex = _executor(tmp_path, client, now=now)
    ex.on_entry(_entry(), 10.0)
    now[0] += 10
    assert ex.housekeep() == []
    assert ex._orders["0xabc:17"]["state"] == "open"
