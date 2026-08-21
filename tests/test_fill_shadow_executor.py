"""Tests for the fill-attainability shadow and quote-only executor."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from polymarket_bot.fill_shadow import FillShadow
from polymarket_bot.live_executor import (
    LiveClobExecutor,
    QuoteOnlyExecutor,
    build_order,
    is_live,
)
from polymarket_bot.paper_follower import simulate_fill


def _book(ask: float = 0.50) -> dict:
    return {
        "token_id": "token-1",
        "best_bid": ask - 0.01,
        "best_ask": ask,
        "best_bid_size": 1000.0,
        "best_ask_size": 1000.0,
        "spread": 0.01,
        "top3_bids": [{"price": ask - 0.01, "size": 1000.0}],
        "top3_asks": [{"price": ask, "size": 1000.0}],
    }


def _entry(sim_price: float | None = None) -> dict:
    if sim_price is None:
        # Mirror production: the sim price is simulate_fill's output on the
        # detection-time book (additive haircut: ask 0.50 -> 0.505).
        sim_price = simulate_fill(_book(0.50), "BUY", 100.0, 0.005)[0]
    return {
        "ts": "2026-08-21T12:00:00+00:00",
        "type": "entry",
        "trade_id": "0xabc:17",
        "token": "token-1",
        "wallet": "0xw",
        "sim_fill_price": sim_price,
        "sim_size": 199.0,
    }


def _shadow(tmp_path: Path, book: dict | None, *, now: list[float]) -> FillShadow:
    return FillShadow(
        tmp_path / "pending.json",
        book_fetcher=lambda token: book,
        fill_simulator=simulate_fill,
        haircut=0.005,
        stake_usd=100.0,
        offsets_seconds=(60, 300),
        now_fn=lambda: now[0],
    )


def test_attainable_when_price_holds(tmp_path: Path) -> None:
    now = [1000.0]
    shadow = _shadow(tmp_path, _book(0.50), now=now)
    shadow.schedule(_entry())
    assert shadow.run_due() == []  # nothing due yet
    now[0] += 61
    rows = shadow.run_due()
    assert len(rows) == 1
    assert rows[0]["type"] == "fill_check"
    assert rows[0]["offset_s"] == 60
    assert rows[0]["attainable"] is True
    assert rows[0]["price_now"] <= rows[0]["sim_fill_price"]
    now[0] += 300
    rows = shadow.run_due()
    assert len(rows) == 1 and rows[0]["offset_s"] == 300
    assert len(shadow) == 0  # fully processed


def test_unattainable_when_price_runs_away(tmp_path: Path) -> None:
    now = [1000.0]
    shadow = _shadow(tmp_path, _book(0.60), now=now)  # ask jumped
    shadow.schedule(_entry())
    now[0] += 61
    rows = shadow.run_due()
    assert rows[0]["attainable"] is False
    assert rows[0]["price_now"] > rows[0]["sim_fill_price"]


def test_book_unavailable_marked_not_attainable(tmp_path: Path) -> None:
    now = [1000.0]
    shadow = _shadow(tmp_path, None, now=now)
    shadow.schedule(_entry())
    now[0] += 61
    rows = shadow.run_due()
    assert rows[0]["attainable"] is False
    assert rows[0]["error"] == "book_unavailable"


def test_pending_survives_restart(tmp_path: Path) -> None:
    now = [1000.0]
    shadow = _shadow(tmp_path, _book(0.50), now=now)
    shadow.schedule(_entry())
    # Simulate restart: new instance reads the pending file.
    shadow2 = _shadow(tmp_path, _book(0.50), now=now)
    assert len(shadow2) == 1
    now[0] += 61
    rows = shadow2.run_due()
    assert len(rows) == 1 and rows[0]["attainable"] is True


def test_duplicate_schedule_ignored(tmp_path: Path) -> None:
    now = [1000.0]
    shadow = _shadow(tmp_path, _book(0.50), now=now)
    shadow.schedule(_entry())
    shadow.schedule(_entry())
    assert len(shadow) == 1


def test_disabled_shadow_is_inert(tmp_path: Path) -> None:
    now = [1000.0]
    shadow = _shadow(tmp_path, _book(0.50), now=now)
    shadow.enabled = False
    shadow.schedule(_entry())
    now[0] += 400
    assert shadow.run_due() == []
    assert len(shadow) == 0


def test_is_live_double_gate() -> None:
    assert is_live({}) is False
    assert is_live({"POLYMARKET_PHASE": "live"}) is False
    assert is_live({"LIVETRADE_ENABLED": "true"}) is False
    assert is_live({"POLYMARKET_PHASE": "live", "LIVETRADE_ENABLED": "true"}) is True
    assert is_live({"POLYMARKET_PHASE": "paper", "LIVETRADE_ENABLED": "true"}) is False


def test_build_order_uses_durable_id_and_sim_price() -> None:
    entry = _entry()
    order = build_order(entry, 100.0)
    assert order["client_order_id"] == "0xabc:17"
    assert order["limit_price"] == entry["sim_fill_price"]
    assert order["side"] == "BUY"
    assert order["size_usd"] == 100.0
    with pytest.raises(ValueError):
        build_order({"trade_id": "x", "token": "t"}, 100.0)


def test_quote_only_executor_journals_without_network(tmp_path: Path) -> None:
    ex = QuoteOnlyExecutor(tmp_path / "quotes.jsonl", now_fn=lambda: 1234.0)
    row = ex.on_entry(_entry(), 100.0)
    assert row["mode"] == "quote_only"
    on_disk = [json.loads(line) for line in (tmp_path / "quotes.jsonl").read_text().splitlines()]
    assert len(on_disk) == 1
    assert on_disk[0]["client_order_id"] == "0xabc:17"
    assert on_disk[0]["ts_epoch"] == 1234.0


def test_live_executor_fails_closed() -> None:
    with pytest.raises(RuntimeError, match="POLYMARKET_PHASE"):
        LiveClobExecutor(env={})
    with pytest.raises(RuntimeError, match="POLYMARKET_PHASE"):
        LiveClobExecutor(env={"POLYMARKET_PHASE": "live"})
    with pytest.raises(RuntimeError, match="API_KEY"):
        LiveClobExecutor(env={"POLYMARKET_PHASE": "live", "LIVETRADE_ENABLED": "true"})
