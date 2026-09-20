"""Paper gates: opposite-leg de-dupe, TOB fill cap, leaky depth, live inert."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from polymarket_bot.archive_config import ArchiveConfig
from polymarket_bot.live_executor import LiveClobExecutor, QuoteOnlyExecutor, is_live
from polymarket_bot.paper_follower import (
    PaperConfig,
    PaperFollowerDaemon,
    iter_new_onchain_fills,
    opposite_leg_open,
    reject_reasons,
    simulate_fill,
    tob_fill_notional,
)


def _cfg(paper: Path, **kwargs) -> PaperConfig:
    defaults = dict(
        paper_dir=paper,
        ledger_path=paper / "ledger.jsonl",
        state_path=paper / "state.json",
        allowlist_path=paper / "allowlist.json",
        data_quality_path=paper / "data_quality.json",
        max_ws_age_seconds=10**9,
        score_ratchet_enabled=False,
        fill_shadow_enabled=False,
        live_quotes_enabled=True,
    )
    defaults.update(kwargs)
    cfg = PaperConfig(**defaults)
    paper.mkdir(parents=True, exist_ok=True)
    cfg.allowlist_path.write_text(json.dumps({"wallets": ["0xw", "0xother"]}))
    return cfg


def _acfg(root: Path) -> ArchiveConfig:
    archive = root / "archive"
    archive.mkdir(parents=True, exist_ok=True)
    return ArchiveConfig(
        archive_dir=archive,
        state_path=root / "shadow_state.json",
        followup_queue_path=archive / "followups.json",
    )


def _book(*, ask: float = 0.50, ask_size: float = 1000.0, extra_asks: list | None = None) -> dict:
    asks = [{"price": ask, "size": ask_size}]
    if extra_asks:
        asks.extend(extra_asks)
    return {
        "token_id": "tok",
        "best_bid": ask - 0.01,
        "best_ask": ask,
        "best_bid_size": 1000.0,
        "best_ask_size": ask_size,
        "spread": 0.01,
        "top3_bids": [{"price": ask - 0.01, "size": 1000.0}],
        "top3_asks": asks,
    }


def _buy(
    *,
    wallet: str = "0xw",
    token: str = "tok-yes",
    condition_id: str = "0xabc",
    trade_id: str = "t1",
    book: dict | None = None,
    price: float = 0.50,
) -> dict:
    return {
        "ts": "2026-09-19T12:00:00+00:00",
        "fill_timestamp": "2026-09-19T12:00:00+00:00",
        "wallet": wallet,
        "trade_id": trade_id,
        "fill_side": "BUY",
        "fill_price": price,
        "trade": {
            "asset": token,
            "side": "BUY",
            "price": price,
            "conditionId": condition_id,
        },
        "book_at_detection": book or _book(),
    }


def test_opposite_leg_rejected_when_other_token_open(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path / "paper")
    acfg = _acfg(tmp_path)
    state = {
        "positions": {
            "0xw:tok-yes": {
                "wallet": "0xw",
                "token": "tok-yes",
                "condition_id": "0xabc",
            }
        }
    }
    reasons = reject_reasons(
        _buy(token="tok-no", trade_id="t-no"),
        cfg,
        acfg,
        state,
        ws_age_seconds=0,
        inside_gap=False,
    )
    assert "opposite_leg" in reasons
    assert opposite_leg_open(state, "0xabc", "tok-no") is True


def test_opposite_leg_gate_is_configurable_off(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path / "paper", block_opposite_leg=False)
    acfg = _acfg(tmp_path)
    state = {
        "positions": {
            "0xw:tok-yes": {
                "wallet": "0xw",
                "token": "tok-yes",
                "condition_id": "0xabc",
            }
        }
    }
    reasons = reject_reasons(
        _buy(token="tok-no"),
        cfg,
        acfg,
        state,
        ws_age_seconds=0,
        inside_gap=False,
    )
    assert "opposite_leg" not in reasons


def test_same_side_other_wallet_is_not_opposite_leg(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path / "paper")
    acfg = _acfg(tmp_path)
    state = {
        "positions": {
            "0xw:tok-yes": {
                "wallet": "0xw",
                "token": "tok-yes",
                "condition_id": "0xabc",
            }
        }
    }
    reasons = reject_reasons(
        _buy(wallet="0xother", token="tok-yes", trade_id="t2"),
        cfg,
        acfg,
        state,
        ws_age_seconds=0,
        inside_gap=False,
    )
    assert "opposite_leg" not in reasons
    assert "duplicate" not in reasons


def test_missing_condition_id_fails_open(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path / "paper")
    acfg = _acfg(tmp_path)
    state = {
        "positions": {
            "0xw:tok-yes": {"wallet": "0xw", "token": "tok-yes", "condition_id": "0xabc"}
        }
    }
    row = _buy(token="tok-no")
    row["trade"].pop("conditionId")
    reasons = reject_reasons(row, cfg, acfg, state, ws_age_seconds=0, inside_gap=False)
    assert "opposite_leg" not in reasons


def test_process_fill_blocks_second_leg_and_stores_condition_id(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path / "paper")
    daemon = PaperFollowerDaemon(cfg, _acfg(tmp_path))
    first = daemon.process_fill(_buy(token="tok-yes", trade_id="yes1"), 0)
    entry = next(r for r in first if r["type"] == "entry")
    assert entry["condition_id"] == "0xabc"
    assert daemon.state["positions"]["0xw:tok-yes"]["condition_id"] == "0xabc"
    second = daemon.process_fill(_buy(token="tok-no", trade_id="no1"), 1)
    reject = next(r for r in second if r["type"] == "reject")
    assert "opposite_leg" in reject["reject_reason"]
    assert "0xw:tok-no" not in daemon.state["positions"]


def test_thin_tob_fat_top3_is_illiquid_depth(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path / "paper")
    acfg = _acfg(tmp_path)
    # Best ask cannot cover $100; levels 2-3 would have (the old leak).
    book = _book(
        ask=0.10,
        ask_size=200.0,
        extra_asks=[{"price": 0.11, "size": 5000.0}, {"price": 0.12, "size": 5000.0}],
    )
    assert tob_fill_notional(book, "BUY", cfg.haircut) < cfg.stake_usd
    reasons = reject_reasons(
        _buy(book=book, price=0.10),
        cfg,
        acfg,
        {"positions": {}},
        ws_age_seconds=0,
        inside_gap=False,
    )
    assert "illiquid_depth" in reasons


def test_tob_depth_gate_can_be_disabled(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path / "paper", min_tob_liquidity_multiple=0.0)
    acfg = _acfg(tmp_path)
    book = _book(
        ask=0.10,
        ask_size=200.0,
        extra_asks=[{"price": 0.11, "size": 5000.0}, {"price": 0.12, "size": 5000.0}],
    )
    reasons = reject_reasons(
        _buy(book=book, price=0.10),
        cfg,
        acfg,
        {"positions": {}},
        ws_age_seconds=0,
        inside_gap=False,
    )
    assert "illiquid_depth" not in reasons


def test_simulate_fill_does_not_walk_past_tob() -> None:
    book = _book(
        ask=0.10,
        ask_size=200.0,
        extra_asks=[{"price": 0.11, "size": 5000.0}],
    )
    price, shares, err = simulate_fill(book, "BUY", 100.0, 0.01)
    assert price is None
    assert shares == 0.0
    assert err == "insufficient_depth"
    walked, walked_shares, walk_err = simulate_fill(
        book, "BUY", 100.0, 0.01, max_levels=None
    )
    assert walk_err is None
    assert walked_shares > 0
    assert walked > 0.10


def test_paper_config_cannot_construct_live_executor(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("POLYMARKET_PHASE", raising=False)
    monkeypatch.delenv("LIVETRADE_ENABLED", raising=False)
    assert is_live() is False
    with pytest.raises(RuntimeError, match="POLYMARKET_PHASE"):
        LiveClobExecutor(tmp_path / "orders.json", env={}, client=object())
    # A single env flag is not enough — config alone cannot arm the lane.
    with pytest.raises(RuntimeError, match="POLYMARKET_PHASE"):
        LiveClobExecutor(
            tmp_path / "orders.json",
            env={"LIVETRADE_ENABLED": "true"},
            client=object(),
        )
    with pytest.raises(RuntimeError, match="POLYMARKET_PHASE"):
        LiveClobExecutor(
            tmp_path / "orders.json",
            env={"POLYMARKET_PHASE": "live"},
            client=object(),
        )


def test_paper_follower_stays_on_quote_only_executor(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("POLYMARKET_PHASE", raising=False)
    monkeypatch.delenv("LIVETRADE_ENABLED", raising=False)
    daemon = PaperFollowerDaemon(_cfg(tmp_path / "paper"), _acfg(tmp_path))
    assert is_live() is False
    assert isinstance(daemon._executor, QuoteOnlyExecutor)
    assert not isinstance(daemon._executor, LiveClobExecutor)


def test_systemd_and_deploy_units_do_not_arm_live() -> None:
    root = Path(__file__).resolve().parents[1]
    paths = list((root / "systemd").glob("*")) + list((root / "deploy").glob("*"))
    assert paths, "expected checked-in unit files"
    for path in paths:
        if not path.is_file():
            continue
        text = path.read_text()
        assert "LIVETRADE_ENABLED=true" not in text, path.name
        assert "POLYMARKET_PHASE=live" not in text, path.name


def test_iter_new_onchain_fills_resets_offset_after_rotation(tmp_path: Path) -> None:
    path = tmp_path / "shadow_onchain.jsonl"
    path.write_bytes(b"x" * 2048)
    fills, offset = iter_new_onchain_fills(path, 2048)
    assert fills == []
    assert offset == 2048
    # Cleanup rename+recreate: live file shrinks. Offset must reset, not replay.
    path.write_text("")
    fills, offset = iter_new_onchain_fills(path, 2048)
    assert fills == []
    assert offset == 0
