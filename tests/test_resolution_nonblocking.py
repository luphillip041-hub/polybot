"""Non-blocking resolution cycle + stale-fill book-fetch skip (Aug 24 stall fixes)."""

from __future__ import annotations

import json
import tempfile
import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

from polymarket_bot.archive_config import ArchiveConfig
from polymarket_bot.paper_follower import (
    PaperConfig,
    PaperFollowerDaemon,
    _fill_is_stale,
    read_jsonl,
)
from tests.test_onchain_primary_follower import _book, _configs, _lane


def _daemon(tmp_path: Path) -> PaperFollowerDaemon:
    cfg = PaperConfig(
        paper_dir=tmp_path / "paper",
        ledger_path=tmp_path / "paper" / "ledger.jsonl",
        state_path=tmp_path / "paper" / "state.json",
        allowlist_path=tmp_path / "paper" / "allowlist.json",
        data_quality_path=tmp_path / "paper" / "data_quality.json",
        resolution_poll_seconds=999999,
    )
    (tmp_path / "paper").mkdir(exist_ok=True)
    cfg.allowlist_path.write_text(json.dumps({"wallets": ["0xw"]}))
    acfg = ArchiveConfig(
        archive_dir=tmp_path / "archive",
        state_path=tmp_path / "state.json",
        followup_queue_path=tmp_path / "followups.json",
    )
    return PaperFollowerDaemon(cfg, acfg)


def test_slow_resolution_check_does_not_block_loop(tmp_path: Path) -> None:
    daemon = _daemon(tmp_path)
    started = threading.Event()

    def slow_check(state, **_kwargs):
        started.set()
        time.sleep(2.0)
        return []

    with patch("polymarket_bot.paper_follower.check_positions_for_resolution", side_effect=slow_check) as m:
        t0 = time.time()
        assert daemon.process_resolution_once(force=True) is None
        assert time.time() - t0 < 1.0  # kicked, not blocked
        assert started.wait(timeout=2)
        # Second kick while the first is still running must not double-fire
        assert daemon.process_resolution_once(force=True) is None
        assert m.call_count == 1
        daemon._resolution_thread.join(timeout=10)
    summary = daemon.process_resolution_once()
    assert summary is not None and summary["checked"] == 0 and "error" not in summary


def test_failed_resolution_check_harvests_error_summary(tmp_path: Path) -> None:
    daemon = _daemon(tmp_path)
    with patch(
        "polymarket_bot.paper_follower.check_positions_for_resolution",
        side_effect=RuntimeError("rpc down"),
    ):
        daemon.process_resolution_once(force=True)
        daemon._resolution_thread.join(timeout=10)
    summary = daemon.process_resolution_once()
    assert summary is not None and summary.get("error") is True


def test_harvest_applies_resolution_rows(tmp_path: Path) -> None:
    daemon = _daemon(tmp_path)
    daemon.state["positions"]["0xw:winner"] = {
        "wallet": "0xw",
        "token": "winner_token",
        "entry_price": 0.50,
        "shares": 200.0,
        "cost_usd": 100.0,
    }
    action = {
        "action": "resolve",
        "pos_id": "0xw:winner",
        "exit_price": 1.0,
        "question": "Q?",
        "side": "PRIMARY",
        "market_id": "m1",
    }
    with patch(
        "polymarket_bot.paper_follower.check_positions_for_resolution",
        return_value=[action],
    ):
        daemon.process_resolution_once(force=True)
        daemon._resolution_thread.join(timeout=10)
        summary = daemon.process_resolution_once()
    assert summary is not None and summary["resolved"] == 1
    assert "0xw:winner" not in daemon.state["positions"]
    rows = [r for r in read_jsonl(daemon.cfg.ledger_path) if r.get("type") == "resolution"]
    assert len(rows) == 1 and rows[0]["pnl"] == 100.0


def test_fill_is_stale_timestamp_only() -> None:
    old = (datetime.now(UTC) - timedelta(hours=1)).isoformat()
    fresh = datetime.now(UTC).isoformat()
    assert _fill_is_stale({"fill_timestamp": old}, 120.0) is True
    assert _fill_is_stale({"fill_timestamp": fresh}, 120.0) is False
    assert _fill_is_stale({"trade": {"timestamp": old}}, 120.0) is True
    assert _fill_is_stale({}, 120.0) is False  # unknown age -> fetch (conservative)


def test_process_once_skips_book_fetch_for_stale_fills(tmp_path: Path, monkeypatch) -> None:
    import polymarket_bot.paper_follower as follower

    cfg, acfg = _configs(tmp_path)
    cfg.onchain_log_path.parent.mkdir()
    stale_lane = _lane()
    stale_lane["durable_trade_id"] = "0x" + "cd" * 32 + ":99"
    stale_lane["transaction_hash"] = "0x" + "cd" * 32
    stale_lane["ground_truth_ts"] = (datetime.now(UTC) - timedelta(hours=2)).isoformat()
    stale_lane["ground_truth_epoch"] = (datetime.now(UTC) - timedelta(hours=2)).timestamp()
    cfg.onchain_log_path.write_text(
        json.dumps(_lane()) + "\n" + json.dumps(stale_lane) + "\n"
    )
    cfg.state_path.write_text(
        json.dumps({"processed_trade_ids": [], "positions": {}, "onchain_log_offset": 0})
    )
    (acfg.archive_dir / "heartbeat_latest.json").write_text(
        json.dumps({"last_ws_message_ts": datetime.now(UTC).isoformat()})
    )
    fetch_calls: list[str] = []

    def fake_book(token: str, *, use_cache: bool = True):
        fetch_calls.append(token)
        return _book()

    monkeypatch.setattr(follower, "live_clob_book", fake_book)
    daemon = PaperFollowerDaemon(cfg, acfg)
    daemon.process_once()
    rows = read_jsonl(cfg.ledger_path)
    rejects = [r for r in rows if r.get("type") == "reject"]
    entries = [r for r in rows if r.get("type") == "entry"]
    assert len(fetch_calls) == 1  # only the fresh fill fetched a book
    assert len(entries) == 1
    assert any("stale_fill" in str(r.get("reject_reason")) for r in rejects)
