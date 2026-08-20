"""Tests for the daily-cap score ratchet in the paper follower."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from polymarket_bot.archive_config import ArchiveConfig
from polymarket_bot.paper_follower import (
    PaperConfig,
    PaperFollowerDaemon,
    ratchet_min_score,
)


def _configs(tmp_path: Path, *, max_signals_per_day: int = 10) -> tuple[PaperConfig, ArchiveConfig]:
    paper_dir = tmp_path / "paper"
    archive_dir = tmp_path / "book_archive"
    paper_dir.mkdir()
    archive_dir.mkdir()
    cfg = PaperConfig(
        root=tmp_path,
        paper_dir=paper_dir,
        ledger_path=paper_dir / "ledger.jsonl",
        state_path=paper_dir / "state.json",
        allowlist_path=paper_dir / "allowlist.json",
        data_quality_path=paper_dir / "data_quality.json",
        onchain_log_path=tmp_path / "onchain" / "shadow_onchain.jsonl",
        onchain_primary_enabled=True,
        max_signals_per_day=max_signals_per_day,
    )
    acfg = ArchiveConfig(
        archive_dir=archive_dir,
        state_path=tmp_path / "shadow_state.json",
        followup_queue_path=archive_dir / "followups.json",
    )
    return cfg, acfg


def _book() -> dict:
    return {
        "token_id": "token-1",
        "best_bid": 0.49,
        "best_ask": 0.50,
        "best_bid_size": 1000.0,
        "best_ask_size": 1000.0,
        "spread": 0.01,
        "top3_bids": [{"price": 0.49, "size": 1000.0}],
        "top3_asks": [{"price": 0.50, "size": 1000.0}],
        "_source": "clob_rest_live",
    }


def _fill(wallet: str, trade_id: str) -> dict:
    now = datetime.now(UTC).isoformat()
    return {
        "type": "fill",
        "ts": now,
        "fill_timestamp": now,
        "wallet": wallet,
        "trade_id": trade_id,
        "fill_side": "BUY",
        "fill_price": 0.50,
        "trade": {
            "transactionHash": trade_id,
            "asset": "token-1",
            "side": "BUY",
            "price": 0.50,
            "timestamp": now,
        },
        "book_at_detection": _book(),
        "detection_source": "polygon_onchain",
    }


def _daemon(tmp_path: Path, scores: dict[str, float], *, max_signals_per_day: int = 10) -> PaperFollowerDaemon:
    cfg, acfg = _configs(tmp_path, max_signals_per_day=max_signals_per_day)
    cfg.state_path.write_text(json.dumps({"processed_trade_ids": [], "positions": {}}))
    (acfg.archive_dir / "heartbeat_latest.json").write_text(
        json.dumps({"last_ws_message_ts": datetime.now(UTC).isoformat()})
    )
    daemon = PaperFollowerDaemon(cfg, acfg)
    daemon._wallet_scores = scores
    daemon._cycle_ws_age_seconds = 0.0
    return daemon


def test_ratchet_min_score_boundaries() -> None:
    assert ratchet_min_score(0, 300) == 0.0
    assert ratchet_min_score(149, 300) == 0.0
    assert ratchet_min_score(150, 300) == 40.0
    assert ratchet_min_score(224, 300) == 40.0
    assert ratchet_min_score(225, 300) == 60.0
    assert ratchet_min_score(269, 300) == 60.0
    assert ratchet_min_score(270, 300) == 80.0
    assert ratchet_min_score(300, 300) == 80.0
    assert ratchet_min_score(5, 0) == 0.0


def test_below_half_cap_accepts_any_wallet(tmp_path: Path) -> None:
    daemon = _daemon(tmp_path, {"0xweak": 10.0})
    rows = daemon.process_fill(_fill("0xweak", "0xaaa"), accepts_today=4)
    entry = [r for r in rows if r["type"] == "entry"]
    assert len(entry) == 1
    assert entry[0]["quality_score"] == 10.0
    assert entry[0]["ratchet_min_score"] == 0.0


def test_past_half_cap_weak_wallet_rejected_strong_accepted(tmp_path: Path) -> None:
    daemon = _daemon(tmp_path, {"0xweak": 10.0, "0xstrong": 90.0})
    weak = daemon.process_fill(_fill("0xweak", "0xbbb"), accepts_today=6)
    rej = [r for r in weak if r["type"] == "reject"]
    assert len(rej) == 1
    assert rej[0]["reject_reason"] == "score_below_ratchet"
    assert rej[0]["quality_score"] == 10.0
    assert rej[0]["ratchet_min_score"] == 40.0

    strong = daemon.process_fill(_fill("0xstrong", "0xccc"), accepts_today=6)
    entry = [r for r in strong if r["type"] == "entry"]
    assert len(entry) == 1
    assert entry[0]["quality_score"] == 90.0


def test_unknown_wallet_scores_zero(tmp_path: Path) -> None:
    daemon = _daemon(tmp_path, {})
    rows = daemon.process_fill(_fill("0xmystery", "0xddd"), accepts_today=9)
    rej = [r for r in rows if r["type"] == "reject"]
    assert len(rej) == 1
    assert rej[0]["reject_reason"] == "score_below_ratchet"
    assert rej[0]["ratchet_min_score"] == 80.0


def test_ratchet_disabled_preserves_first_come_behavior(tmp_path: Path) -> None:
    daemon = _daemon(tmp_path, {})
    daemon.cfg.score_ratchet_enabled = False
    rows = daemon.process_fill(_fill("0xmystery", "0xeee"), accepts_today=9)
    entry = [r for r in rows if r["type"] == "entry"]
    assert len(entry) == 1
    assert entry[0]["ratchet_min_score"] == 0.0


def test_sells_never_ratcheted(tmp_path: Path) -> None:
    daemon = _daemon(tmp_path, {"0xweak": 0.0})
    sell = _fill("0xweak", "0xfff")
    sell["fill_side"] = "SELL"
    sell["trade"]["side"] = "SELL"
    # No open position -> exit simulates to zero shares, but it must not be
    # rejected for score reasons.
    rows = daemon.process_fill(sell, accepts_today=9)
    assert all(r.get("reject_reason") != "score_below_ratchet" for r in rows)


def test_hard_cap_still_applies(tmp_path: Path) -> None:
    daemon = _daemon(tmp_path, {"0xstrong": 100.0})
    rows = daemon.process_fill(_fill("0xstrong", "0x111"), accepts_today=10)
    rej = [r for r in rows if r["type"] == "reject"]
    assert len(rej) == 1
    assert rej[0]["reject_reason"] == "daily_entry_cap"


def test_wallet_score_refresh_loads_from_ledger(tmp_path: Path) -> None:
    daemon = _daemon(tmp_path, {})
    winner = "0x" + "aa" * 20
    loser = "0x" + "bb" * 20
    rows = []
    for i in range(3):
        rows.append({"ts": "2026-08-01T10:00:00+00:00", "type": "entry", "wallet": winner, "token": f"T{i}", "sim_fill_price": 0.5, "sim_size": 200})
        rows.append({"ts": "2026-08-01T12:00:00+00:00", "type": "exit", "wallet": winner, "token": f"T{i}", "pnl": 500.0})
        rows.append({"ts": "2026-08-01T10:00:00+00:00", "type": "entry", "wallet": loser, "token": f"L{i}", "sim_fill_price": 0.5, "sim_size": 200})
        rows.append({"ts": "2026-08-01T12:00:00+00:00", "type": "exit", "wallet": loser, "token": f"L{i}", "pnl": -90.0})
    with daemon.cfg.ledger_path.open("w") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")
    daemon._refresh_wallet_scores(force=True)
    assert daemon._wallet_scores.get(winner, 0.0) > daemon._wallet_scores.get(loser, 0.0)
    assert daemon._wallet_scores.get(winner, 0.0) > 0.0
