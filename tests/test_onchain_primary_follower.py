from __future__ import annotations

import gzip
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from polymarket_bot.archive_config import ArchiveConfig
from polymarket_bot.paper_follower import PaperConfig, PaperFollowerDaemon


def _lane(source: str = "polygon_onchain") -> dict:
    now = datetime.now(UTC).isoformat()
    return {
        "type": "lane_detection",
        "source": source,
        "durable_trade_id": "0x" + "ab" * 32 + ":17",
        "transaction_hash": "0x" + "ab" * 32,
        "log_index": 17,
        "wallet": "0xwallet",
        "token_id": "token-1",
        "side": "BUY",
        "price": 0.50,
        "size": 250.0,
        "market": {"condition_id": "condition-1"},
        "ground_truth_ts": now,
        "ground_truth_epoch": datetime.now(UTC).timestamp(),
        "detection_ts": now,
        "detection_epoch": datetime.now(UTC).timestamp(),
        "detection_latency_seconds": 8.2,
    }


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


def _configs(tmp_path: Path) -> tuple[PaperConfig, ArchiveConfig]:
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
    )
    acfg = ArchiveConfig(
        archive_dir=archive_dir,
        state_path=tmp_path / "shadow_state.json",
        followup_queue_path=archive_dir / "followups.json",
    )
    return cfg, acfg


def test_lane_detection_converts_to_existing_fill_contract() -> None:
    from polymarket_bot.paper_follower import onchain_lane_to_fill

    fill = onchain_lane_to_fill(_lane())
    assert fill is not None
    assert fill["trade_id"].endswith(":17")
    assert fill["fill_timestamp"] == fill["trade"]["timestamp"]
    assert fill["trade"]["transactionHash"].startswith("0x")
    assert fill["trade"]["logIndex"] == 17
    assert fill["trade"]["asset"] == "token-1"
    assert fill["trade"]["conditionId"] == "condition-1"
    assert fill["detection_source"] == "polygon_onchain"


def test_tail_reader_filters_non_detection_rows_and_preserves_offset(tmp_path: Path) -> None:
    from polymarket_bot.paper_follower import iter_new_onchain_fills

    path = tmp_path / "shadow_onchain.jsonl"
    rows = [_lane(), {"type": "rpc_error", "reason": "test"}, _lane("data_api")]
    path.write_text("".join(json.dumps(row) + "\n" for row in rows))
    fills, offset = iter_new_onchain_fills(path, 0)
    assert [row["detection_source"] for row in fills] == ["polygon_onchain", "data_api"]
    assert offset == path.stat().st_size
    assert iter_new_onchain_fills(path, offset) == ([], offset)


def test_missing_onchain_log_preserves_persisted_offset(tmp_path: Path) -> None:
    from polymarket_bot.paper_follower import iter_new_onchain_fills

    missing = tmp_path / "temporarily-missing.jsonl"
    assert iter_new_onchain_fills(missing, 123456) == ([], 123456)


def test_live_clob_book_normalizes_and_sorts_depth(monkeypatch) -> None:
    from polymarket_bot import clob
    from polymarket_bot.paper_follower import live_clob_book

    monkeypatch.setattr(
        clob,
        "order_book",
        lambda token: {
            "bids": [{"price": "0.47", "size": "10"}, {"price": "0.49", "size": "20"}],
            "asks": [{"price": "0.53", "size": "30"}, {"price": "0.50", "size": "40"}],
        },
    )
    book = live_clob_book("token-1", use_cache=False)
    assert book is not None
    assert book["best_bid"] == 0.49
    assert book["best_ask"] == 0.50
    assert book["top3_bids"][0]["price"] == 0.49
    assert book["top3_asks"][0]["price"] == 0.50
    assert book["_source"] == "clob_rest_live"


def test_onchain_config_loads_fallback_and_integration_flags(monkeypatch) -> None:
    from polymarket_bot.onchain_shadow import OnchainShadowConfig

    monkeypatch.setenv(
        "POLYGON_WSS_FALLBACK_URLS",
        "wss://polygon.drpc.org,wss://backup.example",
    )
    monkeypatch.setenv("ONCHAIN_PAPER_FOLLOWER_INTEGRATED", "true")
    cfg = OnchainShadowConfig.from_env()
    cfg.validate()
    assert cfg.fallback_wss_rpc_urls == (
        "wss://polygon.drpc.org",
        "wss://backup.example",
    )
    assert cfg.paper_follower_integrated is True


def test_onchain_primary_precedes_and_disables_raw_api_hot_path(tmp_path: Path, monkeypatch) -> None:
    import polymarket_bot.paper_follower as follower

    cfg, acfg = _configs(tmp_path)
    cfg.onchain_log_path.parent.mkdir()
    lane = _lane()
    cfg.onchain_log_path.write_text(json.dumps(lane) + "\n")
    cfg.state_path.write_text(json.dumps({"processed_trade_ids": [], "positions": {}, "onchain_log_offset": 0}))
    (acfg.archive_dir / "heartbeat_latest.json").write_text(
        json.dumps({"last_ws_message_ts": datetime.now(UTC).isoformat()})
    )

    # This legacy Data API row represents the same transaction but lacks logIndex.
    api_fill = {
        "type": "fill",
        "ts": lane["detection_ts"],
        "fill_timestamp": lane["ground_truth_ts"],
        "wallet": lane["wallet"],
        "trade_id": lane["transaction_hash"],
        "fill_side": "BUY",
        "fill_price": 0.50,
        "trade": {
            "transactionHash": lane["transaction_hash"],
            "asset": lane["token_id"],
            "side": "BUY",
            "price": 0.50,
            "timestamp": lane["ground_truth_ts"],
        },
        "book_at_detection": _book(),
    }
    archive_path = acfg.archive_dir / f"shadow_{datetime.now(UTC):%Y-%m-%d_%H}.jsonl.gz"
    with gzip.open(archive_path, "wt") as handle:
        handle.write(json.dumps(api_fill) + "\n")

    monkeypatch.setattr(follower, "live_clob_book", lambda token: _book())
    daemon = PaperFollowerDaemon(cfg, acfg)
    wrote = daemon.process_once()
    rows = [json.loads(line) for line in cfg.ledger_path.read_text().splitlines()]
    signals = [row for row in rows if row["type"] == "signal"]
    entries = [row for row in rows if row["type"] == "entry"]

    assert wrote == 2
    assert len(signals) == 1
    assert len(entries) == 1
    assert signals[0]["detection_source"] == "polygon_onchain"
    assert signals[0]["detection_latency_s"] < 15
    assert signals[0]["book_snapshot"]["best_ask"] == 0.50


def test_restart_uses_persisted_offset_and_durable_id_dedup(tmp_path: Path, monkeypatch) -> None:
    import polymarket_bot.paper_follower as follower

    cfg, acfg = _configs(tmp_path)
    cfg.onchain_log_path.parent.mkdir()
    cfg.onchain_log_path.write_text(json.dumps(_lane()) + "\n")
    cfg.state_path.write_text(json.dumps({"processed_trade_ids": [], "positions": {}, "onchain_log_offset": 0}))
    (acfg.archive_dir / "heartbeat_latest.json").write_text(
        json.dumps({"last_ws_message_ts": datetime.now(UTC).isoformat()})
    )
    monkeypatch.setattr(follower, "live_clob_book", lambda token: _book())

    first = PaperFollowerDaemon(cfg, acfg)
    assert first.process_once() == 2
    second = PaperFollowerDaemon(cfg, acfg)
    assert second.process_once() == 0
    rows = [json.loads(line) for line in cfg.ledger_path.read_text().splitlines()]
    assert sum(row["type"] == "entry" for row in rows) == 1


def test_backlogged_onchain_row_is_stale_at_follower_observation(tmp_path: Path) -> None:
    from polymarket_bot.paper_follower import onchain_lane_to_fill

    cfg, acfg = _configs(tmp_path)
    cfg.state_path.write_text(json.dumps({"processed_trade_ids": [], "positions": {}}))
    old = datetime.now(UTC) - timedelta(minutes=10)
    lane = _lane()
    lane["ground_truth_ts"] = old.isoformat()
    lane["ground_truth_epoch"] = old.timestamp()
    lane["detection_ts"] = (old + timedelta(seconds=8)).isoformat()
    lane["detection_epoch"] = (old + timedelta(seconds=8)).timestamp()
    fill = onchain_lane_to_fill(lane)
    assert fill is not None
    fill["book_at_detection"] = _book()
    daemon = PaperFollowerDaemon(cfg, acfg)
    rows = daemon.process_fill(fill, 0)
    reject = next(row for row in rows if row["type"] == "reject")
    assert "stale_fill" in reject["reject_reason"]
    assert rows[0]["detection_latency_s"] >= 590
    assert rows[0]["source_detection_latency_s"] == 8.2


def test_ledger_recovers_state_after_crash_before_state_snapshot(tmp_path: Path, monkeypatch) -> None:
    import polymarket_bot.paper_follower as follower

    cfg, acfg = _configs(tmp_path)
    lane = _lane()
    assert cfg.onchain_log_path is not None
    cfg.onchain_log_path.parent.mkdir()
    cfg.onchain_log_path.write_text(json.dumps(lane) + "\n")
    position_id = f"{lane['wallet']}:{lane['token_id']}"
    cfg.ledger_path.write_text(
        json.dumps({
            "ts": lane["detection_ts"],
            "type": "entry",
            "wallet": lane["wallet"],
            "token": lane["token_id"],
            "trade_id": lane["durable_trade_id"],
            "position_id": position_id,
            "sim_fill_price": 0.505,
            "sim_size": 198.01980198,
            "quarantined_low_price": False,
        }) + "\n"
    )
    # Simulate a crash after ledger fsync but before state.json was updated.
    cfg.state_path.write_text(json.dumps({
        "processed_trade_ids": [],
        "positions": {},
        "onchain_log_offset": 0,
    }))
    (acfg.archive_dir / "heartbeat_latest.json").write_text(
        json.dumps({"last_ws_message_ts": datetime.now(UTC).isoformat()})
    )
    monkeypatch.setattr(follower, "live_clob_book", lambda token: _book())
    daemon = PaperFollowerDaemon(cfg, acfg)
    assert position_id in daemon.state["positions"]
    assert lane["durable_trade_id"] in daemon._processed_trade_ids
    assert daemon.process_once() == 0
