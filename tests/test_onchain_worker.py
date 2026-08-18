from __future__ import annotations

import asyncio
import json
from dataclasses import replace
from pathlib import Path

import pytest

from polymarket_bot.onchain_measurement import MeasurementLog
from polymarket_bot.onchain_shadow import MetadataResolver, OnchainShadowConfig
from polymarket_bot.onchain_shadow_worker import OnchainShadowWorker
from tests.test_onchain_shadow import COUNTERPARTY_LOG, TRACKED, TRACKED_MAKER_LOG


class FakeRpc:
    def block(self, block_number: int) -> dict:
        assert block_number == 91_145_435
        return {
            "hash": TRACKED_MAKER_LOG["blockHash"],
            "timestamp": TRACKED_MAKER_LOG["blockTimestamp"],
        }


def config(tmp_path: Path) -> OnchainShadowConfig:
    return OnchainShadowConfig(
        wss_rpc_url="wss://example.invalid",
        http_rpc_url="https://example.invalid",
        confirmations=3,
        reconnect_seconds=1,
        initial_backfill_blocks=10,
        max_backfill_blocks=100,
        heartbeat_seconds=60,
        api_tail_seconds=15,
        output_path=tmp_path / "shadow" / "events.jsonl",
        heartbeat_path=tmp_path / "shadow" / "heartbeat.json",
        allowlist_path=tmp_path / "allowlist.json",
        markets_path=tmp_path / "markets.json",
        archive_dir=tmp_path / "archive",
    )


def test_worker_buffers_only_order_owner_and_asserts_counterparty(tmp_path: Path) -> None:
    cfg = config(tmp_path)
    cfg.allowlist_path.write_text(json.dumps({"wallets": [TRACKED]}))
    worker = OnchainShadowWorker(cfg, rpc=FakeRpc())
    worker.ingest_log(COUNTERPARTY_LOG, origin="live")
    assert len(worker.confirmations) == 0
    assert worker.stats["counterparty_only"] == 1
    worker.ingest_log(TRACKED_MAKER_LOG, origin="live")
    assert len(worker.confirmations) == 1
    assert worker.stats["owner_events_pending"] == 1


def test_worker_finalizes_only_after_confirmations_against_canonical_block(tmp_path: Path) -> None:
    cfg = config(tmp_path)
    cfg.allowlist_path.write_text(json.dumps({"wallets": [TRACKED]}))
    cfg.markets_path.write_text(json.dumps({"tokens": {}}))
    worker = OnchainShadowWorker(cfg, rpc=FakeRpc())
    worker.started_at_epoch = 1785423172.0
    worker.ingest_log(TRACKED_MAKER_LOG, origin="live")
    asyncio.run(worker.finalize_ready(91_145_437, detection_epoch=1785423180.0))
    assert not cfg.output_path.exists()
    asyncio.run(worker.finalize_ready(91_145_438, detection_epoch=1785423181.0))
    rows = [json.loads(line) for line in cfg.output_path.read_text().splitlines()]
    detection = next(row for row in rows if row["type"] == "lane_detection")
    assert detection["source"] == "polygon_onchain"
    assert detection["ground_truth_epoch"] == 1785423173
    assert detection["detection_latency_seconds"] == 8.0
    assert detection["confirmations"] == 3
    assert detection["market"] is None


def test_removed_log_records_reorg_without_detection(tmp_path: Path) -> None:
    cfg = config(tmp_path)
    cfg.allowlist_path.write_text(json.dumps({"wallets": [TRACKED]}))
    worker = OnchainShadowWorker(cfg, rpc=FakeRpc())
    worker.ingest_log(TRACKED_MAKER_LOG, origin="live")
    worker.ingest_log(dict(TRACKED_MAKER_LOG, removed=True), origin="live")
    rows = [json.loads(line) for line in cfg.output_path.read_text().splitlines()]
    assert [row["type"] for row in rows] == ["reorg_removed"]
    assert len(worker.confirmations) == 0


def test_pre_window_backfill_is_not_counted_as_lane_detection(tmp_path: Path) -> None:
    cfg = config(tmp_path)
    cfg.allowlist_path.write_text(json.dumps({"wallets": [TRACKED]}))
    worker = OnchainShadowWorker(cfg, rpc=FakeRpc())
    worker.started_at_epoch = 1785423174.0
    worker.ingest_log(TRACKED_MAKER_LOG, origin="initial_backfill")
    asyncio.run(worker.finalize_ready(91_145_438, detection_epoch=1785423181.0))
    rows = [json.loads(line) for line in cfg.output_path.read_text().splitlines()]
    assert [row["type"] for row in rows] == ["pre_window_event"]
    assert rows[0]["source"] == "polygon_onchain"
    assert rows[0]["ground_truth_epoch"] == 1785423173
    assert worker.log.seen_onchain_event_ids == {
        "0xa695db094e5c603e1e8d65d3dac1fe119475260ad8cffcbc31434ff752be4d99:838"
    }


def test_resilient_log_fetch_splits_provider_limited_ranges(tmp_path: Path) -> None:
    class LimitedRpc(FakeRpc):
        def logs(self, start_block: int, end_block: int) -> list[dict]:
            if end_block - start_block + 1 > 2:
                raise RuntimeError("response too large")
            return [{"block": block} for block in range(start_block, end_block + 1)]

    cfg = config(tmp_path)
    cfg.allowlist_path.write_text(json.dumps({"wallets": [TRACKED]}))
    worker = OnchainShadowWorker(cfg, rpc=LimitedRpc())
    rows = asyncio.run(worker._fetch_logs_resilient(10, 16))
    assert [row["block"] for row in rows] == list(range(10, 17))


class WrongBlockRpc(FakeRpc):
    def block(self, block_number: int) -> dict:
        row = super().block(block_number)
        row["hash"] = "0x" + "00" * 32
        return row


def test_canonical_hash_mismatch_is_reorg_not_detection(tmp_path: Path) -> None:
    cfg = config(tmp_path)
    cfg.allowlist_path.write_text(json.dumps({"wallets": [TRACKED]}))
    worker = OnchainShadowWorker(cfg, rpc=WrongBlockRpc())
    worker.ingest_log(TRACKED_MAKER_LOG, origin="backfill")
    asyncio.run(worker.finalize_ready(91_145_438, detection_epoch=1785423181.0))
    rows = [json.loads(line) for line in cfg.output_path.read_text().splitlines()]
    assert rows[-1]["type"] == "reorg_removed"
    assert rows[-1]["reason"] == "canonical_block_hash_mismatch"
    assert all(row["type"] != "lane_detection" for row in rows)


def test_new_head_updates_independent_head_watchdog_clock(tmp_path: Path) -> None:
    cfg = config(tmp_path)
    cfg.allowlist_path.write_text(json.dumps({"wallets": [TRACKED]}))
    worker = OnchainShadowWorker(cfg, rpc=FakeRpc())
    assert worker.last_head_message_epoch is None
    asyncio.run(
        worker._handle_subscription(
            {"params": {"result": {"number": hex(91_145_438)}}}
        )
    )
    assert worker.last_head_message_epoch is not None
    assert worker.current_head == 91_145_438


def test_failed_backfill_preserves_previous_head_for_retry(tmp_path: Path) -> None:
    class FailingBackfillRpc(FakeRpc):
        def logs(self, _start_block: int, _end_block: int) -> list[dict]:
            raise RuntimeError("provider unavailable")

    cfg = config(tmp_path)
    cfg.allowlist_path.write_text(json.dumps({"wallets": [TRACKED]}))
    worker = OnchainShadowWorker(cfg, rpc=FailingBackfillRpc())
    worker.current_head = 100
    with pytest.raises(RuntimeError, match="provider unavailable"):
        asyncio.run(worker._backfill(102, initial=False))
    assert worker.current_head == 100
    rows = [json.loads(line) for line in cfg.output_path.read_text().splitlines()]
    gap = next(row for row in rows if row["type"] == "rpc_gap")
    assert gap["from_block"] == 101
    assert gap["to_block"] == 102
    assert gap["recovered"] is False
