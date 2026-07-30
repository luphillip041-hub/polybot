from __future__ import annotations

import gzip
import json
from pathlib import Path

from polymarket_bot.onchain_measurement import (
    ApiShadowReader,
    MeasurementLog,
    coverage_report,
    load_tracked_wallets,
    percentile,
)


def test_load_tracked_wallets_reads_without_mutation(tmp_path: Path) -> None:
    path = tmp_path / "allowlist.json"
    path.write_text(json.dumps({"wallets": ["0xABC", "0xabc", "", None]}))
    before = path.read_bytes()
    assert load_tracked_wallets(path) == {"0xabc"}
    assert path.read_bytes() == before


def test_measurement_log_is_append_only_and_reloads_seen_ids(tmp_path: Path) -> None:
    path = tmp_path / "shadow_onchain.jsonl"
    log = MeasurementLog(path)
    log.append({"type": "lane_detection", "source": "polygon_onchain", "durable_trade_id": "0x1:2"})
    log.append({"type": "lane_detection", "source": "data_api", "durable_trade_id": "0x1:2"})
    rows = [json.loads(line) for line in path.read_text().splitlines()]
    assert [row["source"] for row in rows] == ["polygon_onchain", "data_api"]
    reloaded = MeasurementLog(path)
    assert reloaded.seen_lane_ids == {("polygon_onchain", "0x1:2"), ("data_api", "0x1:2")}
    assert reloaded.seen_onchain_event_ids == {"0x1:2"}


def test_api_shadow_reader_tags_existing_collector_without_writing_it(tmp_path: Path) -> None:
    archive = tmp_path / "archive"
    archive.mkdir()
    source = archive / "shadow_2026-07-30_14.jsonl.gz"
    source_rows = [
        {"ts": "2026-07-30T14:00:00+00:00", "type": "book"},
        {
            "ts": "2026-07-30T14:01:00+00:00",
            "type": "fill",
            "wallet": "0xabc",
            "trade_id": "0xtx",
            "trade": {"transactionHash": "0xtx", "side": "BUY"},
        },
    ]
    with gzip.open(source, "wt") as handle:
        for row in source_rows:
            handle.write(json.dumps(row) + "\n")
    before = source.read_bytes()
    reader = ApiShadowReader(archive, started_at_epoch=0)
    rows = reader.read_new()
    assert len(rows) == 1
    assert rows[0]["source"] == "data_api"
    assert reader.read_new() == []
    assert source.read_bytes() == before


def test_coverage_report_uses_same_ground_truth_and_counts_axes(tmp_path: Path) -> None:
    path = tmp_path / "shadow_onchain.jsonl"
    rows = [
        {"type": "lane_detection", "source": "polygon_onchain", "durable_trade_id": "both", "ground_truth_epoch": 100.0, "detection_epoch": 105.0},
        {"type": "lane_detection", "source": "data_api", "durable_trade_id": "both", "ground_truth_epoch": 100.0, "detection_epoch": 160.0},
        {"type": "lane_detection", "source": "polygon_onchain", "durable_trade_id": "chain", "ground_truth_epoch": 200.0, "detection_epoch": 206.0},
        {"type": "lane_detection", "source": "data_api", "durable_trade_id": "api", "ground_truth_epoch": 300.0, "detection_epoch": 370.0},
        {"type": "lane_detection", "source": "polygon_onchain", "durable_trade_id": "both", "ground_truth_epoch": 100.0, "detection_epoch": 106.0},
        {"type": "cross_source_mismatch", "status": "no_exact_match"},
        {"type": "reorg_removed", "durable_trade_id": "gone"},
        {"type": "rpc_gap", "missing_blocks": 3, "recovered": True},
        {"type": "rpc_connected", "ts": "2026-07-30T14:00:00+00:00", "downtime_seconds": 0},
        {"type": "rpc_disconnected", "ts": "2026-07-30T14:10:00+00:00", "connected_seconds": 600},
        {"type": "rpc_connected", "ts": "2026-07-30T14:10:05+00:00", "downtime_seconds": 5},
        {"type": "duplicate_detection", "source": "data_api", "durable_trade_id": "both"},
    ]
    path.write_text("".join(json.dumps(row) + "\n" for row in rows))
    report = coverage_report(path)
    assert report["coverage"] == {"seen_by_both": 1, "onchain_only": 1, "data_api_only": 1}
    assert report["lanes"]["polygon_onchain"]["p50_latency_seconds"] == 5.5
    assert report["lanes"]["data_api"]["p50_latency_seconds"] == 65.0
    assert report["first_seen"] == {"polygon_onchain": 1, "data_api": 0, "tie": 0}
    assert report["duplicates"] == {"polygon_onchain": 1, "data_api": 1}
    assert report["cross_source_mismatches"] == 1
    assert report["reorg_events"] == 1
    assert report["rpc_gaps"] == {"events": 1, "missing_blocks": 3, "recovered_events": 1}
    assert report["rpc_uptime"] == {
        "connections": 2,
        "disconnects": 1,
        "connected_seconds_reported": 600.0,
        "downtime_seconds_reported": 5.0,
        "uptime_percent_reported": 99.17355371900827,
    }


def test_percentile_is_linear_and_empty_safe() -> None:
    assert percentile([], 0.9) is None
    assert percentile([1.0, 2.0], 0.5) == 1.5
