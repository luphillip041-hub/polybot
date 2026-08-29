"""Freshness gate for the Data API fallback lane (restart-replay flood fix)."""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path

import polymarket_bot.onchain_shadow_worker as worker_mod
from tests.test_onchain_worker import config as make_config


def _api_row(ts_epoch: float) -> dict:
    return {
        "type": "fill",
        "ts": ts_epoch,
        "wallet": "0xtracked",
        "trade": {
            "transactionHash": "0xabc",
            "asset": "123",
            "side": "BUY",
            "size": 10,
            "price": 0.5,
        },
        "api_observation_key": f"0xabc|0xtracked|123|BUY|10|{ts_epoch}",
    }


def test_stale_api_row_skipped_without_reconcile(tmp_path: Path, monkeypatch) -> None:
    cfg = make_config(tmp_path)
    cfg.allowlist_path.write_text(json.dumps({"wallets": ["0xtracked"]}))
    worker = worker_mod.OnchainShadowWorker(cfg, rpc=object())

    def _boom(*args, **kwargs):  # reconcile must never run for stale rows
        raise AssertionError("reconcile_data_api_row called for stale row")

    monkeypatch.setattr(worker_mod, "reconcile_data_api_row", _boom)
    old = time.time() - 86_400  # yesterday
    asyncio.run(worker.process_data_api_row(_api_row(old)))
    rows = [json.loads(line) for line in cfg.output_path.read_text().splitlines()]
    assert [r["type"] for r in rows] == ["stale_api_observation"]
    assert rows[0]["api_observation_key"].startswith("0xabc")
    assert worker.stats["stale_api_observations"] == 1
    assert worker.stats.get("data_api_detections", 0) == 0


def test_fresh_api_row_reaches_reconcile(tmp_path: Path, monkeypatch) -> None:
    cfg = make_config(tmp_path)
    cfg.allowlist_path.write_text(json.dumps({"wallets": ["0xtracked"]}))
    worker = worker_mod.OnchainShadowWorker(cfg, rpc=object())
    called = {}

    def _fake_reconcile(row, rpc):
        called["hit"] = True

        class Match:
            status = "no_chain_match"
            fill = None
            candidate_count = 0

        return Match(), None, []

    monkeypatch.setattr(worker_mod, "reconcile_data_api_row", _fake_reconcile)
    asyncio.run(worker.process_data_api_row(_api_row(time.time())))
    assert called.get("hit") is True
    assert worker.stats.get("stale_api_observations", 0) == 0


def test_stale_threshold_is_configurable(tmp_path: Path) -> None:
    from polymarket_bot.onchain_shadow import OnchainShadowConfig

    cfg = make_config(tmp_path)
    assert cfg.api_max_observation_age_seconds == 110.0
    cfg2 = OnchainShadowConfig(
        **{**cfg.__dict__, "api_max_observation_age_seconds": 60.0}
    )
    assert cfg2.api_max_observation_age_seconds == 60.0


def test_boundary_rows_route_by_gate(tmp_path: Path, monkeypatch) -> None:
    """Rows older than the gate are dropped before reconcile; fresher pass."""
    cfg = make_config(tmp_path)
    cfg.allowlist_path.write_text(json.dumps({"wallets": ["0xtracked"]}))
    worker = worker_mod.OnchainShadowWorker(cfg, rpc=object())
    calls = []

    def _fake_reconcile(row, rpc):
        calls.append(row)

        class Match:
            status = "no_chain_match"
            fill = None
            candidate_count = 0

        return Match(), None, []

    monkeypatch.setattr(worker_mod, "reconcile_data_api_row", _fake_reconcile)
    # 115s old: beyond the 110s gate -> dropped as stale_api_observation
    asyncio.run(worker.process_data_api_row(_api_row(time.time() - 115)))
    # 60s old: fresh enough to forward (follower dedupes against primary lane)
    asyncio.run(worker.process_data_api_row(_api_row(time.time() - 60)))
    assert len(calls) == 1
    assert worker.stats["stale_api_observations"] == 1
