"""Tests for ledger rotation, multi-segment history reads, origin-aware
stale classification, and the fill-shadow missed-window guard."""

from __future__ import annotations

import gzip
import json
from pathlib import Path

from polymarket_bot import cleanup as cleanup_mod
from polymarket_bot.ledger_history import (
    iter_ledger_rows,
    ledger_segment_paths,
    read_ledger_history,
)
from polymarket_bot.paper_follower import (
    ArchiveConfig,
    PaperConfig,
    reject_reasons,
)
from polymarket_bot.wallet_quality import compute_wallet_quality


def _write_rows(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")


def _entry(wallet: str, ts: str, token: str = "tok1") -> dict:
    return {
        "type": "entry",
        "wallet": wallet,
        "ts": ts,
        "token": token,
        "sim_fill_price": 0.5,
        "sim_size": 200.0,
    }


def _resolution(wallet: str, ts: str, pnl: float, token: str = "tok1") -> dict:
    return {
        "type": "resolution",
        "wallet": wallet,
        "ts": ts,
        "token": token,
        "pnl": pnl,
    }


def test_rotation_preserves_every_row(tmp_path: Path, monkeypatch) -> None:
    paper = tmp_path / "paper"
    ledger = paper / "ledger.jsonl"
    rows = [{"type": "signal", "ts": f"2026-08-2{i}T00:00:00+00:00", "n": i} for i in range(5)]
    _write_rows(ledger, rows)
    monkeypatch.setattr(cleanup_mod, "LEDGER_ROTATE_BYTES", 10)  # force rotation
    result = cleanup_mod.run_cleanup(runs_dir=tmp_path)
    assert result.ledger_trimmed is True
    assert not result.errors
    # Live file gone (follower recreates on next append); segment holds all rows.
    segments = list((paper / "ledger_archive").glob("ledger-*.jsonl.gz"))
    assert len(segments) == 1
    with gzip.open(segments[0], "rt") as f:
        archived = [json.loads(line) for line in f if line.strip()]
    assert archived == rows


def test_rotation_below_threshold_is_noop(tmp_path: Path) -> None:
    paper = tmp_path / "paper"
    ledger = paper / "ledger.jsonl"
    _write_rows(ledger, [{"type": "signal", "ts": "2026-08-22T00:00:00+00:00"}])
    result = cleanup_mod.run_cleanup(runs_dir=tmp_path)
    assert result.ledger_trimmed is False
    assert ledger.exists()


def test_history_read_spans_archive_and_live(tmp_path: Path) -> None:
    paper = tmp_path / "paper"
    archive = paper / "ledger_archive"
    archive.mkdir(parents=True)
    with gzip.open(archive / "ledger-20260820-000000-000001.jsonl.gz", "wt") as f:
        f.write(json.dumps(_entry("0xa", "2026-08-20T01:00:00+00:00")) + "\n")
    _write_rows(paper / "ledger.jsonl", [_entry("0xb", "2026-08-22T01:00:00+00:00")])
    rows = read_ledger_history(paper)
    assert [r["wallet"] for r in rows] == ["0xa", "0xb"]  # oldest first
    assert [p.name for p in ledger_segment_paths(paper)][0].startswith("ledger-20260820")


def test_wallet_quality_reads_across_segments(tmp_path: Path) -> None:
    paper = tmp_path / "paper"
    archive = paper / "ledger_archive"
    archive.mkdir(parents=True)
    with gzip.open(archive / "ledger-20260820-000000-000001.jsonl.gz", "wt") as f:
        f.write(json.dumps(_entry("0xw", "2026-08-20T01:00:00+00:00")) + "\n")
        f.write(json.dumps(_resolution("0xw", "2026-08-20T05:00:00+00:00", 120.0)) + "\n")
    _write_rows(paper / "ledger.jsonl", [_entry("0xw", "2026-08-22T01:00:00+00:00", "tok2")])
    state = tmp_path / "state.json"
    state.write_text(json.dumps({"positions": {}}))
    rows = compute_wallet_quality(
        ledger_path=paper / "ledger.jsonl",
        ledger_paths=ledger_segment_paths(paper),
        state_path=state,
    )
    w = next(r for r in rows if r["wallet"] == "0xw")
    assert w["accepts"] == 2  # one from archive, one from live
    assert w["exits"] == 1
    assert w["realized_pnl"] == 120.0


def _stale_row(origin: str | None) -> dict:
    return {
        "wallet": "0xw",
        "ts": "2026-08-22T12:00:00+00:00",
        "fill_timestamp": "2026-08-22T11:50:00+00:00",  # 600s old > 120s gate
        "origin": origin,
        "fill_side": "BUY",
        "trade": {"asset": "tok", "side": "BUY", "price": 0.5},
        "book_at_detection": {
            "token_id": "tok",
            "best_bid": 0.49,
            "best_ask": 0.50,
            "spread": 0.01,
            "top3_asks": [{"price": 0.50, "size": 5000.0}],
            "top3_bids": [{"price": 0.49, "size": 5000.0}],
        },
    }


def test_stale_classification_by_origin(tmp_path: Path) -> None:
    cfg = PaperConfig(paper_dir=tmp_path)
    acfg = ArchiveConfig(archive_dir=tmp_path / "arc")
    state = {"positions": {}}
    live = reject_reasons(_stale_row("live"), cfg, acfg, state, ws_age_seconds=0.0, inside_gap=False)
    assert "stale_fill" in live and "stale_recovery" not in live
    gap = reject_reasons(_stale_row("gap_backfill"), cfg, acfg, state, ws_age_seconds=0.0, inside_gap=False)
    assert "stale_recovery" in gap and "stale_fill" not in gap
    init = reject_reasons(_stale_row("initial_backfill"), cfg, acfg, state, ws_age_seconds=0.0, inside_gap=False)
    assert "stale_recovery" in init
    none_origin = reject_reasons(_stale_row(None), cfg, acfg, state, ws_age_seconds=0.0, inside_gap=False)
    assert "stale_fill" in none_origin  # unknown origin = conservative live count


def test_onchain_lane_to_fill_maps_origin() -> None:
    from polymarket_bot.paper_follower import onchain_lane_to_fill

    row = {
        "type": "lane_detection",
        "source": "polygon_onchain",
        "durable_trade_id": "0xabc:1",
        "wallet": "0xW",
        "token_id": "tok",
        "side": "BUY",
        "detection_ts": "2026-08-22T12:00:10+00:00",
        "ground_truth_ts": "2026-08-22T12:00:00+00:00",
        "price": 0.5,
        "size": 100.0,
        "origin": "gap_backfill",
    }
    fill = onchain_lane_to_fill(row)
    assert fill is not None
    assert fill["origin"] == "gap_backfill"
