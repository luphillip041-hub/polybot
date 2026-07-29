import gzip
import json
from pathlib import Path


def test_incremental_shadow_reader_reads_only_appended_member(tmp_path: Path):
    from polymarket_bot.paper_follower import _iter_new_fills_from_path

    path = tmp_path / "shadow.jsonl.gz"
    first = {"type": "fill", "trade_id": "one"}
    second = {"type": "fill", "trade_id": "two"}
    with gzip.open(path, "wt") as handle:
        handle.write(json.dumps(first) + "\n")

    rows, offset = _iter_new_fills_from_path(path, 0)
    assert [row["trade_id"] for row in rows] == ["one"]

    with gzip.open(path, "at") as handle:
        handle.write(json.dumps(second) + "\n")

    rows, new_offset = _iter_new_fills_from_path(path, offset)
    assert [row["trade_id"] for row in rows] == ["two"]
    assert new_offset > offset
    assert _iter_new_fills_from_path(path, new_offset)[0] == []


def test_paper_endpoint_uses_mark_to_market_account_value(monkeypatch):
    from polymarket_bot import status_api

    monkeypatch.setattr(status_api, "paper_status", lambda cfg=None: {"realized_pnl": 12.0})
    monkeypatch.setattr(
        status_api,
        "get_positions",
        lambda: {
            "total_cost": 100.0,
            "total_unrealized": -7.5,
            "generated_at": "now",
            "positions": [],
        },
    )
    status_api._PAPER_CACHE.update({"ts": 0.0, "data": {}})
    status_api._PAPER_STATS_CACHE.update({"ts": 0.0, "ledger_mtime_ns": None, "data": {}})
    payload = status_api.get_paper()
    assert payload["unrealized_pnl"] == -7.5
    assert payload["account_value"] == 104.5
    assert payload["positions_snapshot"]["total_cost"] == 100.0
