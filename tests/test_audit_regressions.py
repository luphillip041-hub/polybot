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


def test_max_open_positions_rejects_buy_but_allows_existing_sell(tmp_path: Path):
    from polymarket_bot.archive_config import ArchiveConfig
    from polymarket_bot.paper_follower import PaperConfig, PaperFollowerDaemon, reject_reasons

    paper = tmp_path / "paper"
    archive = tmp_path / "archive"
    paper.mkdir()
    archive.mkdir()
    cfg = PaperConfig(
        paper_dir=paper,
        ledger_path=paper / "ledger.jsonl",
        state_path=paper / "state.json",
        allowlist_path=paper / "allowlist.json",
        data_quality_path=paper / "data_quality.json",
        max_open_positions=150,
    )
    acfg = ArchiveConfig(
        archive_dir=archive,
        state_path=tmp_path / "shadow.json",
        followup_queue_path=archive / "followups.json",
    )
    positions = {
        f"wallet:token-{i}": {
            "wallet": "wallet",
            "token": f"token-{i}",
            "entry_price": 0.5,
            "shares": 200.0,
            "cost_usd": 100.0,
        }
        for i in range(150)
    }
    state = {"positions": positions, "processed_trade_ids": []}
    cfg.state_path.write_text(json.dumps(state))
    book = {
        "token_id": "new-token",
        "best_bid": 0.49,
        "best_ask": 0.50,
        "spread": 0.01,
        "top3_bids": [{"price": 0.49, "size": 1000}],
        "top3_asks": [{"price": 0.50, "size": 1000}],
    }
    buy = {
        "ts": "2026-07-30T12:00:00+00:00",
        "fill_timestamp": "2026-07-30T12:00:00+00:00",
        "wallet": "wallet",
        "fill_side": "BUY",
        "trade": {"asset": "new-token", "side": "BUY"},
        "book_at_detection": book,
    }
    assert "max_positions" in reject_reasons(
        buy, cfg, acfg, state, ws_age_seconds=0, inside_gap=False
    )

    sell_book = dict(book, token_id="token-0")
    sell = {
        "ts": "2026-07-30T12:00:00+00:00",
        "fill_timestamp": "2026-07-30T12:00:00+00:00",
        "wallet": "wallet",
        "trade_id": "sell-at-cap",
        "fill_side": "SELL",
        "trade": {"asset": "token-0", "side": "SELL"},
        "book_at_detection": sell_book,
    }
    assert "max_positions" not in reject_reasons(
        sell, cfg, acfg, state, ws_age_seconds=0, inside_gap=False
    )
    daemon = PaperFollowerDaemon(cfg, acfg)
    daemon._cycle_ws_age_seconds = 0
    output = daemon.process_fill(sell, accepts_today=0)
    assert output[1]["type"] == "exit"
    assert "wallet:token-0" not in daemon.state["positions"]


def test_positions_marks_many_positions_with_one_archive_lookup_and_no_rest(
    tmp_path: Path, monkeypatch
):
    from polymarket_bot import status_api
    from polymarket_bot.paper_follower import PaperConfig

    state_path = tmp_path / "state.json"
    positions = {
        f"wallet:token-{i}": {
            "wallet": "wallet",
            "token": f"token-{i}",
            "entry_price": 0.40,
            "shares": 10.0,
            "cost_usd": 4.0,
            "opened_at": "2026-07-30T12:00:00+00:00",
        }
        for i in range(200)
    }
    state_path.write_text(json.dumps({"positions": positions, "processed_trade_ids": []}))
    monkeypatch.setattr(
        PaperConfig,
        "load",
        classmethod(lambda cls: PaperConfig(state_path=state_path)),
    )
    calls = []

    def fake_archive_lookup(tokens, archive_dir=None):
        calls.append(set(tokens))
        return {token: {"best_bid": 0.50, "best_ask": 0.51} for token in tokens}

    monkeypatch.setattr(status_api, "_latest_archived_books", fake_archive_lookup)
    status_api._POS_CACHE.update({"ts": 0.0, "data": {}})
    payload = status_api.get_positions()

    assert payload["count"] == 200
    assert payload["stale_marks"] == 0
    assert len(calls) == 1
    assert len(calls[0]) == 200


def test_live_resolution_uses_fractional_payout_and_preserves_binary_outcomes(monkeypatch):
    from polymarket_bot import paper_follower

    state = {
        "positions": {
            "split": {"token": "split", "shares": 10.0, "cost_usd": 4.0},
            "winner": {"token": "winner", "shares": 10.0, "cost_usd": 4.0},
            "loser": {"token": "loser", "shares": 10.0, "cost_usd": 4.0},
        }
    }
    outcomes = {
        "split": {"denom": 2, "n0": 1, "n1": 1, "side": "PRIMARY", "resolution_status": "PRIMARY"},
        "winner": {"denom": 1, "n0": 1, "n1": 0, "side": "PRIMARY", "resolution_status": "PRIMARY"},
        "loser": {"denom": 1, "n0": 0, "n1": 1, "side": "PRIMARY", "resolution_status": "SECONDARY"},
    }

    def fake_resolution(token, **kwargs):
        return {"resolved": True, "question": "Q", "market_id": "M", **outcomes[token]}

    monkeypatch.setattr(paper_follower, "_onchain_resolved_outcome_for_token", fake_resolution)
    actions = paper_follower.check_positions_for_resolution(state)
    prices = {action["pos_id"]: action["exit_price"] for action in actions}

    assert prices == {"split": 0.5, "winner": 1.0, "loser": 0.0}
    split_row = paper_follower.apply_resolution(
        state, next(action for action in actions if action["pos_id"] == "split")
    )
    assert split_row is not None
    assert split_row["sim_fill_price"] == 0.5
    assert split_row["pnl"] == 1.0
