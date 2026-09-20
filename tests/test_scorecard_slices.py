"""Deduped / both-sided scorecard slice."""

from __future__ import annotations

from polymarket_bot.scorecard_slices import (
    both_sided_condition_ids,
    classify_closed,
    condition_id_from_row,
    slice_pnl,
)
from scripts.paper_scorecard import window_stats


def _entry(pid: str, token: str, cid: str) -> dict:
    return {
        "type": "entry",
        "position_id": pid,
        "token": token,
        "condition_id": cid,
        "market": cid,
    }


def _close(pid: str, pnl: float, token: str = "", cid: str = "") -> dict:
    row = {"type": "resolution", "position_id": pid, "pnl": pnl, "token": token}
    if cid:
        row["condition_id"] = cid
    return row


def test_condition_id_ignores_question_strings() -> None:
    assert condition_id_from_row({"market": "Will X happen?"}) == ""
    assert condition_id_from_row({"market": "0xabc", "condition_id": ""}) == "0xabc"
    assert condition_id_from_row({"condition_id": "0xDEF"}) == "0xdef"


def test_both_sided_detected_across_tokens() -> None:
    entries = [
        _entry("w:yes", "yes", "0xcond"),
        _entry("w:no", "no", "0xcond"),
        _entry("w:solo", "solo", "0xother"),
    ]
    assert both_sided_condition_ids(entries) == {"0xcond"}


def test_classify_closed_joins_via_position_id() -> None:
    rows = [
        _entry("w:yes", "yes", "0xcond"),
        _entry("w:no", "no", "0xcond"),
        _entry("w:solo", "solo", "0xother"),
        _close("w:yes", 80.0),
        _close("w:no", -100.0),
        _close("w:solo", 50.0),
    ]
    slices = classify_closed(rows)
    assert slices["both_sided_markets"] == 1
    assert slices["single_sided_markets"] == 1
    assert slice_pnl(slices["both_sided_closed"])["pnl"] == -20.0
    assert slice_pnl(slices["single_sided_closed"])["pnl"] == 50.0
    assert slice_pnl(slices["single_sided_closed"])["win_rate_pct"] == 100.0


def test_window_stats_exposes_deduped_view() -> None:
    rows = [
        {"type": "signal", "ts": "2026-09-19T00:00:00+00:00"},
        _entry("w:yes", "yes", "0xcond"),
        _entry("w:no", "no", "0xcond"),
        _entry("w:solo", "solo", "0xother"),
        _close("w:yes", 80.0),
        _close("w:no", -100.0),
        _close("w:solo", 50.0),
    ]
    stats = window_stats(rows)
    assert stats["closed"] == 3
    assert stats["pnl_total"] == 30.0
    assert stats["both_sided_markets"] == 1
    assert stats["both_sided_pnl"] == -20.0
    assert stats["deduped_markets"] == 1
    assert stats["deduped_closed"] == 1
    assert stats["deduped_pnl"] == 50.0
    assert stats["deduped_win_rate_pct"] == 100.0
