"""Scorecard slices: both-sided vs one-market-once (deduped) views.

Both-sided markets — paper holding YES and NO of the same condition_id —
are a mechanical pair whose formula EV is slightly negative.  Headline
P&L that counts both legs is not a directional test.  Callers should
report the single-sided / deduped slice next to the raw totals.

TODO: prove each entry timestamp precedes the real-world outcome (not
only on-chain resolution).  That measurement is out of scope here.
"""

from __future__ import annotations

from typing import Any


def condition_id_from_row(row: dict[str, Any]) -> str:
    """Prefer an explicit condition_id; accept a 0x ``market`` field as fallback."""
    for key in ("condition_id", "market"):
        value = str(row.get(key) or "").strip().lower()
        if value.startswith("0x"):
            return value
    return ""


def both_sided_condition_ids(entries: list[dict[str, Any]]) -> set[str]:
    """condition_ids that appear with two or more distinct tokens (both legs)."""
    tokens_by_cid: dict[str, set[str]] = {}
    for entry in entries:
        cid = condition_id_from_row(entry)
        token = str(entry.get("token") or "")
        if cid and token:
            tokens_by_cid.setdefault(cid, set()).add(token)
    return {cid for cid, tokens in tokens_by_cid.items() if len(tokens) >= 2}


def classify_closed(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Split closed rows into both-sided vs single-sided (deduped) markets.

    Join is by ``position_id`` → entry so historical resolution rows that
    only carry a question string still classify correctly.
    """
    entries = [r for r in rows if r.get("type") == "entry"]
    closed = [r for r in rows if r.get("type") in {"resolution", "exit"}]
    by_pos = {
        str(entry.get("position_id")): entry
        for entry in entries
        if entry.get("position_id")
    }
    both = both_sided_condition_ids(entries)

    def cid_of(closed_row: dict[str, Any]) -> str:
        pid = str(closed_row.get("position_id") or "")
        if pid in by_pos:
            return condition_id_from_row(by_pos[pid])
        return condition_id_from_row(closed_row)

    both_closed: list[dict[str, Any]] = []
    single_closed: list[dict[str, Any]] = []
    for row in closed:
        cid = cid_of(row)
        if cid and cid in both:
            both_closed.append(row)
        else:
            single_closed.append(row)

    single_markets = {
        cid_of(row) or str(row.get("token") or row.get("position_id") or "")
        for row in single_closed
    }
    single_markets.discard("")
    return {
        "both_sided_condition_ids": both,
        "both_sided_closed": both_closed,
        "single_sided_closed": single_closed,
        "both_sided_markets": len(both),
        "single_sided_markets": len(single_markets),
    }


def slice_pnl(closed: list[dict[str, Any]]) -> dict[str, Any]:
    """Win/loss/pnl summary for a closed-row slice."""
    wins = sum(1 for r in closed if float(r.get("pnl") or 0) > 0)
    losses = sum(1 for r in closed if float(r.get("pnl") or 0) <= 0)
    pnl = sum(float(r.get("pnl") or 0) for r in closed)
    n = wins + losses
    return {
        "closed": len(closed),
        "wins": wins,
        "losses": losses,
        "win_rate_pct": round(wins / n * 100, 2) if n else None,
        "pnl": round(pnl, 2),
    }
