#!/usr/bin/env python3
"""Append idempotent PnL corrections for settled crossed-book paper entries.

Dry-run is the default. Pass --apply to append correction rows. Historical rows
are never edited or deleted.
"""
from __future__ import annotations

import argparse
import fcntl
import json
import math
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))


def _num(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _is_crossed_entry(row: dict[str, Any]) -> bool:
    raw_book = row.get("book_snapshot")
    book: dict[str, Any] = raw_book if isinstance(raw_book, dict) else {}
    bid = _num(book.get("best_bid"))
    ask = _num(book.get("best_ask"))
    return bid is not None and ask is not None and bid >= ask


def build_void_corrections(rows: list[dict[str, Any]], *, now: str | None = None) -> list[dict[str, Any]]:
    """Return one append-only correction for each uncorrected crossed close."""
    entries = {
        str(row.get("position_id")): row
        for row in rows
        if row.get("type") == "entry" and row.get("position_id")
    }
    corrected = {
        str(row.get("position_id"))
        for row in rows
        if row.get("type") == "void_correction"
        and row.get("void_reason") == "crossed_book"
        and row.get("position_id")
    }
    stamp = now or datetime.now(UTC).isoformat(timespec="seconds")
    corrections: list[dict[str, Any]] = []
    seen: set[str] = set()
    for close in rows:
        if close.get("type") not in {"exit", "resolution"}:
            continue
        position_id = str(close.get("position_id") or "")
        entry = entries.get(position_id)
        pnl = _num(close.get("pnl"))
        if (
            not position_id
            or position_id in corrected
            or position_id in seen
            or entry is None
            or pnl is None
        ):
            continue
        if not _is_crossed_entry(entry):
            continue
        corrections.append({
            "ts": stamp,
            "type": "void_correction",
            "position_id": position_id,
            "wallet": close.get("wallet") or entry.get("wallet"),
            "market": entry.get("market") or close.get("market"),
            "token": close.get("token") or entry.get("token"),
            "pnl": -pnl,
            "void_reason": "crossed_book",
            "voided_row_type": close.get("type"),
            "voided_row_ts": close.get("ts"),
            "voided_pnl": pnl,
            "entry_ts": entry.get("ts"),
            "entry_book_snapshot": entry.get("book_snapshot"),
            "quarantined_low_price": bool(entry.get("quarantined_low_price"))
                or ((_num(entry.get("wallet_fill_price")) or 1.0) < 0.10),
            "correction_id": f"void-crossed-book:{position_id}",
        })
        seen.add(position_id)
    return corrections


def _realized(rows: list[dict[str, Any]]) -> float:
    return sum(
        _num(row.get("pnl")) or 0.0
        for row in rows
        if row.get("type") in {"exit", "resolution", "void_correction"}
    )


def _quarantined_position_ids(rows: list[dict[str, Any]]) -> set[str]:
    result: set[str] = set()
    for row in rows:
        if row.get("type") != "entry" or not row.get("position_id"):
            continue
        price = _num(row.get("wallet_fill_price"))
        if row.get("quarantined_low_price") or (price is not None and price < 0.10):
            result.add(str(row["position_id"]))
    return result


def _headline_excluding_quarantine(rows: list[dict[str, Any]]) -> float:
    quarantined = _quarantined_position_ids(rows)
    return sum(
        _num(row.get("pnl")) or 0.0
        for row in rows
        if row.get("type") in {"exit", "resolution", "void_correction"}
        and str(row.get("position_id") or "") not in quarantined
    )


def _load_lines(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text().splitlines():
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(row, dict):
            rows.append(row)
    return rows


def _print_summary(rows: list[dict[str, Any]], corrections: list[dict[str, Any]], *, apply: bool) -> None:
    projected = rows + corrections
    reversed_pnl = -sum(_num(row.get("pnl")) or 0.0 for row in corrections)
    print(f"mode: {'APPLY' if apply else 'DRY_RUN'}")
    print(f"ledger rows: {len(rows)}")
    print(f"crossed settled positions to void: {len(corrections)}")
    print(f"crossed PnL to reverse: {reversed_pnl:+.4f}")
    print(f"realized before: {_realized(rows):+.4f}")
    print(f"realized after crossed-book voids: {_realized(projected):+.4f}")
    print(f"headline after voids + low-price quarantine: {_headline_excluding_quarantine(projected):+.4f}")
    for row in corrections:
        print(f"VOID position={row['position_id']} original_pnl={row['voided_pnl']:+.4f} correction={row['pnl']:+.4f}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Append correction rows; default is dry-run")
    parser.add_argument("--paper-dir", default=None, help="Override runs/paper directory")
    args = parser.parse_args()
    paper_dir = Path(args.paper_dir) if args.paper_dir else HERE.parent / "runs" / "paper"
    ledger_path = paper_dir / "ledger.jsonl"
    if not ledger_path.exists():
        print(f"no ledger at {ledger_path}")
        return 1

    if not args.apply:
        rows = _load_lines(ledger_path)
        corrections = build_void_corrections(rows)
        _print_summary(rows, corrections, apply=False)
        return 0

    with ledger_path.open("a+b") as ledger:
        fcntl.flock(ledger.fileno(), fcntl.LOCK_EX)
        ledger.seek(0)
        rows: list[dict[str, Any]] = []
        for raw in ledger:
            try:
                row = json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                continue
            if isinstance(row, dict):
                rows.append(row)
        corrections = build_void_corrections(rows)
        _print_summary(rows, corrections, apply=True)
        for row in corrections:
            payload = json.dumps(row, separators=(",", ":"), sort_keys=True) + "\n"
            ledger.write(payload.encode("utf-8"))
        ledger.flush()
        if corrections:
            import os
            os.fsync(ledger.fileno())
        fcntl.flock(ledger.fileno(), fcntl.LOCK_UN)
    print(f"appended correction rows: {len(corrections)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
