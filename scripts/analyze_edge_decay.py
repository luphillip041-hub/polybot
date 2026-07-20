#!/usr/bin/env python3
"""Edge decay analysis — memory-efficient: index only needed tokens."""
from __future__ import annotations

import gzip
import json
import logging
import sys
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
LOG = logging.getLogger("edge_decay")

CUTOFF_TS = datetime(2026, 7, 8, 23, 16, 31, tzinfo=UTC)
ARCHIVE_DIR = Path(__file__).resolve().parents[1] / "runs" / "book_archive"


def parse_ts(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value), UTC)
        except Exception:
            return None
    if isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return dt if dt.tzinfo else dt.replace(tzinfo=UTC)
        except Exception:
            return None
    return None


def read_shadow_rows(since: datetime) -> tuple[list[dict], list[dict], set[str]]:
    fills: list[dict] = []
    followup_books: list[dict] = []
    needed_tokens: set[str] = set()

    for path in sorted(ARCHIVE_DIR.glob("shadow_*.jsonl.gz")):
        try:
            with gzip.open(path, "rt") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        row = json.loads(line)
                    except Exception:
                        continue
                    rt = row.get("type") or ""
                    rk = row.get("kind") or ""
                    if rk == "fill" and not rt:
                        rt = "fill"
                    ts = parse_ts(row.get("fill_timestamp"))
                    if ts is None or ts < since:
                        continue
                    if rt == "fill":
                        fills.append(row)
                        trade = row.get("trade") if isinstance(row.get("trade"), dict) else {}
                        tok = trade.get("asset") or row.get("token_id") or ""
                        if tok:
                            needed_tokens.add(tok)
                    elif rt == "followup_book":
                        followup_books.append(row)
                    elif rk == "followup_missed" or rt == "followup_missed":
                        pass
        except Exception as exc:
            LOG.warning("failed reading %s: %s", path.name, exc)

    return fills, followup_books, needed_tokens


def load_needed_books(needed_tokens: set[str], fill_ts_min: datetime, fill_ts_max: datetime) -> dict[str, list[dict]]:
    """Load only book rows for needed tokens in the fill time window + margin."""
    margin = timedelta(seconds=120)
    t_min = fill_ts_min - margin
    t_max = fill_ts_max + margin

    index: dict[str, list[dict]] = defaultdict(list)
    count = 0

    # Determine which book files could contain data in our time range
    # Each file is book_YYYY-MM-DD_HH.jsonl.gz or book_YYYY-MM-DD.jsonl.gz
    paths = sorted(ARCHIVE_DIR.glob("book_*.jsonl.gz"))
    LOG.info("scanning %d book files for %d needed tokens...", len(paths), len(needed_tokens))

    for path in paths:
        try:
            with gzip.open(path, "rt") as f:
                for line in f:
                    try:
                        row = json.loads(line)
                    except Exception:
                        continue
                    if row.get("type") != "book":
                        continue
                    tid = row.get("token_id")
                    if not tid or tid not in needed_tokens:
                        continue
                    row_ts = parse_ts(row.get("ts"))
                    if row_ts is None or row_ts < t_min or row_ts > t_max:
                        continue
                    index[tid].append(row)
                    count += 1
        except Exception as exc:
            LOG.warning("failed %s: %s", path.name, exc)

    LOG.info("loaded %d book rows for %d/%d tokens", count, len(index), len(needed_tokens))
    # Sort each token's rows by timestamp for binary search
    for tid in index:
        index[tid].sort(key=lambda r: parse_ts(r.get("ts")).timestamp() if parse_ts(r.get("ts")) else 0)
    return dict(index)


def find_entry_book(fill_ts: datetime, token_id: str, book_index: dict[str, list[dict]], tolerance_s: float = 60.0) -> dict | None:
    rows = book_index.get(token_id, [])
    if not rows:
        return None
    ts_epoch = fill_ts.timestamp()
    lo, hi = 0, len(rows) - 1
    while lo < hi:
        mid = (lo + hi) // 2
        mid_ts = parse_ts(rows[mid].get("ts"))
        if mid_ts and mid_ts.timestamp() < ts_epoch:
            lo = mid + 1
        else:
            hi = mid
    # Search ±10 around the binary search result
    best = None
    best_delta = float("inf")
    for i in range(max(0, lo - 10), min(len(rows), lo + 10)):
        row_ts = parse_ts(rows[i].get("ts"))
        if row_ts is None:
            continue
        delta = abs(row_ts.timestamp() - ts_epoch)
        if delta < best_delta and delta <= tolerance_s:
            best_delta = delta
            best = rows[i]
    return best


def find_followup_books(fills: list[dict], followup_books: list[dict]) -> dict[str, dict[int, dict]]:
    fb_by_trade: dict[str, dict[int, dict]] = defaultdict(dict)
    for fb in followup_books:
        tid = fb.get("trade_id", "")
        offset = fb.get("offset_seconds", 0)
        if tid and offset:
            fb_by_trade[tid][offset] = fb
    return dict(fb_by_trade)


def get_entry_price(book: dict, side: str) -> float | None:
    return book.get("best_ask") if side == "BUY" else book.get("best_bid")


def get_exit_price(book: dict, side: str) -> float | None:
    return book.get("best_bid") if side == "BUY" else book.get("best_ask")


def percentile(sorted_vals: list[float], pct: float) -> float:
    if not sorted_vals:
        return 0.0
    k = (pct / 100.0) * (len(sorted_vals) - 1)
    f = int(k)
    c = f + 1
    if c >= len(sorted_vals):
        return sorted_vals[-1]
    return sorted_vals[f] + (k - f) * (sorted_vals[c] - sorted_vals[f])


def main() -> None:
    LOG.info("reading shadow rows since %s...", CUTOFF_TS.isoformat())
    fills, fbooks, needed_tokens = read_shadow_rows(CUTOFF_TS)
    LOG.info("fills=%d followup_books=%d needed_tokens=%d", len(fills), len(fbooks), len(needed_tokens))

    fb_by_trade = find_followup_books(fills, fbooks)
    LOG.info("fills with followup_books: %d / %d", len(fb_by_trade), len(fills))

    # Find fill time range
    fill_times = [parse_ts(f.get("fill_timestamp")) for f in fills if parse_ts(f.get("fill_timestamp"))]
    if not fill_times:
        print("NO FILLS IN WINDOW")
        return
    ts_min = min(fill_times)
    ts_max = max(fill_times)

    book_index = load_needed_books(needed_tokens, ts_min, ts_max)
    LOG.info("tokens with book data: %d / %d", len(book_index), len(needed_tokens))

    horizons_map = {60: "1m", 300: "5m", 900: "15m"}
    decay: dict = {
        "1m": {"pnls": [], "per_wallet": defaultdict(list)},
        "5m": {"pnls": [], "per_wallet": defaultdict(list)},
        "15m": {"pnls": [], "per_wallet": defaultdict(list)},
    }
    wallet_names: dict[str, str] = {}
    excluded_no_entry_book: set[str] = set()
    excluded_no_followup: set[str] = set()
    fills_with_any_pnl: set[str] = set()
    per_fill_detail: list[dict] = []

    for i, fill in enumerate(fills):
        tid = fill.get("trade_id", "")
        side = (fill.get("fill_side") or "").upper()
        wallet = (fill.get("wallet") or "").lower()
        fill_ts = parse_ts(fill.get("fill_timestamp"))
        trade = fill.get("trade") if isinstance(fill.get("trade"), dict) else {}
        token_id = trade.get("asset") or fill.get("token_id") or ""
        wname = trade.get("pseudonym") or wallet[:10]
        wallet_names[wallet] = wname

        if i % 400 == 0 and i > 0:
            LOG.info("processed %d/%d fills", i, len(fills))

        if not fill_ts or not token_id:
            excluded_no_entry_book.add(tid)
            continue

        entry_book = find_entry_book(fill_ts, token_id, book_index)
        if not entry_book:
            excluded_no_entry_book.add(tid)
            continue

        entry_price = get_entry_price(entry_book, side)
        if entry_price is None:
            excluded_no_entry_book.add(tid)
            continue

        followups = fb_by_trade.get(tid, {})
        fill_detail = {
            "trade_id": tid,
            "wallet": wallet[:12],
            "side": side,
            "fill_ts": fill_ts.isoformat() if fill_ts else "?",
            "fill_price": fill.get("fill_price"),
            "entry_price": entry_price,
        }

        for offset_s in (60, 300, 900):
            if offset_s not in followups:
                excluded_no_followup.add(tid)
                continue
            fb_row = followups[offset_s]
            fb_book = fb_row.get("book") if isinstance(fb_row.get("book"), dict) else {}
            if not fb_book:
                excluded_no_followup.add(tid)
                continue
            exit_price = get_exit_price(fb_book, side)
            if exit_price is None:
                excluded_no_followup.add(tid)
                continue

            pnl = (exit_price - entry_price) if side == "BUY" else (entry_price - exit_price)
            horizon = horizons_map[offset_s]
            decay[horizon]["pnls"].append(pnl)
            decay[horizon]["per_wallet"][wallet].append(pnl)
            fills_with_any_pnl.add(tid)
            fill_detail[f"pnl_{horizon}"] = round(pnl, 6)

        per_fill_detail.append(fill_detail)

    # ---- REPORT ----
    print("=" * 110)
    print("  TASK-ID: decay-01 — EDGE DECAY ANALYSIS")
    print(f"  Cutoff: fill_ts > {CUTOFF_TS.isoformat()}")
    print(f"  Total fills in window: {len(fills)}")
    print("=" * 110)
    print()

    # Decay table
    header = f"{'Horizon':>10} | {'n':>6} | {'Mean PnL/shr':>14} | {'Median':>11} | {'% Pos':>8} | {'Min':>10} | {'Max':>10}"
    print(header)
    print("-" * len(header))

    for label in ("1m", "5m", "15m"):
        data = decay[label]
        pnls = data["pnls"]
        n = len(pnls)
        if n == 0:
            print(f"{label:>10} | {0:>6} | {'—':>14} | {'—':>11} | {'—':>8} | {'—':>10} | {'—':>10}")
            continue
        mean = sum(pnls) / n
        sp = sorted(pnls)
        med = percentile(sp, 50)
        pct_pos = sum(1 for p in pnls if p > 0) / n * 100
        print(f"{label:>10} | {n:>6} | {mean:>+13.6f} | {med:>+10.6f} | {pct_pos:>6.1f}% | {min(pnls):>+9.4f} | {max(pnls):>+9.4f}")

    print()

    # Per-wallet breakdown
    print("-" * 110)
    print("  PER-WALLET BREAKDOWN")
    print("-" * 110)
    for label in ("1m", "5m", "15m"):
        data = decay[label]
        pw = data["per_wallet"]
        if not pw:
            continue
        print(f"\n  {label}:")
        h2 = f"    {'Wallet':>48} | {'n':>5} | {'Mean PnL/shr':>14} | {'Median':>11} | {'% Pos':>8}"
        print(h2)
        print("    " + "-" * (len(h2) - 4))
        for waddr, pnls in sorted(pw.items(), key=lambda x: len(x[1]), reverse=True):
            name = wallet_names.get(waddr, waddr[:10])
            wn = len(pnls)
            wmean = sum(pnls) / wn
            ws = sorted(pnls)
            wmed = percentile(ws, 50)
            wpct = sum(1 for p in pnls if p > 0) / wn * 100
            display = f"{name[:30]} ({waddr[:8]}...)"[:48]
            print(f"    {display:>48} | {wn:>5} | {wmean:>+13.6f} | {wmed:>+10.6f} | {wpct:>6.1f}%")

    print()
    print("=" * 110)
    print("  EXCLUDED ROWS & DATA QUALITY")
    print("=" * 110)
    total_excl_entry = len(excluded_no_entry_book)
    total_excl_ff = len(excluded_no_followup)
    print(f"  Excluded — no entry book at fill_ts: {total_excl_entry}")
    print(f"  Excluded — missing followup_book(s):  {total_excl_ff}")
    print(f"  Fills with ≥1 PnL computed:           {len(fills_with_any_pnl)}")
    print(f"  Unique wallets:                        {len(wallet_names)}")

    print()
    print("=" * 110)
    print("  PER-FILL DETAIL (first 10)")
    print("=" * 110)
    for detail in per_fill_detail[:10]:
        line = f"  {detail['fill_ts'][:19]} | {detail['wallet']:>12} | {detail['side']:>4} | entry={detail['entry_price']:>8} | fill={detail.get('fill_price','?'):>8}"
        for h in ("1m", "5m", "15m"):
            pnl = detail.get(f"pnl_{h}")
            if pnl is not None:
                line += f" | {h}={pnl:>+.5f}"
        print(line)


if __name__ == "__main__":
    main()
