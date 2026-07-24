#!/usr/bin/env python3
"""capacity.py — quantify deployable notional at acceptable slippage.

Item #4 for polymarket-copybot. Answers: across the markets the tracked
wallets actually traded, how much size can we push before slippage exceeds
budget — and on what fraction of signals?

Design notes (differs from the naive spec on purpose):
 * Slippage budget is in TICKS, not percent. tick_size is 0.01 or 0.001
   depending on market; at 5c a single tick is 20%, so percent budgets are
   unreachable exactly where the PnL lives.
 * Walks the ask/bid ladder. The book-archive only stores top-3
   asks/bids — capacity is therefore 'fillable within visible top-3 depth',
   which is an UPPER BOUND on true depth (the true ladder runs deeper,
   but we have no record of it).
 * Book source is pluggable. ArchiveBookSource (point-in-time, taken near
   the fill timestamp) is the correct one. LiveBookSource is a smoke-test
   only: querying today's book for a fill from three weeks ago measures
   nothing.
 * Latency haircut is opt-in and currently disabled: the ledger does not
   persist the wallet's actual fill size. Enabling it requires fetching
   trade details from the Polymarket trade API by trade_id.
 * Output is a fillability CURVE plus percentiles, not a single number.
 * Report gaps explicitly (archive token missing, no snapshot within
   tolerance) — don't silently substitute live data.

Usage:
    python scripts/capacity.py --ledger runs/paper/ledger.jsonl --days 30 \\
        --source archive --budget-ticks 2 --out capacity_report.json
"""
from __future__ import annotations

import argparse
import gzip
import json
import statistics
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Protocol

import requests

CLOB = "https://clob.polymarket.com"

# ---------------------------------------------------------------------------
# Ledger field mapping (verified against runs/paper/ledger.jsonl schema).
# Earlier draft of this script in the handoff PDF used 'price' and
# 'token_id' — both wrong. Actual keys are 'wallet_fill_price' and 'token'.
# ---------------------------------------------------------------------------
F_WALLET = "wallet"
F_TOKEN = "token"
F_MARKET = "market"
F_SIDE = "side"
F_PRICE = "wallet_fill_price"
F_TS = "ts"
# F_SIZE: not present in ledger. Latency haircut is opt-in via
# --with-haircut and currently a no-op (see CLI warning).

NOTIONALS = [100, 500, 1000, 5000, 10000]
PRICE_BUCKETS = [
    (0.0, 0.10, "<10c"),
    (0.10, 0.30, "10-30c"),
    (0.30, 0.70, "30-70c"),
    (0.70, 1.01, ">70c"),
]

# Polymarket tick sizes: 0.01 for most markets, 0.001 for tight-spread
# markets. Inferred from the snapshot's recorded spread when tick_size is
# not directly available (the archive does not carry tick_size).
TICK_LARGE = 0.01
TICK_SMALL = 0.001
TICK_THRESHOLD = 0.005  # if spread < this, use TICK_SMALL


def bucket_of(price: float) -> str:
    for lo, hi, name in PRICE_BUCKETS:
        if lo <= price < hi:
            return name
    return "unknown"


def parse_ts(v) -> datetime:
    if isinstance(v, (int, float)):
        return datetime.fromtimestamp(v, tz=timezone.utc)
    s = str(v).replace("Z", "+00:00")
    d = datetime.fromisoformat(s)
    return d if d.tzinfo else d.replace(tzinfo=timezone.utc)


def infer_tick_size(spread: float | None) -> float:
    if spread is None:
        return TICK_LARGE
    return TICK_SMALL if spread < TICK_THRESHOLD else TICK_LARGE


# ---------------------------------------------------------------------------
# Book sources
# ---------------------------------------------------------------------------
class BookSource(Protocol):
    def get(self, token_id: str, ts: datetime) -> dict | None: ...


def _normalize_live(raw: dict) -> dict:
    """Normalize a live /book response. May carry a full ladder."""
    asks = sorted(
        ((float(l["price"]), float(l["size"])) for l in raw.get("asks", [])),
        key=lambda x: x[0],
    )
    bids = sorted(
        ((float(l["price"]), float(l["size"])) for l in raw.get("bids", [])),
        key=lambda x: x[0],
        reverse=True,
    )
    tick = float(raw.get("tick_size") or TICK_LARGE)
    return {
        "asks": asks,
        "bids": bids,
        "tick_size": tick,
        "is_top3_only": False,
    }


def _normalize_archive(raw: dict) -> dict:
    """Normalize an archive row. Archive only carries top3_asks/top3_bids;
    capacity measured against archive is therefore an upper bound on true
    fillable depth (real ladders run deeper)."""
    asks = sorted(
        ((float(l["price"]), float(l["size"])) for l in raw.get("top3_asks", [])),
        key=lambda x: x[0],
    )
    bids = sorted(
        ((float(l["price"]), float(l["size"])) for l in raw.get("top3_bids", [])),
        key=lambda x: x[0],
        reverse=True,
    )
    return {
        "asks": asks,
        "bids": bids,
        "tick_size": infer_tick_size(raw.get("spread")),
        "is_top3_only": True,
        "snapshot_ts": raw.get("ts"),
    }


class LiveBookSource:
    """Smoke-test only. Ignores ts — measures today's liquidity, which is
    NOT what the wallet filled against. Cross-checks /midpoint to catch
    the known ghost-book bug where /book returns 0.01/0.99 on live markets."""

    def __init__(self, batch: int = 50, pause: float = 0.4, ghost_tol: float = 0.05):
        self.batch, self.pause, self.ghost_tol = batch, pause, ghost_tol
        self._cache: dict[str, dict | None] = {}

    def prefetch(self, token_ids: Iterable[str]) -> None:
        todo = [t for t in dict.fromkeys(token_ids) if t not in self._cache]
        for i in range(0, len(todo), self.batch):
            chunk = todo[i : i + self.batch]
            try:
                r = requests.post(
                    f"{CLOB}/books",
                    json=[{"token_id": t} for t in chunk],
                    timeout=20,
                )
                r.raise_for_status()
                for raw in r.json():
                    tid = raw.get("asset_id") or raw.get("token_id")
                    self._cache[tid] = _normalize_live(raw)
            except Exception as e:  # noqa: BLE001
                print(
                    f" book batch failed ({e}); marking {len(chunk)} unavailable",
                    file=sys.stderr,
                )
                for t in chunk:
                    self._cache[t] = None
            time.sleep(self.pause)
        self._drop_ghosts([t for t in todo if self._cache.get(t)])

    def _drop_ghosts(self, token_ids: list[str]) -> None:
        for i in range(0, len(token_ids), self.batch):
            chunk = token_ids[i : i + self.batch]
            try:
                r = requests.post(
                    f"{CLOB}/midpoints",
                    json=[{"token_id": t} for t in chunk],
                    timeout=20,
                )
                r.raise_for_status()
                mids = r.json()
            except Exception:  # noqa: BLE001
                return
            for tid in chunk:
                b = self._cache.get(tid)
                ref = mids.get(tid) if isinstance(mids, dict) else None
                if not b or ref is None or not b["asks"] or not b["bids"]:
                    continue
                book_mid = (b["asks"][0][0] + b["bids"][0][0]) / 2
                if abs(book_mid - float(ref)) > self.ghost_tol:
                    self._cache[tid] = None
            time.sleep(self.pause)

    def get(self, token_id: str, ts: datetime) -> dict | None:
        if token_id not in self._cache:
            self.prefetch([token_id])
        return self._cache.get(token_id)


class ArchiveBookSource:
    """Point-in-time snapshots from runs/book_archive/. Only top-3, but
    point-in-time is what we need — capacity is a property of the ladder
    at fill time, not today."""

    def __init__(
        self,
        root: Path,
        tolerance: timedelta = timedelta(minutes=5),
        suspect_before: datetime | None = None,
        verbose: bool = True,
    ):
        self.root = Path(root)
        self.tolerance = tolerance
        self.suspect_before = suspect_before
        self.verbose = verbose
        self._index: dict[str, list[tuple[datetime, dict]]] | None = None

    def _build_index(self) -> None:
        if self._index is not None:
            return
        idx: dict[str, list[tuple[datetime, dict]]] = defaultdict(list)
        files = sorted(self.root.glob("book_*.jsonl.gz"))
        if self.verbose:
            print(
                f"  indexing archive at {self.root} ({len(files)} hourly files)...",
                file=sys.stderr,
            )
        n_snaps = 0
        for fp in files:
            try:
                with gzip.open(fp, "rt") as f:
                    for line in f:
                        try:
                            r = json.loads(line)
                            if r.get("event_type") != "book":
                                continue
                            ts = parse_ts(r["ts"])
                            if self.suspect_before and ts < self.suspect_before:
                                continue
                            idx[r["token_id"]].append((ts, r))
                            n_snaps += 1
                        except (json.JSONDecodeError, KeyError, ValueError):
                            continue
            except OSError as e:
                if self.verbose:
                    print(f"  failed to read {fp.name}: {e}", file=sys.stderr)
        for tk in idx:
            idx[tk].sort(key=lambda x: x[0])
        self._index = dict(idx)
        if self.verbose:
            print(
                f"  indexed {len(self._index)} tokens, {n_snaps} snapshots",
                file=sys.stderr,
            )

    def get(self, token_id: str, ts: datetime) -> dict | None:
        self._build_index()
        snaps = self._index.get(token_id) if self._index else None
        if not snaps:
            return None
        # Linear scan; for typical per-token snapshot counts (<200) this is
        # fine. If profiling shows it matters, switch to bisect on the
        # already-sorted snapshot list.
        best_d = None
        best_raw = None
        for snap_ts, raw in snaps:
            d = abs((snap_ts - ts).total_seconds())
            if best_d is None or d < best_d:
                best_d = d
                best_raw = raw
        if best_raw is None or best_d > self.tolerance.total_seconds():
            return None
        return _normalize_archive(best_raw)


# ---------------------------------------------------------------------------
# Fill simulation
# ---------------------------------------------------------------------------
def walk(
    ladder: list[tuple[float, float]], notional: float, skip_shares: float = 0.0
) -> tuple[float, float] | None:
    """Spend `notional` dollars against `ladder`, after removing `skip_shares`
    from the top (latency haircut). Returns (vwap, shares) or None if the
    ladder is exhausted before the notional is filled."""
    spent = shares = 0.0
    for price, size in ladder:
        if skip_shares > 0:
            take = min(skip_shares, size)
            size -= take
            skip_shares -= take
            if size <= 0:
                continue
        cost = price * size
        if spent + cost >= notional:
            need = (notional - spent) / price
            return ((spent + need * price) / (shares + need), shares + need)
        spent += cost
        shares += size
    return None


def analyze(
    fills: list[dict], src: BookSource, budget_ticks: float, haircut: bool
) -> dict:
    rows: list[dict] = []
    gaps = 0
    top3_used = 0
    for f in fills:
        book = src.get(f["token"], f["ts"])
        if not book or not book["asks"]:
            gaps += 1
            continue
        ladder = book["asks"] if f["side"] == "BUY" else book["bids"]
        if not ladder:
            gaps += 1
            continue
        best, tick = ladder[0][0], book["tick_size"]
        skip = f["size"] if haircut else 0.0
        rec = {
            "wallet": f["wallet"],
            "bucket": bucket_of(f["price"]),
            "market": f["market"],
            "side": f["side"],
            "best": best,
            "is_top3_only": book.get("is_top3_only", False),
        }
        for n in NOTIONALS:
            res = walk(ladder, n, skip)
            if res is None:
                rec[str(n)] = None
                continue
            vwap = res[0]
            slip = (vwap - best) if f["side"] == "BUY" else (best - vwap)
            rec[str(n)] = {
                "vwap": round(vwap, 5),
                "slip_cents": round(slip * 100, 3),
                "slip_ticks": round(slip / tick, 2),
                "ok": (slip / tick) <= budget_ticks,
            }
        rows.append(rec)
        if rec["is_top3_only"]:
            top3_used += 1

    curve: dict[str, dict[str, dict]] = {}
    for scope in ["ALL"] + [b[2] for b in PRICE_BUCKETS]:
        subset = rows if scope == "ALL" else [r for r in rows if r["bucket"] == scope]
        if not subset:
            continue
        scope_cells: dict[str, dict] = {}
        for n in NOTIONALS:
            cells = [r[str(n)] for r in subset]
            ok = [c for c in cells if c and c["ok"]]
            slips = sorted(c["slip_ticks"] for c in cells if c)
            scope_cells[str(n)] = {
                "n_signals": len(cells),
                "pct_fillable": round(100 * len(ok) / len(cells), 1),
                "median_slip_ticks": round(statistics.median(slips), 2)
                if slips
                else None,
                "p80_slip_ticks": (
                    round(slips[int(0.8 * (len(slips) - 1))], 2) if slips else None
                ),
                "ladder_exhausted": sum(1 for c in cells if c is None),
            }
        curve[scope] = scope_cells

    return {
        "curve": curve,
        "rows": rows,
        "n_fills": len(fills),
        "n_analyzed": len(rows),
        "n_book_gaps": gaps,
        "n_top3_only_used": top3_used,
        "budget_ticks": budget_ticks,
        "latency_haircut": haircut,
    }


def deployable(curve: dict, scope: str, threshold: float) -> int | None:
    """Largest notional where >= `threshold`% of signals fill within budget."""
    best = None
    for n in NOTIONALS:
        c = curve.get(scope, {}).get(str(n))
        if c and c["pct_fillable"] >= threshold:
            best = n
    return best


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Capacity study: deployable notional at acceptable slippage."
    )
    ap.add_argument("--ledger", type=Path, required=True)
    ap.add_argument("--days", type=int, default=30)
    ap.add_argument(
        "--source", choices=["archive", "live"], default="archive"
    )
    ap.add_argument(
        "--archive-root", type=Path, default=Path("runs/book_archive")
    )
    ap.add_argument("--budget-ticks", type=float, default=2.0)
    ap.add_argument(
        "--threshold",
        type=float,
        default=80.0,
        help="pct of signals that must fill within budget",
    )
    ap.add_argument("--sides", default="BUY", help="BUY, SELL, or BUY,SELL")
    ap.add_argument(
        "--with-haircut",
        action="store_true",
        help="enable latency haircut (requires fetching wallet fill size "
        "from Polymarket trade API; currently disabled — see warnings)",
    )
    ap.add_argument(
        "--tolerance-minutes",
        type=float,
        default=5.0,
        help="archive snapshot must be within this many minutes of fill ts",
    )
    ap.add_argument(
        "--suspect-before",
        default="2026-07-08T23:16:31+00:00",
        help="ignore archive snapshots before this ISO ts (suspect-data flag "
        "from prior session). Pass empty string to disable.",
    )
    ap.add_argument(
        "--out", type=Path, default=Path("capacity_report.json")
    )
    args = ap.parse_args()

    suspect_before = (
        parse_ts(args.suspect_before) if args.suspect_before else None
    )
    cutoff = datetime.now(timezone.utc) - timedelta(days=args.days)
    sides = {s.strip().upper() for s in args.sides.split(",")}

    fills: list[dict] = []
    parse_errors = 0
    with args.ledger.open() as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                r = json.loads(line)
                # any row with wallet_fill_price set is a real wallet fill,
                # regardless of follower's signal/reject/ineligible label.
                if r.get(F_PRICE) is None or r.get(F_TOKEN) is None:
                    continue
                ts = parse_ts(r[F_TS])
                if ts < cutoff:
                    continue
                if str(r[F_SIDE]).upper() not in sides:
                    continue
                fills.append(
                    {
                        "wallet": r.get(F_WALLET),
                        "token": str(r[F_TOKEN]),
                        "market": r.get(F_MARKET),
                        "side": str(r[F_SIDE]).upper(),
                        "price": float(r[F_PRICE]),
                        "size": 0.0,  # not in ledger; see --with-haircut
                        "ts": ts,
                        "trade_id": r.get("trade_id"),
                    }
                )
            except (KeyError, ValueError, TypeError, json.JSONDecodeError):
                parse_errors += 1
                continue

    if not fills:
        print(
            "no fills matched — check the F_* field mapping at the top of this file",
            file=sys.stderr,
        )
        return 1

    n_tokens = len({f["token"] for f in fills})
    print(
        f"{len(fills)} fills over {args.days}d, {n_tokens} tokens"
        + (f" ({parse_errors} parse errors skipped)" if parse_errors else "")
    )

    if args.source == "live":
        print(
            "WARNING: live books do not reflect the state these fills traded "
            "against. Smoke test only.",
            file=sys.stderr,
        )
        src: BookSource = LiveBookSource()
        src.prefetch(f["token"] for f in fills)
    else:
        src = ArchiveBookSource(
            args.archive_root,
            tolerance=timedelta(minutes=args.tolerance_minutes),
            suspect_before=suspect_before,
        )

    if args.with_haircut:
        print(
            "WARNING: --with-haircut requires fetching wallet fill sizes from\n"
            "  the Polymarket trade API by trade_id. Not implemented in this\n"
            "  pass; running with haircut=0. The fills[] records already\n"
            "  carry size=0 for this reason. To enable, fetch from\n"
            "  /data/trades/<trade_id> and merge by trade_id before analyze().",
            file=sys.stderr,
        )
        haircut = False
    else:
        haircut = False

    rep = analyze(fills, src, args.budget_ticks, haircut=haircut)
    args.out.write_text(json.dumps(rep, indent=2))

    print(
        f"\nanalyzed {rep['n_analyzed']}/{rep['n_fills']} "
        f"({rep['n_book_gaps']} book gaps, "
        f"{rep['n_top3_only_used']} top-3-only books) | "
        f"budget {args.budget_ticks} ticks | haircut {rep['latency_haircut']}\n"
    )

    hdr = f"{'scope':<10}" + "".join(f"{'$'+str(n):>12}" for n in NOTIONALS)
    print(hdr)
    print("-" * len(hdr))
    for scope, cells in rep["curve"].items():
        line = f"{scope:<10}"
        for n in NOTIONALS:
            c = cells.get(str(n))
            line += f"{(str(c['pct_fillable'])+'%') if c else '-':>12}"
        print(line)
    print("\n(cells = % of signals fillable within budget)\n")

    for scope in rep["curve"]:
        d = deployable(rep["curve"], scope, args.threshold)
        print(
            f" max deployable notional @ {args.threshold:.0f}% fill, {scope}: "
            f"{('$'+str(d)) if d else 'below $'+str(NOTIONALS[0])}"
        )

    print(f"\nwrote {args.out}")
    print(
        "\nNOTE: archive stores only top-3 asks/bids. Capacity reported\n"
        "  here is 'fillable within visible top-3 depth' — an UPPER bound\n"
        "  on true capacity. At notionals where top-3 doesn't exhaust\n"
        "  (typically <= $1000 at typical spreads), this equals true\n"
        "  capacity. Above that, true capacity is <= reported value.\n"
        "  Real ladders run deeper; we have no record of them."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
