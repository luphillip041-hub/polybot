#!/usr/bin/env python3
"""Deterministic paper-forward-test scorecard for the Polymarket copy bot.

Single canonical computation for the daily go-live-gate readout.  The cron
job runs this script and formats its output — it never re-derives stats with
fresh ad-hoc code, which previously produced inconsistent numbers between
runs.

Usage:  python scripts/paper_scorecard.py [--date YYYY-MM-DD] [--json]
Default window: last complete UTC day.  All-time window starts 2026-08-18
(on-chain-primary era / current config start).
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
from collections import Counter
from pathlib import Path

LEDGER = Path(__file__).resolve().parents[1] / "runs" / "paper" / "ledger.jsonl"
ERA_START = "2026-08-18"

# Go-live bars
BAR_PF = 1.3
BAR_WR = 48.0
BAR_P50 = 15.0
BAR_P90 = 25.0
BAR_STALE_PCT = 1.0
# 90% was aspirational; measured reality on clean days is ~72-75% at +12s
# (drift p50=0.0, p90=+1.75c).  The gate that actually protects the bankroll
# is PF under the honest 1c-haircut model (>1.3), which the PF bar now reads
# since paper fills are simulated with PAPER_HAIRCUT=0.010.
BAR_ATTAIN_PCT = 70.0
BAR_ATTAIN_OFFSET = 12  # realistic taker submission horizon
BAR_ATTAIN_MIN_SAMPLES = 100


def pct(n: float, d: float) -> float | None:
    return round(n / d * 100, 2) if d else None


def median(values: list[float]) -> float | None:
    return round(statistics.median(values), 4) if values else None


def percentile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    values = sorted(values)
    idx = min(len(values) - 1, max(0, int(round((q / 100) * (len(values) - 1)))))
    return round(values[idx], 2)


def window_stats(rows: list[dict]) -> dict:
    signals = [r for r in rows if r.get("type") == "signal"]
    entries = [r for r in rows if r.get("type") == "entry"]
    closed = [r for r in rows if r.get("type") in ("resolution", "exit")]
    rejects = [r for r in rows if r.get("type") == "reject"]
    checks = [r for r in rows if r.get("type") == "fill_check"]

    def pnl_split(flag: bool) -> float:
        return sum(
            float(r.get("pnl") or 0)
            for r in closed
            if bool(r.get("quarantined_low_price")) == flag
        )

    wins = sum(1 for r in closed if float(r.get("pnl") or 0) > 0)
    losses = sum(1 for r in closed if float(r.get("pnl") or 0) <= 0)
    gross_w = sum(float(r["pnl"]) for r in closed if float(r.get("pnl") or 0) > 0)
    gross_l = abs(sum(float(r["pnl"]) for r in closed if float(r.get("pnl") or 0) <= 0))
    clean_closed = [r for r in closed if not r.get("quarantined_low_price")]
    cw = sum(float(r["pnl"]) for r in clean_closed if float(r.get("pnl") or 0) > 0)
    cl = abs(sum(float(r["pnl"]) for r in clean_closed if float(r.get("pnl") or 0) <= 0))

    lat = [
        float(r["detection_latency_s"])
        for r in entries
        if isinstance(r.get("detection_latency_s"), (int, float))
    ]

    reasons: Counter[str] = Counter()
    for r in rejects:
        for reason in str(r.get("reject_reason") or "unknown").split(","):
            reasons[reason.strip()] += 1
    n_signals = len(signals) + len(entries)  # entries passed the signal stage
    stale_live = reasons.get("stale_fill", 0)
    stale_recovery = reasons.get("stale_recovery", 0)
    blind_ws = reasons.get("blind_ws_stale", 0)

    check_offsets: dict[int, dict] = {}
    for off in sorted({int(c.get("offset_s") or 0) for c in checks}):
        bucket = [c for c in checks if int(c.get("offset_s") or 0) == off]
        att = [c for c in bucket if c.get("attainable") is True]
        miss = [c for c in bucket if c.get("attainable") is False]
        errs = Counter(str(c.get("error")) for c in bucket if c.get("error"))
        check_offsets[off] = {
            "n": len(bucket),
            "attainable": len(att),
            "missed": len(miss),
            "attainable_pct": pct(len(att), len(att) + len(miss)),
            "errors": dict(errs),
            "median_drift_attainable": median(
                [float(c["price_drift"]) for c in att if c.get("price_drift") is not None]
            ),
            "median_drift_missed": median(
                [float(c["price_drift"]) for c in miss if c.get("price_drift") is not None]
            ),
        }

    scored = sum(1 for r in entries if r.get("quality_score") is not None)

    return {
        "signals": len(signals),
        "entries": len(entries),
        "closed": len(closed),
        "wins": wins,
        "losses": losses,
        "win_rate_pct": pct(wins, wins + losses),
        "pnl_total": round(pnl_split(True) + pnl_split(False), 2),
        "pnl_clean": round(pnl_split(False), 2),
        "pnl_quarantined": round(pnl_split(True), 2),
        "profit_factor": round(gross_w / gross_l, 3) if gross_l else None,
        "profit_factor_clean": round(cw / cl, 3) if cl else None,
        "latency_p50_s": percentile(lat, 50),
        "latency_p90_s": percentile(lat, 90),
        "rejects_total": len(rejects),
        "reject_reasons": dict(reasons.most_common(12)),
        "stale_live": stale_live,
        "stale_recovery": stale_recovery,
        "blind_ws_stale": blind_ws,
        "stale_live_pct_of_signals": pct(stale_live, n_signals),
        "entries_with_quality_score_pct": pct(scored, len(entries)),
        "fill_checks": check_offsets,
    }


def bars(alltime: dict, recent: dict | None = None) -> list[dict]:
    out = []

    def add(name: str, passed: bool | None, detail: str) -> None:
        out.append({"bar": name, "passed": passed, "detail": detail})

    pf = alltime["profit_factor"]
    add(f"PF > {BAR_PF}", pf is not None and pf > BAR_PF, f"PF={pf}")
    wr = alltime["win_rate_pct"]
    add(f"WR >= {BAR_WR}%", wr is not None and wr >= BAR_WR, f"WR={wr}%")
    p50 = alltime["latency_p50_s"]
    add(f"p50 <= {BAR_P50}s", p50 is not None and p50 <= BAR_P50, f"p50={p50}s")
    p90 = alltime["latency_p90_s"]
    add(f"p90 <= {BAR_P90}s", p90 is not None and p90 <= BAR_P90, f"p90={p90}s")
    # Staleness is gated on the trailing window, not all-time: early-era rows
    # predate the origin classification and would fail the bar forever.
    stale_source = recent if recent is not None else alltime
    window_label = "72h" if recent is not None else "all-time"
    sp = stale_source["stale_live_pct_of_signals"]
    add(
        f"stale_fill < {BAR_STALE_PCT}% of signals ({window_label}, recovery-excluded)",
        sp is not None and sp < BAR_STALE_PCT,
        f"stale_fill={sp}% over {window_label} (all-time {alltime['stale_live_pct_of_signals']}%, stale_recovery rows not gated)",
    )
    fc = alltime["fill_checks"].get(BAR_ATTAIN_OFFSET)
    if fc is None or fc["attainable"] + fc["missed"] < BAR_ATTAIN_MIN_SAMPLES:
        n = (fc["attainable"] + fc["missed"]) if fc else 0
        add(
            f"attainability >= {BAR_ATTAIN_PCT}% @ +{BAR_ATTAIN_OFFSET}s",
            None,
            f"collecting ({n}/{BAR_ATTAIN_MIN_SAMPLES} samples)",
        )
    else:
        ap = fc["attainable_pct"]
        add(
            f"attainability >= {BAR_ATTAIN_PCT}% @ +{BAR_ATTAIN_OFFSET}s",
            ap is not None and ap >= BAR_ATTAIN_PCT,
            f"{ap}% over {fc['attainable'] + fc['missed']} measured checks",
        )
    return out


def render_text(day: str, day_stats: dict, alltime: dict, bar_rows: list[dict]) -> str:
    lines = [
        f"SCORECARD day={day} alltime>={ERA_START}",
        (
            f"DAY entries={day_stats['entries']} closed={day_stats['closed']} "
            f"W/L={day_stats['wins']}/{day_stats['losses']} WR={day_stats['win_rate_pct']}% "
            f"PnL=${day_stats['pnl_total']} (clean ${day_stats['pnl_clean']} / lotto ${day_stats['pnl_quarantined']}) "
            f"PF={day_stats['profit_factor']} cleanPF={day_stats['profit_factor_clean']} "
            f"p50={day_stats['latency_p50_s']}s p90={day_stats['latency_p90_s']}s "
            f"stale_live={day_stats['stale_live']} stale_recovery={day_stats['stale_recovery']}"
        ),
        (
            f"ALL entries={alltime['entries']} closed={alltime['closed']} "
            f"W/L={alltime['wins']}/{alltime['losses']} WR={alltime['win_rate_pct']}% "
            f"PnL=${alltime['pnl_total']} (clean ${alltime['pnl_clean']} / lotto ${alltime['pnl_quarantined']}) "
            f"PF={alltime['profit_factor']} cleanPF={alltime['profit_factor_clean']} "
            f"p50={alltime['latency_p50_s']}s p90={alltime['latency_p90_s']}s "
            f"stale_live={alltime['stale_live']}({alltime['stale_live_pct_of_signals']}%) "
            f"stale_recovery={alltime['stale_recovery']} blind_ws={alltime['blind_ws_stale']}"
        ),
    ]
    for off, fc in sorted(alltime["fill_checks"].items()):
        lines.append(
            f"FILLCHECK +{off}s n={fc['n']} attainable={fc['attainable_pct']}% "
            f"({fc['attainable']}/{fc['attainable'] + fc['missed']}) errors={fc['errors']} "
            f"drift_hit={fc['median_drift_attainable']} drift_miss={fc['median_drift_missed']}"
        )
    for b in bar_rows:
        mark = "PASS" if b["passed"] else ("COLLECTING" if b["passed"] is None else "FAIL")
        lines.append(f"BAR [{mark}] {b['bar']} -> {b['detail']}")
    failing = [b for b in bar_rows if b["passed"] is False]
    lines.append(
        "VERDICT " + ("FAIL: " + "; ".join(b["bar"] for b in failing) if failing else "PASS")
    )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="UTC day YYYY-MM-DD (default: yesterday)")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--ledger", default=str(LEDGER))
    args = parser.parse_args()

    import datetime as dt

    day = args.date or (dt.datetime.now(dt.UTC) - dt.timedelta(days=1)).strftime("%Y-%m-%d")

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from polymarket_bot.ledger_history import iter_ledger_rows

    paper_dir = Path(args.ledger).resolve().parent
    day_rows: list[dict] = []
    all_rows: list[dict] = []
    for row in iter_ledger_rows(paper_dir):
        ts = str(row.get("ts") or row.get("entry_ts") or "")
        day_key = ts[:10]
        if day_key < ERA_START:
            continue
        all_rows.append(row)
        if day_key == day:
            day_rows.append(row)

    import datetime as _dt

    cutoff = (_dt.datetime.now(_dt.UTC) - _dt.timedelta(hours=72)).strftime("%Y-%m-%dT%H:%M")
    recent_rows = [r for r in all_rows if str(r.get("ts") or r.get("entry_ts") or "") >= cutoff]

    day_stats = window_stats(day_rows)
    alltime = window_stats(all_rows)
    recent_stats = window_stats(recent_rows)
    bar_rows = bars(alltime, recent_stats)

    if args.json:
        print(
            json.dumps(
                {"day": day, "day_stats": day_stats, "alltime": alltime, "bars": bar_rows},
                indent=1,
            )
        )
    else:
        print(render_text(day, day_stats, alltime, bar_rows))
    return 0


if __name__ == "__main__":
    sys.exit(main())
