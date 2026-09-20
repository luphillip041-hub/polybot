#!/usr/bin/env python3
"""Prove (or fail to prove) that paper entries precede the real-world outcome.

Joins ledger entry timestamps to Gamma ``endDate`` / ``closedTime`` and to
the follower's on-chain resolution journal time.  Does **not** place orders
and does not read live keys.

Usage:
    python scripts/analyze_entry_timing.py
    python scripts/analyze_entry_timing.py --ledger runs/paper/ledger.jsonl --json
    python scripts/analyze_entry_timing.py --offline --meta tests/fixtures/gamma_timing_meta.json

``--offline`` never calls Gamma (required for tests / air-gapped review).
Without ``--meta`` every row is classified unknown rather than guessed.

See polymarket_bot/entry_timing.py for field assumptions and limitations.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_LEDGER = ROOT / "runs" / "paper" / "ledger.jsonl"
DEFAULT_OUTPUT = ROOT / "runs" / "paper" / "analysis_entry_timing.json"


def _load_meta(path: Path | None) -> tuple[dict, dict]:
    from polymarket_bot.entry_timing import load_offline_meta

    if path is None:
        return {}, {}
    payload = json.loads(path.read_text())
    return load_offline_meta(payload)


def _fetcher(*, offline: bool, meta_path: Path | None):
    from polymarket_bot.entry_timing import GammaTimingFetcher

    by_token, by_cid = _load_meta(meta_path)
    if offline:
        return GammaTimingFetcher(meta_by_token=by_token, meta_by_condition=by_cid)
    from polymarket_bot.gamma import event_by_clob_token, event_by_condition, markets_by_token

    return GammaTimingFetcher(
        event_by_token=lambda token: event_by_clob_token(token),
        markets_by_token=lambda token: markets_by_token(token),
        events_by_condition=lambda cid: event_by_condition(cid),
        meta_by_token=by_token,
        meta_by_condition=by_cid,
    )


def render_text(summary: dict, suspicious: list[dict], *, max_flags: int = 15) -> str:
    lead = summary["lead_s"]
    hold = summary["ledger_hold_s"]
    wins = summary["wins"]
    losses = summary["losses"]
    lines = [
        "ENTRY TIMING vs REAL-WORLD OUTCOME (Gamma endDate/closedTime)",
        (
            f"paired={summary['closed_paired']} known={summary['known']} "
            f"unknown={summary['unknown']}({summary['unknown_pct']}%) "
            f"precedes={summary['precedes']}({summary['precedes_pct_of_known']}% of known) "
            f"suspicious={summary['suspicious']}({summary['suspicious_pct_of_known']}% of known)"
        ),
        (
            f"WINS n={wins['n']} known={wins['known']} "
            f"precedes={wins['precedes']}({wins['precedes_pct_of_known']}%) "
            f"suspicious={wins['suspicious']}"
        ),
        (
            f"LOSSES n={losses['n']} known={losses['known']} "
            f"precedes={losses['precedes']}({losses['precedes_pct_of_known']}%) "
            f"suspicious={losses['suspicious']}"
        ),
        (
            f"LEAD_S (real_world_end - entry) n={lead['n']} "
            f"min={lead['min']} p5={lead['p5']} p50={lead['p50']} "
            f"p95={lead['p95']} max={lead['max']}"
        ),
        (
            f"LEDGER_HOLD_S (observer resolution - entry; NOT real-world) n={hold['n']} "
            f"min={hold['min']} p50={hold['p50']} p95={hold['p95']} max={hold['max']}"
        ),
        (
            f"PNL precedes=${summary['precedes_pnl']} suspicious=${summary['suspicious_pnl']}"
        ),
        "UNKNOWN_REASONS " + json.dumps(summary["unknown_reasons"], sort_keys=True),
    ]
    if suspicious:
        lines.append(f"SUSPICIOUS flags (entry >= Gamma end, before/at on-chain journal), showing {min(max_flags, len(suspicious))}/{len(suspicious)}:")
        for row in suspicious[:max_flags]:
            lines.append(
                f"  {row.get('entry_ts')} pid={row.get('position_id')} "
                f"end={row.get('real_world_end_ts')} src={row.get('real_world_end_source')} "
                f"onchain={row.get('onchain_resolution_ts')} pnl={row.get('pnl')} "
                f"cid={(row.get('condition_id') or '')[:18]}"
            )
    lines.append("LIMITATIONS:")
    for note in summary["limitations"]:
        lines.append(f"  - {note}")
    known = summary["known"]
    if known == 0:
        lines.append("VERDICT INCONCLUSIVE: no row had a parseable Gamma endDate/closedTime.")
    elif summary["unknown"] and summary["unknown_pct"] and summary["unknown_pct"] > 25:
        lines.append("VERDICT INCONCLUSIVE: too many unknown rows to claim the book is clean.")
    elif summary["suspicious"]:
        lines.append("VERDICT FAIL: suspicious entries after the public outcome window exist.")
    else:
        lines.append(
            "VERDICT PASS among known rows (entry < Gamma end). "
            "This is not proof against lookahead on news that printed before endDate."
        )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Paper entry vs Gamma real-world end timing")
    parser.add_argument("--ledger", default=str(DEFAULT_LEDGER))
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--meta", type=Path, help="Offline Gamma timing JSON")
    parser.add_argument("--offline", action="store_true", help="Do not call Gamma (fail closed)")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--max-flags", type=int, default=15)
    args = parser.parse_args()

    sys.path.insert(0, str(ROOT))
    from polymarket_bot.entry_timing import classify_ledger, timing_summary, VERDICT_SUSPICIOUS
    from polymarket_bot.ledger_history import iter_ledger_rows

    ledger = Path(args.ledger)
    paper_dir = ledger.resolve().parent
    if not ledger.exists() and not (paper_dir / "ledger_archive").exists():
        print(f"ERROR: ledger not found at {ledger}", file=sys.stderr)
        return 1

    rows = list(iter_ledger_rows(paper_dir))
    fetcher = _fetcher(offline=args.offline, meta_path=args.meta)
    classified = classify_ledger(rows, fetcher, limit=args.limit)
    summary = timing_summary(classified)
    summary["fetcher"] = {
        "offline": args.offline,
        "network_lookups": fetcher.lookups,
        "offline_hits": fetcher.hits_offline,
        "lookup_failures": fetcher.failures,
    }
    suspicious = [r for r in classified if r.get("verdict") == VERDICT_SUSPICIOUS]
    payload = {"summary": summary, "suspicious": suspicious, "rows": classified}

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, indent=1) + "\n")

    if args.json:
        print(json.dumps({"summary": summary, "suspicious": suspicious}, indent=1))
    else:
        print(render_text(summary, suspicious, max_flags=args.max_flags))
        print(f"wrote {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
