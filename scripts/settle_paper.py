#!/usr/bin/env python3
"""Settle paper-folder open positions via on-chain ConditionalTokens reads.

Usage:
    python scripts/settle_paper.py [--apply] [--limit N] [--no-rpc]

By default the script runs in DRY mode — it prints what it would do without
modifying state.json or ledger.jsonl. Pass `--apply` to write.

Reads from the live state.json (runs/paper/state.json) and writes
ledger.jsonl resolution rows when --apply is set. Removes settled positions
from state.json. Idempotent — re-running is safe.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# Repo root for module imports
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

from polymarket_bot.resolution import settle_open_positions, TokenMap
from polymarket_bot.paper_follower import PaperConfig, PaperFollowerDaemon, append_jsonl_fsync, save_state
from polymarket_bot.config import CONFIG


def main() -> int:
    parser = argparse.ArgumentParser(description="Settle paper positions via on-chain data")
    parser.add_argument("--apply", action="store_true", help="Write resolution rows and update state.json (else dry-run)")
    parser.add_argument("--limit", type=int, default=0, help="Limit to first N positions for verification (default: all)")
    parser.add_argument("--paper-dir", default=None, help="Override paper dir (default: runs/paper)")
    args = parser.parse_args()

    cfg = PaperConfig.load()
    paper_dir = Path(args.paper_dir) if args.paper_dir else cfg.paper_dir
    state_path = paper_dir / "state.json"
    ledger_path = paper_dir / "ledger.jsonl"

    if not state_path.exists():
        print(f"no state at {state_path}; nothing to settle")
        return 1
    state = json.loads(state_path.read_text())
    if args.limit > 0:
        positions_full = state.get("positions", {})
        keep = dict(list(positions_full.items())[:args.limit])
        # Track which we're settling vs skipping (for accurate apply)
        state = dict(state)
        state["positions"] = keep

    print(f"open positions to scan: {len(state.get('positions', {}))}")
    print(f"paper dir:              {paper_dir}")
    print(f"RPC endpoints:          {len(['tenderly', 'publicnode', 'drpc', 'polygon-rpc'])} with failover")
    print()

    summary = settle_open_positions(state, paper_dir=paper_dir, config=CONFIG)
    print(f"checked={summary['checked']}  resolved={summary['resolved']}  skipped={summary['skipped']}")
    if "rpc_endpoints_attempted" in summary:
        print(f"rpc_endpoints_attempted={summary['rpc_endpoints_attempted']}")
    print()

    res_rows_written = 0
    for row in summary["by_token"]:
        if row["action"] == "skip":
            print(f"  SKIP  pos={row['pos_id'][:24]}... token={row['token'][:14]}... ({row.get('reason','?')})")
            continue
        won = row["won"]
        pnl = row["pnl"]
        side = row["side"]
        cond = row["condition_id"]
        payout = row["payout_per_share"]
        print(f"  {'WIN  ' if won else 'LOSS '} pos={row['pos_id'][:24]}... side={side} payout={payout:.2f} pnl={pnl:+.2f}  cond={cond[:14]}...")
        if args.apply:
            exit_row = {
                "ts": __import__("datetime").datetime.now(__import__("datetime").UTC).isoformat(timespec="seconds"),
                "type": "resolution",
                "wallet": row["wallet"],
                "market": f"on-chain payoutDenominator={row['denom']} n0={row['n0']} n1={row['n1']}",
                "token": row["token"],
                "side": "BUY",
                "detection_latency_s": None,
                "wallet_fill_price": row["entry_price"],
                "sim_fill_price": payout,
                "sim_size": row["shares"],
                "reject_reason": None,
                "position_id": row["pos_id"],
                "pnl": pnl,
                "book_snapshot": None,
                "trade_id": None,
                "resolution_side": side,
                "resolution_condition_id": cond,
                "denom": row["denom"],
                "n0": row["n0"],
                "n1": row["n1"],
            }
            append_jsonl_fsync(ledger_path, exit_row)
            res_rows_written += 1

    if args.apply and res_rows_written:
        if args.limit > 0:
            print(f"\nWARNING: --limit {args.limit} was set; state.json NOT modified (you may have unsettled positions beyond the limit)")
            print("Re-run without --limit to apply settlement to all positions.")
        else:
            # Persist: remove settled positions
            settled_ids = {r["pos_id"] for r in summary["by_token"] if r["action"] == "resolve"}
            kept = {pid: pos for pid, pos in state["positions"].items() if pid not in settled_ids}
            new_state = dict(state)
            new_state["positions"] = kept
            tmp = state_path.with_suffix(".tmp")
            tmp.write_text(json.dumps(new_state, indent=2, sort_keys=True, default=str))
            tmp.replace(state_path)
            print(f"\nwrote {res_rows_written} resolution row(s) to ledger and removed {len(settled_ids)} position(s) from state.json")
    elif not args.apply:
        print(f"\nDry-run. Re-run with --apply to write {summary['resolved']} resolution rows + update state.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
