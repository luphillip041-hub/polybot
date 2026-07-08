from __future__ import annotations

import argparse
import json
from dataclasses import asdict

from .clob import best_bid_ask
from .config import CONFIG
from .data import leaderboard, score_wallet, user_trades
from .gamma import active_events, flatten_markets
from .paper import append_jsonl, decide_paper, render_dashboard, write_json
from .scoring import score_market


def scan(limit: int) -> list[dict]:
    events = active_events(limit=limit)
    markets = flatten_markets(events)
    enriched = []
    for market in markets:
        s = score_market(market)
        row = dict(market)
        row["score"] = s.score
        row["score_reasons"] = s.reasons
        row["blocked_reasons"] = s.blocked_reasons
        row["candidate_outcome"] = s.outcome
        row["candidate_token_id"] = s.token_id
        enriched.append(row)
    enriched.sort(key=lambda x: x.get("score", 0), reverse=True)
    write_json(CONFIG.runs_dir / "scan_latest.json", enriched)
    return enriched


def run_paper(limit: int, max_orders: int) -> list[dict]:
    markets = scan(limit)
    decisions = []
    orders = 0
    for market in markets:
        s = score_market(market)
        bbo = best_bid_ask(s.token_id) if s.token_id else {"ok": False, "raw_error": "missing token"}
        decision = decide_paper(market, s, bbo)
        row = asdict(decision)
        row["bbo"] = bbo
        decisions.append(row)
        append_jsonl(CONFIG.runs_dir / "decision_journal.jsonl", row)
        if decision.decision == "paper_buy":
            append_jsonl(CONFIG.runs_dir / "paper_ledger.jsonl", row)
            orders += 1
            if orders >= max_orders:
                break
    write_json(CONFIG.runs_dir / "decisions_latest.json", decisions)
    render_dashboard(markets, decisions)
    return decisions


def scan_wallets(limit: int, trades_limit: int) -> list[dict]:
    rows = []
    for wallet in leaderboard(limit):
        addr = wallet.get("proxyWallet")
        trades = user_trades(addr, trades_limit) if addr else []
        rows.append(score_wallet(wallet, trades))
    rows.sort(key=lambda x: x.get("copy_score", 0), reverse=True)
    write_json(CONFIG.runs_dir / "wallet_scores_latest.json", rows)
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Standalone paper-only Polymarket copybot")
    sub = parser.add_subparsers(dest="cmd", required=True)
    p_scan = sub.add_parser("scan")
    p_scan.add_argument("--limit", type=int, default=25)
    p_run = sub.add_parser("run-paper")
    p_run.add_argument("--limit", type=int, default=25)
    p_run.add_argument("--max-orders", type=int, default=1)
    p_wallets = sub.add_parser("wallets")
    p_wallets.add_argument("--limit", type=int, default=10)
    p_wallets.add_argument("--trades-limit", type=int, default=25)
    sub.add_parser("dashboard")
    args = parser.parse_args()

    if args.cmd == "scan":
        rows = scan(args.limit)
        print(json.dumps({"markets": len(rows), "top": rows[:5]}, indent=2, ensure_ascii=False))
    elif args.cmd == "run-paper":
        rows = run_paper(args.limit, args.max_orders)
        print(json.dumps({"decisions": len(rows), "paper_buys": sum(1 for r in rows if r["decision"] == "paper_buy"), "top": rows[:3]}, indent=2, ensure_ascii=False))
    elif args.cmd == "wallets":
        rows = scan_wallets(args.limit, args.trades_limit)
        print(json.dumps({"wallets": len(rows), "top": rows[:5]}, indent=2, ensure_ascii=False))
    elif args.cmd == "dashboard":
        scan_rows = json.loads((CONFIG.runs_dir / "scan_latest.json").read_text()) if (CONFIG.runs_dir / "scan_latest.json").exists() else []
        dec_rows = json.loads((CONFIG.runs_dir / "decisions_latest.json").read_text()) if (CONFIG.runs_dir / "decisions_latest.json").exists() else []
        print(render_dashboard(scan_rows, dec_rows))


if __name__ == "__main__":
    main()
