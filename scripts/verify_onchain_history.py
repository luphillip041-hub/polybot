#!/usr/bin/env python3
from __future__ import annotations

import argparse
import gzip
import json
import os
from pathlib import Path
from typing import Any

from polymarket_bot.onchain_measurement import PolygonHttpRpc, reconcile_data_api_row


def recent_fill_rows(archive_dir: Path, limit: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for path in sorted(archive_dir.glob("shadow_*.jsonl.gz"), reverse=True):
        try:
            with gzip.open(path, "rt") as handle:
                for line in handle:
                    try:
                        row = json.loads(line)
                    except (ValueError, TypeError):
                        continue
                    if (row.get("type") or row.get("kind")) != "fill":
                        continue
                    trade = row.get("trade") or {}
                    tx_hash = str(trade.get("transactionHash") or row.get("trade_id") or "").lower()
                    wallet = str(row.get("wallet") or "").lower()
                    key = f"{tx_hash}:{wallet}:{trade.get('asset')}:{trade.get('size')}"
                    if not tx_hash.startswith("0x") or key in seen:
                        continue
                    seen.add(key)
                    rows.append(row)
        except (OSError, EOFError, gzip.BadGzipFile):
            continue
    rows.sort(key=lambda row: str(row.get("ts") or ""), reverse=True)
    return rows[:limit]


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify the V2 OrderFilled decoder against archived Data API fills")
    parser.add_argument("--archive-dir", type=Path, default=Path("runs/book_archive"))
    parser.add_argument("--limit", type=int, default=5)
    parser.add_argument("--rpc-url", default=os.getenv("POLYGON_HTTP_RPC_URL", "https://polygon-bor-rpc.publicnode.com"))
    args = parser.parse_args()
    rpc = PolygonHttpRpc(args.rpc_url)
    results: list[dict[str, Any]] = []
    for row in recent_fill_rows(args.archive_dir, args.limit):
        match, ground_truth_epoch, fills = reconcile_data_api_row(row, rpc)
        trade = row.get("trade") or {}
        results.append(
            {
                "transaction_hash": trade.get("transactionHash") or row.get("trade_id"),
                "wallet": row.get("wallet"),
                "data_api_side": trade.get("side"),
                "data_api_token_id": trade.get("asset"),
                "data_api_size": trade.get("size"),
                "data_api_price": trade.get("price"),
                "status": match.status,
                "durable_trade_id": match.fill.durable_trade_id if match.fill else None,
                "decoded_side": match.fill.side if match.fill else None,
                "decoded_token_id": match.fill.token_id if match.fill else None,
                "ground_truth_epoch": ground_truth_epoch,
                "order_filled_logs_in_receipt": len(fills),
                "owner_candidates": match.candidate_count,
            }
        )
    print(
        json.dumps(
            {
                "checked": len(results),
                "matched": sum(row["status"] == "matched" for row in results),
                "failed": sum(row["status"] != "matched" for row in results),
                "results": results,
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
