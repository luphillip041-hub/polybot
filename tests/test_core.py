import unittest
import gzip
import json
import tempfile
from pathlib import Path

from polymarket_bot.gamma import flatten_markets
from polymarket_bot.scoring import score_market
from polymarket_bot.paper import decide_paper
from polymarket_bot.data import score_wallet
from polymarket_bot.book_archive import normalize_levels, bbo_from_levels, trade_id, trade_fill_context, BookArchiveDaemon
from polymarket_bot.archive_config import ArchiveConfig


class CoreTests(unittest.TestCase):
    def test_flatten_market_maps_outcomes_prices_tokens(self):
        rows = flatten_markets([{"id":"e1","slug":"event","title":"Event","markets":[{"id":"m1","question":"Q","enableOrderBook":True,"outcomes":"['Yes','No']","outcomePrices":"['0.45','0.55']","clobTokenIds":"['y','n']","volume24hr":"10000","liquidity":"5000"}]}])
        self.assertEqual(rows[0]["outcomes"], ["Yes", "No"])
        self.assertEqual(rows[0]["outcome_prices"], [0.45, 0.55])
        self.assertEqual(rows[0]["clob_token_ids"], ["y", "n"])

    def test_score_blocks_missing_token(self):
        s = score_market({"enable_order_book": True, "volume_24h": 99999, "liquidity": 99999, "outcomes": ["Yes"], "outcome_prices": [0.5], "clob_token_ids": []})
        self.assertIn("missing outcomes/prices/token ids", s.blocked_reasons)

    def test_decision_blocks_wide_spread(self):
        m = {"market_slug":"x", "question":"Q"}
        s = score_market({"enable_order_book": True, "volume_24h": 99999, "liquidity": 99999, "outcomes": ["Yes","No"], "outcome_prices": [0.5,0.5], "clob_token_ids": ["t1","t2"]})
        d = decide_paper(m, s, {"ok": True, "best_bid": 0.3, "best_ask": 0.6, "spread": 0.3})
        self.assertEqual(d.decision, "blocked")
        self.assertTrue(any("spread" in x for x in d.blocked_reasons))

    def test_wallet_score_copyability_not_just_profit(self):
        row = {"proxyWallet": "0xabc", "userName": "demo", "rank": "1", "vol": 100000, "pnl": 20000}
        trades = [
            {"conditionId": f"m{i}", "side": "BUY", "size": 10, "price": 0.5}
            for i in range(6)
        ]
        scored = score_wallet(row, trades)
        self.assertGreaterEqual(scored["copy_score"], 80)
        self.assertIn("copyable average trade size", scored["reasons"])

    def test_book_levels_top_three_and_bbo(self):
        bids = normalize_levels([{"price": "0.40", "size": "10"}, {"price": "0.45", "size": "5"}, {"price": "0.39", "size": "7"}, {"price": "0.30", "size": "9"}], reverse=True)
        asks = normalize_levels([{"price": "0.60", "size": "2"}, {"price": "0.55", "size": "4"}, {"price": "0.70", "size": "8"}], reverse=False)
        self.assertEqual([x["price"] for x in bids], [0.45, 0.4, 0.39])
        self.assertEqual([x["price"] for x in asks], [0.55, 0.6, 0.7])
        self.assertEqual(bbo_from_levels(bids, asks)["spread"], 0.10000000000000003)

    def test_trade_id_prefers_chain_identity(self):
        self.assertEqual(trade_id({"transactionHash": "0xabc", "logIndex": 7, "id": "fallback"}), "0xabc:7")
        self.assertEqual(trade_id({"transactionHash": "0xabc", "id": "fallback"}), "0xabc")

    def test_fill_context_is_denormalized_for_followups(self):
        ctx = trade_fill_context({"price": "0.42", "side": "BUY", "size": "12.5", "timestamp": 123, "outcome": "Yes"})
        self.assertEqual(ctx["fill_price"], 0.42)
        self.assertEqual(ctx["fill_side"], "BUY")
        self.assertEqual(ctx["fill_size"], 12.5)

    def test_append_row_batches_single_gzip_member_and_hourly_paths(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            cfg = ArchiveConfig(archive_dir=root, state_path=root / "state.json", followup_queue_path=root / "followups.json")
            daemon = BookArchiveDaemon(cfg)
            daemon.append_row("book", {"type": "book", "n": 1})
            daemon.append_row("book", {"type": "book", "n": 2})
            self.assertEqual(list(root.glob("book_*.jsonl.gz")), [])
            daemon.flush_all()
            files = list(root.glob("book_*.jsonl.gz"))
            self.assertEqual(len(files), 1)
            self.assertRegex(files[0].name, r"book_\d{4}-\d{2}-\d{2}_\d{2}\.jsonl\.gz")
            with gzip.open(files[0], "rt") as f:
                rows = [json.loads(line) for line in f]
            self.assertEqual([r["n"] for r in rows], [1, 2])
            self.assertGreater(daemon.stats.rolling_disk_bytes_per_day, 0)
            self.assertEqual(daemon.stats.retention_footprint_bytes, daemon.stats.rolling_disk_bytes_per_day * cfg.retention_days)

    def test_startup_missed_followups_written_and_queue_persisted(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            q = root / "followups.json"
            q.write_text(json.dumps([{"due_ts": 1, "offset_seconds": 60, "wallet": "0xw", "trade_id": "fill1", "trade": {"price": 0.5, "side": "SELL"}}]))
            cfg = ArchiveConfig(archive_dir=root, state_path=root / "state.json", followup_queue_path=q)
            BookArchiveDaemon(cfg)
            shadows = list(root.glob("shadow_*.jsonl.gz"))
            self.assertEqual(len(shadows), 1)
            with gzip.open(shadows[0], "rt") as f:
                row = json.loads(next(f))
            self.assertEqual(row["type"], "followup_missed")
            self.assertEqual(row["offsets_missed"], [60])
            self.assertEqual(row["fill_price"], 0.5)
            self.assertEqual(json.loads(q.read_text()), [])

    def test_gap_marker_schema(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            cfg = ArchiveConfig(archive_dir=root, state_path=root / "state.json", followup_queue_path=root / "followups.json")
            daemon = BookArchiveDaemon(cfg)
            row = daemon.record_gap("2026-01-01T00:00:00+00:00", "2026-01-01T00:00:05+00:00", ["t1", "t2"], "unit_test")
            self.assertEqual(row["type"], "gap")
            self.assertEqual(row["tokens_affected"], ["t1", "t2"])
            with gzip.open(next(root.glob("book_*.jsonl.gz")), "rt") as f:
                persisted = json.loads(next(f))
            self.assertEqual(persisted["reason"], "unit_test")


if __name__ == "__main__":
    unittest.main()
