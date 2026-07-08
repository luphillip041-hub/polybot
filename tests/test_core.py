import unittest
import asyncio
import gzip
import json
import tempfile
from pathlib import Path
from unittest.mock import patch

from polymarket_bot.gamma import flatten_markets
from polymarket_bot.scoring import score_market
from polymarket_bot.paper import decide_paper
from polymarket_bot.data import score_wallet
from polymarket_bot.book_archive import normalize_levels, bbo_from_levels, trade_id, trade_fill_context, BookArchiveDaemon
from polymarket_bot import book_archive as book_archive_module
from polymarket_bot.archive_config import ArchiveConfig
from polymarket_bot.status_api import RollingState, duration_s


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

    def test_status_shape_helpers(self):
        st = RollingState()
        st.last_refresh = 10**12  # prevent filesystem refresh in unit test
        st.heartbeat = {
            "stats": {"markets_covered": 2, "tokens_covered": 4},
            "disk_estimate": {"compressed_mb_per_day": 12.5, "retention_days": 45, "retention_gb": 0.56},
            "pending_followups": 3,
        }
        now = __import__("polymarket_bot.status_api", fromlist=["utc_now"]).utc_now()
        today = now.replace(hour=0, minute=0, second=0, microsecond=0)
        st.book_rows = [
            {"type": "book", "source": "websocket", "ts": now.isoformat()},
            {"type": "gap", "start_ts": today.isoformat(), "end_ts": (today.replace(minute=1)).isoformat(), "reason": "unit"},
        ]
        st.shadow_rows = [
            {"type": "fill", "ts": now.isoformat(), "wallet": "0xw", "trade": {"name": "demo", "conditionId": "m1"}},
            {"type": "followup_book", "ts": now.isoformat(), "wallet": "0xw", "fill_price": 0.5, "fill_side": "BUY"},
            {"type": "followup_missed", "ts": now.isoformat(), "wallet": "0xw"},
        ]
        out = st.status()
        self.assertEqual(set(out.keys()), {"generated_at", "archiver", "gaps_today", "coverage_pct_today", "shadow", "wallets"})
        self.assertEqual(set(out["archiver"].keys()), {"service_active", "ws_connected", "last_ws_message_age_s", "markets", "tokens", "book_rows_this_hour", "mb_per_day", "retention_days", "retention_gb"})
        self.assertEqual(out["shadow"]["fills_today"], 1)
        self.assertEqual(out["shadow"]["followups_completed_today"], 1)
        self.assertEqual(out["shadow"]["followups_missed_today"], 1)
        self.assertEqual(out["wallets"][0]["name"], "demo")
        self.assertEqual(duration_s(today.isoformat(), (today.replace(minute=1)).isoformat()), 60.0)

    def test_seen_but_unjournaled_wallet_trade_gets_shadow_row(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            state = root / "state.json"
            tid = "0xabc"
            state.write_text(json.dumps({"seen_trade_ids": [tid]}))
            cfg = ArchiveConfig(
                archive_dir=root,
                state_path=state,
                followup_queue_path=root / "followups.json",
                tracked_wallets=["0xw"],
                followup_offsets_seconds=(60, 300, 900),
            )
            daemon = BookArchiveDaemon(cfg)
            trade = {"transactionHash": tid, "timestamp": 1, "price": 0.4, "side": "BUY", "size": 10, "asset": "no-match"}
            with patch.object(book_archive_module, "user_trades", return_value=[trade]):
                daemon.poll_wallets_once()
            with gzip.open(next(root.glob("shadow_*.jsonl.gz")), "rt") as f:
                rows = [json.loads(line) for line in f]
            self.assertEqual(rows[0]["type"], "fill")
            self.assertEqual(rows[0]["trade_id"], tid)
            saved = json.loads(state.read_text())
            self.assertIn(tid, saved["journaled_trade_ids"])

    def test_ws_stale_timeout_writes_gap_and_resubscribes(self):
        class FakeWebsocket:
            connects = 0
            sends = 0

            async def __aenter__(self):
                type(self).connects += 1
                return self

            async def __aexit__(self, *_args):
                return False

            async def send(self, _payload):
                type(self).sends += 1
                if type(self).sends >= 2:
                    daemon.running = False

            async def recv(self):
                raise asyncio.TimeoutError()

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            cfg = ArchiveConfig(archive_dir=root, state_path=root / "state.json", followup_queue_path=root / "followups.json")
            daemon = BookArchiveDaemon(cfg)
            daemon.token_meta = {"t1": {"token_id": "t1"}}
            daemon.ws_stale_timeout_seconds = 0.01
            daemon.last_ws_message_ts = "2026-01-01T00:00:00+00:00"
            with patch.object(book_archive_module.websockets, "connect", return_value=FakeWebsocket()):
                asyncio.run(daemon.ws_loop())
            self.assertGreaterEqual(FakeWebsocket.connects, 2)
            self.assertGreaterEqual(FakeWebsocket.sends, 2)
            with gzip.open(next(root.glob("book_*.jsonl.gz")), "rt") as f:
                rows = [json.loads(line) for line in f]
            gap = next(row for row in rows if row.get("type") == "gap")
            self.assertEqual(gap["reason"], "ws_stale")
            self.assertEqual(gap["tokens_affected_count"], 1)


if __name__ == "__main__":
    unittest.main()
