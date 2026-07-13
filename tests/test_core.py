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
import polymarket_bot.book_archive as book_archive_module
from polymarket_bot.archive_config import ArchiveConfig
from polymarket_bot.status_api import RollingState, duration_s
from polymarket_bot.paper_follower import PaperConfig, PaperFollowerDaemon, paper_status, read_jsonl, render_trade_webhook, simulate_fill


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
        self.assertEqual(set(out["archiver"].keys()), {"service_active", "ws_connected", "last_ws_message_age_s", "markets", "tokens", "book_rows_this_hour", "mb_per_day", "retention_days", "retention_gb", "wallet_driven_tokens", "wallet_token_coverage_pct"})
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

    def test_paper_fill_model_walks_levels_and_haircuts(self):
        book = {
            "top3_asks": [{"price": 0.5, "size": 100}, {"price": 0.6, "size": 100}],
            "top3_bids": [{"price": 0.4, "size": 100}, {"price": 0.3, "size": 100}],
        }
        buy_price, buy_size, err = simulate_fill(book, "BUY", 100, 0.005)
        self.assertIsNone(err)
        self.assertGreater(buy_price, 0.5)
        self.assertGreater(buy_size, 0)
        sell_price, sell_size, err = simulate_fill(book, "SELL", 60, 0.005)
        self.assertIsNone(err)
        self.assertLess(sell_price, 0.4)
        self.assertGreater(sell_size, 0)

    def test_paper_follower_rejects_stale_fill_and_status_shape(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            archive = root / "archive"
            paper = root / "paper"
            archive.mkdir()
            cfg = PaperConfig(paper_dir=paper, ledger_path=paper / "ledger.jsonl", state_path=paper / "state.json", allowlist_path=paper / "allowlist.json", data_quality_path=paper / "data_quality.json")
            paper.mkdir()
            cfg.allowlist_path.write_text(json.dumps({"wallets": ["0xw"]}))
            acfg = ArchiveConfig(archive_dir=archive, state_path=root / "shadow_state.json", followup_queue_path=archive / "followups.json")
            daemon = PaperFollowerDaemon(cfg, acfg)
            row = {
                "ts": "2026-01-01T00:10:00+00:00",
                "type": "fill",
                "wallet": "0xw",
                "trade_id": "t1",
                "archive_matched": True,
                "fill_timestamp": 1,
                "fill_side": "BUY",
                "fill_price": 0.5,
                "trade": {"asset": "tok", "side": "BUY", "timestamp": 1, "price": 0.5, "conditionId": "m"},
                "book_at_detection": {"token_id": "tok", "best_bid": 0.49, "best_ask": 0.5, "best_bid_size": 1000, "best_ask_size": 1000, "spread": 0.01, "top3_asks": [{"price": 0.5, "size": 1000}], "top3_bids": [{"price": 0.49, "size": 1000}]},
            }
            rows = daemon.process_fill(row, 0)
            for out in rows:
                from polymarket_bot.paper_follower import append_jsonl_fsync
                append_jsonl_fsync(cfg.ledger_path, out)
            self.assertEqual(rows[1]["type"], "reject")
            self.assertIn("stale_fill", rows[1]["reject_reason"])
            self.assertEqual(set(rows[1]["book_snapshot"].keys()), {"best_bid", "best_ask", "bid_size", "ask_size", "spread"})
            status = paper_status(cfg)
            self.assertEqual(set(status.keys()), {"positions_open", "signals_today", "accepts_today", "accepts_by_latency", "rejects_today", "rejects_by_reason", "realized_pnl", "unrealized_pnl", "open_notional", "account_value", "avg_detection_latency_s", "detection_latency_p50", "detection_latency_p90", "poll_interval_s", "per_wallet"})
            self.assertGreaterEqual(status["rejects_today"], 1)
    def test_paper_follower_entry_and_exit_rows_include_book_snapshot(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            archive = root / "archive"
            paper = root / "paper"
            archive.mkdir(); paper.mkdir()
            cfg = PaperConfig(paper_dir=paper, ledger_path=paper / "ledger.jsonl", state_path=paper / "state.json", allowlist_path=paper / "allowlist.json", data_quality_path=paper / "data_quality.json", max_ws_age_seconds=999999999)
            cfg.allowlist_path.write_text(json.dumps({"wallets": ["0xw"]}))
            acfg = ArchiveConfig(archive_dir=archive, state_path=root / "shadow_state.json", followup_queue_path=archive / "followups.json")
            daemon = PaperFollowerDaemon(cfg, acfg)
            book = {"token_id": "tok", "best_bid": 0.49, "best_ask": 0.5, "best_bid_size": 1000, "best_ask_size": 1000, "spread": 0.01, "top3_asks": [{"price": 0.5, "size": 1000}], "top3_bids": [{"price": 0.49, "size": 1000}]}
            buy = {"ts": "2026-01-01T00:00:00+00:00", "wallet": "0xw", "trade_id": "buy1", "fill_timestamp": "2026-01-01T00:00:00+00:00", "fill_side": "BUY", "fill_price": 0.5, "trade": {"asset": "tok", "side": "BUY", "timestamp": "2026-01-01T00:00:00+00:00", "price": 0.5, "conditionId": "m"}, "book_at_detection": book}
            buy_rows = daemon.process_fill(buy, 0)
            entry = next(row for row in buy_rows if row["type"] == "entry")
            self.assertEqual(set(entry["book_snapshot"].keys()), {"best_bid", "best_ask", "bid_size", "ask_size", "spread"})
            sell = {"ts": "2026-01-01T00:00:01+00:00", "wallet": "0xw", "trade_id": "sell1", "fill_timestamp": "2026-01-01T00:00:01+00:00", "fill_side": "SELL", "fill_price": 0.49, "trade": {"asset": "tok", "side": "SELL", "timestamp": "2026-01-01T00:00:01+00:00", "price": 0.49, "conditionId": "m"}, "book_at_detection": book}
            sell_rows = daemon.process_fill(sell, 1)
            exit_row = next(row for row in sell_rows if row["type"] == "exit")
            self.assertEqual(set(exit_row["book_snapshot"].keys()), {"best_bid", "best_ask", "bid_size", "ask_size", "spread"})
            msg = render_trade_webhook(entry, {"account_value": 100, "open_notional": 100, "realized_pnl": 0})
            self.assertIn("PAPER BUY", msg)
            self.assertIn("Paper account value", msg)

    def test_wallet_driven_token_added_on_unmatched_trade(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            cfg = ArchiveConfig(archive_dir=root, state_path=root / "state.json", followup_queue_path=root / "followups.json", max_tokens=400)
            daemon = BookArchiveDaemon(cfg)
            trade = {"asset": "0xnewtoken", "side": "BUY", "price": 0.5, "size": 10, "timestamp": 100, "conditionId": "0xcond"}
            daemon._ensure_wallet_trade_tokens(trade)
            self.assertIn("0xnewtoken", daemon.wallet_driven_tokens)
            self.assertEqual(len(daemon.token_meta), 1)

    def test_eviction_removes_old_non_wallet_tokens_first(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            cfg = ArchiveConfig(archive_dir=root, state_path=root / "state.json", followup_queue_path=root / "followups.json", max_tokens=10)
            daemon = BookArchiveDaemon(cfg)
            # Add 8 wallet-driven tokens
            for i in range(8):
                tid = f"wallet_{i}"
                daemon.wallet_driven_tokens.add(tid)
                daemon.token_meta[tid] = {"token_id": tid, "wallet_driven": True}
            # Add 5 top-50 baseline tokens
            for i in range(5):
                tid = f"base_{i}"
                daemon.token_meta[tid] = {"token_id": tid}
            daemon._evict_excess_tokens()
            self.assertLessEqual(len(daemon.token_meta), 10)
            # All wallet-driven tokens survive
            for i in range(8):
                self.assertIn(f"wallet_{i}", daemon.token_meta)
            # Only 2 baseline tokens survive (8 + 2 = 10)
            baseline_survivors = [tid for tid in daemon.token_meta if tid.startswith("base_")]
            self.assertEqual(len(baseline_survivors), 2)

    def test_configured_wallets_resolves_path_via_archive_dir_parent(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            archive = root / "book_archive"
            archive.mkdir()
            # Write a scores file in the parent dir (the runs root)
            scores = root / "wallet_scores_latest.json"
            scores.write_text(json.dumps([{"wallet": "0xscores_wallet", "user_name": "scorebot"}]))
            cfg = ArchiveConfig(
                archive_dir=archive,
                state_path=root / "state.json",
                followup_queue_path=root / "followups.json",
                tracked_wallets=["0xtracked_wallet"],
                tracked_wallet_limit_from_scores=5,
            )
            daemon = BookArchiveDaemon(cfg)
            wallets = daemon._configured_wallets()
            self.assertIn("0xtracked_wallet", wallets)
            self.assertIn("0xscores_wallet", wallets)

    def test_daily_entry_cap_replaces_daily_signal_cap(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            archive = root / "archive"
            paper = root / "paper"
            archive.mkdir(); paper.mkdir()
            cfg = PaperConfig(paper_dir=paper, ledger_path=paper / "ledger.jsonl", state_path=paper / "state.json", allowlist_path=paper / "allowlist.json", data_quality_path=paper / "data_quality.json", max_ws_age_seconds=999999999, max_signals_per_day=2)
            cfg.allowlist_path.write_text(json.dumps({"wallets": ["0xw"]}))
            acfg = ArchiveConfig(archive_dir=archive, state_path=root / "shadow_state.json", followup_queue_path=archive / "followups.json")
            daemon = PaperFollowerDaemon(cfg, acfg)
            book = {"token_id": "tok1", "best_bid": 0.49, "best_ask": 0.5, "best_bid_size": 1000, "best_ask_size": 1000, "spread": 0.01, "top3_asks": [{"price": 0.5, "size": 1000}], "top3_bids": [{"price": 0.49, "size": 1000}]}
            # First fill -> entry (accepts_today=0)
            buy1 = {"ts": "2026-01-01T00:00:00+00:00", "wallet": "0xw", "trade_id": "buy1", "fill_timestamp": "2026-01-01T00:00:00+00:00", "fill_side": "BUY", "fill_price": 0.5, "trade": {"asset": "tok1", "side": "BUY", "timestamp": "2026-01-01T00:00:00+00:00", "price": 0.5, "conditionId": "m1"}, "book_at_detection": book}
            r1 = daemon.process_fill(buy1, 0)
            self.assertEqual(r1[1]["type"], "entry")
            # Second fill -> entry (accepts_today=1)
            buy2 = {"ts": "2026-01-01T00:00:01+00:00", "wallet": "0xw", "trade_id": "buy2", "fill_timestamp": "2026-01-01T00:00:01+00:00", "fill_side": "BUY", "fill_price": 0.5, "trade": {"asset": "tok2", "side": "BUY", "timestamp": "2026-01-01T00:00:01+00:00", "price": 0.5, "conditionId": "m2"}, "book_at_detection": book}
            r2 = daemon.process_fill(buy2, 1)
            self.assertEqual(r2[1]["type"], "entry")
            # Third fill -> reject daily_entry_cap (accepts_today=2, cap=2)
            buy3 = {"ts": "2026-01-01T00:00:02+00:00", "wallet": "0xw", "trade_id": "buy3", "fill_timestamp": "2026-01-01T00:00:02+00:00", "fill_side": "BUY", "fill_price": 0.5, "trade": {"asset": "tok3", "side": "BUY", "timestamp": "2026-01-01T00:00:02+00:00", "price": 0.5, "conditionId": "m3"}, "book_at_detection": book}
            r3 = daemon.process_fill(buy3, 2)
            self.assertEqual(r3[1]["type"], "reject")
            self.assertIn("daily_entry_cap", r3[1]["reject_reason"])


if __name__ == "__main__":
    unittest.main()
