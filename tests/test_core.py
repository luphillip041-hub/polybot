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
from polymarket_bot.paper_follower import (
    PaperConfig,
    PaperFollowerDaemon,
    paper_status,
    read_jsonl,
    render_trade_webhook,
    simulate_fill,
    resolution_exit_price,
    apply_resolution,
    run_resolution_cycle,
    check_positions_for_resolution,
    render_resolution_webhook,
)
from polymarket_bot.gamma import resolved_outcome_for_token, markets_by_token


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
            self.assertEqual(set(status.keys()), {"positions_open", "signals_today", "accepts_today", "accepts_by_latency", "rejects_today", "rejects_by_reason", "realized_pnl", "realized_pnl_today", "unrealized_pnl", "open_notional", "account_value", "avg_detection_latency_s", "detection_latency_p50", "detection_latency_p90", "poll_interval_s", "per_wallet"})
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

    def test_resolution_exit_price_mapping(self):
        # PRIMARY/SECONDARY (on-chain) — long-PRIMARY wins when status==PRIMARY
        self.assertEqual(resolution_exit_price("PRIMARY", "PRIMARY"), 1.0)
        self.assertEqual(resolution_exit_price("PRIMARY", "SECONDARY"), 0.0)
        self.assertEqual(resolution_exit_price("SECONDARY", "SECONDARY"), 1.0)
        self.assertEqual(resolution_exit_price("SECONDARY", "PRIMARY"), 0.0)
        # YES/NO (legacy Gamma) still works
        self.assertEqual(resolution_exit_price("YES", "YES"), 1.0)
        self.assertEqual(resolution_exit_price("YES", "NO"), 0.0)
        self.assertEqual(resolution_exit_price("YES", "1"), 1.0)
        self.assertEqual(resolution_exit_price("NO", "NO"), 1.0)
        self.assertEqual(resolution_exit_price("NO", "YES"), 0.0)
        self.assertEqual(resolution_exit_price("YES", "MAYBE"), None)
        self.assertEqual(resolution_exit_price(None, "YES"), None)
        self.assertEqual(resolution_exit_price("YES", None), None)

    def test_apply_resolution_writes_correct_pnl(self):
        state = {
            "positions": {
                "0xw:tok1": {
                    "wallet": "0xw",
                    "token": "tok1",
                    "entry_price": 0.50,
                    "shares": 200.0,
                    "cost_usd": 100.0,
                }
            }
        }
        action = {"action": "resolve", "pos_id": "0xw:tok1", "exit_price": 1.0, "side": "YES", "question": "Will it rain?", "market_id": "m1"}
        row = apply_resolution(state, action)
        self.assertEqual(row["type"], "resolution")
        self.assertEqual(row["sim_fill_price"], 1.0)
        # pnl = (1.0 * 200) - 100 = 100
        self.assertAlmostEqual(row["pnl"], 100.0, places=4)
        # Position popped
        self.assertNotIn("0xw:tok1", state["positions"])

        # Loss case
        state["positions"]["0xw:tok2"] = {"wallet": "0xw", "token": "tok2", "entry_price": 0.40, "shares": 250.0, "cost_usd": 100.0}
        row2 = apply_resolution(state, {"action": "resolve", "pos_id": "0xw:tok2", "exit_price": 0.0, "side": "YES", "question": "Will it rain?", "market_id": "m1"})
        # pnl = (0.0 * 250) - 100 = -100
        self.assertAlmostEqual(row2["pnl"], -100.0, places=4)

    def test_check_positions_resolution_skips_unresolved(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            paper = root / "paper"
            paper.mkdir()
            cfg = PaperConfig(paper_dir=paper, ledger_path=paper / "ledger.jsonl", state_path=paper / "state.json", allowlist_path=paper / "allowlist.json", data_quality_path=paper / "data_quality.json")
            cfg.allowlist_path.write_text(json.dumps({"wallets": ["0xw"]}))
            archive_cfg = ArchiveConfig(archive_dir=root / "archive", state_path=root / "state.json", followup_queue_path=root / "followups.json")
            daemon = PaperFollowerDaemon(cfg, archive_cfg)
            # Inject a synthetic open position
            daemon.state["positions"]["0xw:tok_unres"] = {"wallet": "0xw", "token": "tok_unres", "entry_price": 0.50, "shares": 200.0, "cost_usd": 100.0}
            daemon.state["positions"]["0xw:tok_unknown"] = {"wallet": "0xw", "token": "tok_unknown", "entry_price": 0.50, "shares": 200.0, "cost_usd": 100.0}
            with patch("polymarket_bot.paper_follower._onchain_resolved_outcome_for_token") as mock:
                from unittest.mock import MagicMock
                mock.side_effect = [
                    {"resolved": False, "resolution_status": None, "side": "PRIMARY", "question": "Q", "market_id": "m1", "closed": False, "active": True},
                    None,  # gamma unknown
                ]
                actions = check_positions_for_resolution(daemon.state)
            skip_reasons = [a.get("reason") for a in actions if a["action"] == "skip"]
            self.assertIn("not_resolved", skip_reasons)
            self.assertIn("no_condition_id", skip_reasons)
            # No resolve actions emitted
            self.assertFalse(any(a["action"] == "resolve" for a in actions))

    def test_run_resolution_cycle_emits_exits_and_persists(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            paper = root / "paper"
            paper.mkdir()
            cfg = PaperConfig(paper_dir=paper, ledger_path=paper / "ledger.jsonl", state_path=paper / "state.json", allowlist_path=paper / "allowlist.json", data_quality_path=paper / "data_quality.json")
            cfg.allowlist_path.write_text(json.dumps({"wallets": ["0xw"]}))
            archive_cfg = ArchiveConfig(archive_dir=root / "archive", state_path=root / "state.json", followup_queue_path=root / "followups.json")
            daemon = PaperFollowerDaemon(cfg, archive_cfg)
            # Inject two open positions — one resolves YES, one stays unresolved
            daemon.state["positions"]["0xw:winner"] = {"wallet": "0xw", "token": "winner_token", "entry_price": 0.50, "shares": 200.0, "cost_usd": 100.0}
            daemon.state["positions"]["0xw:loser_unres"] = {"wallet": "0xw", "token": "loser_token", "entry_price": 0.50, "shares": 200.0, "cost_usd": 100.0}
            with patch("polymarket_bot.paper_follower._onchain_resolved_outcome_for_token") as mock:
                mock.side_effect = [
                    {"resolved": True, "resolution_status": "PRIMARY", "side": "PRIMARY", "question": "Q?", "market_id": "m1", "closed": True},
                    {"resolved": False, "resolution_status": None, "side": "PRIMARY", "question": "Q?", "market_id": "m2", "closed": False},
                ]
                # Pass config explicitly to bypass monkeypatch on import
                summary = run_resolution_cycle(daemon.state, cfg, config=None)
            self.assertEqual(summary["checked"], 2)
            self.assertEqual(summary["resolved"], 1)
            self.assertEqual(summary["skipped"], 1)
            # winner popped
            self.assertNotIn("0xw:winner", daemon.state["positions"])
            # loser still there
            self.assertIn("0xw:loser_unres", daemon.state["positions"])
            # Ledger has a resolution row
            ledger_rows = read_jsonl(cfg.ledger_path)
            res_rows = [r for r in ledger_rows if r.get("type") == "resolution"]
            self.assertEqual(len(res_rows), 1)
            self.assertEqual(res_rows[0]["position_id"], "0xw:winner")
            self.assertAlmostEqual(res_rows[0]["pnl"], 100.0, places=4)
            # Status reflects realized PnL
            status = paper_status(cfg)
            self.assertAlmostEqual(status["realized_pnl"], 100.0, places=4)
            self.assertAlmostEqual(status["realized_pnl_today"], 100.0, places=4)

    def test_process_resolution_once_throttles_and_force(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            paper = root / "paper"
            paper.mkdir()
            cfg = PaperConfig(paper_dir=paper, ledger_path=paper / "ledger.jsonl", state_path=paper / "state.json", allowlist_path=paper / "allowlist.json", data_quality_path=paper / "data_quality.json", resolution_poll_seconds=999999)
            cfg.allowlist_path.write_text(json.dumps({"wallets": ["0xw"]}))
            acfg = ArchiveConfig(archive_dir=root / "archive", state_path=root / "state.json", followup_queue_path=root / "followups.json")
            daemon = PaperFollowerDaemon(cfg, acfg)
            with patch("polymarket_bot.paper_follower.run_resolution_cycle") as m:
                m.return_value = {"checked": 0, "resolved": 0, "skipped": 0, "last_checked_at": "x"}
                self.assertIsNone(daemon.process_resolution_once())
                # Force bypasses throttle
                summary = daemon.process_resolution_once(force=True)
                self.assertEqual(summary["checked"], 0)
                self.assertGreaterEqual(m.call_count, 1)

    def test_render_resolution_webhook_label(self):
        # Resolution row renders as "MARKET RESOLVED"
        row = {"type": "resolution", "wallet": "0xw", "market": "Q?", "token": "tok123", "side": "BUY", "sim_fill_price": 1.0, "pnl": 50.0}
        status = {"account_value": 1000, "open_notional": 100, "realized_pnl": 50}
        msg = render_trade_webhook(row, status) or render_resolution_webhook(row, status)
        self.assertIn("MARKET RESOLVED", msg)
        self.assertIn("`0xw`", msg)

    def test_resolution_calldata_encoders(self):
        from eth_abi.abi import decode
        from polymarket_bot.resolution import (
            encode_payout_denominator_call,
            encode_payout_numerators_call,
            encode_try_aggregate,
            decode_try_aggregate_result,
            CTF_ADDRESS,
            SEL_PAYOUT_DENOM,
            SEL_PAYOUT_NUMS,
        )
        cond_id = "0x31336effb831028d68e8ba3ef775a83ef77600fa932052b865996838e3cbc226"
        cd = encode_payout_denominator_call(cond_id)
        # First 4 bytes should be the selector; the rest is ABI-encoded bytes32
        self.assertEqual(cd[:4].hex(), SEL_PAYOUT_DENOM)
        arg = cd[4:]
        [decoded] = decode(["bytes32"], arg)
        self.assertEqual(bytes.fromhex(cond_id[2:]), decoded)

        # payoutNumerators(bytes32, uint256)
        cd2 = encode_payout_numerators_call(cond_id, 1)
        self.assertEqual(cd2[:4].hex(), SEL_PAYOUT_NUMS)
        [cbn, idx] = decode(["bytes32", "uint256"], cd2[4:])
        self.assertEqual(bytes.fromhex(cond_id[2:]), cbn)
        self.assertEqual(idx, 1)

        # Multicall3 encoding round-trip
        ctf_bytes = bytes.fromhex(CTF_ADDRESS[2:].lower())
        calls = [
            (ctf_bytes, encode_payout_denominator_call(cond_id)),
            (ctf_bytes, encode_payout_numerators_call(cond_id, 1)),
        ]
        agg_cd = encode_try_aggregate(calls)
        self.assertTrue(agg_cd.startswith(bytes.fromhex("bce38bd7")))

    def test_clob_condition_id_lookup_real(self):
        """Real CLOB round-trip — confirms we can map token → condition_id via the CLOB service."""
        from polymarket_bot.resolution import map_token_to_condition
        # Known token from our open position book — should resolve to a real condition_id via CLOB
        token = "114904041730147599562228540677844419716758281936953961039142785465496240966083"
        info = map_token_to_condition(token)
        self.assertIsNotNone(info)
        self.assertTrue(info["condition_id"].startswith("0x"))
        self.assertEqual(info["primary_token_id"].lower(), token.lower())


if __name__ == "__main__":
    unittest.main()


class ConfigTests(unittest.TestCase):
    """Tests for env-based config helpers (from the reviewed zip's approach)."""

    def test_env_int(self):
        """env_int reads integers from environment."""
        from polymarket_bot.config import env_int
        with patch.dict("os.environ", {"TEST_INT": "42"}):
            self.assertEqual(env_int("TEST_INT", 0), 42)

    def test_env_int_fallback(self):
        """env_int falls back to default for missing keys."""
        from polymarket_bot.config import env_int
        self.assertEqual(env_int("MISSING_VAR_XYZ", 150), 150)

    def test_env_str(self):
        """env_str reads strings from environment."""
        from polymarket_bot.config import env_str
        with patch.dict("os.environ", {"TEST_STR": "hello"}):
            self.assertEqual(env_str("TEST_STR", "fallback"), "hello")

    def test_env_str_fallback(self):
        """env_str falls back to default for missing keys."""
        from polymarket_bot.config import env_str
        self.assertEqual(env_str("MISSING_VAR_XYZ", "default_val"), "default_val")

    def test_env_float(self):
        """env_float reads floats from environment."""
        from polymarket_bot.config import env_float
        with patch.dict("os.environ", {"TEST_FLOAT": "3.14"}):
            self.assertEqual(env_float("TEST_FLOAT", 0.0), 3.14)

    def test_archive_config_load_from_file(self):
        """ArchiveConfig.load reads from a real JSON file."""
        import tempfile, json
        from polymarket_bot.archive_config import ArchiveConfig
        cfg_data = {"top_n_markets": 5, "max_tokens": 100}
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(cfg_data, f)
            cfg_path = f.name
        try:
            cfg = ArchiveConfig.load(cfg_path)
            self.assertEqual(cfg.top_n_markets, 5)
            self.assertEqual(cfg.max_tokens, 100)
        finally:
            import os as _os
            _os.unlink(cfg_path)


class AlertsTests(unittest.TestCase):
    """Tests for Telegram alerts module."""

    def test_send_telegram_noop_when_not_configured(self):
        """send_telegram silently returns True when Telegram not configured."""
        from polymarket_bot.alerts import send_telegram
        with patch.dict("os.environ", {}, clear=True):
            result = send_telegram("test message")
            self.assertTrue(result)

    def test_send_telegram_noop_on_empty_token(self):
        """send_telegram returns True when only chat_id is missing."""
        from polymarket_bot.alerts import send_telegram
        with patch.dict("os.environ", {"TELEGRAM_BOT_TOKEN": "test", "TELEGRAM_CHAT_ID": ""}):
            result = send_telegram("test")
            self.assertTrue(result)

    def test_send_telegram_http_call_mocked(self):
        """send_telegram makes HTTP POST to Telegram API."""
        from unittest.mock import patch as mock_patch
        from polymarket_bot.alerts import send_telegram
        with patch.dict("os.environ", {"TELEGRAM_BOT_TOKEN": "123:abc", "TELEGRAM_CHAT_ID": "456"}):
            with mock_patch("requests.post") as mock_post:
                mock_post.return_value.status_code = 200
                result = send_telegram("hello")
                self.assertTrue(result)
                mock_post.assert_called_once()
                url = mock_post.call_args[0][0]
                self.assertIn("123:abc", url)


class PreflightTests(unittest.TestCase):
    """Tests for preflight script."""

    def test_check_python_version_passes(self):
        """check_python_version returns True on current Python."""
        from scripts.preflight import check_python_version
        import sys
        expected = sys.version_info[:2]
        with patch.object(sys, "version_info", sys.version_info):
            result = check_python_version()
            self.assertTrue(result)

    def test_check_config_root_exists(self):
        """check_config passes when root/runs exist."""
        from scripts.preflight import check_config
        with patch("pathlib.Path.exists", return_value=True):
            result = check_config()
            self.assertTrue(result)

