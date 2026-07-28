"""Tests for the polymarket /api/positions endpoint."""

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch, MagicMock

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))


class PositionsEndpointTest(unittest.TestCase):
    """Mock the CLOB + state, hit the endpoint via TestClient."""

    def setUp(self):
        from fastapi.testclient import TestClient
        from polymarket_bot import status_api
        from polymarket_bot.paper_follower import PaperConfig

        # Build a fake state with 2 positions
        self.tmpdir = tempfile.mkdtemp()
        self.state_path = Path(self.tmpdir) / "state.json"
        state = {
            "processed_trade_ids": [],
            "positions": {
                "wallet1:token_AAA": {
                    "position_id": "wallet1:token_AAA",
                    "wallet": "wallet1",
                    "token": "token_AAA",
                    "cost_usd": 100.0,
                    "entry_price": 0.50,
                    "shares": 200.0,
                    "opened_at": "2026-07-28T12:00:00+00:00",
                },
                "wallet2:token_BBB": {
                    "position_id": "wallet2:token_BBB",
                    "wallet": "wallet2",
                    "token": "token_BBB",
                    "cost_usd": 50.0,
                    "entry_price": 0.10,
                    "shares": 500.0,
                    "opened_at": "2026-07-28T13:00:00+00:00",
                },
            },
        }
        self.state_path.write_text(json.dumps(state))
        # Patch PaperConfig to point at our state
        self._pc_patch = patch.object(
            PaperConfig, "load",
            classmethod(lambda cls: PaperConfig(state_path=self.state_path)),
        )
        self._pc_patch.start()

        # Patch best_bid_ask to return deterministic prices
        def fake_best_bid_ask(token, config=None):
            return {
                "ok": True,
                "best_bid": {"token_AAA": 0.75, "token_BBB": 0.99}.get(token, 0.5),
                "best_ask": 0.99,
                "spread": 0.05,
                "tick_size": None,
                "min_order_size": None,
                "raw_error": None,
            }
        self._clob_patch = patch("polymarket_bot.status_api.best_bid_ask", fake_best_bid_ask)
        self._clob_patch.start()

        # Reset cache
        status_api._POS_CACHE["ts"] = 0.0

        self.client = TestClient(status_api.app)

    def tearDown(self):
        self._pc_patch.stop()
        self._clob_patch.stop()
        import shutil
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_positions_endpoint(self):
        r = self.client.get("/api/positions")
        self.assertEqual(r.status_code, 200)
        data = r.json()
        self.assertEqual(data["count"], 2)
        self.assertEqual(data["total_cost"], 150.0)
        # Token_AAA: 200 shares * 0.75 = 150 mkt, -100 cost = +50 uPnL
        # Token_BBB: 500 * 0.99 = 495 mkt, -50 cost = +445 uPnL
        self.assertAlmostEqual(data["total_unrealized"], 495.0, places=2)

    def test_positions_have_all_fields(self):
        r = self.client.get("/api/positions")
        data = r.json()
        for p in data["positions"]:
            self.assertIn("position_id", p)
            self.assertIn("wallet", p)
            self.assertIn("token", p)
            self.assertIn("cost_usd", p)
            self.assertIn("shares", p)
            self.assertIn("entry_price", p)
            self.assertIn("current_price", p)
            self.assertIn("market_value", p)
            self.assertIn("unrealized_pnl", p)
            self.assertIn("unrealized_pct", p)
            self.assertIn("opened_at", p)

    def test_positions_sorted_newest_first(self):
        r = self.client.get("/api/positions")
        data = r.json()
        # Token_BBB opened later, should come first
        self.assertEqual(data["positions"][0]["token"], "token_BBB")
        self.assertEqual(data["positions"][1]["token"], "token_AAA")

    def test_positions_uses_bid_not_ask(self):
        r = self.client.get("/api/positions")
        data = r.json()
        # Token_AAA: bid 0.75, ask 0.99 — must use bid for long
        tok_aaa = next(p for p in data["positions"] if p["token"] == "token_AAA")
        self.assertEqual(tok_aaa["current_price"], 0.75)
        # market_value = 200 * 0.75 = 150
        self.assertAlmostEqual(tok_aaa["market_value"], 150.0, places=2)
        # uPnL = 150 - 100 = +50
        self.assertAlmostEqual(tok_aaa["unrealized_pnl"], 50.0, places=2)
        self.assertEqual(tok_aaa["unrealized_pct"], 50.0)

    def test_positions_cache(self):
        # First call fills cache
        r1 = self.client.get("/api/positions")
        # Now change state — but cache should return old data
        state = json.loads(self.state_path.read_text())
        state["positions"] = {}
        self.state_path.write_text(json.dumps(state))
        r2 = self.client.get("/api/positions")
        self.assertEqual(r1.json()["count"], 2)
        self.assertEqual(r2.json()["count"], 2)  # cached, not 0

    def test_positions_handles_empty_state(self):
        from polymarket_bot.paper_follower import PaperConfig
        empty_state = Path(self.tmpdir) / "empty.json"
        empty_state.write_text(json.dumps({"positions": {}, "processed_trade_ids": []}))
        with patch.object(
            PaperConfig, "load",
            classmethod(lambda cls: PaperConfig(state_path=empty_state)),
        ):
            from polymarket_bot import status_api
            status_api._POS_CACHE["ts"] = 0.0
            r = self.client.get("/api/positions")
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()["count"], 0)
        self.assertEqual(r.json()["total_cost"], 0.0)


if __name__ == "__main__":
    unittest.main()