"""Tests for the wallet_quality module + /api/wallets/quality endpoint."""

import json
import sys
import tempfile
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))


class WalletQualityTest(unittest.TestCase):
    def setUp(self):
        from polymarket_bot import wallet_quality
        wallet_quality.clear_cache()
        self.tmpdir = tempfile.mkdtemp()
        self.ledger = Path(self.tmpdir) / "ledger.jsonl"
        # Two wallets: one winning, one losing
        rows = [
            # Winning wallet: 4 wins, 2 losses
            {"ts": "2026-07-25T10:00:00+00:00", "type": "signal", "wallet": "0xWIN", "pnl": None},
            {"ts": "2026-07-25T11:00:00+00:00", "type": "entry", "wallet": "0xWIN", "token": "T1", "sim_fill_price": 0.5, "sim_size": 200, "pnl": None},
            {"ts": "2026-07-25T13:00:00+00:00", "type": "exit", "wallet": "0xWIN", "token": "T1", "pnl": 50.0},
            {"ts": "2026-07-26T11:00:00+00:00", "type": "entry", "wallet": "0xWIN", "token": "T2", "sim_fill_price": 0.5, "sim_size": 200, "pnl": None},
            {"ts": "2026-07-26T13:00:00+00:00", "type": "exit", "wallet": "0xWIN", "token": "T2", "pnl": 30.0},
            {"ts": "2026-07-27T10:00:00+00:00", "type": "entry", "wallet": "0xWIN", "token": "T3", "sim_fill_price": 0.5, "sim_size": 200, "pnl": None},
            {"ts": "2026-07-27T13:00:00+00:00", "type": "exit", "wallet": "0xWIN", "token": "T3", "pnl": -50.0},
            # Losing wallet: 0 wins, 3 losses
            {"ts": "2026-07-25T10:00:00+00:00", "type": "signal", "wallet": "0xLOSE", "pnl": None},
            {"ts": "2026-07-25T11:00:00+00:00", "type": "entry", "wallet": "0xLOSE", "token": "T1", "sim_fill_price": 0.5, "sim_size": 200, "pnl": None},
            {"ts": "2026-07-25T13:00:00+00:00", "type": "exit", "wallet": "0xLOSE", "token": "T1", "pnl": -50.0},
            {"ts": "2026-07-26T13:00:00+00:00", "type": "exit", "wallet": "0xLOSE", "token": "T2", "pnl": -50.0},
        ]
        with open(self.ledger, "w") as f:
            for r in rows:
                f.write(json.dumps(r) + "\n")

    def tearDown(self):
        import shutil
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_winning_wallet_scores_higher(self):
        from polymarket_bot.wallet_quality import compute_wallet_quality
        results = compute_wallet_quality(ledger_path=self.ledger)
        by_wallet = {w["wallet"]: w for w in results}
        self.assertIn("0xwin", by_wallet)
        self.assertIn("0xlose", by_wallet)
        self.assertGreater(by_wallet["0xwin"]["quality_score"],
                          by_wallet["0xlose"]["quality_score"])

    def test_winning_wallet_metrics(self):
        from polymarket_bot.wallet_quality import compute_wallet_quality
        results = compute_wallet_quality(ledger_path=self.ledger)
        w = next(w for w in results if w["wallet"] == "0xwin")
        # 3 exits, 2 wins, 1 loss, total pnl = 30
        self.assertEqual(w["exits"], 3)
        self.assertEqual(w["wins"], 2)
        self.assertAlmostEqual(w["realized_pnl"], 30.0, places=2)
        self.assertAlmostEqual(w["win_rate"], 66.7, places=1)

    def test_holding_period_computed(self):
        from polymarket_bot.wallet_quality import compute_wallet_quality
        results = compute_wallet_quality(ledger_path=self.ledger)
        w = next(w for w in results if w["wallet"] == "0xwin")
        # 3 trades: 2h, 2h, 3h → avg = 2.33h
        self.assertAlmostEqual(w["avg_holding_hours"], 2.33, places=1)

    def test_losing_wallet_low_score(self):
        from polymarket_bot.wallet_quality import compute_wallet_quality
        results = compute_wallet_quality(ledger_path=self.ledger)
        w = next(w for w in results if w["wallet"] == "0xlose")
        self.assertLess(w["quality_score"], 50)

    def test_results_sorted_by_score_desc(self):
        from polymarket_bot.wallet_quality import compute_wallet_quality
        results = compute_wallet_quality(ledger_path=self.ledger)
        scores = [w["quality_score"] for w in results]
        self.assertEqual(scores, sorted(scores, reverse=True))

    def test_empty_ledger(self):
        from polymarket_bot.wallet_quality import compute_wallet_quality
        empty = Path(self.tmpdir) / "empty.jsonl"
        results = compute_wallet_quality(ledger_path=empty)
        self.assertEqual(results, [])

    def test_open_positions_from_state(self):
        from polymarket_bot import wallet_quality
        wallet_quality.clear_cache()
        # Write a state.json with an open position
        state_path = Path("/root/flip/projects/polymarket-copybot/runs/paper/state.json")
        orig_state = state_path.read_text() if state_path.exists() else "{}"
        try:
            state_path.write_text(json.dumps({
                "positions": {
                    "0xwin:T1": {"wallet": "0xwin", "token": "T1", "cost_usd": 100},
                }
            }))
            results = wallet_quality.compute_wallet_quality(ledger_path=self.ledger)
            w = next(w for w in results if w["wallet"] == "0xwin")
            self.assertEqual(w["open_positions"], 1)
        finally:
            state_path.write_text(orig_state)


class WalletsQualityEndpointTest(unittest.TestCase):
    def test_endpoint_shape(self):
        from fastapi.testclient import TestClient
        from polymarket_bot import status_api
        client = TestClient(status_api.app)
        r = client.get("/api/wallets/quality")
        self.assertEqual(r.status_code, 200)
        data = r.json()
        self.assertIn("wallets", data)
        self.assertIn("count", data)
        self.assertIn("generated_at", data)
        self.assertIsInstance(data["wallets"], list)


if __name__ == "__main__":
    unittest.main()