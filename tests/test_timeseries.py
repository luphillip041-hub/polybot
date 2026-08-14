"""Tests for the timeseries module + /api/pnl/timeseries endpoint."""

import json
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))


class TimeseriesTest(unittest.TestCase):
    def setUp(self):
        from polymarket_bot import timeseries
        # Reset cache between tests
        timeseries.clear_cache()
        self.tmpdir = tempfile.mkdtemp()
        self.ledger = Path(self.tmpdir) / "ledger.jsonl"
        # Write a small ledger with exit events on the final three days of
        # the rolling four-day window. Relative dates keep this test stable.
        today = datetime.now(timezone.utc).date()
        day_1 = today - timedelta(days=2)
        day_2 = today - timedelta(days=1)
        day_3 = today

        def ts(day, hour):
            return datetime.combine(
                day, datetime.min.time(), tzinfo=timezone.utc
            ).replace(hour=hour).isoformat()

        rows = [
            {"ts": ts(day_1, 10), "type": "exit", "wallet": "0xAAA", "pnl": 100.0},
            {"ts": ts(day_1, 14), "type": "exit", "wallet": "0xBBB", "pnl": -50.0},
            {"ts": ts(day_2, 9), "type": "exit", "wallet": "0xAAA", "pnl": 200.0},
            {"ts": ts(day_3, 11), "type": "exit", "wallet": "0xBBB", "pnl": 75.0},
            {"ts": ts(day_3, 12), "type": "signal", "wallet": "0xAAA", "pnl": 999.0},  # not exit
        ]
        with open(self.ledger, "w") as f:
            for r in rows:
                f.write(json.dumps(r) + "\n")

    def tearDown(self):
        import shutil
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_daily_pnl_cumulative(self):
        from polymarket_bot.timeseries import compute_daily_pnl
        result = compute_daily_pnl(ledger_path=self.ledger, days=4)
        # Last 4 days cumulative should be: 0, 50, 250, 325
        # 07-25: 0, 07-26: 50 (100 - 50), 07-27: 250, 07-28: 325
        non_zero = [d for d in result if d["daily_pnl"] != 0]
        self.assertEqual(len(non_zero), 3)
        # Verify cumulative at the last non-zero day
        self.assertAlmostEqual(result[-1]["cumulative_pnl"], 325.0, places=2)

    def test_daily_pnl_excludes_signals(self):
        from polymarket_bot.timeseries import compute_daily_pnl
        result = compute_daily_pnl(ledger_path=self.ledger, days=4)
        # The signal row has pnl=999 but type=signal — should NOT be counted
        # Total PnL should be 100 - 50 + 200 + 75 = 325
        self.assertAlmostEqual(result[-1]["cumulative_pnl"], 325.0, places=2)

    def test_daily_pnl_zero_for_no_data(self):
        from polymarket_bot.timeseries import compute_daily_pnl
        empty = Path(self.tmpdir) / "nonexistent.jsonl"
        result = compute_daily_pnl(ledger_path=empty, days=3)
        # Missing file → returns empty list (callers handle empty case)
        self.assertEqual(result, [])

    def test_per_wallet_daily(self):
        from polymarket_bot.timeseries import compute_per_wallet_daily
        result = compute_per_wallet_daily(ledger_path=self.ledger, days=4, top_n=5)
        series = result["series"]
        # Should have AAA (+300) and BBB (+25), sorted by total desc
        names = list(series.keys())
        self.assertGreaterEqual(len(names), 1)
        # Top wallet is AAA with total 300
        top = series[names[0]]
        self.assertAlmostEqual(top["total"], 300.0, places=2)

    def test_per_wallet_daily_excludes_signals(self):
        from polymarket_bot.timeseries import compute_per_wallet_daily
        result = compute_per_wallet_daily(ledger_path=self.ledger, days=4, top_n=5)
        # AAA signal row had pnl=999, but it's type=signal, not exit
        # Total AAA = 100 + 200 = 300, not 1299
        for name, info in result["series"].items():
            if "AAA" in info["wallet_address"]:
                self.assertAlmostEqual(info["total"], 300.0, places=2)


class PnlEndpointTest(unittest.TestCase):
    def test_endpoint_returns_daily_and_per_wallet(self):
        from fastapi.testclient import TestClient
        from polymarket_bot import status_api
        client = TestClient(status_api.app)
        r = client.get("/api/pnl/timeseries?days=7")
        self.assertEqual(r.status_code, 200)
        data = r.json()
        self.assertIn("daily", data)
        self.assertIn("per_wallet", data)
        self.assertEqual(data["days"], 7)
        self.assertIsInstance(data["daily"], list)
        self.assertIsInstance(data["per_wallet"], dict)


if __name__ == "__main__":
    unittest.main()