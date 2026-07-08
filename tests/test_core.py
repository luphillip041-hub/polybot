import unittest

from polymarket_bot.gamma import flatten_markets
from polymarket_bot.scoring import score_market
from polymarket_bot.paper import decide_paper
from polymarket_bot.data import score_wallet
from polymarket_bot.book_archive import normalize_levels, bbo_from_levels, trade_id


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


if __name__ == "__main__":
    unittest.main()
