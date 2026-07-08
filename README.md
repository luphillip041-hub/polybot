# Polymarket Copybot

Standalone paper-only Polymarket bot, separate from the trading desk.

What it does now:

- discovers active Polymarket events from Gamma public API
- scores markets for copy-trading suitability using liquidity/volume/probability/spread proxies
- reads CLOB books when token IDs are available
- paper-enters the best candidate only when guardrails pass
- journals every candidate, block, and paper order to JSONL
- renders a static dashboard artifact

What it does **not** do:

- no live trading
- no wallet private keys
- no CLOB order posting
- no trading-desk imports or shared state

## Quick start

```bash
python -m polymarket_bot.cli wallets --limit 10
python -m polymarket_bot.cli scan --limit 25
python -m polymarket_bot.cli run-paper --limit 25 --max-orders 1
python -m polymarket_bot.cli dashboard
```

Artifacts are written under `runs/`.
