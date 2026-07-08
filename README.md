# Polymarket Copybot

Standalone paper-only Polymarket bot, separate from the trading desk.

What it does now:

- discovers active Polymarket events from Gamma public API
- scores markets for copy-trading suitability using liquidity/volume/probability/spread proxies
- reads CLOB books when token IDs are available
- paper-enters the best candidate only when guardrails pass
- journals every candidate, block, and paper order to JSONL
- renders a static dashboard artifact
- archives top-N liquid public CLOB order books to compressed daily JSONL
- shadow-journals tracked wallet fills plus 1/5/15 minute post-fill book states

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
python -m polymarket_bot.book_archive
```

Artifacts are written under `runs/`.

## Forward-looking book archive + shadow journal

Config lives in `archive_config.json` and defaults to the top 50 active, CLOB-enabled markets ranked by liquidity/24h volume. Runtime artifacts are isolated under `runs/book_archive/`:

- `book_YYYY-MM-DD.jsonl.gz` — compressed public book snapshots with timestamp, market/token metadata, best bid/ask, size, spread, and top 3 bid/ask levels.
- `shadow_YYYY-MM-DD.jsonl.gz` — tracked-wallet fills in archived markets plus follow-up book states at 1/5/15 minutes.
- `markets_latest.json` — currently covered markets/tokens.
- `heartbeat_latest.json` — daemon heartbeat, coverage, row counters, wallet matches, and estimated daily disk usage.

The daemon is research/paper only: it uses public Gamma/Data/CLOB endpoints, does not load keys, and has no order-submission code path.

Systemd install/run:

```bash
cp systemd/polymarket-copybot-book-archive.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now polymarket-copybot-book-archive.service
journalctl -u polymarket-copybot-book-archive.service -f
```
