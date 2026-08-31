# Micro-Live Runbook — Polymarket Copybot (gate: 2026-08-28)

Arming sequence for the $1k / $10-per-trade micro-live phase. Do NOT skip
steps; each one exists because something already broke once.

## 1. Prerequisites (operator provides)

- [ ] Polymarket account funded with **$1,000 USDC** (Polygon)
- [ ] USDC allowance approved for the CLOB exchange contracts (do it from the
      Polymarket UI once — the bot never handles approvals)
- [ ] The account's **private key** (L1 signing) — EOA holding the funds, or
      the proxy-signer setup if using a Polymarket proxy wallet
- [ ] Geo: account and VPS egress must both be in a permitted jurisdiction.
      Polymarket blocks US users on the main venue. If the account is US-based
      this stops here.

- [ ] **Permanent dashboard URL** — quick-tunnel hostnames rotate; before real money, stand up a named Cloudflare tunnel / domain for the ops dashboard (decision logged 2026-08-31)

## 2. Credentials file

Create `/root/flip/secrets/polymarket_live.env`, `chmod 600`, owner root:

```
POLYMARKET_PRIVATE_KEY=0x...            # L1 key (never in the repo)
# Either provide derived L2 creds explicitly:
POLYMARKET_CLOB_API_KEY=...
POLYMARKET_CLOB_SECRET=...
POLYMARKET_CLOB_PASSPHRASE=...
# ...or omit all three and the executor derives them at startup.
# POLYMARKET_FUNDER=0x...               # only for proxy/safe signature types
# POLYMARKET_SIGNATURE_TYPE=0           # 0 EOA (default), 1 proxy, 2 safe
```

## 3. Arming (in order)

```bash
# 3a. Micro-live sizing: $10/trade, tight caps. Daily cap stays 300 but at
#     $10 that's $3k/day theoretical max — bankroll binds first.
systemctl edit polymarket-copybot-paper-follower.service
# add:
#   [Service]
#   Environment=PAPER_STAKE_USD=10
#   Environment=POLYMARKET_PHASE=live
#   Environment=LIVETRADE_ENABLED=true
#   EnvironmentFile=/root/flip/secrets/polymarket_live.env

systemctl daemon-reload
systemctl restart polymarket-copybot-paper-follower.service
# VERIFY: journal shows LiveClobExecutor constructed (no exception), else the
# service crash-loops — that is intentional (misconfig fails loud).
```

## 4. Kill switch (memorize before arming)

```bash
systemctl edit polymarket-copybot-paper-follower.service   # set LIVETRADE_ENABLED=false
systemctl daemon-reload && systemctl restart polymarket-copybot-paper-follower.service
```
Open orders are NOT auto-canceled on shutdown in v1 — cancel them from the
Polymarket UI if you flip the switch with orders resting.

## 5. Micro-live acceptance (first 3 days)

- Every `live_fill` in `runs/live/live_fills.jsonl` must reconcile to a paper
  entry (same client_order_id = trade id). Zero unreconciled fills allowed.
- Realized slippage = fill price vs paper sim price. Kill if >2x model (1c).
- Daily loss cap: -$100 (10 trades) => manual review before continuing.
- Scorecard still runs daily; micro-live fill stats get added to it.

## 6. Scale gates (after micro-live)

- 3 clean days → $25/trade → 3 more clean days → $50 → $100 at the Sep 17
  30-day mark if the paper forward test stayed green the whole way.
