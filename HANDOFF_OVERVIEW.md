# polymarket-copybot — handoff overview for Claude

You are continuing work on `/root/flip/projects/polymarket-copybot` — a paper-trading bot that mirrors Polymarket trades from 5 tracked wallets. Today's session (2026-07-23) flipped two filters from gates to tags per a critique; #4 (capacity calc) is still in progress.

## Project context

- **Repo:** `/root/flip/projects/polymarket-copybot`
- **Remote:** `github.com/luphillip041-hub/polybot` (luphillip041-hub account; Leesa is the committer)
- **Stack:** Python 3.12, systemd services (`book-archive`, `discord-monitor`, `paper-follower`, `status-api`)
- **Data sources:**
  - Polymarket CLOB (live book depth, gamma API for market discovery)
  - On-chain (`eth_abi`, `eth-utils`, `pycryptodome` for resolution via `ConditionalTokens`)
  - `runs/paper/ledger.jsonl` — paper_follower output (12,171 rows as of 2026-07-23)
  - `runs/paper/allowlist.json` — gitignored, runtime config
  - `runs/paper/state.json` — positions, processed_trade_ids, rolling state
  - `runs/wallet_scores_latest.json` — gitignored, wallet scoring output
- **Stake config:** $100/trade, 20 entries/day → theoretical $2k/day, $60k/mo
- **Hard-reject filters** (still gates): `illiquid_depth`, `stale_fill`, `near_resolution`, `blind_ws_stale`, `blind_gap`, `no_archived_book`, `illiquid_spread`, `duplicate`, `sell_no_position`, `daily_entry_cap`

## Tracked wallets (`configured_wallets()` output)

| # | address | name |
|---|---------|------|
| 1 | `0xfe787d2da716d60e8acff57fb13cd4d10319` | `ferrariChampions2026` |
| 2 | `0x224a89dbe0db0d6124b335edabd15b3f877da3d5` | `wr0ngw4yb3tt0r` |
| 3 | `0xee00ba338c59557141789b127927a55f5cc5cea1` | `S-Works` |
| 4 | `0x4be1fa92e6ceaf886aac0bbec3be6c527133aa70` | `Eztennis` |
| 5 | `0x4bff30af91642dc7d2b19a8664378fe55c45fc26` | `Sassy-Bucket` |

## Today's commits (5 pushed to `origin/main`)

| sha | message |
|-----|---------|
| `9a9e571` | fix: drop rejected gamma API sort params + add missing on-chain resolution deps *(was unpushed)* |
| `34fb4ac` | fix(status-api): bound memory by streaming archives + caching paper_status *(was unpushed; the commit item #5 was worried about losing)* |
| `94c8c9a` | feat(paper-follower): skip BUYs at <10¢ (lottery filter) *(was unpushed)* |
| `b9fecd0` | fix: add pycryptodome (missed in 9a9e571 resolution-deps commit) |
| `531ff3a` | refactor(paper-follower): revert lottery filter + 4-wallet prune to eligible_live tag |

Working tree clean post-push. Nothing local-only anymore. No secrets in any commit.

## Today's refactor (#1 done)

Both filters are now tags, not gates:

- `runs/paper/allowlist.json` — all 5 wallets (was Ferrari-only)
- `polymarket_bot/paper_follower.py`:
  - `reject_reasons()` no longer appends `wallet_not_allowlisted` or `lottery_price_band`
  - new `is_lottery_band(row)` helper (tag-only check; doesn't gate execution directly)
  - `signal_row()` carries `eligible_live` bool (True default; overridden in `process_fill`)
  - `process_fill()`: lottery rows emit `type='ineligible'` with `eligible_live=False` and `reject_reason='lottery_price_band'`. Hard rejects keep `type='reject'` so they're distinguishable. Execution behavior on lottery trades is unchanged (still don't execute) — the change is purely in classification + persistence.
  - syntax check passes

## Original critique (flipphill 2026-07-23, ranked)

1. ✅ **Filter as tag, not prune** — done. Both lottery + wallet exclusion are tags now.
2. ⏸️ **Widen band** — gated on #4. If capacity is real, the band's edge is testable from the populated ledger (not another 30d wait).
3. ⏸️ **Drop WR for calibration** — gated on #4. Bucket every fill by price, compare realized resolution frequency to fill price. Testable at ~50-100 trades per bucket, works pooled across all 5 wallets.
4. 🔄 **Capacity before signal** — pending. The 5x depth wall is unquantified; this is the gating question for #2/#3. Compute max deployable notional at acceptable slippage.
5. ✅ **Push commits** — done. 5 commits on `origin/main`. Single-disk-event risk on the OOM fix is gone.
6. ⏸️ **CI gating** — deferred per original framing. Don't build CI until signal exists.

## Pending (#4 — what to do next)

**Compute deployable notional at acceptable slippage on the markets the 5 wallets trade.**

The "5x depth wall" needs a quantified number — that's the gating input for #2 and #3.

Approach:

1. Pull the set of distinct `market` (conditionId) values from `runs/paper/ledger.jsonl` over the last 30d
2. For each, query Polymarket CLOB `/book?token=<token_id>` for top-3 asks
3. Walk fill simulation at $100 / $500 / $1000 / $5000 / $10000 notional — record slippage vs best ask
4. Aggregate: at 1% / 2% / 5% acceptable slippage, what's max deployable notional per market? Across the 5 wallets' active universe?
5. Report the answer as a single number: "**max deployable notional at acceptable slippage = $X**"

If the honest answer is $3-5k, then even a real edge is a hobby, and #2/#3 are not worth doing. If it's $50k+, then #3 calibration is the right next move.

CLI endpoint shape (gamma): `https://gamma-api.polymarket.com/markets?condition_id=...`
Book endpoint: `https://clob.polymarket.com/book?token=<token_id>`

## Forward-test implications

- Next 30d populates the ledger with all 5 wallets × all price bands (since #1 reverted)
- Bucket-level calibration (#3) becomes a single SQL/JSON query: `group by wallet, price_bucket, eligible_live`
- Longshot-edge hypothesis testable in weeks, not months
- If Ferrari's <10¢ wins are variance → #2's hypothesis is falsified within weeks
- If they're real edge → #3 calibration shows Ferrari is +pts at the 5¢ bucket specifically

## Dashboard

Streamlit app at **http://147.93.186.165:8503**

- File: `/root/flip/projects/polymarket-copybot/dashboard/app.py`
- Reads `runs/paper/ledger.jsonl` with 15s cache
- Sections: entry classification (counts), per-wallet breakdown, ineligible reasons bar (post-revert — currently empty since ledger is pre-revert data), hard reject reasons bar, capacity (placeholder until live CLOB is wired), recent activity (last 100 non-signal rows)
- Running headless, refreshes every 15s

## Open questions for Claude

1. What's the capacity ceiling at 1%, 2%, 5% acceptable slippage on the markets in the ledger?
2. Is the longshot (<10¢) band genuinely informed on Ferrari, or is it variance?
3. Which wallet has +pts calibration at which bucket — and at what sample size?
4. Should the configured wallet set be expanded beyond 5? (current set is whatever `configured_wallets()` returns; not a choice we made)

## Risks / notes

- Today's `paper_follower.py` change is purely additive — no behavior change for hard rejects
- All 5 commits on `origin/main` — nothing local-only
- `runs/` directory is gitignored; runtime config changes there don't go through git
- The dashboard's "ineligible reasons" section is currently empty because the ledger is pre-revert data. It will populate naturally as the follower runs post-deploy.
- `pycryptodome` was missed in the original `9a9e571` commit; `b9fecd0` fixes it.

## Useful file paths

- Repo root: `/root/flip/projects/polymarket-copybot`
- Filter logic: `polymarket_bot/paper_follower.py` (lines 320-450 = `reject_reasons`, `is_lottery_band`, `signal_row`, `process_fill`)
- Allowlist: `runs/paper/allowlist.json`
- Ledger: `runs/paper/ledger.jsonl`
- State: `runs/paper/state.json`
- Dashboard: `dashboard/app.py`
- Wallet scoring: `runs/wallet_scores_latest.json` (gitignored)

## Conversation/contact

- Owner: flipphill (Discord `832503719866007552`)
- User: Slam (Discord `286978631916453889`, github `in-fused`)
- Committer/operator on this session: Leesa (`leesa@openclaw.local`)
- Channel: #bot-spam (Discord)