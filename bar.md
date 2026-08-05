# bar.md — "Profitable" Definition (v1)

> **Status:** DRAFT v1, paper-only. No behavior change to the bot. **All numeric thresholds are placeholders — Phillip to finalize.**

## Purpose

Define what "profitable" means for the Polymarket copybot so that capacity, latency, and concentration findings can be judged against a bar instead of intuition. This is a definition document; it does not change runtime behavior. Live execution remains gated and disabled.

## Day-One Verdict: **HOLD**

Per the per-wallet gates below, only **two** wallets currently clear the sample bar:

| wallet (addr prefix) | label           | entries | clean P&L  | clean ROI |
|----------------------|-----------------|--------:|-----------:|----------:|
| `0xfe787d2da716…`    | ferrariChampions2026 | 281 | +$15,837 | +56% |
| `0x224a89dbe0db0…`  | wr0ngw4yb3tt0r      | 143 | +$3,681  | +26% |
| `0x4be1fa92e6cea…`  | Eztennis            |  21 | +$435    | +21% |
| `0x4bff30af9164…`   | Sassy-Bucket        |   6 | +$517    | +86% |
| `0xee00ba338c59…`   | ~~S-Works~~ (dropped) | — | — | — |

`S-Works` is removed from the allowlist in the same PR as this doc (4 entries, −$400, −100% ROI, n too small in either direction).

The `≥3 wallets clearing the per-wallet bar` rule is **not met**. Bar's first output: **keep accumulating paper until a third wallet crosses ≥50 closed** (Eztennis is the most likely next; needs ~30 more closes).

## Scope & Definitions

- **Universe:** closed positions only. Open positions do not count toward any threshold.
- **Clean:** ex-voids AND ex-quarantine. `void_correction` and `quarantined_low_price` rows are stripped from both numerator and denominator.
- **Per-wallet, not blended.** Two whales can flatter any blended Sharpe; aggregation hides concentration.
- **Paper-only.** These gates describe what "go live" would have to look like. They do not enable anything.

## Gates

Each gate is applied **per wallet**. A wallet must clear all per-wallet gates to count toward the `≥3 wallets` rule.

### 1. Sample size

- **Closed positions, clean:** `≥ 50`  *(# placeholder — Phillip to finalize)*

Why: at n<50, a single streak dominates the variance estimate and the bootstrap CI is wide enough that almost anything passes. A wallet must show a stable edge across a meaningful number of closes before the rest of the gates are meaningful.

### 2. Edge — clean ROI, lower confidence bound

- **Clean ROI point estimate:** `> 0%`
- **Lower bound of the ROI confidence interval:** `> 0`  *(# placeholder — Phillip to finalize; bootstrap or Wilson — TBD which)*

Why: a lucky +2% on n=50 is not edge. The point estimate catches the obvious losers; the lower bound catches the "lucky streak" survivors. We use a **lower confidence bound > 0**, not a p-value, because we care about the magnitude of plausible worse-case outcomes, not just whether the mean differs from zero.

### 3. Risk — drawdown bound

- **Metric:** peak-to-trough drawdown of cumulative clean P&L (not % of allocated notional — notional is `$100 × n`, a moving target that would let a wallet "spend more" to look safer).
- **Threshold:** placeholder, flag as TBD.  *(# placeholder — Phillip to finalize)*

Why: a wallet can be net-positive with a brutal drawdown. A drawdown bound is the only risk metric that doesn't get washed out by averaging across many small wins.

### 4. Coverage — book availability

- **Coverage:** `≥ 85%` covered-fill on post-fix days  *(# placeholder — Phillip to finalize)*
- **Broken out per price band.** The **<10¢ band must not be starved.**
- A blended 85% that hides a starved low-vol band **fails** this gate.

Why: if we're missing the book on most fills, faster detection buys us nothing. Coverage is the prerequisite for any speed investment. The per-band breakout is mandatory because a blended number can hide a single-band collapse that makes the strategy structurally un-tradable in the band that actually contains the cheap entries.

### 5. Feed viability — wallet count

- **Surviving wallets (clearing all per-wallet gates above):** `≥ 3`  *(# placeholder — Phillip to finalize)*

Why: the count is **downstream** of the per-wallet gate, never reached by loosening it. Two wallets is concentration dressed up as diversification.

**v2 refinement (intentionally NOT in v1):** a concentration cap (no wallet > X% of expected edge). Currently unsatisfiable (Ferrari ~60% of clean P&L), so adding it to v1 would force an immediate HOLD that the per-wallet gates already enforce. Re-evaluate at the 30-day review.

**Caveat (called out for the doc, not for action):** `≥3` wallets clearing the per-wallet bar does not prove statistical independence. If two of them share a strategy, signal source, or operator, the portfolio is still effectively single-wallet. v2 should add a correlation check between surviving wallets' P&L streams.

### 6. Minimum viable run

- **Closed trades per week across surviving wallets:** `≥ 5`  *(# placeholder — Phillip to finalize)*

Why: below this rate we cannot measure drift. A strategy that trades once a month cannot distinguish edge from luck within a reasonable review window. This gate is enforced across the **surviving** wallets, not per-wallet — the per-wallet rate may legitimately be small.

## Review Cadence

- **Re-measure:** at the **30-day** mark from the start of paper observation (2026-07-30 → 2026-08-29).
- **Inputs to the review:** per-wallet clean P&L, drawdown, coverage by band, weekly trade count.
- **Outputs:** pass / hold / drop verdict per wallet; pass / hold verdict on the feed (≥3 wallets); go-live / stay-paper verdict overall.

## What This Doc Does NOT Change

- `wallet_poll_interval_seconds` stays at 15.
- `stale_fill_seconds` stays at 120.
- DRY_RUN stays on. No executor. No sizing.
- `--apply` flag exists but is not used.
- No live CLOB order path is enabled.

Speed (B), concentration (A), and capacity (C) findings are **measurements against this bar** at the 30-day review, not code changes made now.

## Companion Change in This PR

- `runs/paper/allowlist.json`: remove `0xee00ba338c59557141789b127927a55f5cc5cea1` (S-Works). 4 entries, −$400, −100% ROI. Pure drag at any threshold.