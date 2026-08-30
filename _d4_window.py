#!/usr/bin/env python3
"""D4 coverage-bias check (full version, no scipy).

Side-by-side distributions + KS / chi-square stats across 4 dimensions:
  1. Price band (<10c, 10-30c, 30-70c, >70c)
  2. Spread at fill (from inline book_snapshot, where populated)
  3. Fill age / recency (days since fill)
  4. Per-market liquidity proxy (inline ask_size)

Verdict: POWER (distributions match) vs BIAS (uncovered skews thin/quiet).

Read-only. Not committed.
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, "/root/flip/projects/polymarket-copybot")
from scripts.capacity import parse_ts, ArchiveBookSource, PRICE_BUCKETS

LEDGER = Path("/root/flip/projects/polymarket-copybot/runs/paper/ledger.jsonl")
ARCHIVE_ROOT = Path("/root/flip/projects/polymarket-copybot/runs/book_archive")
DAYS = 30


def parse_days_arg(s: str) -> set:
    days = set()
    for tok in s.split(","):
        tok = tok.strip()
        if not tok:
            continue
        days.add(datetime.strptime(tok, "%Y-%m-%d").date())
    return days


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--include-days", default="",
                    help="comma-separated YYYY-MM-DD list. Empty = all days.")
    ap.add_argument("--label", default="all",
                    help="label for the report header (e.g. W95, W70).")
    return ap.parse_args()


_args = main()
INCLUDE_DAYS = parse_days_arg(_args.include_days) if _args.include_days else set()
LABEL = _args.label
print(f"\n{'#'*78}\nD4 coverage-bias check — window: {LABEL}\n"
      f"  include_days: {sorted(INCLUDE_DAYS) if INCLUDE_DAYS else '(none)'}\n{'#'*78}\n",
      file=sys.stderr)


def bband(p):
    for lo, hi, n in PRICE_BUCKETS:
        if lo <= p < hi:
            return n
    return "unk"


def q(data, p):
    s = sorted(data)
    return s[int(p * (len(s) - 1))] if s else None


def ks_stat(s1, s2):
    """Two-sample KS statistic (max |F1 - F2|). Returns float or None."""
    s1, s2 = sorted(s1), sorted(s2)
    n1, n2 = len(s1), len(s2)
    if n1 == 0 or n2 == 0:
        return None
    i = j = 0
    d = 0.0
    # walk through both sorted arrays; at each unique value, advance both
    # pointers and check the CDF difference just after.
    while i < n1 or j < n2:
        f1_before = i / n1
        f2_before = j / n2
        if i >= n1:
            j += 1
            d = max(d, abs(f1_before - j / n2))
            continue
        if j >= n2:
            i += 1
            d = max(d, abs(i / n1 - f2_before))
            continue
        if s1[i] < s2[j]:
            i += 1
            d = max(d, abs(i / n1 - f2_before))
        elif s2[j] < s1[i]:
            j += 1
            d = max(d, abs(f1_before - j / n2))
        else:  # s1[i] == s2[j]: advance both, check post-step diff
            i += 1
            j += 1
            d = max(d, abs(i / n1 - j / n2))
    return d


def chi2_contingency(table):
    """Chi-square test on a 2D contingency table. Returns (chi2, dof)."""
    n_rows = len(table)
    n_cols = len(table[0]) if table else 0
    row_totals = [sum(table[i]) for i in range(n_rows)]
    col_totals = [sum(table[i][j] for i in range(n_rows)) for j in range(n_cols)]
    grand = sum(row_totals)
    chi2 = 0.0
    for i in range(n_rows):
        for j in range(n_cols):
            o = table[i][j]
            e = row_totals[i] * col_totals[j] / grand if grand else 0
            if e > 0:
                chi2 += (o - e) ** 2 / e
    dof = (n_rows - 1) * (n_cols - 1) if n_rows > 1 and n_cols > 1 else 0
    return chi2, dof


# chi-square critical values for dof=3 (4-band x 2-coverage)
# alpha 0.05: 7.815; alpha 0.01: 11.345; alpha 0.001: 16.27
CHI2_CRIT_3 = [(0.05, 7.815), (0.01, 11.345), (0.001, 16.27)]


def chi2_p_approx(stat, dof):
    """Approximate p-value label from chi-square critical values (dof=3 only)."""
    if dof != 3:
        return f"(dof={dof}; critical values not tabulated)"
    if stat >= 16.27:
        return "p<0.001"
    if stat >= 11.345:
        return "p<0.01"
    if stat >= 7.815:
        return "p<0.05"
    return "p>=0.05 (cannot reject H0 of independence)"


def ks_p_approx(stat, n1, n2):
    """Approximate p-value for two-sample KS using Smirnov's asymptotic.
    Critical values: alpha=0.05 -> 1.358, alpha=0.01 -> 1.628, alpha=0.001 -> 1.95."""
    eff_n = (n1 * n2) / (n1 + n2)
    z = stat * (eff_n ** 0.5)
    if z >= 1.95:
        return f"p<0.001 (z={z:.2f})"
    if z >= 1.628:
        return f"p<0.01 (z={z:.2f})"
    if z >= 1.358:
        return f"p<0.05 (z={z:.2f})"
    return f"p>=0.05 (z={z:.2f}, cannot reject H0)"


# ---------------------------------------------------------------------------
print("building archive index...", file=sys.stderr)
src = ArchiveBookSource(
    ARCHIVE_ROOT,
    tolerance=timedelta(minutes=5),
    suspect_before=parse_ts("2026-07-08T23:16:31+00:00"),
)
src._build_index()

now = datetime.now(timezone.utc)
cutoff = now - timedelta(days=DAYS)

covered_fills = []
uncovered_fills = []

with LEDGER.open() as fh:
    for line in fh:
        line = line.strip()
        if not line:
            continue
        try:
            r = json.loads(line)
            if r.get("wallet_fill_price") is None or r.get("token") is None:
                continue
            ts = parse_ts(r["ts"])
            if ts < cutoff:
                continue
            if str(r["side"]).upper() != "BUY":
                continue
            if INCLUDE_DAYS and ts.date() not in INCLUDE_DAYS:
                continue
            tk = str(r["token"])
            price = float(r["wallet_fill_price"])
            bs = r.get("book_snapshot") or {}
            inline_spread = bs.get("spread") if bs.get("best_ask") is not None else None
            inline_ask_size = bs.get("ask_size") if bs.get("best_ask") is not None else None
            inline_bid_size = bs.get("bid_size") if bs.get("best_ask") is not None else None

            book = src.get(tk, ts)
            covered = book is not None and book["asks"]

            rec = {
                "band": bband(price),
                "price": price,
                "age_days": (now - ts).total_seconds() / 86400.0,
                "ts": ts,
                "inline_spread": inline_spread,
                "inline_ask_size": inline_ask_size,
                "inline_bid_size": inline_bid_size,
                "detection_latency_s": r.get("detection_latency_s"),
            }
            (covered_fills if covered else uncovered_fills).append(rec)
        except Exception:
            pass

n_cov = len(covered_fills)
n_unc = len(uncovered_fills)
n_total = n_cov + n_unc
print(
    f"\ncovered: {n_cov} ({100*n_cov/n_total:.1f}%) | uncovered: {n_unc} "
    f"({100*n_unc/n_total:.1f}%) | total: {n_total}\n"
)

# ===========================================================================
# Dimension 1: Price band (chi-square)
# ===========================================================================
print("=" * 78)
print("DIMENSION 1: Price band (chi-square on 2x4 contingency)")
print("=" * 78)
bands = [b[2] for b in PRICE_BUCKETS]
cov_b = defaultdict(int)
unc_b = defaultdict(int)
for f in covered_fills:
    cov_b[f["band"]] += 1
for f in uncovered_fills:
    unc_b[f["band"]] += 1

print(f"\n{'band':<10}{'covered':>10}{'uncovered':>10}{'%cov':>9}")
contingency = []
for b_ in bands:
    c, u = cov_b[b_], unc_b[b_]
    t = c + u
    pct = 100 * c / t if t else 0
    print(f"{b_:<10}{c:>10}{u:>10}{pct:>8.1f}%")
    contingency.append([c, u])

chi2, dof = chi2_contingency(contingency)
print(f"\nchi-square = {chi2:.2f}, dof = {dof}, {chi2_p_approx(chi2, dof)}")
# Cramer's V for effect size: V = sqrt(chi2 / (n * min(r-1, c-1)))
v = (chi2 / (n_total * 1)) ** 0.5  # min(1, 3) = 1 for 2x4
print(f"Cramer's V = {v:.4f}  (effect size: <0.05 negligible, 0.05-0.10 small, "
      f"0.10-0.20 medium, >0.20 large)")

# ===========================================================================
# Dimension 2: Spread at fill (KS)
# ===========================================================================
print("\n" + "=" * 78)
print("DIMENSION 2: Spread at fill (KS test, inline book_snapshot only)")
print("=" * 78)
cov_spreads = [f["inline_spread"] for f in covered_fills if f["inline_spread"] is not None]
unc_spreads = [f["inline_spread"] for f in uncovered_fills if f["inline_spread"] is not None]
print(f"covered with inline spread: n={len(cov_spreads)} ({100*len(cov_spreads)/n_cov:.1f}% of covered)")
print(f"uncovered with inline spread: n={len(unc_spreads)} ({100*len(unc_spreads)/n_unc:.1f}% of uncovered)")

print(f"\n{'group':<12}{'n':>6}{'p25':>10}{'p50':>10}{'p75':>10}{'p90':>10}{'mean':>10}")
if cov_spreads:
    print(
        f"{'covered':<12}{len(cov_spreads):>6}"
        f"{q(cov_spreads,0.25):>10.5f}{q(cov_spreads,0.50):>10.5f}"
        f"{q(cov_spreads,0.75):>10.5f}{q(cov_spreads,0.90):>10.5f}"
        f"{sum(cov_spreads)/len(cov_spreads):>10.5f}"
    )
if unc_spreads:
    print(
        f"{'uncovered':<12}{len(unc_spreads):>6}"
        f"{q(unc_spreads,0.25):>10.5f}{q(unc_spreads,0.50):>10.5f}"
        f"{q(unc_spreads,0.75):>10.5f}{q(unc_spreads,0.90):>10.5f}"
        f"{sum(unc_spreads)/len(unc_spreads):>10.5f}"
    )

if cov_spreads and unc_spreads:
    ks = ks_stat(cov_spreads, unc_spreads)
    print(f"\nKS statistic = {ks:.4f}, {ks_p_approx(ks, len(cov_spreads), len(unc_spreads))}")
    print(f"  (effect size: <0.05 small, 0.05-0.10 moderate, >0.10 large for these sample sizes)")
    median_ratio = q(cov_spreads, 0.5) / q(unc_spreads, 0.5) if q(unc_spreads, 0.5) else None
    if median_ratio:
        print(f"  median ratio (covered / uncovered) = {median_ratio:.3f}")

# ===========================================================================
# Dimension 3: Fill age / recency (KS)
# ===========================================================================
print("\n" + "=" * 78)
print("DIMENSION 3: Fill age / recency (KS test, all fills)")
print("=" * 78)
cov_ages = [f["age_days"] for f in covered_fills]
unc_ages = [f["age_days"] for f in uncovered_fills]

print(f"\n{'group':<12}{'n':>6}{'p25':>10}{'p50':>10}{'p75':>10}{'p90':>10}{'mean':>10}")
print(
    f"{'covered':<12}{len(cov_ages):>6}"
    f"{q(cov_ages,0.25):>10.2f}{q(cov_ages,0.50):>10.2f}"
    f"{q(cov_ages,0.75):>10.2f}{q(cov_ages,0.90):>10.2f}"
    f"{sum(cov_ages)/len(cov_ages):>10.2f}"
)
print(
    f"{'uncovered':<12}{len(unc_ages):>6}"
    f"{q(unc_ages,0.25):>10.2f}{q(unc_ages,0.50):>10.2f}"
    f"{q(unc_ages,0.75):>10.2f}{q(unc_ages,0.90):>10.2f}"
    f"{sum(unc_ages)/len(unc_ages):>10.2f}"
)

if cov_ages and unc_ages:
    ks = ks_stat(cov_ages, unc_ages)
    print(f"\nKS statistic = {ks:.4f}, {ks_p_approx(ks, len(cov_ages), len(unc_ages))}")

# Coverage rate by day
print("\nCoverage rate by day:")
day_cov = defaultdict(lambda: [0, 0])
for f in covered_fills:
    day_cov[f["ts"].date()][0] += 1
for f in uncovered_fills:
    day_cov[f["ts"].date()][1] += 1
print(f"{'date':<12}{'covered':>10}{'uncovered':>10}{'%cov':>9}")
for day in sorted(day_cov.keys()):
    c, u = day_cov[day]
    t = c + u
    pct = 100 * c / t if t else 0
    print(f"{str(day):<12}{c:>10}{u:>10}{pct:>8.1f}%")

# ===========================================================================
# Dimension 4: Per-market liquidity (KS on inline ask_size)
# ===========================================================================
print("\n" + "=" * 78)
print("DIMENSION 4: Per-market liquidity (KS test on inline ask_size)")
print("=" * 78)
cov_asks = [f["inline_ask_size"] for f in covered_fills if f["inline_ask_size"] is not None]
unc_asks = [f["inline_ask_size"] for f in uncovered_fills if f["inline_ask_size"] is not None]
print(f"covered with inline ask_size: n={len(cov_asks)} ({100*len(cov_asks)/n_cov:.1f}% of covered)")
print(f"uncovered with inline ask_size: n={len(unc_asks)} ({100*len(unc_asks)/n_unc:.1f}% of uncovered)")

# ask_size is heavy-tailed; report both raw and log
import math
cov_log = [math.log(a) for a in cov_asks if a > 0]
unc_log = [math.log(a) for a in unc_asks if a > 0]

print(f"\n{'group':<12}{'n':>6}{'p25':>14}{'p50':>14}{'p75':>14}{'p90':>14}{'mean':>14}")
if cov_asks:
    print(
        f"{'covered (raw)':<12}{len(cov_asks):>6}"
        f"{q(cov_asks,0.25):>14.0f}{q(cov_asks,0.50):>14.0f}"
        f"{q(cov_asks,0.75):>14.0f}{q(cov_asks,0.90):>14.0f}"
        f"{sum(cov_asks)/len(cov_asks):>14.0f}"
    )
if unc_asks:
    print(
        f"{'uncovered (raw)':<12}{len(unc_asks):>6}"
        f"{q(unc_asks,0.25):>14.0f}{q(unc_asks,0.50):>14.0f}"
        f"{q(unc_asks,0.75):>14.0f}{q(unc_asks,0.90):>14.0f}"
        f"{sum(unc_asks)/len(unc_asks):>14.0f}"
    )

print()
if cov_log:
    print(
        f"{'covered (log)':<12}{len(cov_log):>6}"
        f"{q(cov_log,0.25):>14.2f}{q(cov_log,0.50):>14.2f}"
        f"{q(cov_log,0.75):>14.2f}{q(cov_log,0.90):>14.2f}"
        f"{sum(cov_log)/len(cov_log):>14.2f}"
    )
if unc_log:
    print(
        f"{'uncovered (log)':<12}{len(unc_log):>6}"
        f"{q(unc_log,0.25):>14.2f}{q(unc_log,0.50):>14.2f}"
        f"{q(unc_log,0.75):>14.2f}{q(unc_log,0.90):>14.2f}"
        f"{sum(unc_log)/len(unc_log):>14.2f}"
    )

if cov_asks and unc_asks:
    ks_raw = ks_stat(cov_asks, unc_asks)
    print(f"\nKS (raw) = {ks_raw:.4f}, {ks_p_approx(ks_raw, len(cov_asks), len(unc_asks))}")
    if cov_log and unc_log:
        ks_log = ks_stat(cov_log, unc_log)
        print(f"KS (log) = {ks_log:.4f}, {ks_p_approx(ks_log, len(cov_log), len(unc_log))}")
        print(f"  → log-space is the more meaningful test (raw is dominated by long right tail)")
        median_log_ratio = (
            q(cov_log, 0.5) - q(unc_log, 0.5)
        )
        print(f"  log-median difference (cov - unc) = {median_log_ratio:+.2f} "
              f"(ratio {math.exp(median_log_ratio):.2f}x)")

# ===========================================================================
# VERDICT
# ===========================================================================
print("\n" + "=" * 78)
print("VERDICT")
print("=" * 78)
print(
    f"""
Question: is the missing 42% a POWER problem (uncovered looks like covered)
or a BIAS problem (uncovered skews thin/quiet)?

Decision rule (effect-size based, not just p-values given large n):
  - Dimension is "different" if effect size exceeds practical threshold:
      chi-square Cramer's V > 0.05, KS stat > 0.05
  - Verdict = POWER if no dimension differs; BIAS otherwise, with the
    biasing dimension(s) named.

Effect sizes (see dimension outputs above):
  1. Price band: Cramer's V = {v:.4f}
"""
)

# Recompute KS for spread / age for the verdict
ks_spread = ks_stat(cov_spreads, unc_spreads) if cov_spreads and unc_spreads else None
ks_age = ks_stat(cov_ages, unc_ages) if cov_ages and unc_ages else None
ks_log = ks_stat(cov_log, unc_log) if cov_log and unc_log else None

print(f"  2. Spread:       KS = {ks_spread}")
print(f"  3. Age:          KS = {ks_age}")
print(f"  4. ask_size log: KS = {ks_log}")

issues = []
if v > 0.05:
    issues.append(f"price band (V={v:.3f})")
if ks_spread and ks_spread > 0.05:
    issues.append(f"spread (KS={ks_spread:.3f})")
if ks_age and ks_age > 0.05:
    issues.append(f"age (KS={ks_age:.3f})")
if ks_log and ks_log > 0.05:
    issues.append(f"ask_size log (KS={ks_log:.3f})")

if not issues:
    print("\n>>> VERDICT: POWER PROBLEM <<<")
    print("    Covered and uncovered fills have similar distributions on all 4 dimensions.")
    print("    Widening tolerance / window should fix the missing 42%.")
else:
    print(f"\n>>> VERDICT: BIAS PROBLEM (skew on: {', '.join(issues)}) <<<")
    print("    Uncovered fills skew differently from covered — capacity estimate is biased.")
    print("    Widening tolerance won't fix this; need a different approach (live fetch,")
    print("    restricted to subsets where coverage is uniform, etc.)")
