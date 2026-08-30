"""polymarket-copybot forward-test dashboard.

Reads runs/paper/ledger.jsonl and shows eligible/ineligible counts,
per-wallet breakdown, and recent activity. Capacity section is a
placeholder until live CLOB depth is wired in.

Run:
    streamlit run dashboard/app.py --server.port 8503 --server.address 0.0.0.0
"""
from __future__ import annotations

import json
from collections import Counter, defaultdict
from pathlib import Path

import pandas as pd
import streamlit as st

REPO = Path("/root/flip/projects/polymarket-copybot")
LEDGER = REPO / "runs" / "paper" / "ledger.jsonl"
ALLOWLIST = REPO / "runs" / "paper" / "allowlist.json"

st.set_page_config(page_title="polymarket-copybot", layout="wide", page_icon="📊")
st.title("polymarket-copybot — forward-test dashboard")


@st.cache_data(ttl=15)
def load_ledger() -> list[dict]:
    if not LEDGER.exists():
        return []
    out: list[dict] = []
    with open(LEDGER) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return out


@st.cache_data(ttl=300)
def load_allowlist() -> dict:
    if not ALLOWLIST.exists():
        return {"wallets": []}
    return json.loads(ALLOWLIST.read_text())


def short(w: str) -> str:
    if not w or len(w) < 12:
        return w or "?"
    return f"{w[:8]}…{w[-4:]}"


rows = load_ledger()
if not rows:
    st.warning(f"ledger not found or empty at {LEDGER}")
    st.stop()

last_ts = max((r.get("ts") for r in rows if r.get("ts")), default=None)
allowlist = load_allowlist()
allowlist_wallets = {w.lower() for w in allowlist.get("wallets", [])}

st.caption(
    f"ledger: **{len(rows):,}** rows · last entry: **{last_ts}** · "
    f"allowlist: **{len(allowlist_wallets)}** wallets"
)

# Section 1: type counts
st.subheader("entry classification")
type_counts = Counter(r.get("type") for r in rows)
order = ["signal", "entry", "exit", "ineligible", "reject"]
cols = st.columns(len(order))
for col, t in zip(cols, order):
    col.metric(t, f"{type_counts.get(t, 0):,}")
hard_reject = type_counts.get("reject", 0)
ineligible = type_counts.get("ineligible", 0)
total_decisions = hard_reject + ineligible + type_counts.get("entry", 0) + type_counts.get("exit", 0)
if total_decisions:
    st.caption(
        f"rejection ratio (decisions only): "
        f"hard={hard_reject/total_decisions:.1%} · "
        f"tag-only(ineligible)={ineligible/total_decisions:.1%} · "
        f"executed={(type_counts.get('entry',0)+type_counts.get('exit',0))/total_decisions:.1%}"
    )

# Section 2: per-wallet
st.subheader("by wallet")
wallet_counts: dict[str, Counter] = defaultdict(Counter)
for r in rows:
    wallet_counts[r.get("wallet", "unknown")][r.get("type")] += 1
df_w = pd.DataFrame(
    [
        {"wallet (short)": short(w), "wallet": w, **dict(c)}
        for w, c in sorted(wallet_counts.items())
    ]
)
df_w = df_w.drop(columns=["wallet"]).rename(columns={"wallet (short)": "wallet"})
st.dataframe(df_w, use_container_width=True, hide_index=True)

# Section 3: ineligible breakdown by reason (since #1 revert)
st.subheader("ineligible reasons (lottery band tag, post-revert)")
ineligible_reasons = Counter()
for r in rows:
    if r.get("type") != "ineligible":
        continue
    for reason in str(r.get("reject_reason") or "unknown").split(","):
        reason = reason.strip()
        if reason:
            ineligible_reasons[reason] += 1
if ineligible_reasons:
    st.bar_chart(pd.DataFrame(
        [{"reason": k, "count": v} for k, v in ineligible_reasons.most_common()]
    ).set_index("reason"))
else:
    st.caption("no ineligible rows in current ledger (filter was a hard reject pre-revert).")

# Section 4: hard reject reasons (still a gate)
st.subheader("hard reject reasons (still a gate)")
hard_reasons = Counter()
for r in rows:
    if r.get("type") != "reject":
        continue
    for reason in str(r.get("reject_reason") or "unknown").split(","):
        reason = reason.strip()
        if reason and reason != "daily_entry_cap":
            hard_reasons[reason] += 1
if hard_reasons:
    st.bar_chart(pd.DataFrame(
        [{"reason": k, "count": v} for k, v in hard_reasons.most_common()]
    ).set_index("reason"))
else:
    st.caption("no hard rejects in current ledger.")

# Section 5: capacity placeholder
st.subheader("capacity (deployable notional ceiling)")
st.caption(
    "stake config: $100/trade, 20 entries/day → theoretical $2k/day, $60k/mo. "
    "actual ceiling is set by book depth on the markets the 5 wallets trade — "
    "live CLOB query not yet wired (item #4 in progress)."
)

# Section 6: recent activity
st.subheader("recent activity (last 100 non-signal rows)")
recent = [
    {
        "ts": r.get("ts"),
        "type": r.get("type"),
        "wallet": short(r.get("wallet", "")),
        "side": r.get("side"),
        "fill_price": r.get("wallet_fill_price"),
        "sim_fill_price": r.get("sim_fill_price"),
        "pnl": r.get("pnl"),
        "reject_reason": r.get("reject_reason"),
        "eligible_live": r.get("eligible_live"),
    }
    for r in rows[-200:]
    if r.get("type") in {"entry", "exit", "ineligible", "reject"}
][-100:]
if recent:
    st.dataframe(pd.DataFrame(recent), use_container_width=True, hide_index=True)

st.caption(f"refreshes every 15s · repo: {REPO} · ledger: {LEDGER.stat().st_size:,} bytes")