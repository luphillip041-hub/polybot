"""Pure HTML renderer for the copybot ops dashboard. No mutations, no JS deps."""

from __future__ import annotations

import html
from typing import Any

CSS = """
:root { color-scheme: dark; }
* { box-sizing: border-box; }
body { margin:0; background:#0b0e14; color:#d7dee9;
  font:14px/1.45 'SF Mono', ui-monospace, Menlo, Consolas, monospace;
  background-image:radial-gradient(#1a2130 1px, transparent 1px);
  background-size:22px 22px; }
.wrap { max-width:1100px; margin:0 auto; padding:22px 16px 60px; }
h1 { font-size:20px; margin:0; letter-spacing:1px; }
h1 .accent { color:#4ade80; }
.sub { color:#7a8699; font-size:12px; margin-top:4px; }
.grid { display:grid; gap:12px; margin-top:18px; }
.tiles { grid-template-columns:repeat(auto-fit,minmax(150px,1fr)); }
.card { background:#111724cc; border:1px solid #232d40; border-radius:10px; padding:12px 14px; }
.card .label { color:#7a8699; font-size:11px; text-transform:uppercase; letter-spacing:.8px; }
.card .value { font-size:22px; margin-top:4px; }
.pos { color:#4ade80; } .neg { color:#f87171; } .warn { color:#fbbf24; }
.bars { grid-template-columns:repeat(auto-fit,minmax(240px,1fr)); }
.bar { display:flex; justify-content:space-between; align-items:center; gap:8px; }
.pill { padding:2px 10px; border-radius:999px; font-size:11px; font-weight:700; }
.pill.ok { background:#052e16; color:#4ade80; border:1px solid #166534; }
.pill.bad { background:#2d0a0a; color:#f87171; border:1px solid #7f1d1d; }
table { width:100%; border-collapse:collapse; font-size:12px; }
th { text-align:left; color:#7a8699; font-weight:400; padding:6px 8px; border-bottom:1px solid #232d40; }
td { padding:6px 8px; border-bottom:1px solid #1a2130; }
.feed { max-height:340px; overflow-y:auto; }
.tag { font-size:10px; padding:1px 6px; border-radius:4px; background:#1a2130; color:#9fb0c8; }
.tag.lotto { background:#2d1b4e; color:#c4b5fd; }
.svc { display:flex; justify-content:space-between; padding:6px 0; border-bottom:1px solid #1a2130; }
h2 { font-size:13px; text-transform:uppercase; letter-spacing:1px; color:#9fb0c8; margin:26px 0 10px; }
.footer { margin-top:30px; color:#525f73; font-size:11px; }
a { color:#4ade80; }
"""


def esc(v: Any) -> str:
    return html.escape("" if v is None else str(v))


def tile(label: str, value: str, cls: str = "") -> str:
    return f'<div class="card"><div class="label">{esc(label)}</div><div class="value {cls}">{esc(value)}</div></div>'


def render_html(snap: dict[str, Any]) -> str:
    a = snap["all_time"]
    t = snap["today"]
    pnl_cls = "pos" if a["pnl"] >= 0 else "neg"
    t_cls = "pos" if t["pnl"] >= 0 else "neg"

    bar_rows = "".join(
        f'<div class="card bar"><span>{esc(b["name"])}</span>'
        f'<span><b>{esc(b["value"])}</b> <span class="pill {"ok" if b["ok"] else "bad"}">{"PASS" if b["ok"] else "FAIL"}</span></span></div>'
        for b in snap["bars"]
    )

    pos_rows = "".join(
        f'<tr><td>{esc(p["token"])}</td><td>{esc(p["entry_price"])}</td><td>${esc(p["stake"])}</td>'
        f'<td>{esc(p["age_h"])}h</td><td>{esc(p["wallet"])}</td></tr>'
        for p in snap["open_positions"]
    ) or '<tr><td colspan="5" style="color:#525f73">no open positions</td></tr>'

    feed_rows = "".join(
        f'<tr><td>{esc(r["ts"])}</td><td>{esc(r["type"])}</td><td>{esc(r["detail"])}'
        f'{" <span class=tag lotto>lotto</span>" if r["quarantined"] else ""}</td></tr>'
        for r in snap["recent"]
    )

    svc_rows = "".join(
        f'<div class="svc"><span>{esc(s["name"])}</span>'
        f'<span class="{"pos" if s["state"]=="active" else "neg"}">{esc(s["state"])} · restarts {esc(s["restarts"])}</span></div>'
        for s in snap["services"]
    )

    sh = snap["shadow"]
    wss = '<span class="pos">connected</span>' if sh["wss"] else '<span class="neg">DOWN</span>'
    creds = '<span class="pos">present</span>' if snap["readiness"]["creds_file"] else '<span class="warn">missing</span>'

    return f"""<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta http-equiv="refresh" content="30">
<title>FLIP//COPYBOT</title><style>{CSS}</style></head><body><div class="wrap">
<h1>FLIP<span class="accent">//</span>COPYBOT <span class="tag">paper · read-only</span></h1>
<div class="sub">snapshot {esc(snap["generated_at"])} · auto-refresh 30s</div>

<div class="grid tiles">
{tile("All-time P&L", f"${a['pnl']:,.0f}", pnl_cls)}
{tile("Today P&L", f"${t['pnl']:,.0f}", t_cls)}
{tile("Profit factor", a["pf"], "pos" if a["pf"] > 1.3 else "neg")}
{tile("Win rate", f"{a['wr']}%")}
{tile("Entries / closed", f"{a['entries']} / {a['closed']}")}
{tile("Latency p50", f"{a['p50']}s")}
{tile("Attainability +12s", f"{snap['attainability_12s']['pct']}%", "pos" if snap["attainability_12s"]["pct"] >= 70 else "warn")}
{tile("Open positions", snap["open_count"])}
</div>

<h2>Go-live gate — {snap["bars_green"]}/6 green</h2>
<div class="grid bars">{bar_rows}</div>

<h2>Live readiness</h2>
<div class="card">
<div class="svc"><span>CLOB credentials file</span>{creds}</div>
<div class="svc"><span>Onchain shadow WSS</span>{wss} <span style="color:#525f73">confs {esc(sh["confirmations"])} · beat {esc(sh["beat_age_s"])}s ago</span></div>
<div class="svc"><span>Executor mode</span><span class="warn">quote-only (gates off)</span></div>
</div>

<h2>Open positions</h2>
<div class="card"><table><tr><th>token</th><th>entry</th><th>stake</th><th>age</th><th>wallet</th></tr>{pos_rows}</table></div>

<h2>Recent activity</h2>
<div class="card feed"><table>{feed_rows}</table></div>

<h2>Services</h2>
<div class="card">{svc_rows}</div>

<div class="footer">clean P&L ${a["clean_pnl"]:,.0f} · lotto sleeve ${a["lotto_pnl"]:,.0f} · stale 72h {snap["stale_72h"]["pct"]}% · no auth — anyone with this URL can view</div>
</div></body></html>"""
