"""Read-only snapshot for the copybot ops dashboard.

Single streaming pass over ledger history (never materializes rows), plus
state.json, the onchain-shadow heartbeat, systemd unit states, and go-live
readiness signals.  Refreshed by the server's background thread; requests
serve the cached dict.
"""

from __future__ import annotations

import json
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .ledger_history import iter_ledger_rows

UNITS = [
    "polymarket-copybot-paper-follower.service",
    "polymarket-copybot-onchain-shadow.service",
    "polymarket-copybot-book-archive.service",
    "polymarket-copybot-status-api.service",
    "polymarket-copybot-discord-monitor.service",
]

BAR_PF = 1.3
BAR_WR = 48.0
BAR_P50 = 15.0
BAR_P90 = 25.0
BAR_STALE_PCT = 1.0
BAR_ATTAIN = 70.0


def _ts(r: dict[str, Any]) -> str:
    return str(r.get("ts") or "")


def _parse(t: str) -> datetime | None:
    try:
        return datetime.fromisoformat(t.replace("Z", "+00:00"))
    except ValueError:
        return None


def _pct(a: float, b: float) -> float:
    return round(100.0 * a / b, 2) if b else 0.0


def _unit_states() -> list[dict[str, str]]:
    try:
        out = subprocess.run(
            ["systemctl", "show", *UNITS, "-p", "Id,ActiveState,NRestarts", "--no-pager"],
            capture_output=True, text=True, timeout=10,
        ).stdout
    except Exception:
        return [{"name": u, "state": "unknown", "restarts": "?"} for u in UNITS]
    states: dict[str, dict[str, str]] = {}
    cur: dict[str, str] = {}
    for line in out.splitlines():
        if not line.strip():
            if cur.get("Id"):
                states[cur["Id"]] = cur
            cur = {}
            continue
        k, _, v = line.partition("=")
        cur[k] = v
    if cur.get("Id"):
        states[cur["Id"]] = cur
    return [
        {
            "name": u.replace("polymarket-copybot-", "").replace(".service", ""),
            "state": states.get(u, {}).get("ActiveState", "unknown"),
            "restarts": states.get(u, {}).get("NRestarts", "?"),
        }
        for u in UNITS
    ]


def build_snapshot(root: Path) -> dict[str, Any]:
    paper = root / "runs" / "paper"
    now = datetime.now(timezone.utc)
    today = now.strftime("%Y-%m-%d")
    day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    w72 = now.timestamp() - 72 * 3600

    n_entries = n_closed = wins = 0
    pnl = clean_pnl = 0.0
    gross_w = gross_l = 0.0
    lats: list[float] = []
    t_entries = t_closed = t_wins = 0
    t_pnl = 0.0
    t_lats: list[float] = []
    stale72 = sigs72 = 0
    att_n = att_ok = 0
    recent: list[dict[str, Any]] = []

    for r in iter_ledger_rows(paper):
        typ = r.get("type")
        t = _parse(_ts(r))
        ts_epoch = t.timestamp() if t else 0.0
        if typ == "entry":
            n_entries += 1
            lat = r.get("detection_latency_s")
            if isinstance(lat, (int, float)):
                lats.append(float(lat))
            if ts_epoch >= day_start.timestamp():
                t_entries += 1
                if isinstance(lat, (int, float)):
                    t_lats.append(float(lat))
        elif typ == "signal":
            if ts_epoch >= w72:
                sigs72 += 1
        elif typ == "reject":
            if "stale_fill" in str(r.get("reject_reason") or "") and ts_epoch >= w72:
                stale72 += 1
        elif typ in ("resolution", "exit"):
            n_closed += 1
            p = float(r.get("pnl") or 0)
            pnl += p
            if not r.get("quarantined_low_price"):
                clean_pnl += p
            if p > 0:
                wins += 1
                gross_w += p
            else:
                gross_l += abs(p)
            if ts_epoch >= day_start.timestamp():
                t_closed += 1
                t_pnl += p
                if p > 0:
                    t_wins += 1
        elif typ == "fill_check" and int(r.get("offset_s") or 0) == 12:
            att_n += 1
            if r.get("attainable") is True:
                att_ok += 1
        if typ in ("entry", "resolution", "exit", "reject"):
            recent.append(r)
            if len(recent) > 30:
                recent.pop(0)

    lats.sort()
    t_lats.sort()
    p50 = round(lats[len(lats) // 2], 2) if lats else 0.0
    p90 = round(lats[9 * len(lats) // 10], 2) if lats else 0.0
    t_p50 = round(t_lats[len(t_lats) // 2], 2) if t_lats else 0.0
    wr = _pct(wins, n_closed)
    pf = round(gross_w / gross_l, 3) if gross_l else 0.0
    stale_pct = _pct(stale72, sigs72)
    attain_pct = _pct(att_ok, att_n)

    state: dict[str, Any] = {}
    try:
        state = json.loads((paper / "state.json").read_text())
    except Exception:
        pass
    positions = state.get("positions") or {}
    open_positions = [
        {
            "token": str(p.get("token"))[:12],
            "stake": p.get("stake_usd"),
            "entry_price": p.get("sim_fill_price") or p.get("wallet_fill_price"),
            "age_h": round((now.timestamp() - (_parse(str(p.get("entry_ts") or "")) or now).timestamp()) / 3600, 1)
            if p.get("entry_ts") else None,
            "wallet": str(p.get("wallet") or "")[:10],
        }
        for p in list(positions.values())[:40]
    ]

    hb: dict[str, Any] = {}
    hb_age = None
    try:
        hb_path = root / "runs" / "onchain_shadow" / "heartbeat.json"
        hb = json.loads(hb_path.read_text())
        hb_age = round(time.time() - hb_path.stat().st_mtime)
    except Exception:
        pass

    creds_present = Path("/root/flip/secrets/polymarket_live.env").exists()

    bars = [
        {"name": f"PF > {BAR_PF}", "ok": pf > BAR_PF, "value": str(pf)},
        {"name": f"WR ≥ {BAR_WR}%", "ok": wr >= BAR_WR, "value": f"{wr}%"},
        {"name": f"p50 ≤ {BAR_P50}s", "ok": p50 <= BAR_P50, "value": f"{p50}s"},
        {"name": f"p90 ≤ {BAR_P90}s", "ok": p90 <= BAR_P90, "value": f"{p90}s"},
        {"name": f"stale_fill < {BAR_STALE_PCT}% (72h)", "ok": stale_pct < BAR_STALE_PCT, "value": f"{stale_pct}%"},
        {"name": f"attainability ≥ {BAR_ATTAIN}% @+12s", "ok": attain_pct >= BAR_ATTAIN and att_n >= 100, "value": f"{attain_pct}% (n={att_n})"},
    ]

    return {
        "generated_at": now.strftime("%Y-%m-%d %H:%M:%S UTC"),
        "all_time": {
            "entries": n_entries, "closed": n_closed, "wr": wr, "pf": pf,
            "pnl": round(pnl, 2), "clean_pnl": round(clean_pnl, 2),
            "lotto_pnl": round(pnl - clean_pnl, 2),
            "p50": p50, "p90": p90,
        },
        "today": {"entries": t_entries, "closed": t_closed, "wr": _pct(t_wins, t_closed), "pnl": round(t_pnl, 2), "p50": t_p50},
        "stale_72h": {"pct": stale_pct, "n": stale72, "signals": sigs72},
        "attainability_12s": {"pct": attain_pct, "n": att_n},
        "bars": bars,
        "bars_green": sum(1 for b in bars if b["ok"]),
        "open_positions": open_positions,
        "open_count": len(positions),
        "recent": [
            {
                "ts": _ts(r)[11:19],
                "type": r.get("type"),
                "detail": (
                    f"pnl {float(r.get('pnl') or 0):+.0f}" if r.get("type") in ("resolution", "exit")
                    else str(r.get("reject_reason") or "")[:40] if r.get("type") == "reject"
                    else f"@ {r.get('sim_fill_price')}"
                ),
                "quarantined": bool(r.get("quarantined_low_price")),
            }
            for r in reversed(recent)
        ],
        "services": _unit_states(),
        "shadow": {
            "wss": bool(hb.get("wss_connected")),
            "confirmations": hb.get("confirmations"),
            "head": hb.get("head"),
            "beat_age_s": hb_age,
        },
        "readiness": {
            "creds_file": creds_present,
            "phase_live_gates": False,  # env gates are per-service; dashboard never reads secrets
        },
    }
