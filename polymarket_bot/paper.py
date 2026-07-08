from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .config import CONFIG, BotConfig


@dataclass
class PaperDecision:
    ts: str
    market_slug: str | None
    question: str | None
    token_id: str | None
    outcome: str | None
    side: str
    score: float
    decision: str
    reasons: list[str]
    blocked_reasons: list[str]
    paper_price: float | None
    paper_size_usd: float


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def append_jsonl(path: Path, row: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def decide_paper(market: dict[str, Any], score: Any, bbo: dict[str, Any], config: BotConfig = CONFIG) -> PaperDecision:
    blocked = list(score.blocked_reasons)
    price = bbo.get("best_ask")
    if not bbo.get("ok"):
        blocked.append("orderbook unavailable")
    if bbo.get("spread") is not None and float(bbo["spread"]) > config.max_spread:
        blocked.append(f"spread {float(bbo['spread']):.3f} > {config.max_spread:.3f}")
    if price is None:
        blocked.append("missing executable ask")
    decision = "paper_buy" if not blocked else "blocked"
    return PaperDecision(
        ts=now_iso(),
        market_slug=market.get("market_slug") or market.get("event_slug"),
        question=market.get("question"),
        token_id=score.token_id,
        outcome=score.outcome,
        side=score.side,
        score=score.score,
        decision=decision,
        reasons=list(score.reasons),
        blocked_reasons=blocked,
        paper_price=price,
        paper_size_usd=config.paper_order_size_usd,
    )


def render_dashboard(scan: list[dict[str, Any]], decisions: list[dict[str, Any]], config: BotConfig = CONFIG) -> Path:
    config.runs_dir.mkdir(parents=True, exist_ok=True)
    rows = "\n".join(
        f"<tr><td>{d.get('decision')}</td><td>{d.get('score')}</td><td>{d.get('outcome')}</td><td>{d.get('market_slug')}</td><td>{', '.join(d.get('blocked_reasons') or [])}</td></tr>"
        for d in decisions[:50]
    )
    html = f"""<!doctype html><html><head><meta charset='utf-8'><title>Polymarket Copybot</title>
<style>body{{font-family:system-ui;background:#0b1020;color:#e8eefc;margin:32px}}table{{border-collapse:collapse;width:100%}}td,th{{border-bottom:1px solid #26324d;padding:8px;text-align:left}}.ok{{color:#67e8a4}}.warn{{color:#fbbf24}}</style></head>
<body><h1>Polymarket Copybot</h1><p>Standalone paper-only bot. Markets scanned: {len(scan)}. Decisions: {len(decisions)}.</p>
<table><thead><tr><th>Decision</th><th>Score</th><th>Outcome</th><th>Market</th><th>Blocks</th></tr></thead><tbody>{rows}</tbody></table></body></html>"""
    out = config.runs_dir / "dashboard.html"
    out.write_text(html, encoding="utf-8")
    return out
