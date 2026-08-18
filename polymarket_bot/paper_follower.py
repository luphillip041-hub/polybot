from __future__ import annotations

import gzip
import json
import logging
import os
import signal
import time
import urllib.request
from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from .archive_config import ArchiveConfig
from .book_archive import bbo_from_levels, normalize_levels, trade_id
from .config import BotConfig, CONFIG
from .alerts import send_telegram
from .resolution import TokenMap, resolved_outcome_for_token as _onchain_resolved_outcome_for_token, RpcClient

LOG = logging.getLogger("polymarket_paper_follower")


@dataclass
class PaperConfig:
    root: Path = Path(__file__).resolve().parents[1]
    paper_dir: Path = root / "runs" / "paper"
    ledger_path: Path = paper_dir / "ledger.jsonl"
    state_path: Path = paper_dir / "state.json"
    allowlist_path: Path = paper_dir / "allowlist.json"
    data_quality_path: Path = paper_dir / "data_quality.json"
    onchain_log_path: Path | None = None
    onchain_primary_enabled: bool = field(
        default_factory=lambda: os.getenv(
            "POLYMARKET_ONCHAIN_PRIMARY", "true"
        ).strip().lower() not in {"0", "false", "no", "off"}
    )
    stake_usd: float = 100.0
    max_signals_per_day: int = int(os.getenv("PAPER_MAX_SIGNALS_PER_DAY", "60"))
    max_open_positions: int = int(os.getenv("PAPER_MAX_OPEN_POSITIONS", "150"))
    max_spread: float = 0.04
    min_top3_liquidity_multiple: float = 2.0
    stale_fill_seconds: float = field(
        default_factory=lambda: float(os.getenv("STALE_FILL_SECONDS", "120"))
    )
    max_ws_age_seconds: float = 60.0
    haircut: float = 0.005
    poll_interval_seconds: float = 3.0
    stale_position_days: int = 14
    resolution_poll_seconds: float = float(os.getenv("POLYMARKET_RESOLUTION_POLL_SECONDS", "1800"))
    quarantine_low_price: bool = os.getenv(
        "PAPER_QUARANTINE_LOW_PRICE", "true"
    ).strip().lower() not in {"0", "false", "no", "off"}

    @classmethod
    def load(cls) -> "PaperConfig":
        cfg = cls()
        cfg.onchain_log_path = Path(
            os.getenv(
                "POLYMARKET_ONCHAIN_LOG",
                str(cfg.root / "runs" / "onchain_shadow" / "shadow_onchain.jsonl"),
            )
        )
        cfg.paper_dir.mkdir(parents=True, exist_ok=True)
        if not cfg.allowlist_path.exists():
            cfg.allowlist_path.write_text(json.dumps({"wallets": [w["wallet"] for w in configured_wallets()]}, indent=2))
        if not cfg.data_quality_path.exists():
            cfg.data_quality_path.write_text(json.dumps({
                "poller_verdict": "matching_bug",
                "fix_commit": "766ee64",
                "fix_timestamp": "2026-07-08T23:16:31+00:00",
                "evidence": "Manual /trades returned recent trades for zero-fill wallets; seen_trade_ids had advanced before shadow fills existed. Fixed by separating journaled_trade_ids from seen_trade_ids and backfilling.",
                "shadow_data_before_fix_suspect": True,
            }, indent=2))
        return cfg


def read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text())
    except Exception:
        return None


def parse_ts(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value), UTC)
        except Exception:
            return None
    if not isinstance(value, str):
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=UTC)
    except Exception:
        return None


def jsonl_paths(prefix: str, start: datetime, end: datetime, archive_dir: Path | None = None) -> list[Path]:
    archive_dir = archive_dir or ArchiveConfig.load().archive_dir
    dates = {(start + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(max(1, (end.date() - start.date()).days + 1))}
    out: list[Path] = []
    for date in dates:
        out.extend(archive_dir.glob(f"{prefix}_{date}*.jsonl.gz"))
    return sorted(set(out))


def configured_wallets() -> list[dict[str, str]]:
    cfg = ArchiveConfig.load()
    wallets: list[dict[str, str]] = []
    seen: set[str] = set()

    def add(address: Any, name: Any = None) -> None:
        if not address:
            return
        wallet = str(address).lower()
        if wallet in seen:
            return
        seen.add(wallet)
        wallets.append({"wallet": wallet, "name": str(name or wallet)})

    for wallet in cfg.tracked_wallets:
        add(wallet)
    rows = read_json(cfg.archive_dir.parent / "wallet_scores_latest.json")
    if isinstance(rows, list):
        for row in rows:
            if isinstance(row, dict):
                add(row.get("wallet"), row.get("user_name") or row.get("name") or row.get("pseudonym"))
            if len(wallets) >= cfg.tracked_wallet_limit_from_scores:
                break
    return wallets


def utc_now() -> datetime:
    return datetime.now(UTC)


def iso_now() -> str:
    return utc_now().isoformat(timespec="seconds")


def num(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return default
        return float(value)
    except Exception:
        return default


def side_norm(value: Any) -> str:
    return str(value or "").upper()


def wallet_name_map() -> dict[str, str]:
    return {w["wallet"].lower(): w["name"] for w in configured_wallets()}


def load_allowlist(path: Path) -> set[str]:
    data = read_json(path)
    if isinstance(data, list):
        return {str(x).lower() for x in data}
    if isinstance(data, dict) and isinstance(data.get("wallets"), list):
        return {str(x).lower() for x in data["wallets"]}
    return {w["wallet"].lower() for w in configured_wallets()}


def book_levels(book: dict[str, Any], side: str) -> list[dict[str, float]]:
    raw = book.get("top3_asks") if side == "BUY" else book.get("top3_bids")
    if not isinstance(raw, list):
        return []
    out: list[dict[str, float]] = []
    for level in raw:
        if not isinstance(level, dict):
            continue
        price = num(level.get("price"), -1)
        size = num(level.get("size"), -1)
        if price > 0 and size > 0:
            out.append({"price": price, "size": size})
    return out


def book_snapshot(book: dict[str, Any] | None) -> dict[str, Any]:
    book = book or {}
    return {
        "best_bid": book.get("best_bid"),
        "best_ask": book.get("best_ask"),
        "bid_size": book.get("best_bid_size"),
        "ask_size": book.get("best_ask_size"),
        "spread": book.get("spread"),
    }


# Cache for live REST BBO backstop. Tokens we fetched live get cached for
# BBO_BACKSTOP_TTL_SEC so we don't double-snap. Helps paper fills that arrive
# before the archive WS catches up — eliminates most no_archived_book rejects.
_BBO_BACKSTOP_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
# Execution-time books must stay fresh.  The follower loops every three seconds,
# so a two-second cache only coalesces same-burst wallet fills.
BBO_BACKSTOP_TTL_SEC = 2.0


def _clean_bbo_cache() -> None:
    """Drop expired entries from the backstop cache."""
    now = time.time()
    expired = [t for t, (ts, _) in _BBO_BACKSTOP_CACHE.items() if now - ts > BBO_BACKSTOP_TTL_SEC]
    for t in expired:
        _BBO_BACKSTOP_CACHE.pop(t, None)


def live_clob_book(token: str, *, use_cache: bool = True) -> dict[str, Any] | None:
    """Fetch and normalize an executable top-three CLOB book.

    One `/book` request supplies both BBO and depth.  Returning only BBO would
    make `simulate_fill` reject every on-chain signal as `missing_book`.
    """
    from .clob import order_book

    _clean_bbo_cache()
    if not token:
        return None
    cached = _BBO_BACKSTOP_CACHE.get(token)
    if use_cache and cached:
        return cached[1]
    try:
        raw = order_book(token)
        bids = normalize_levels(raw.get("bids"), reverse=True)
        asks = normalize_levels(raw.get("asks"), reverse=False)
        if not bids or not asks:
            return None
        snap = {
            "token_id": token,
            "top3_bids": bids,
            "top3_asks": asks,
            **bbo_from_levels(bids, asks),
            "_source": "clob_rest_live",
        }
        _BBO_BACKSTOP_CACHE[token] = (time.time(), snap)
        return snap
    except Exception:
        LOG.warning("rest_backstop_bbo failed for token=%s", token[:12])
        return None


def rest_backstop_bbo(token: str) -> dict[str, Any] | None:
    """Backward-compatible alias for the full live CLOB book fetch."""
    return live_clob_book(token)


def top3_notional(book: dict[str, Any], side: str) -> float:
    return sum(level["price"] * level["size"] for level in book_levels(book, side))


def simulate_fill(book: dict[str, Any], side: str, stake_usd: float, haircut: float = 0.005) -> tuple[float | None, float, str | None]:
    levels = book_levels(book, side)
    if not levels:
        return None, 0.0, "missing_book"
    remaining = stake_usd
    shares = 0.0
    spent = 0.0
    for level in levels:
        px = level["price"] + haircut if side == "BUY" else level["price"] - haircut
        px = min(0.999, max(0.001, px))
        level_notional = px * level["size"]
        use = min(remaining, level_notional)
        if use <= 0:
            continue
        shares += use / px
        spent += use
        remaining -= use
        if remaining <= 1e-9:
            break
    if remaining > 1e-6 or shares <= 0:
        return None, 0.0, "insufficient_depth"
    return spent / shares, shares, None


def simulate_fill_shares(
    book: dict[str, Any], side: str, shares_to_fill: float, haircut: float = 0.005
) -> tuple[float | None, float, str | None]:
    """Walk book depth for a fixed quantity, used when closing a position."""
    levels = book_levels(book, side)
    if not levels:
        return None, 0.0, "missing_book"
    remaining = max(0.0, float(shares_to_fill))
    if remaining <= 0:
        return None, 0.0, "invalid_position_size"
    filled = 0.0
    proceeds = 0.0
    for level in levels:
        px = level["price"] + haircut if side == "BUY" else level["price"] - haircut
        px = min(0.999, max(0.001, px))
        use = min(remaining, level["size"])
        if use <= 0:
            continue
        filled += use
        proceeds += use * px
        remaining -= use
        if remaining <= 1e-9:
            break
    if remaining > 1e-6 or filled <= 0:
        return None, 0.0, "insufficient_depth"
    return proceeds / filled, filled, None


def shadow_paths(archive_cfg: ArchiveConfig) -> list[Path]:
    start = utc_now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
    end = utc_now() + timedelta(days=1)
    return jsonl_paths("shadow", start, end, archive_cfg.archive_dir)


def iter_shadow_fills(archive_cfg: ArchiveConfig) -> list[dict[str, Any]]:
    """Load shadow fills, optionally filtered by time window.

    Without `since`, loads all fills from the past 24 hours. With `since`,
    only loads files modified after that timestamp AND filters rows by ts.
    """
    rows: list[dict[str, Any]] = []
    paths = shadow_paths(archive_cfg)
    for path in paths:
        try:
            with gzip.open(path, "rt") as f:
                for line in f:
                    if not line.strip():
                        continue
                    try:
                        row = json.loads(line)
                    except Exception:
                        continue
                    if (row.get("type") or row.get("kind")) == "fill":
                        rows.append(row)
        except Exception:
            LOG.exception("failed reading shadow path=%s", path)
    rows.sort(key=lambda r: parse_ts(r.get("ts")) or utc_now())
    return rows


def _iter_fills_from_path(path: Path) -> list[dict[str, Any]]:
    """Load fills from a single gzipped JSONL shadow file."""
    rows: list[dict[str, Any]] = []
    try:
        with gzip.open(path, "rt") as f:
            for line in f:
                if not line.strip():
                    continue
                try:
                    row = json.loads(line)
                except Exception:
                    continue
                if (row.get("type") or row.get("kind")) == "fill":
                    rows.append(row)
    except Exception:
        LOG.exception("failed reading shadow path=%s", path)
    return rows


def _iter_new_fills_from_path(path: Path, offset: int = 0) -> tuple[list[dict[str, Any]], int]:
    """Read only complete gzip members appended after a compressed offset."""
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows, 0
    size = path.stat().st_size
    if offset < 0 or offset > size:
        offset = 0
    try:
        with path.open("rb") as raw:
            raw.seek(offset)
            with gzip.GzipFile(fileobj=raw, mode="rb") as gz:
                for raw_line in gz:
                    try:
                        row = json.loads(raw_line.decode("utf-8"))
                    except Exception:
                        continue
                    if (row.get("type") or row.get("kind")) == "fill":
                        rows.append(row)
    except (EOFError, OSError):
        # Retry the member next cycle if the writer is between append/close.
        return [], offset
    return rows, size


def onchain_lane_to_fill(row: dict[str, Any]) -> dict[str, Any] | None:
    """Translate a reconciled on-chain/data-api lane row into follower input."""
    if row.get("type") != "lane_detection":
        return None
    source = str(row.get("source") or "")
    if source not in {"polygon_onchain", "data_api"}:
        return None
    durable_id = str(row.get("durable_trade_id") or "")
    wallet = str(row.get("wallet") or "").lower()
    token = str(row.get("token_id") or "")
    side = side_norm(row.get("side"))
    detected = row.get("detection_ts") or row.get("detection_epoch") or row.get("ts")
    filled = row.get("ground_truth_ts") or row.get("ground_truth_epoch")
    if not durable_id or not wallet or not token or side not in {"BUY", "SELL"} or not filled:
        return None
    market_raw = row.get("market")
    market: dict[str, Any] = market_raw if isinstance(market_raw, dict) else {}
    condition_id = (
        market.get("condition_id")
        or market.get("conditionId")
        or market.get("condition")
    )
    source_detection = parse_ts(detected)
    fill_time = parse_ts(filled)
    follower_observed = utc_now()
    return {
        "type": "fill",
        # Staleness is measured when the follower actually sees the row.  Using
        # the worker timestamp here would incorrectly accept a backlog after a
        # follower outage as if it were still an eight-second-old signal.
        "ts": follower_observed.isoformat(),
        "fill_timestamp": (fill_time or follower_observed).isoformat(),
        "source_detection_ts": source_detection.isoformat() if source_detection else None,
        "source_detection_latency_s": row.get("detection_latency_seconds"),
        "wallet": wallet,
        "trade_id": durable_id,
        "fill_side": side,
        "fill_price": row.get("price"),
        "detection_source": source,
        "trade": {
            "transactionHash": row.get("transaction_hash"),
            "logIndex": row.get("log_index"),
            "asset": token,
            "side": side,
            "price": row.get("price"),
            "size": row.get("size"),
            "conditionId": condition_id,
            "timestamp": (fill_time or follower_observed).isoformat(),
        },
    }


def iter_new_onchain_fills(path: Path, offset: int = 0) -> tuple[list[dict[str, Any]], int]:
    """Tail complete on-chain measurement rows without replaying the log."""
    if not path.exists():
        # A transient missing mount/path must not reset a persisted offset and
        # replay the entire historical measurement log when it reappears.
        return [], offset
    size = path.stat().st_size
    if offset < 0 or offset > size:
        offset = 0
    fills: list[dict[str, Any]] = []
    new_offset = offset
    with path.open("rb") as handle:
        handle.seek(offset)
        while True:
            start = handle.tell()
            raw = handle.readline()
            if not raw:
                break
            if not raw.endswith(b"\n"):
                new_offset = start
                break
            new_offset = handle.tell()
            try:
                row = json.loads(raw.decode("utf-8"))
            except Exception:
                continue
            fill = onchain_lane_to_fill(row)
            if fill is not None:
                fills.append(fill)
    return fills, new_offset


# Only load files modified in the last N seconds (filesystem mtime check)
def shadow_paths_recent(archive_cfg: ArchiveConfig, since_seconds: int) -> list[Path]:
    """Like shadow_paths, but only return files modified within since_seconds."""
    cutoff = time.time() - since_seconds
    return [p for p in shadow_paths(archive_cfg) if p.exists() and p.stat().st_mtime >= cutoff]


# Cap on memory usage: drop oldest in-memory entries if we exceed this
MAX_PROCESSED_TRADE_IDS_INMEM = 30000


def append_jsonl_fsync(path: Path, row: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(row, separators=(",", ":"), sort_keys=True, default=str) + "\n"
    with path.open("ab") as f:
        f.write(payload.encode("utf-8"))
        f.flush()
        os.fsync(f.fileno())


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
    with path.open("rb") as f:
        for raw in f:
            try:
                rows.append(json.loads(raw.decode("utf-8")))
            except Exception:
                continue
    return rows


def load_state(path: Path) -> dict[str, Any]:
    data = read_json(path)
    if isinstance(data, dict):
        data.setdefault("processed_trade_ids", [])
        data.setdefault("positions", {})
        return data
    return {"processed_trade_ids": [], "positions": {}}


def reconcile_state_from_ledger(
    state: dict[str, Any], ledger_path: Path
) -> dict[str, Any]:
    """Recover processed decisions and open positions after an interrupted cycle.

    Ledger rows are append+fsync before the state snapshot.  Rebuilding terminal
    decisions here closes the crash window without treating a lone `signal` row
    as complete; a signal-only interrupted decision is safe to retry.
    """
    if not ledger_path.exists():
        return state
    positions: dict[str, dict[str, Any]] = {}
    completed_ids: list[str] = []
    for row in read_jsonl(ledger_path):
        row_type = row.get("type")
        trade = row.get("trade") if isinstance(row.get("trade"), dict) else {}
        tid = str(row.get("trade_id") or (trade_id(trade) if trade else ""))
        if row_type in {"entry", "exit", "reject", "ineligible"} and tid:
            completed_ids.append(tid)
        position_id = str(row.get("position_id") or "")
        if row_type == "entry" and position_id:
            price = num(row.get("sim_fill_price"), 0)
            shares = num(row.get("sim_size"), 0)
            positions[position_id] = {
                "position_id": position_id,
                "wallet": str(row.get("wallet") or "").lower(),
                "token": str(row.get("token") or ""),
                "entry_price": price,
                "shares": shares,
                "cost_usd": price * shares,
                "opened_at": row.get("ts"),
                "quarantined_low_price": bool(row.get("quarantined_low_price")),
            }
        elif row_type in {"exit", "resolution"} and position_id:
            positions.pop(position_id, None)
    state["positions"] = positions
    prior = [str(value) for value in state.get("processed_trade_ids", [])]
    state["processed_trade_ids"] = list(dict.fromkeys(prior + completed_ids))[-20000:]
    return state


def save_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2, sort_keys=True, default=str))
    tmp.replace(path)


def latest_ws_age_seconds(archive_cfg: ArchiveConfig) -> float:
    latest: datetime | None = None
    for path in sorted(archive_cfg.archive_dir.glob("book_*.jsonl.gz"))[-4:]:
        try:
            with gzip.open(path, "rt") as f:
                for line in f:
                    try:
                        row = json.loads(line)
                    except Exception:
                        continue
                    if row.get("type") == "book" and row.get("source") == "websocket":
                        ts = parse_ts(row.get("ts"))
                        if ts and (latest is None or ts > latest):
                            latest = ts
        except Exception:
            continue
    return (utc_now() - latest).total_seconds() if latest else 999999999.0


def detection_inside_gap(archive_cfg: ArchiveConfig, ts: datetime) -> bool:
    start = ts - timedelta(minutes=5)
    end = ts + timedelta(minutes=5)
    for path in jsonl_paths("book", start, end, archive_cfg.archive_dir):
        try:
            with gzip.open(path, "rt") as f:
                for line in f:
                    try:
                        row = json.loads(line)
                    except Exception:
                        continue
                    if row.get("type") != "gap":
                        continue
                    s = parse_ts(row.get("start_ts")); e = parse_ts(row.get("end_ts"))
                    if s and e and s <= ts <= e:
                        return True
        except Exception:
            continue
    return False


_RECENT_GAP_CACHE: dict[str, Any] = {"ts": 0.0, "intervals": []}
RECENT_GAP_CACHE_TTL_SECONDS = 15.0


def recent_gap_intervals(
    archive_cfg: ArchiveConfig, *, minutes: int = 15
) -> list[tuple[datetime, datetime]]:
    """Load recent gap intervals once per follower cycle."""
    if time.time() - float(_RECENT_GAP_CACHE["ts"]) < RECENT_GAP_CACHE_TTL_SECONDS:
        return list(_RECENT_GAP_CACHE["intervals"])
    now = utc_now()
    start = now - timedelta(minutes=minutes)
    end = now + timedelta(minutes=1)
    intervals: list[tuple[datetime, datetime]] = []
    for path in jsonl_paths("book", start, end, archive_cfg.archive_dir):
        try:
            with gzip.open(path, "rt") as f:
                for line in f:
                    try:
                        row = json.loads(line)
                    except Exception:
                        continue
                    if row.get("type") != "gap":
                        continue
                    gap_start = parse_ts(row.get("start_ts"))
                    gap_end = parse_ts(row.get("end_ts"))
                    if gap_start and gap_end and gap_end >= start:
                        intervals.append((gap_start, gap_end))
        except Exception:
            continue
    _RECENT_GAP_CACHE["ts"] = time.time()
    _RECENT_GAP_CACHE["intervals"] = intervals
    return intervals


def position_key(wallet: str, token: str) -> str:
    return f"{wallet.lower()}:{token}"


def market_resolution_soon(book: dict[str, Any], now: datetime) -> bool:
    market = book.get("market") if isinstance(book.get("market"), dict) else {}
    for key in ("end_date", "endDate", "end_ts", "resolution_ts"):
        ts = parse_ts(market.get(key))
        if ts and ts - now < timedelta(hours=24):
            return True
    return False


def reject_reasons(
    row: dict[str, Any],
    cfg: PaperConfig,
    archive_cfg: ArchiveConfig,
    state: dict[str, Any],
    *,
    ws_age_seconds: float | None = None,
    inside_gap: bool | None = None,
) -> list[str]:
    reasons: list[str] = []
    wallet = str(row.get("wallet") or "").lower()
    # REMOVED 2026-07-23 (per review): wallet_not_allowlisted rejection.
    # allowlist.json now includes all 5 configured wallets; exclusion is a tag (eligible_live)
    # not a gate, so observation keeps every wallet's signals.
    # allowlist = load_allowlist(cfg.allowlist_path)
    # if wallet not in allowlist:
    #     reasons.append("wallet_not_allowlisted")
    fill_ts = parse_ts(row.get("fill_timestamp") or (row.get("trade") or {}).get("timestamp"))
    detect_ts = parse_ts(row.get("ts")) or utc_now()
    if fill_ts and (detect_ts - fill_ts).total_seconds() > cfg.stale_fill_seconds:
        reasons.append("stale_fill")
    book = row.get("book_at_detection") if isinstance(row.get("book_at_detection"), dict) else None
    if not book:
        reasons.append("no_archived_book")
        return reasons
    snap = book_snapshot(book)
    spread = snap.get("spread")
    best_bid = num(snap.get("best_bid"), float("nan"))
    best_ask = num(snap.get("best_ask"), float("nan"))
    if best_bid >= best_ask:
        reasons.append("crossed_book")
    if spread is None or num(spread) > cfg.max_spread:
        reasons.append("illiquid_spread")
    side = side_norm(row.get("fill_side") or (row.get("trade") or {}).get("side"))
    if side not in {"BUY", "SELL"}:
        reasons.append("unknown_side")
    # Lottery-ticket check: REVERTED 2026-07-23 (per review) from gate to tag.
    # The 5d sample showed two <10¢ trades were 84% of PnL — pure variance — so the filter
    # still exists at execution via eligible_live, but observation now keeps the rows so we can
    # re-derive the band's behavior without another 30d wait. SELLs are unaffected (handled
    # by is_lottery_band below; called from process_fill so rejection reason stays separately).
    # fill_price = row.get("fill_price")
    # if side == "BUY" and fill_price is not None and num(fill_price) < 0.10:
    #     reasons.append("lottery_price_band")
    if top3_notional(book, side if side in {"BUY", "SELL"} else "BUY") < cfg.stake_usd * cfg.min_top3_liquidity_multiple:
        reasons.append("illiquid_depth")
    if market_resolution_soon(book, detect_ts):
        reasons.append("near_resolution")
    ws_age = latest_ws_age_seconds(archive_cfg) if ws_age_seconds is None else ws_age_seconds
    if ws_age > cfg.max_ws_age_seconds:
        reasons.append("blind_ws_stale")
    gap_hit = detection_inside_gap(archive_cfg, detect_ts) if inside_gap is None else inside_gap
    if gap_hit:
        reasons.append("blind_gap")
    token = str((row.get("trade") or {}).get("asset") or book.get("token_id") or "")
    if side == "BUY" and position_key(wallet, token) in state.get("positions", {}):
        reasons.append("duplicate")
    if side == "BUY" and len(state.get("positions", {})) >= cfg.max_open_positions:
        reasons.append("max_positions")
    if side == "SELL" and position_key(wallet, token) not in state.get("positions", {}):
        reasons.append("sell_no_position")
    return reasons


def is_lottery_band(row: dict[str, Any]) -> bool:
    """Tag-only check (2026-07-23): returns True for BUY fills below 10¢.
    Doesn't gate execution directly — process_fill() consults this and tags the row
    eligible_live=False (with type='ineligible'), preserving the original observation
    data so the band's behavior can be re-derived from the ledger without rerunning."""
    fill_price = row.get("fill_price") or (row.get("trade") or {}).get("price")
    side = side_norm(row.get("fill_side") or (row.get("trade") or {}).get("side"))
    return side == "BUY" and fill_price is not None and num(fill_price) < 0.10


def quarantined_position_ids(rows: list[dict[str, Any]], cfg: PaperConfig) -> set[str]:
    """Identify tagged and legacy sub-10-cent entries while quarantine is on."""
    if not cfg.quarantine_low_price:
        return set()
    result: set[str] = set()
    for row in rows:
        if row.get("type") != "entry" or not row.get("position_id"):
            continue
        price = row.get("wallet_fill_price")
        if row.get("quarantined_low_price") or (
            price is not None and num(price, 1.0) < 0.10
        ):
            result.add(str(row["position_id"]))
    return result


# Path to a small JSON file the paper-follower writes tokens to that need
# archive WS priority. Book archive reads it on each refresh cycle and adds
# new tokens to its wallet_driven queue. Live coupling without dependencies.
ARCHIVE_PRIORITY_FILE = Path(__file__).resolve().parents[1] / "runs" / "paper" / "archive_priority.jsonl"
ARCHIVE_PRIORITY_TTL_SEC = 600  # 10 min before archive picks up


def _flag_token_for_archive_fn(token: str) -> None:
    """Append a token to the archive priority file with a TTL marker.

    Archive daemon reads this file each cycle and adds fresh tokens to
    its wallet_driven queue (which forces a WS subscribe). Decoupled —
    no direct import. The archive daemon picks it up on its next tick.
    """
    if not token:
        return
    try:
        ARCHIVE_PRIORITY_FILE.parent.mkdir(parents=True, exist_ok=True)
        rec = {"token": token, "ts": time.time()}
        with ARCHIVE_PRIORITY_FILE.open("a") as f:
            f.write(json.dumps(rec) + "\n")
    except Exception:
        pass


class PaperFollowerDaemon:
    def __init__(self, cfg: PaperConfig | None = None, archive_cfg: ArchiveConfig | None = None) -> None:
        self.cfg = cfg or PaperConfig.load()
        self.archive_cfg = archive_cfg or ArchiveConfig.load()
        first_start = not self.cfg.state_path.exists()
        self.state = load_state(self.cfg.state_path)
        if first_start:
            # Forward-test only: do not spend the daily cap replaying historical
            # shadow rows/backfills. Start from the current tail and journal only
            # newly observed fills after service activation.
            self.state["processed_trade_ids"] = [str(r.get("trade_id") or trade_id(r.get("trade") or {})) for r in iter_shadow_fills(self.archive_cfg)]
            save_state(self.cfg.state_path, self.state)
        self.state = reconcile_state_from_ledger(self.state, self.cfg.ledger_path)
        save_state(self.cfg.state_path, self.state)
        self._processed_trade_ids = set(map(str, self.state.get("processed_trade_ids", [])))
        self._onchain_log_path = self.cfg.onchain_log_path or (
            self.cfg.root / "runs" / "onchain_shadow" / "shadow_onchain.jsonl"
        )
        persisted_offset = self.state.get("onchain_log_offset")
        if persisted_offset is None:
            # First activation starts at the live tail.  Historical measurement
            # rows must never become paper entries after a deploy.
            persisted_offset = (
                self._onchain_log_path.stat().st_size
                if self._onchain_log_path.exists()
                else 0
            )
            self.state["onchain_log_offset"] = persisted_offset
            save_state(self.cfg.state_path, self.state)
        self._onchain_offset = int(persisted_offset)
        self._shadow_offsets: dict[str, int] = {}
        self._daily_key = ""
        self._accepts_today = 0
        self._pnl_today = 0.0
        self._cycle_ws_age_seconds: float | None = None
        self._cycle_gap_intervals: list[tuple[datetime, datetime]] = []
        self._refresh_daily_counters(force=True)
        self.running = True
        self._last_resolution_at = time.time()
        self._resolution_summary: dict[str, Any] = {"last_checked_at": None, "checked": 0, "resolved": 0, "skipped": 0}

    def _refresh_daily_counters(self, *, force: bool = False) -> None:
        today = utc_now().strftime("%Y-%m-%d")
        if not force and today == self._daily_key:
            return
        start = utc_now().replace(hour=0, minute=0, second=0, microsecond=0)
        rows = read_jsonl(self.cfg.ledger_path)
        todays_rows = [r for r in rows if (parse_ts(r.get("ts")) or start) >= start]
        quarantined = quarantined_position_ids(rows, self.cfg)
        self._daily_key = today
        self._accepts_today = sum(1 for r in todays_rows if r.get("type") == "entry")
        self._pnl_today = sum(
            num(r.get("pnl"), 0)
            for r in todays_rows
            if r.get("type") in {"exit", "resolution", "void_correction"}
            and str(r.get("position_id") or "") not in quarantined
        )

    def signal_row(self, row: dict[str, Any], tid: str, latency: float | None) -> dict[str, Any]:
        trade = row.get("trade") if isinstance(row.get("trade"), dict) else {}
        book = row.get("book_at_detection") if isinstance(row.get("book_at_detection"), dict) else {}
        return {
            "ts": iso_now(),
            "type": "signal",
            "wallet": row.get("wallet"),
            "market": trade.get("conditionId") or trade.get("slug") or trade.get("eventSlug"),
            "token": trade.get("asset") or book.get("token_id"),
            "side": row.get("fill_side") or trade.get("side"),
            "detection_latency_s": latency,
            "source_detection_ts": row.get("source_detection_ts"),
            "source_detection_latency_s": row.get("source_detection_latency_s"),
            "detection_source": row.get("detection_source") or "data_api_archive",
            "wallet_fill_price": row.get("fill_price") if row.get("fill_price") is not None else trade.get("price"),
            "sim_fill_price": None,
            "sim_size": None,
            "book_snapshot": book_snapshot(book),
            "reject_reason": None,
            "position_id": None,
            "pnl": None,
            "eligible_live": True,  # overridden in process_fill; True means execution-path-eligible
            "trade_id": tid,
        }

    def process_fill(self, row: dict[str, Any], accepts_today: int) -> list[dict[str, Any]]:
        trade = row.get("trade") if isinstance(row.get("trade"), dict) else {}
        tid = str(row.get("trade_id") or trade_id(trade))
        if tid in self._processed_trade_ids:
            return []
        detect_ts = parse_ts(row.get("ts")) or utc_now()
        fill_ts = parse_ts(row.get("fill_timestamp") or trade.get("timestamp"))
        latency = (detect_ts - fill_ts).total_seconds() if fill_ts else None
        # Backstop BBO: if archive hasn't caught up yet, hit CLOB REST once
        # (cached for 30s by token) so the signal can pass rejection checks
        book = row.get("book_at_detection") if isinstance(row.get("book_at_detection"), dict) else {}
        if not book:
            _token = str(trade.get("asset") or book.get("token_id") or "")
            backstop = rest_backstop_bbo(_token)
            if backstop:
                row["book_at_detection"] = backstop
                book = backstop
        book = book or {}
        # Build the signal after the backstop so the journal captures the actual
        # detection-time execution evidence rather than an empty snapshot.
        out = [self.signal_row(row, tid, latency)]
        # Tell the archive daemon to prioritize this token for WS subscription
        if not row.get("book_at_detection") or row.get("book_at_detection", {}).get("_source") in {"rest_backstop", "clob_rest_live"}:
            _token_for_archive = str(trade.get("asset") or book.get("token_id") or "")
            if _token_for_archive:
                _flag_token_for_archive_fn(_token_for_archive)
        inside_gap = any(
            start <= detect_ts <= end for start, end in self._cycle_gap_intervals
        )
        reasons = reject_reasons(
            row,
            self.cfg,
            self.archive_cfg,
            self.state,
            ws_age_seconds=self._cycle_ws_age_seconds,
            inside_gap=inside_gap,
        )
        quarantined_low_price = self.cfg.quarantine_low_price and is_lottery_band(row)
        eligible_live = (len(reasons) == 0) and not quarantined_low_price
        out[0]["eligible_live"] = eligible_live
        out[0]["quarantined_low_price"] = quarantined_low_price
        wallet = str(row.get("wallet") or "").lower()
        snap = book_snapshot(book)
        token = str(trade.get("asset") or book.get("token_id") or "")
        side = side_norm(row.get("fill_side") or trade.get("side"))
        if reasons:
            # Hard rejection (illiquid, stale, etc.) — unchanged behavior; existing reject_type intact.
            rej = dict(out[0]); rej.update({"ts": iso_now(), "type": "reject", "reject_reason": ",".join(reasons), "book_snapshot": snap})
            out.append(rej)
        elif side != "SELL" and accepts_today >= self.cfg.max_signals_per_day:
            rej = dict(out[0]); rej.update({"ts": iso_now(), "type": "reject", "reject_reason": "daily_entry_cap", "book_snapshot": snap})
            out.append(rej)
        elif side == "SELL":
            pos_id = position_key(wallet, token)
            pos = self.state.get("positions", {}).get(pos_id) or {}
            price, shares, fill_err = simulate_fill_shares(
                book, "SELL", num(pos.get("shares"), 0), self.cfg.haircut
            )
            if fill_err:
                ex = dict(out[0]); ex.update({"ts": iso_now(), "type": "reject", "reject_reason": fill_err, "position_id": pos_id, "book_snapshot": snap})
            else:
                proceeds = shares * (price or 0)
                pnl = proceeds - num(pos.get("cost_usd"), 0)
                self.state.get("positions", {}).pop(pos_id, None)
                ex = dict(out[0]); ex.update({"ts": iso_now(), "type": "exit", "sim_fill_price": price, "sim_size": shares, "position_id": pos_id, "pnl": pnl, "book_snapshot": snap, "quarantined_low_price": bool(pos.get("quarantined_low_price"))})
            out.append(ex)
        else:
            price, shares, fill_err = simulate_fill(book, "BUY", self.cfg.stake_usd, self.cfg.haircut)
            if fill_err:
                ent = dict(out[0]); ent.update({"ts": iso_now(), "type": "reject", "reject_reason": fill_err, "book_snapshot": snap})
                out.append(ent)
            else:
                pos_id = position_key(wallet, token)
                self.state.setdefault("positions", {})[pos_id] = {"position_id": pos_id, "wallet": wallet, "token": token, "entry_price": price, "shares": shares, "cost_usd": self.cfg.stake_usd, "opened_at": iso_now(), "quarantined_low_price": quarantined_low_price}
                ent = dict(out[0]); ent.update({"ts": iso_now(), "type": "entry", "sim_fill_price": price, "sim_size": shares, "position_id": pos_id, "book_snapshot": snap, "quarantined_low_price": quarantined_low_price})
                out.append(ent)
        self.state.setdefault("processed_trade_ids", []).append(tid)
        self.state["processed_trade_ids"] = list(dict.fromkeys(self.state["processed_trade_ids"]))[-20000:]
        self._processed_trade_ids.add(tid)
        if len(self._processed_trade_ids) > MAX_PROCESSED_TRADE_IDS_INMEM:
            self._processed_trade_ids = set(
                self.state["processed_trade_ids"][-MAX_PROCESSED_TRADE_IDS_INMEM:]
            )
        return out

    def process_once(self) -> int:
        self._refresh_daily_counters()
        # Auto-pause: kill switch file
        if (Path("/root/flip/projects/polymarket-copybot/optsig-paper.disabled")).exists():
            LOG.info("Paper trading paused (kill switch exists). Skipping cycle.")
            return 0
        # Auto-pause: daily loss cap
        daily_cap_file = Path("/root/flip/projects/polymarket-copybot/runs/paper/.daily_cap")
        if daily_cap_file.exists():
            try:
                cap = json.loads(daily_cap_file.read_text())
                max_loss = float(cap.get("max_usd", 0))
                if self._pnl_today < -max_loss:
                    LOG.warning("Daily loss cap hit (PnL today: $%.2f, cap: $%.2f). Auto-pausing.",
                                self._pnl_today, max_loss)
                    Path("/root/flip/projects/polymarket-copybot/optsig-paper.disabled").touch()
                    return 0
            except Exception as e:
                LOG.debug("daily cap check failed: %s", e)
        accepts_today = self._accepts_today
        wrote = 0
        notify_rows: list[dict[str, Any]] = []
        heartbeat = read_json(self.archive_cfg.archive_dir / "heartbeat_latest.json")
        heartbeat_ts = (
            parse_ts(heartbeat.get("last_ws_message_ts"))
            if isinstance(heartbeat, dict) else None
        )
        self._cycle_ws_age_seconds = (
            max(0.0, (utc_now() - heartbeat_ts).total_seconds())
            if heartbeat_ts else float("inf")
        )
        self._cycle_gap_intervals = recent_gap_intervals(self.archive_cfg)
        if self.cfg.onchain_primary_enabled:
            # Polygon is the primary trigger; reconciled Data API lane rows are
            # fallback.  Both carry the same durable tx:log ID, so one dedup path
            # covers both lanes.  The legacy raw API archive hot path is disabled
            # in this mode because its tx-only IDs can double-enter the same fill.
            fills, new_offset = iter_new_onchain_fills(
                self._onchain_log_path, self._onchain_offset
            )
            for fill in fills:
                token = str((fill.get("trade") or {}).get("asset") or "")
                fill["book_at_detection"] = live_clob_book(token)
                rows = self.process_fill(fill, accepts_today)
                if rows:
                    entry_present = any(r.get("type") == "entry" for r in rows)
                    if entry_present:
                        accepts_today += 1
                for row in rows:
                    append_jsonl_fsync(self.cfg.ledger_path, row)
                    if row.get("type") in {"entry", "exit"}:
                        notify_rows.append(row)
                    if row.get("type") == "exit":
                        self._pnl_today += num(row.get("pnl"), 0)
                    wrote += 1
            self._onchain_offset = new_offset
            self.state["onchain_log_offset"] = new_offset
        else:
            # Legacy Data API archive follower retained as a fail-safe rollback.
            recent_paths = shadow_paths_recent(self.archive_cfg, since_seconds=3600)
            for path in recent_paths:
                key = str(path)
                fills, new_offset = _iter_new_fills_from_path(
                    path, self._shadow_offsets.get(key, 0)
                )
                for fill in fills:
                    rows = self.process_fill(fill, accepts_today)
                    if rows:
                        entry_present = any(r.get("type") == "entry" for r in rows)
                        if entry_present:
                            accepts_today += 1
                    for row in rows:
                        append_jsonl_fsync(self.cfg.ledger_path, row)
                        if row.get("type") in {"entry", "exit"}:
                            notify_rows.append(row)
                        if row.get("type") == "exit":
                            self._pnl_today += num(row.get("pnl"), 0)
                        wrote += 1
                self._shadow_offsets[key] = new_offset
        self._accepts_today = accepts_today
        # Memory: cap processed_trade_ids growth in memory between writes
        ptids = self.state.get("processed_trade_ids", [])
        if len(ptids) > MAX_PROCESSED_TRADE_IDS_INMEM:
            self.state["processed_trade_ids"] = ptids[-MAX_PROCESSED_TRADE_IDS_INMEM:]
        save_state(self.cfg.state_path, self.state)
        webhook_url = os.getenv("POLYMARKET_PAPER_WEBHOOK_URL") or os.getenv("DISCORD_WEBHOOK_URL")
        if notify_rows:
            status = paper_status(self.cfg)
            for row in notify_rows:
                message = render_trade_webhook(row, status)
                if message:
                    if webhook_url:
                        post_discord_webhook(webhook_url, message)
                    send_telegram(message)
        return wrote

    def process_resolution_once(self, *, force: bool = False) -> dict[str, Any] | None:
        """Run a single resolution cycle if the throttle window has elapsed (or force=True).

        Returns the summary dict on a real cycle, None if throttled.
        """
        now = time.time()
        interval = float(self.cfg.resolution_poll_seconds or 1800)
        if not force and (now - self._last_resolution_at) < interval:
            return None
        self._last_resolution_at = now
        try:
            summary = run_resolution_cycle(self.state, self.cfg)
        except Exception:
            LOG.exception("resolution cycle failed")
            return {"checked": 0, "resolved": 0, "skipped": 0, "last_checked_at": iso_now(), "error": True}
        self._resolution_summary = summary
        for row in summary.get("exit_rows", []):
            self._pnl_today += num(row.get("pnl"), 0)
        return summary

    def run(self) -> None:
        for sig in (signal.SIGTERM, signal.SIGINT):
            signal.signal(sig, lambda *_: self.stop())
        while self.running:
            cycle_start = time.time()
            wrote = self.process_once()
            self.process_resolution_once(force=False)
            elapsed = time.time() - cycle_start
            if wrote:
                LOG.info("paper follower wrote ledger rows=%s cycle=%.3fs", wrote, elapsed)
            sleep_left = max(0.0, self.cfg.poll_interval_seconds - elapsed)
            time.sleep(sleep_left)

    def stop(self) -> None:
        self.running = False
        save_state(self.cfg.state_path, self.state)


def paper_status(cfg: PaperConfig | None = None) -> dict[str, Any]:
    cfg = cfg or PaperConfig.load()
    rows = read_jsonl(cfg.ledger_path)
    today = utc_now().replace(hour=0, minute=0, second=0, microsecond=0)
    today_rows = [r for r in rows if (parse_ts(r.get("ts")) or today) >= today]
    signals = [r for r in today_rows if r.get("type") == "signal"]
    entries = [r for r in today_rows if r.get("type") == "entry"]
    rejects = [r for r in today_rows if r.get("type") == "reject"]
    ineligible = [r for r in today_rows if r.get("type") == "ineligible"]
    signals_by_source = dict(
        Counter(str(r.get("detection_source") or "legacy") for r in signals)
    )
    accepts_by_source = dict(
        Counter(str(r.get("detection_source") or "legacy") for r in entries)
    )
    pnl_rows = [r for r in rows if r.get("type") in {"exit", "resolution", "void_correction"}]
    quarantined = quarantined_position_ids(rows, cfg)
    rejects_by_reason: dict[str, int] = {}
    for row in rejects + ineligible:
        for reason in str(row.get("reject_reason") or "unknown").split(","):
            if reason.strip():
                rejects_by_reason[reason.strip()] = rejects_by_reason.get(reason.strip(), 0) + 1

    # Coverage metric: % of today's signals that had BBO at detection
    sig_with_book = sum(1 for r in signals if r.get("book_snapshot", {}) and any(v for v in (r.get("book_snapshot") or {}).values()))
    coverage_pct = round(sig_with_book / len(signals) * 100, 1) if signals else 0.0
    state = load_state(cfg.state_path)
    positions = state.get("positions", {}) if isinstance(state.get("positions"), dict) else {}
    names = wallet_name_map()
    per: dict[str, dict[str, Any]] = {}
    for row in rows:
        wallet = str(row.get("wallet") or "").lower()
        b = per.setdefault(wallet, {"name": names.get(wallet, wallet), "signals": 0, "accepts": 0, "pnl": 0.0})
        if row.get("type") == "signal":
            b["signals"] += 1
        if row.get("type") == "entry":
            b["accepts"] += 1
        if (
            row.get("type") in {"exit", "resolution", "void_correction"}
            and str(row.get("position_id") or "") not in quarantined
        ):
            b["pnl"] += num(row.get("pnl"), 0)
    latencies = [num(r.get("detection_latency_s"), 0) for r in signals if r.get("detection_latency_s") is not None]
    realized_including_quarantine = sum(num(r.get("pnl"), 0) for r in pnl_rows)
    quarantined_realized = sum(
        num(r.get("pnl"), 0)
        for r in pnl_rows
        if str(r.get("position_id") or "") in quarantined
    )
    realized = realized_including_quarantine - quarantined_realized
    open_notional = sum(num(pos.get("cost_usd"), 0) for pos in positions.values() if isinstance(pos, dict))
    unrealized = 0.0
    account_value = realized + unrealized + open_notional
    sorted_lat = sorted(latencies)
    n = len(sorted_lat)
    latency_p50 = sorted_lat[n // 2] if n else 0.0
    latency_p90 = sorted_lat[int(n * 0.9)] if n else 0.0
    # Bucket accepts by latency
    entries_by_latency = {"<120s": 0, "120-300s": 0, ">300s": 0}
    for r in entries:
        lat = num(r.get("detection_latency_s"), -1)
        if lat < 0:
            continue
        if lat < 120:
            entries_by_latency["<120s"] += 1
        elif lat <= 300:
            entries_by_latency["120-300s"] += 1
        else:
            entries_by_latency[">300s"] += 1
    # Realized PnL today (closes today) vs all-time
    realized_today = sum(
        num(r.get("pnl"), 0)
        for r in pnl_rows
        if (parse_ts(r.get("ts")) or today) >= today
        and str(r.get("position_id") or "") not in quarantined
    )
    last_ledger_ts = next(
        (row.get("ts") for row in reversed(rows) if row.get("ts")),
        None,
    )
    return {
        "positions_open": len(positions),
        "signals_today": len(signals),
        "accepts_today": len(entries),
        "accepts_by_latency": entries_by_latency,
        "signals_by_source": signals_by_source,
        "accepts_by_source": accepts_by_source,
        "onchain_primary_enabled": cfg.onchain_primary_enabled,
        "rejects_today": len(rejects),
        "rejects_by_reason": rejects_by_reason,
        "realized_pnl": round(realized, 4),
        "realized_pnl_including_quarantine": round(realized_including_quarantine, 4),
        "quarantined_realized_pnl": round(quarantined_realized, 4),
        "realized_pnl_today": round(realized_today, 4),
        "unrealized_pnl": round(unrealized, 4),
        "open_notional": round(open_notional, 4),
        "account_value": round(account_value, 4),
        "last_ledger_ts": last_ledger_ts,
        "avg_detection_latency_s": round(sum(latencies) / len(latencies), 3) if latencies else 0.0,
        "detection_latency_p50": round(latency_p50, 3),
        "detection_latency_p90": round(latency_p90, 3),
        "poll_interval_s": cfg.poll_interval_seconds,
        "signal_coverage_pct": coverage_pct,
        "per_wallet": sorted(per.values(), key=lambda x: x["signals"], reverse=True),
    }


def render_resolution_webhook(row: dict[str, Any], status: dict[str, Any]) -> str | None:
    """Webhook formatter for resolution exits — reuses the entry/exit card shape."""
    return render_trade_webhook(row, status)


def resolution_exit_price(side: str | None, outcome_status: str | object) -> float | None:
    """Translate (side, status) into an exit price for a long-only paper holder.

    Sources of (side, status):
      - Gamma paths: side="YES"|"NO"|<other>, status="YES"|"NO"|...
      - On-chain path: side="PRIMARY"|"SECONDARY", status="PRIMARY"|"SECONDARY"

    Rules:
      - For both mapping styles, the principle is: did the index our token
        represents win? If yes -> exit at 1.0 (pays out $1 per share), else 0.0.
      - The follower can hold either PRIMARY or SECONDARY because it copies the
        token traded by the leader.
    """
    status_upper = str(outcome_status or "").strip().upper()
    if status_upper in {"", "UNKNOWN", "NONE"}:
        return None
    side_upper = str(side or "").strip().upper()

    # PRIMARY/SECONDARY (on-chain) — our PRIMARY long wins when status=="PRIMARY"
    if side_upper == "PRIMARY":
        return 1.0 if status_upper == "PRIMARY" else 0.0
    if side_upper == "SECONDARY":
        return 1.0 if status_upper == "SECONDARY" else 0.0

    # YES/NO (legacy Gamma)
    if side_upper == "YES":
        if status_upper in {"YES", "TRUE", "1", "1.0"}:
            return 1.0
        if status_upper in {"NO", "FALSE", "0", "0.0"}:
            return 0.0
        return None
    if side_upper == "NO":
        if status_upper in {"NO", "FALSE", "0", "0.0"}:
            return 1.0
        if status_upper in {"YES", "TRUE", "1", "1.0"}:
            return 0.0
    return None


def check_positions_for_resolution(
    state: dict[str, Any],
    config: BotConfig | None = None,
    token_map: TokenMap | None = None,
    rpc: RpcClient | None = None,
) -> list[dict[str, Any]]:
    """For each open position, attempt resolution lookup via on-chain settlement.

    Each action is one of:
      {"action": "resolve", "pos_id": ..., "exit_price": 1.0|0.0, "side": "PRIMARY"|"SECONDARY"|...}
      {"action": "skip",    "pos_id": ..., "reason": "not_resolved"|"no_condition_id"|...}
    """
    from .config import CONFIG as _default_cfg
    effective_cfg: BotConfig = config if isinstance(config, BotConfig) else _default_cfg  # type: ignore[assignment]
    # paper_dir for TokenMap — fall back to default cfg's runs/paper
    paper_dir = Path(getattr(effective_cfg, 'paper_dir', None) or (_default_cfg.runs_dir / "paper"))
    token_map = token_map or TokenMap.load(paper_dir)
    positions = state.get("positions", {}) if isinstance(state.get("positions"), dict) else {}
    actions: list[dict[str, Any]] = []
    rpc = rpc or RpcClient()
    for pos_id, pos in positions.items():
        if not isinstance(pos, dict):
            continue
        token = str(pos.get("token") or "")
        if not token:
            actions.append({"action": "skip", "pos_id": pos_id, "reason": "no_token"})
            continue
        try:
            info = _onchain_resolved_outcome_for_token(token, token_map=token_map, rpc=rpc, config=effective_cfg)
        except Exception:
            actions.append({"action": "skip", "pos_id": pos_id, "reason": "rpc_error"})
            continue
        if info is None:
            actions.append({"action": "skip", "pos_id": pos_id, "reason": "no_condition_id"})
            continue
        if not info.get("resolved"):
            actions.append({"action": "skip", "pos_id": pos_id, "reason": "not_resolved"})
            continue
        raw_value = info.get("raw")
        raw: dict[str, Any] = raw_value if isinstance(raw_value, dict) else {}
        denom = num(info.get("denom", raw.get("denom")), 0.0)
        n0 = num(info.get("n0", raw.get("n0")), 0.0)
        n1 = num(info.get("n1", raw.get("n1")), 0.0)
        side = str(info.get("side") or "").upper()
        if denom > 0 and side in {"PRIMARY", "SECONDARY"}:
            numerator = n0 if side == "PRIMARY" else n1
            exit_price = numerator / denom
        else:
            exit_price = resolution_exit_price(info.get("side"), info.get("resolution_status"))
        if exit_price is None:
            actions.append({"action": "skip", "pos_id": pos_id, "reason": "unmappable_outcome", "info": info})
            continue
        actions.append({
            "action": "resolve",
            "pos_id": pos_id,
            "exit_price": exit_price,
            "question": info.get("question"),
            "side": info.get("side"),
            "market_id": info.get("market_id"),
            "denom": denom,
            "n0": n0,
            "n1": n1,
        })
    return actions


def apply_resolution(state: dict[str, Any], action: dict[str, Any]) -> dict[str, Any] | None:
    """Pop the position from state and build a ledger exit/resolution row.

    Returns the row dict (caller persists it). Returns None if position was already gone.
    """
    pos_id = action.get("pos_id")
    if not pos_id:
        return None
    pos = state.get("positions", {}).pop(pos_id, None)
    if not pos:
        return None
    entry_price = num(pos.get("entry_price"), 0.0)
    shares = num(pos.get("shares"), 0.0)
    cost_usd = num(pos.get("cost_usd"), 0.0)
    exit_price = float(action.get("exit_price") or 0.0)
    proceeds = shares * exit_price
    pnl = proceeds - cost_usd
    return {
        "ts": iso_now(),
        "type": "resolution",
        "wallet": pos.get("wallet"),
        "market": action.get("question") or "resolved",
        "token": pos.get("token"),
        "side": "BUY",  # paper follower is long-only
        "detection_latency_s": None,
        "wallet_fill_price": entry_price,
        "sim_fill_price": exit_price,
        "sim_size": shares,
        "reject_reason": None,
        "position_id": pos_id,
        "pnl": pnl,
        "book_snapshot": None,
        "trade_id": None,
        "resolution_side": action.get("side"),
        "resolution_market_id": action.get("market_id"),
        "quarantined_low_price": bool(pos.get("quarantined_low_price")),
    }


def run_resolution_cycle(state: dict[str, Any], cfg: PaperConfig, config: BotConfig | None = None) -> dict[str, Any]:
    """Check all open positions for resolution; persist exit rows and fire webhook.

    Returns a summary dict with resolution-side stats for the API surface.
    """
    from .config import CONFIG as _default_cfg
    cfg_bot: BotConfig = config if isinstance(config, BotConfig) else _default_cfg  # type: ignore[assignment]
    actions = check_positions_for_resolution(state, config=cfg_bot)
    summary: dict[str, Any] = {
        "checked": len(actions),
        "resolved": 0,
        "skipped": 0,
        "exit_rows": [],
    }
    webhook_url = os.getenv("POLYMARKET_PAPER_WEBHOOK_URL") or os.getenv("DISCORD_WEBHOOK_URL")
    for action in actions:
        if action.get("action") == "resolve":
            row = apply_resolution(state, action)
            if row is None:
                summary["skipped"] += 1
                continue
            append_jsonl_fsync(cfg.ledger_path, row)
            summary["exit_rows"].append(row)
            summary["resolved"] += 1
            LOG.info("resolution exit pos=%s exit=%.4f pnl=%.2f question=%s", row["position_id"], row["sim_fill_price"], row["pnl"], row["market"])
        else:
            summary["skipped"] += 1
    save_state(cfg.state_path, state)
    if summary["exit_rows"]:
        status = paper_status(cfg)
        for row in summary["exit_rows"]:
            message = render_trade_webhook(row, status)
            if message:
                if webhook_url:
                    post_discord_webhook(webhook_url, message)
                send_telegram(message)
    summary["last_checked_at"] = iso_now()
    return summary


def render_trade_webhook(row: dict[str, Any], status: dict[str, Any]) -> str | None:
    kind = str(row.get("type") or "")
    if kind not in {"entry", "exit", "resolution"}:
        return None
    emoji = "🟢" if kind == "entry" else "🔴"
    label = "PAPER BUY" if kind == "entry" else "PAPER SELL" if kind == "exit" else "MARKET RESOLVED"
    wallet = str(row.get("wallet") or "unknown")
    market = str(row.get("market") or "unknown")
    token = str(row.get("token") or "")
    price = row.get("sim_fill_price")
    size = row.get("sim_size")
    pnl = row.get("pnl")
    account_value = status.get("account_value")
    open_notional = status.get("open_notional")
    realized = status.get("realized_pnl")
    snap = row.get("book_snapshot") if isinstance(row.get("book_snapshot"), dict) else {}
    lines = [
        f"{emoji} **{label}**",
        f"Wallet: `{wallet}`",
        f"Market: `{market}`",
        f"Token: `{token[:12]}…`" if token else "Token: `unknown`",
        f"Fill: `{price}` x `{size}` | side `{row.get('side')}`",
        f"Book: bid `{snap.get('best_bid')}` ask `{snap.get('best_ask')}` spread `{snap.get('spread')}`",
        f"Paper account value: `${account_value}` | open `${open_notional}` | realized PnL `${realized}`",
    ]
    if pnl is not None:
        lines.append(f"Trade PnL: `${pnl}`")
    return "\n".join(lines)[:1900]


def post_discord_webhook(url: str, content: str) -> bool:
    payload = json.dumps({"content": content}).encode("utf-8")
    req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json", "User-Agent": "polymarket-copybot-paper/1.0"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=8) as resp:
            return 200 <= resp.status < 300
    except Exception:
        LOG.exception("paper webhook post failed")
        return False


def configure_logging() -> None:
    logging.basicConfig(level=os.getenv("POLYMARKET_PAPER_LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(name)s: %(message)s")


def main() -> None:
    configure_logging()
    PaperFollowerDaemon().run()


if __name__ == "__main__":
    main()
