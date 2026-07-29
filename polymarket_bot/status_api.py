from __future__ import annotations

import gzip
import json
import os
import subprocess
import threading
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Iterator

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware

from .archive_config import ArchiveConfig
from .clob import best_bid_ask
from .paper_follower import PaperConfig, paper_status, load_state

app = FastAPI(title="Polymarket Copybot Status API", version="0.1.0", docs_url=None, redoc_url=None, openapi_url=None)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)
CONFIG = ArchiveConfig.load()
ARCHIVE_DIR = CONFIG.archive_dir
ROOT = Path(__file__).resolve().parents[1]
DASHBOARD_FILE = ROOT / "dashboard" / "copybot_dash.html"
SERVICE_NAME = "polymarket-copybot-book-archive.service"
CACHE_TTL_SECONDS = 5.0


def utc_now() -> datetime:
    return datetime.now(UTC)


def iso_now() -> str:
    return utc_now().isoformat(timespec="seconds")


def parse_ts(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, (int, float)):
        # Polymarket Data API trade timestamps are usually epoch seconds.
        try:
            return datetime.fromtimestamp(float(value), UTC)
        except Exception:
            return None
    if not isinstance(value, str):
        return None
    text = value.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(text)
        return dt if dt.tzinfo else dt.replace(tzinfo=UTC)
    except Exception:
        return None


def day_bounds(days_ago: int = 0) -> tuple[datetime, datetime]:
    start = utc_now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=days_ago)
    return start, start + timedelta(days=1)


def day_key(dt: datetime) -> str:
    return dt.astimezone(UTC).strftime("%Y-%m-%d")


def duration_s(start_ts: Any, end_ts: Any) -> float:
    start = parse_ts(start_ts)
    end = parse_ts(end_ts)
    if not start or not end:
        return 0.0
    return max(0.0, (end - start).total_seconds())


def service_active() -> bool:
    try:
        proc = subprocess.run(["systemctl", "is-active", "--quiet", SERVICE_NAME], timeout=1.5)
        return proc.returncode == 0
    except Exception:
        return False


def read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text())
    except Exception:
        return None


def configured_wallets() -> list[dict[str, str]]:
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

    for wallet in CONFIG.tracked_wallets:
        add(wallet)

    scores_path = CONFIG.archive_dir.parent / "wallet_scores_latest.json"
    rows = read_json(scores_path)
    if isinstance(rows, list):
        for row in rows:
            if not isinstance(row, dict):
                continue
            add(row.get("wallet"), row.get("user_name") or row.get("name") or row.get("pseudonym"))
            if len(wallets) >= CONFIG.tracked_wallet_limit_from_scores:
                break
    return wallets


def iter_gzip_jsonl(path: Path, offset: int = 0) -> Iterator[dict[str, Any]]:
    if not path.exists():
        return
    size = path.stat().st_size
    if offset > size:
        offset = 0
    try:
        with path.open("rb") as raw:
            raw.seek(offset)
            with gzip.GzipFile(fileobj=raw, mode="rb") as gz:
                for raw_line in gz:
                    line = raw_line.decode("utf-8", "ignore")
                    if not line.strip():
                        continue
                    try:
                        row = json.loads(line)
                    except Exception:
                        continue
                    if isinstance(row, dict):
                        yield row
        return
    except Exception:
        # A crash can leave a partial final gzip member. Re-read from zero and keep
        # all valid prior members; gzip will still expose complete earlier members.
        try:
            with gzip.open(path, "rt") as gz:
                for line in gz:
                    try:
                        row = json.loads(line)
                    except Exception:
                        continue
                    if isinstance(row, dict):
                        yield row
        except Exception:
            return


def jsonl_paths(prefix: str, start: datetime, end: datetime) -> list[Path]:
    dates = {(start + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(max(1, (end.date() - start.date()).days + 1))}
    out: list[Path] = []
    for date in dates:
        out.extend(ARCHIVE_DIR.glob(f"{prefix}_{date}*.jsonl.gz"))
    return sorted(set(out))


@dataclass
class RollingState:
    last_refresh: float = 0.0
    offsets: dict[str, int] = field(default_factory=dict)
    # Don't store full book rows — they accumulate to millions and OOM the service.
    # Track only what /api/status and /api/gaps actually surface.
    gap_rows: list[dict[str, Any]] = field(default_factory=list)  # sparse, all gap rows in 14d
    book_count_hour: int = 0  # count of book rows in current hour
    current_hour_start: datetime = field(
        default_factory=lambda: utc_now().replace(minute=0, second=0, microsecond=0)
    )
    last_ws_ts: datetime | None = None
    # Shadow rows are smaller; cap to 7d (used for fills_today, wallets 7d, tokens_7d)
    shadow_rows: list[dict[str, Any]] = field(default_factory=list)
    heartbeat: dict[str, Any] = field(default_factory=dict)
    _refresh_lock: threading.RLock = field(default_factory=threading.RLock, repr=False)

    def refresh(self, force: bool = False) -> None:
        # FastAPI runs sync endpoints in a threadpool; ensure only one archive
        # scan can allocate/merge rows at a time.
        with self._refresh_lock:
            self._refresh_unlocked(force)

    def _refresh_unlocked(self, force: bool = False) -> None:
        now = time.time()
        if not force and now - self.last_refresh < CACHE_TTL_SECONDS:
            return
        self.last_refresh = now
        hb = read_json(ARCHIVE_DIR / "heartbeat_latest.json")
        if isinstance(hb, dict):
            self.heartbeat = hb
        # Reset hour counter when hour rolls over
        new_hour = utc_now().replace(minute=0, second=0, microsecond=0)
        if new_hour != self.current_hour_start:
            self.current_hour_start = new_hour
            self.book_count_hour = 0
        # Cold-start only the current UTC day. Scanning the full multi-gigabyte
        # retention set inside a request made /api/status an OOM/DoS surface.
        # The daemon accumulates bounded history from here.
        start = utc_now().replace(hour=0, minute=0, second=0, microsecond=0)
        end = utc_now() + timedelta(days=1)
        for path in jsonl_paths("book", start, end):
            old_offset = self.offsets.get(str(path), 0)
            for row in iter_gzip_jsonl(path, old_offset):
                rtype = row.get("type")
                if rtype == "book":
                    ts = parse_ts(row.get("ts"))
                    if row.get("source") == "websocket" and ts and (
                        self.last_ws_ts is None or ts > self.last_ws_ts
                    ):
                        self.last_ws_ts = ts
                    if ts and ts >= self.current_hour_start:
                        self.book_count_hour += 1
                elif rtype == "gap":
                    self.gap_rows.append(row)
            if path.exists():
                self.offsets[str(path)] = path.stat().st_size
        for path in jsonl_paths("shadow", start, end):
            for row in iter_gzip_jsonl(path, self.offsets.get(str(path), 0)):
                self.shadow_rows.append(row)
                if len(self.shadow_rows) > 30_000:
                    del self.shadow_rows[:10_000]
            if path.exists():
                self.offsets[str(path)] = path.stat().st_size
        cutoff_14d = utc_now() - timedelta(days=14)
        cutoff_7d = utc_now() - timedelta(days=7)
        self.gap_rows = [
            r for r in self.gap_rows
            if (parse_ts(r.get("end_ts") or r.get("ts")) or utc_now()) >= cutoff_14d
        ]
        self.shadow_rows = [
            r for r in self.shadow_rows
            if (parse_ts(r.get("ts") or r.get("fill_timestamp")) or utc_now()) >= cutoff_7d
        ][-30_000:]
        self.gap_rows = self.gap_rows[-10_000:]

    def status(self) -> dict[str, Any]:
        self.refresh()
        now = utc_now()
        today_start, today_end = day_bounds(0)
        hb_stats = self.heartbeat.get("stats") if isinstance(self.heartbeat.get("stats"), dict) else {}
        disk = self.heartbeat.get("disk_estimate") if isinstance(self.heartbeat.get("disk_estimate"), dict) else {}
        today_gaps = self._gaps_between(today_start, now)
        gap_seconds = sum(g["duration_s"] for g in today_gaps)
        elapsed_today = max(1.0, (now - today_start).total_seconds())
        last_ws = self.last_ws_ts
        last_ws_age = (now - last_ws).total_seconds() if last_ws else 999999999.0
        fills_today = self._shadow_rows("fill", today_start, today_end)
        followups_done = self._shadow_rows("followup_book", today_start, today_end)
        missed = self._shadow_rows("followup_missed", today_start, today_end)
        last_fill = max((parse_ts(r.get("ts")) for r in fills_today), default=None)
        return {
            "generated_at": iso_now(),
            "archiver": {
                "service_active": service_active(),
                "ws_connected": bool(last_ws and last_ws_age <= 180 and service_active()),
                "last_ws_message_age_s": float(round(last_ws_age, 3)),
                "markets": int(hb_stats.get("markets_covered") or 0),
                "tokens": int(hb_stats.get("tokens_covered") or 0),
                "book_rows_this_hour": int(self.book_count_hour),
                "mb_per_day": float(disk.get("compressed_mb_per_day") or 0.0),
                "retention_days": int(disk.get("retention_days") or CONFIG.retention_days),
                "retention_gb": float(disk.get("retention_gb") or 0.0),
                "wallet_driven_tokens": int(self.heartbeat.get("wallet_driven_tokens") or 0),
                "wallet_token_coverage_pct": self._compute_wallet_coverage(),
            },
            "gaps_today": today_gaps,
            "coverage_pct_today": float(round(max(0.0, min(100.0, 100.0 * (elapsed_today - gap_seconds) / elapsed_today)), 6)),
            "shadow": {
                "fills_today": int(len(fills_today)),
                "followups_pending": int(self.heartbeat.get("pending_followups") or len(read_json(CONFIG.followup_queue_path) or [])),
                "followups_completed_today": int(len(followups_done)),
                "followups_missed_today": int(len(missed)),
                "last_fill_ts": last_fill.isoformat(timespec="seconds") if last_fill else None,
            },
            "wallets": self._wallets(),
        }

    def gaps(self, days: int) -> list[dict[str, Any]]:
        self.refresh()
        out: list[dict[str, Any]] = []
        now = utc_now()
        for i in range(days):
            start, end = day_bounds(i)
            actual_end = min(end, now)
            gaps = self._gaps_between(start, actual_end)
            gap_seconds = sum(g["duration_s"] for g in gaps)
            denom = max(1.0, (actual_end - start).total_seconds()) if actual_end > start else 86400.0
            out.append({
                "date": day_key(start),
                "coverage_pct": float(round(max(0.0, min(100.0, 100.0 * (denom - gap_seconds) / denom)), 6)),
                "gaps": gaps,
            })
        return list(reversed(out))

    def _last_ws_ts(self) -> datetime | None:
        return self.last_ws_ts

    def _gaps_between(self, start: datetime, end: datetime) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for r in self.gap_rows:
            ts = parse_ts(r.get("end_ts") or r.get("ts"))
            if not ts or ts < start or ts >= end:
                continue
            rows.append({
                "start_ts": str(r.get("start_ts") or ""),
                "end_ts": str(r.get("end_ts") or ""),
                "duration_s": float(round(duration_s(r.get("start_ts"), r.get("end_ts")), 3)),
                "reason": str(r.get("reason") or "unknown"),
            })
        return rows

    def _shadow_rows(self, kind: str, start: datetime, end: datetime) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for r in self.shadow_rows:
            if (r.get("type") or r.get("kind")) != kind:
                continue
            ts = parse_ts(r.get("ts") or r.get("fill_timestamp"))
            if ts and start <= ts < end:
                out.append(r)
        return out

    def _tokens_from_shadow_fills(self, days: int = 7) -> set[str]:
        """Return unique token IDs seen in shadow fills over the last N days."""
        cutoff = utc_now() - timedelta(days=days)
        tokens: set[str] = set()
        for r in self.shadow_rows:
            rtype = r.get("type") or r.get("kind")
            if rtype not in ("fill",):
                continue
            ts = parse_ts(r.get("ts") or r.get("fill_timestamp"))
            if ts and ts < cutoff:
                continue
            # Token from trade.asset for new format, or token from fill data
            trade = r.get("trade") if isinstance(r.get("trade"), dict) else {}
            tok = trade.get("asset") or r.get("token_id") or ""
            if tok:
                tokens.add(tok)
            # Also try other known token fields
            for key in ("asset", "assetId", "token_id", "tokenId", "clobTokenId"):
                val = trade.get(key) or r.get(key)
                if val:
                    tokens.add(str(val))
        return tokens

    def _compute_wallet_coverage(self) -> float:
        """wallet_token_coverage_pct = wallet-driven subscribed / tokens seen in last 7d of wallet fills."""
        wd_tokens = int(self.heartbeat.get("wallet_driven_tokens") or 0)
        tokens_7d = self._tokens_from_shadow_fills(days=7)
        total = len(tokens_7d)
        if total == 0:
            return 100.0 if wd_tokens > 0 else 0.0
        return round(min(100.0, wd_tokens / total * 100), 1)

    def _wallets(self) -> list[dict[str, Any]]:
        now = utc_now()
        today_start, today_end = day_bounds(0)
        week_start = now - timedelta(days=7)
        by_wallet: dict[str, dict[str, Any]] = {}
        for configured in configured_wallets():
            wallet = configured["wallet"]
            by_wallet[wallet] = {"name": configured["name"], "fills_today": 0, "fills_7d": 0, "last_fill_ts": None, "markets": set()}
        for r in self.shadow_rows:
            if (r.get("type") or r.get("kind")) != "fill":
                continue
            ts = parse_ts(r.get("ts") or r.get("fill_timestamp"))
            if not ts or ts < week_start:
                continue
            trade = r.get("trade") if isinstance(r.get("trade"), dict) else {}
            wallet = str(r.get("wallet") or trade.get("proxyWallet") or "unknown").lower()
            bucket = by_wallet.setdefault(wallet, {"name": trade.get("name") or trade.get("pseudonym") or wallet, "fills_today": 0, "fills_7d": 0, "last_fill_ts": None, "markets": set()})
            if bucket["name"] == wallet and (trade.get("name") or trade.get("pseudonym")):
                bucket["name"] = trade.get("name") or trade.get("pseudonym")
            bucket["fills_7d"] += 1
            if today_start <= ts < today_end:
                bucket["fills_today"] += 1
            if bucket["last_fill_ts"] is None or ts > bucket["last_fill_ts"]:
                bucket["last_fill_ts"] = ts
            market = trade.get("conditionId") or trade.get("slug") or trade.get("eventSlug")
            if market:
                bucket["markets"].add(str(market))
        rows: list[dict[str, Any]] = []
        for bucket in by_wallet.values():
            last = bucket["last_fill_ts"]
            rows.append({
                "name": str(bucket["name"]),
                "fills_today": int(bucket["fills_today"]),
                "fills_7d": int(bucket["fills_7d"]),
                "last_fill_ts": last.isoformat(timespec="seconds") if last else None,
                "markets_touched_7d": int(len(bucket["markets"])),
            })
        rows.sort(key=lambda x: (x["fills_7d"], x["fills_today"], x["last_fill_ts"] or "", x["name"]), reverse=True)
        return rows


STATE = RollingState()

# Cache paper_status() — it loads the full ledger into memory. /api/paper is polled
# every ~60s by the discord monitor, so refresh at most every PAPER_CACHE_TTL.
_PAPER_CACHE: dict[str, Any] = {"ts": 0.0, "data": {}}
_PAPER_STATS_CACHE: dict[str, Any] = {"ts": 0.0, "ledger_mtime_ns": None, "data": {}}
PAPER_STATS_CACHE_TTL = 60.0
PAPER_CACHE_TTL = 15.0
_POS_CACHE: dict[str, Any] = {"ts": 0.0, "data": {}}
POSITIONS_CACHE_TTL = 15.0
_DISK_CACHE: dict[str, Any] = {"ts": 0.0, "data": {}}
DISK_CACHE_TTL = 30.0
_PNL_CACHE: dict[str, Any] = {"ts": 0.0, "days": None, "data": {}}
_WALLET_CACHE: dict[str, Any] = {"ts": 0.0, "data": {}}
ANALYTICS_CACHE_TTL = 60.0
_CACHE_LOCK = threading.RLock()


def dashboard_response() -> FileResponse:
    if not DASHBOARD_FILE.exists():
        raise HTTPException(status_code=404, detail="dashboard/copybot_dash.html not installed")
    return FileResponse(DASHBOARD_FILE, media_type="text/html")


@app.get("/", include_in_schema=False)
def get_root() -> FileResponse:
    return dashboard_response()


@app.get("/dashboard", include_in_schema=False)
def get_dashboard() -> FileResponse:
    return dashboard_response()


@app.get("/api/status")
def get_status() -> dict[str, Any]:
    return STATE.status()


@app.get("/api/gaps")
def get_gaps(days: int = Query(7, ge=1, le=14)) -> list[dict[str, Any]]:
    return STATE.gaps(days)


@app.get("/api/paper")
def get_paper() -> dict[str, Any]:
    now_ts = time.time()
    with _CACHE_LOCK:
        if now_ts - _PAPER_CACHE["ts"] > PAPER_CACHE_TTL:
            cfg = PaperConfig.load()
            try:
                ledger_mtime_ns = cfg.ledger_path.stat().st_mtime_ns
            except OSError:
                ledger_mtime_ns = None
            if (
                not _PAPER_STATS_CACHE["data"]
                or (
                    ledger_mtime_ns != _PAPER_STATS_CACHE["ledger_mtime_ns"]
                    and now_ts - _PAPER_STATS_CACHE["ts"] >= PAPER_STATS_CACHE_TTL
                )
            ):
                _PAPER_STATS_CACHE["data"] = paper_status(cfg)
                _PAPER_STATS_CACHE["ledger_mtime_ns"] = ledger_mtime_ns
                _PAPER_STATS_CACHE["ts"] = now_ts
            payload = dict(_PAPER_STATS_CACHE["data"])
            marks = get_positions()
            unrealized = float(marks.get("total_unrealized") or 0)
            open_notional = float(marks.get("total_cost") or 0)
            payload["unrealized_pnl"] = round(unrealized, 4)
            payload["open_notional"] = round(open_notional, 4)
            payload["account_value"] = round(
                float(payload.get("realized_pnl") or 0) + open_notional + unrealized, 4
            )
            payload["marks_generated_at"] = marks.get("generated_at")
            payload["positions_snapshot"] = marks
            _PAPER_CACHE["data"] = payload
            _PAPER_CACHE["ts"] = now_ts
        return _PAPER_CACHE["data"]


@app.get("/api/positions")
def get_positions() -> dict[str, Any]:
    """List all open paper positions with current mark-to-market PnL.

    For each open position, fetches a fresh top-of-book from CLOB REST to
    compute unrealized PnL. Cached for 5s to avoid hammering the API.
    """
    now_ts = time.time()
    if now_ts - _POS_CACHE["ts"] < POSITIONS_CACHE_TTL:
        return _POS_CACHE["data"]

    cfg = PaperConfig.load()
    state = load_state(cfg.state_path)
    raw_positions = state.get("positions", {}) if isinstance(state.get("positions"), dict) else {}
    previous_positions = {
        str(row.get("position_id")): row
        for row in (_POS_CACHE.get("data", {}).get("positions", []) or [])
        if isinstance(row, dict)
    }
    out_positions: list[dict[str, Any]] = []
    total_unrealized = 0.0
    total_cost = 0.0
    stale_marks = 0
    for pos_id, pos in raw_positions.items():
        if not isinstance(pos, dict):
            continue
        token = pos.get("token", "")
        wallet = pos.get("wallet", "")
        cost_usd = float(pos.get("cost_usd") or 0)
        shares = float(pos.get("shares") or 0)
        entry_price = float(pos.get("entry_price") or 0)
        opened_at = pos.get("opened_at", "")
        # Live mark via REST — use bid for long (mark to liquidation value)
        mark_status = "live"
        try:
            book = best_bid_ask(token)
            # A long can be liquidated only at the bid; the ask is not a valid mark.
            bid = book.get("best_bid")
            if bid and float(bid) > 0:
                cur_price = float(bid)
            else:
                raise ValueError("no executable bid")
        except Exception:
            previous = previous_positions.get(str(pos_id), {})
            previous_price = float(previous.get("current_price") or 0)
            cur_price = previous_price if previous_price > 0 else entry_price
            mark_status = "stale" if previous_price > 0 else "entry_fallback"
            stale_marks += 1
        market_value = shares * cur_price
        unrealized = market_value - cost_usd
        total_unrealized += unrealized
        total_cost += cost_usd
        out_positions.append({
            "position_id": pos_id,
            "wallet": wallet,
            "token": token,
            "cost_usd": round(cost_usd, 4),
            "shares": round(shares, 4),
            "entry_price": round(entry_price, 4),
            "current_price": round(cur_price, 4),
            "mark_status": mark_status,
            "market_value": round(market_value, 4),
            "unrealized_pnl": round(unrealized, 4),
            "unrealized_pct": round((unrealized / cost_usd * 100) if cost_usd else 0, 2),
            "opened_at": opened_at,
        })
    out_positions.sort(key=lambda p: p["opened_at"], reverse=True)
    payload = {
        "generated_at": iso_now(),
        "count": len(out_positions),
        "total_cost": round(total_cost, 4),
        "total_unrealized": round(total_unrealized, 4),
        "stale_marks": stale_marks,
        "positions": out_positions,
    }
    _POS_CACHE["data"] = payload
    _POS_CACHE["ts"] = now_ts
    return payload


@app.get("/api/disk")
def get_disk() -> dict[str, Any]:
    """runs/ directory size, file counts by age, oldest/newest file.

    Cheap to compute. Cached 30s.
    """
    now_ts = time.time()
    if now_ts - _DISK_CACHE["ts"] < DISK_CACHE_TTL:
        return _DISK_CACHE["data"]
    runs_dir = ROOT / "runs"
    total_bytes = 0
    file_count = 0
    by_age: dict[str, int] = {"<1d": 0, "1-7d": 0, "7-30d": 0, ">30d": 0}
    by_age_bytes: dict[str, int] = {"<1d": 0, "1-7d": 0, "7-30d": 0, ">30d": 0}
    oldest: dict[str, Any] = {"path": "", "mtime": 0, "size": 0}
    newest: dict[str, Any] = {"path": "", "mtime": 0, "size": 0}
    for path in runs_dir.rglob("*"):
        if not path.is_file():
            continue
        try:
            stat = path.stat()
        except (FileNotFoundError, OSError):
            continue
        file_count += 1
        size = stat.st_size
        total_bytes += size
        age_days = (now_ts - stat.st_mtime) / 86400.0
        if age_days < 1:
            bucket = "<1d"
        elif age_days < 7:
            bucket = "1-7d"
        elif age_days < 30:
            bucket = "7-30d"
        else:
            bucket = ">30d"
        by_age[bucket] += 1
        by_age_bytes[bucket] += size
        if not oldest["path"] or stat.st_mtime < oldest["mtime"]:
            oldest = {"path": str(path), "mtime": stat.st_mtime, "size": size}
        if stat.st_mtime > newest["mtime"]:
            newest = {"path": str(path), "mtime": stat.st_mtime, "size": size}
    payload = {
        "generated_at": iso_now(),
        "runs_dir": str(runs_dir),
        "total_bytes": total_bytes,
        "total_mb": round(total_bytes / 1e6, 2),
        "file_count": file_count,
        "by_age": {k: {"count": by_age[k], "bytes": by_age_bytes[k],
                       "mb": round(by_age_bytes[k] / 1e6, 2)} for k in by_age},
        "oldest_file": oldest,
        "newest_file": newest,
        "threshold_mb": 2000,  # cleanup hard cap default
    }
    _DISK_CACHE["data"] = payload
    _DISK_CACHE["ts"] = now_ts
    return payload


@app.get("/api/pnl/timeseries")
def get_pnl_timeseries(days: int = 30) -> dict[str, Any]:
    """Daily PnL time series for sparkline charts.

    Cached 60s. Returns:
      - daily: [{date, daily_pnl, cumulative_pnl}, ...]
      - per_wallet: {wallets: [...], series: {name: {total, points: [...]}}}
    """
    now_ts = time.time()
    with _CACHE_LOCK:
        if now_ts - _PNL_CACHE["ts"] < ANALYTICS_CACHE_TTL and _PNL_CACHE["days"] == days:
            return _PNL_CACHE["data"]
    from .timeseries import compute_daily_pnl, compute_per_wallet_daily
    daily = compute_daily_pnl(days=days)
    per_wallet = compute_per_wallet_daily(days=days, top_n=5)
    payload = {
        "generated_at": iso_now(),
        "days": days,
        "daily": daily,
        "per_wallet": per_wallet,
    }
    with _CACHE_LOCK:
        _PNL_CACHE.update({"ts": now_ts, "days": days, "data": payload})
    return payload


@app.get("/api/wallets/quality")
def get_wallets_quality() -> dict[str, Any]:
    """Per-wallet quality scoring: PnL, win rate, holding period, active.

    Cached 60s. Sorted by quality_score desc.
    """
    now_ts = time.time()
    with _CACHE_LOCK:
        if now_ts - _WALLET_CACHE["ts"] < ANALYTICS_CACHE_TTL:
            return _WALLET_CACHE["data"]
    from .wallet_quality import compute_wallet_quality
    wallets = compute_wallet_quality()
    payload = {
        "generated_at": iso_now(),
        "count": len(wallets),
        "wallets": wallets,
    }
    with _CACHE_LOCK:
        _WALLET_CACHE.update({"ts": now_ts, "data": payload})
    return payload


@app.get("/api/digest")
def get_digest() -> dict[str, Any]:
    """Build the same digest the daily timer would post to Discord.

    Inlines the build so the polybot service doesn't need to import
    the optsig digest module. If the dashboard is unreachable, returns
    a minimal message from local data.

    Cached 60s.
    """
    import json as _json
    import urllib.request
    from datetime import datetime, UTC
    dashboard_url = os.environ.get("DASHBOARD_URL", "http://localhost:8730")
    try:
        req = urllib.request.Request(f"{dashboard_url}/api/bots")
        with urllib.request.urlopen(req, timeout=5) as resp:
            payload = _json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        return {"error": f"could not reach dashboard: {e}", "generated_at": iso_now()}

    # Inline digest builder (matches optsig/digest.py)
    poly = payload.get("polybot") or {}
    optsig = payload.get("optsig") or {}
    optsig_signals = payload.get("optsig_signals") or []
    errs = payload.get("errors") or {}
    ts = datetime.now(UTC).isoformat(timespec="seconds")
    lines = [f"📈 **Daily Trading Digest** — `{ts}`", "", "**🪙 Polybot (Polymarket)**"]
    if poly:
        pnl = poly.get("realized_pnl", 0)
        pnl_emoji = "🟢" if pnl >= 0 else "🔴"
        rate = (poly["accepts_today"] / poly["signals_today"] * 100) if poly.get("signals_today") else 0
        lines.append(f"  • PnL: {pnl_emoji} ${pnl:+,.0f}")
        lines.append(f"  • Account: ${poly.get('account_value', 0):,.0f}")
        lines.append(f"  • Today: {poly.get('accepts_today', 0)}/{poly.get('signals_today', 0)} signals ({rate:.1f}%)")
    else:
        lines.append("  • _unreachable_")
    lines.append("")
    lines.append("**📊 Optsig (Options)**")
    if optsig:
        o_last = optsig.get("last_heartbeat") or {}
        lines.append(f"  • Signals total: {optsig.get('signals_total', 0)}")
        lines.append(f"  • Cycles: {optsig.get('cycles_total', 0)}")
        lines.append(f"  • Last cycle: `{o_last.get('cycle_ts', '?')[:19]}`")
    if optsig_signals:
        lines.append("")
        lines.append(f"Recent signals ({len(optsig_signals)}):")
        for s in optsig_signals[:5]:
            lines.append(f"  • {s.get('ticker','')} ({s.get('producer','')}) {s.get('action','')} score={s.get('score',0):.0f}")
    msg = "\n".join(lines)[:1900]
    return {
        "generated_at": iso_now(),
        "message": msg,
        "length": len(msg),
        "dashboard_url": dashboard_url,
    }


def main() -> None:
    import uvicorn

    host = os.getenv("POLYMARKET_STATUS_HOST", "127.0.0.1")
    port = int(os.getenv("POLYMARKET_STATUS_PORT", "8710"))
    uvicorn.run("polymarket_bot.status_api:app", host=host, port=port, log_level="info")


if __name__ == "__main__":
    main()

