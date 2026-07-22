from __future__ import annotations

import gzip
import json
import os
import subprocess
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Iterator

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware

from .archive_config import ArchiveConfig
from .paper_follower import paper_status

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
                for line in gz.read().decode("utf-8", "ignore").splitlines():
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

    def refresh(self, force: bool = False) -> None:
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
        # Keep enough rolling context for today + 7d wallets + max 14d gaps.
        start = utc_now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=14)
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
        ]

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
PAPER_CACHE_TTL = 5.0


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
    if now_ts - _PAPER_CACHE["ts"] > PAPER_CACHE_TTL:
        _PAPER_CACHE["data"] = paper_status()
        _PAPER_CACHE["ts"] = now_ts
    return _PAPER_CACHE["data"]


def main() -> None:
    import uvicorn

    host = os.getenv("POLYMARKET_STATUS_HOST", "127.0.0.1")
    port = int(os.getenv("POLYMARKET_STATUS_PORT", "8710"))
    uvicorn.run("polymarket_bot.status_api:app", host=host, port=port, log_level="info")


if __name__ == "__main__":
    main()
