from __future__ import annotations

import asyncio
import gzip
import json
import logging
import os
import signal
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import websockets

from .archive_config import ArchiveConfig
from .clob import order_book
from .data import user_trades
from .gamma import active_events, flatten_markets
from .paper import write_json

WS_MARKET_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
LOG = logging.getLogger("polymarket_book_archive")


def utc_now() -> datetime:
    return datetime.now(UTC)


def iso_now() -> str:
    return utc_now().isoformat(timespec="seconds")


def _num(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except Exception:
        return None


def normalize_levels(levels: Any, *, reverse: bool = False, limit: int = 3) -> list[dict[str, float]]:
    rows: list[dict[str, float]] = []
    if not isinstance(levels, list):
        return rows
    for level in levels:
        if isinstance(level, dict):
            price = _num(level.get("price"))
            size = _num(level.get("size"))
        elif isinstance(level, (list, tuple)) and len(level) >= 2:
            price = _num(level[0])
            size = _num(level[1])
        else:
            continue
        if price is None or size is None:
            continue
        rows.append({"price": price, "size": size})
    rows.sort(key=lambda x: x["price"], reverse=reverse)
    return rows[:limit]


def bbo_from_levels(bids: list[dict[str, float]], asks: list[dict[str, float]]) -> dict[str, float | None]:
    bid = bids[0] if bids else {}
    ask = asks[0] if asks else {}
    best_bid = bid.get("price")
    best_ask = ask.get("price")
    return {
        "best_bid": best_bid,
        "best_bid_size": bid.get("size"),
        "best_ask": best_ask,
        "best_ask_size": ask.get("size"),
        "spread": (best_ask - best_bid) if best_bid is not None and best_ask is not None else None,
    }


def trade_id(trade: dict[str, Any]) -> str:
    for keys in (("transactionHash", "logIndex"), ("transaction_hash", "log_index"), ("hash", "logIndex"), ("transactionHash",), ("transaction_hash",), ("hash",), ("id",)):
        vals = [str(trade.get(k, "")) for k in keys if trade.get(k) not in (None, "")]
        if len(vals) == len(keys) and vals:
            return ":".join(vals)
    return json.dumps(trade, sort_keys=True, default=str)[:512]


def token_ids_for_market(market: dict[str, Any]) -> list[str]:
    return [str(x) for x in (market.get("clob_token_ids") or []) if x not in (None, "")]


@dataclass
class ArchiveStats:
    markets_covered: int = 0
    tokens_covered: int = 0
    book_rows_written: int = 0
    ws_messages: int = 0
    rest_snapshots: int = 0
    wallet_trades_seen: int = 0
    wallet_trades_matched: int = 0
    shadow_rows_written: int = 0
    started_at: str = ""
    last_heartbeat_at: str = ""
    daily_disk_bytes_estimate: int = 0


class BookArchiveDaemon:
    def __init__(self, config: ArchiveConfig | None = None) -> None:
        self.config = config or ArchiveConfig.load()
        self.config.archive_dir.mkdir(parents=True, exist_ok=True)
        self.book_state: dict[str, dict[str, Any]] = {}
        self.token_meta: dict[str, dict[str, Any]] = {}
        self.market_condition_ids: set[str] = set()
        self.running = True
        self.last_write_by_token: dict[str, float] = {}
        self.stats = ArchiveStats(started_at=iso_now())
        self.state = self._load_state()

    def _load_state(self) -> dict[str, Any]:
        path = self.config.state_path
        if path.exists():
            try:
                data = json.loads(path.read_text())
                data.setdefault("seen_trade_ids", [])
                data.setdefault("pending_observations", [])
                return data
            except Exception:
                LOG.exception("failed to load state; starting fresh")
        return {"seen_trade_ids": [], "pending_observations": []}

    def _save_state(self) -> None:
        seen = list(dict.fromkeys(self.state.get("seen_trade_ids", [])))[-10000:]
        pending = self.state.get("pending_observations", [])[-10000:]
        self.state = {"seen_trade_ids": seen, "pending_observations": pending}
        self.config.state_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.config.state_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.state, indent=2, sort_keys=True))
        tmp.replace(self.config.state_path)

    def daily_path(self, prefix: str, day: datetime | None = None) -> Path:
        d = (day or utc_now()).strftime("%Y-%m-%d")
        return self.config.archive_dir / f"{prefix}_{d}.jsonl.gz"

    def append_row(self, prefix: str, row: dict[str, Any]) -> int:
        path = self.daily_path(prefix)
        payload = json.dumps(row, separators=(",", ":"), sort_keys=True, default=str) + "\n"
        with gzip.open(path, "at", encoding="utf-8") as f:
            f.write(payload)
        return len(payload.encode("utf-8"))

    def discover_markets(self) -> None:
        events = active_events(limit=self.config.gamma_event_limit)
        markets = flatten_markets(events)
        tradable = [
            m for m in markets
            if m.get("enable_order_book") and m.get("active") is not False and m.get("closed") is not True and token_ids_for_market(m)
        ]
        tradable.sort(key=lambda m: (float(m.get("liquidity") or 0), float(m.get("volume_24h") or 0), float(m.get("volume") or 0)), reverse=True)
        selected = tradable[: self.config.top_n_markets]
        self.token_meta.clear()
        self.market_condition_ids.clear()
        for market in selected:
            condition = market.get("condition_id") or market.get("conditionId")
            if condition:
                self.market_condition_ids.add(str(condition).lower())
            outcomes = market.get("outcomes") or []
            for idx, token_id in enumerate(token_ids_for_market(market)):
                self.token_meta[token_id] = {
                    "token_id": token_id,
                    "market_id": market.get("market_id"),
                    "market_slug": market.get("market_slug"),
                    "event_slug": market.get("event_slug"),
                    "question": market.get("question"),
                    "outcome": str(outcomes[idx]) if idx < len(outcomes) else None,
                    "liquidity": market.get("liquidity"),
                    "volume_24h": market.get("volume_24h"),
                }
        self.stats.markets_covered = len(selected)
        self.stats.tokens_covered = len(self.token_meta)
        write_json(self.config.archive_dir / "markets_latest.json", {"ts": iso_now(), "markets": selected, "tokens": self.token_meta})
        LOG.info("discovered markets=%s tokens=%s", self.stats.markets_covered, self.stats.tokens_covered)

    def record_book(self, token_id: str, bids: Any, asks: Any, *, source: str, event_type: str | None = None, force: bool = False) -> None:
        if token_id not in self.token_meta:
            return
        now = time.time()
        if not force and now - self.last_write_by_token.get(token_id, 0) < self.config.max_write_interval_per_token_seconds:
            return
        bid_rows = normalize_levels(bids, reverse=True)
        ask_rows = normalize_levels(asks, reverse=False)
        row = {
            "ts": iso_now(),
            "source": source,
            "event_type": event_type,
            "token_id": token_id,
            "market": self.token_meta[token_id],
            "top3_bids": bid_rows,
            "top3_asks": ask_rows,
            **bbo_from_levels(bid_rows, ask_rows),
        }
        self.book_state[token_id] = row
        self.last_write_by_token[token_id] = now
        size = self.append_row("book", row)
        self.stats.book_rows_written += 1
        self.stats.daily_disk_bytes_estimate = int(self.stats.book_rows_written * size * 86400 / max(1, now - datetime.fromisoformat(self.stats.started_at).timestamp()))

    def rest_snapshot_once(self) -> None:
        for token_id in list(self.token_meta):
            try:
                book = order_book(token_id)
                self.record_book(token_id, book.get("bids") or [], book.get("asks") or [], source="rest_snapshot", event_type="snapshot", force=True)
                self.stats.rest_snapshots += 1
            except Exception:
                LOG.exception("REST snapshot failed token=%s", token_id)

    def handle_ws_message(self, raw: str) -> None:
        self.stats.ws_messages += 1
        try:
            msg = json.loads(raw)
        except Exception:
            return
        items = msg if isinstance(msg, list) else [msg]
        for item in items:
            if not isinstance(item, dict):
                continue
            token_id = str(item.get("asset_id") or item.get("assetId") or item.get("token_id") or item.get("tokenId") or "")
            event_type = item.get("event_type") or item.get("type")
            if not token_id or token_id not in self.token_meta:
                continue
            bids = item.get("bids") or item.get("buys") or item.get("buy")
            asks = item.get("asks") or item.get("sells") or item.get("sell")
            if bids is not None or asks is not None:
                prior = self.book_state.get(token_id, {})
                self.record_book(token_id, bids if bids is not None else prior.get("top3_bids", []), asks if asks is not None else prior.get("top3_asks", []), source="websocket", event_type=str(event_type) if event_type else None)

    async def ws_loop(self) -> None:
        while self.running:
            tokens = list(self.token_meta)
            if not tokens:
                await asyncio.sleep(5)
                continue
            try:
                async with websockets.connect(WS_MARKET_URL, ping_interval=20, open_timeout=20) as ws:
                    await ws.send(json.dumps({"assets_ids": tokens, "type": "market", "custom_feature_enabled": True}))
                    LOG.info("websocket subscribed tokens=%s", len(tokens))
                    while self.running:
                        raw = await asyncio.wait_for(ws.recv(), timeout=max(30, self.config.snapshot_interval_seconds * 2))
                        if isinstance(raw, bytes):
                            raw = raw.decode("utf-8", "ignore")
                        self.handle_ws_message(raw)
            except asyncio.TimeoutError:
                LOG.warning("websocket timeout; reconnecting")
            except Exception:
                LOG.exception("websocket loop failed; reconnecting")
                await asyncio.sleep(10)

    async def _sleep(self, seconds: float) -> None:
        deadline = time.time() + seconds
        while self.running and time.time() < deadline:
            await asyncio.sleep(min(1.0, max(0.0, deadline - time.time())))

    async def snapshot_loop(self) -> None:
        while self.running:
            self.rest_snapshot_once()
            self.apply_retention()
            await self._sleep(self.config.snapshot_interval_seconds)

    def _configured_wallets(self) -> list[str]:
        wallets = [w.lower() for w in self.config.tracked_wallets if w]
        scores_path = Path("runs/wallet_scores_latest.json")
        if not scores_path.is_absolute():
            scores_path = Path(__file__).resolve().parents[1] / scores_path
        if scores_path.exists() and len(wallets) < self.config.tracked_wallet_limit_from_scores:
            try:
                rows = json.loads(scores_path.read_text())
                for row in rows:
                    wallet = str(row.get("wallet") or "").lower()
                    if wallet and wallet not in wallets:
                        wallets.append(wallet)
                    if len(wallets) >= self.config.tracked_wallet_limit_from_scores:
                        break
            except Exception:
                LOG.exception("failed to load wallet_scores_latest")
        return wallets

    def _trade_matches_archive(self, trade: dict[str, Any]) -> bool:
        token = str(trade.get("asset") or trade.get("assetId") or trade.get("token_id") or trade.get("tokenId") or trade.get("clobTokenId") or "")
        if token and token in self.token_meta:
            return True
        condition = str(trade.get("conditionId") or trade.get("condition_id") or "").lower()
        return bool(condition and condition in self.market_condition_ids)

    def _trade_token(self, trade: dict[str, Any]) -> str | None:
        token = str(trade.get("asset") or trade.get("assetId") or trade.get("token_id") or trade.get("tokenId") or trade.get("clobTokenId") or "")
        return token if token in self.token_meta else None

    def _current_book_for_trade(self, trade: dict[str, Any]) -> dict[str, Any] | None:
        token = self._trade_token(trade)
        if token:
            return self.book_state.get(token)
        # If only conditionId is present, return all archived token books for that market-ish context.
        slug = trade.get("marketSlug") or trade.get("slug")
        candidates = [state for tid, state in self.book_state.items() if not slug or state.get("market", {}).get("market_slug") == slug]
        return {"tokens": candidates, "ts": iso_now()} if candidates else None

    def poll_wallets_once(self) -> None:
        seen: set[str] = set(self.state.get("seen_trade_ids", []))
        pending: list[dict[str, Any]] = self.state.get("pending_observations", [])
        for wallet in self._configured_wallets():
            try:
                trades = user_trades(wallet, self.config.trade_poll_limit)
            except Exception:
                LOG.exception("wallet trade poll failed wallet=%s", wallet)
                continue
            for trade in trades:
                self.stats.wallet_trades_seen += 1
                tid = trade_id(trade)
                if tid in seen:
                    continue
                seen.add(tid)
                matched = self._trade_matches_archive(trade)
                if matched:
                    self.stats.wallet_trades_matched += 1
                    row = {"ts": iso_now(), "kind": "fill", "wallet": wallet, "trade_id": tid, "trade": trade, "book_at_detection": self._current_book_for_trade(trade)}
                    self.append_row("shadow", row)
                    self.stats.shadow_rows_written += 1
                    now = time.time()
                    for offset in self.config.followup_offsets_seconds:
                        pending.append({"due_ts": now + offset, "offset_seconds": offset, "wallet": wallet, "trade_id": tid, "trade": trade})
        self.state["seen_trade_ids"] = list(seen)
        self.state["pending_observations"] = pending
        self._process_due_followups()
        self._save_state()

    def _process_due_followups(self) -> None:
        now = time.time()
        remaining: list[dict[str, Any]] = []
        for item in self.state.get("pending_observations", []):
            if float(item.get("due_ts") or 0) > now:
                remaining.append(item)
                continue
            trade = item.get("trade") or {}
            row = {
                "ts": iso_now(),
                "kind": "followup_book",
                "wallet": item.get("wallet"),
                "trade_id": item.get("trade_id"),
                "offset_seconds": item.get("offset_seconds"),
                "book": self._current_book_for_trade(trade),
            }
            self.append_row("shadow", row)
            self.stats.shadow_rows_written += 1
        self.state["pending_observations"] = remaining

    async def wallet_loop(self) -> None:
        while self.running:
            self.poll_wallets_once()
            await self._sleep(self.config.wallet_poll_interval_seconds)

    def apply_retention(self) -> None:
        cutoff = utc_now() - timedelta(days=self.config.retention_days)
        for path in self.config.archive_dir.glob("*.jsonl.gz"):
            try:
                if datetime.fromtimestamp(path.stat().st_mtime, UTC) < cutoff:
                    path.unlink()
                    LOG.info("retention removed %s", path)
            except Exception:
                LOG.exception("retention failed path=%s", path)

    async def heartbeat_loop(self) -> None:
        while self.running:
            self.stats.last_heartbeat_at = iso_now()
            report = {
                "ts": self.stats.last_heartbeat_at,
                "config": self.config.to_jsonable(),
                "stats": self.stats.__dict__,
                "wallets_tracked": len(self._configured_wallets()),
                "pending_followups": len(self.state.get("pending_observations", [])),
                "archive_dir": str(self.config.archive_dir),
            }
            write_json(self.config.archive_dir / "heartbeat_latest.json", report)
            LOG.info("heartbeat markets=%s tokens=%s book_rows=%s ws=%s wallet_seen=%s matched=%s shadow_rows=%s est_daily_mb=%.2f",
                     self.stats.markets_covered, self.stats.tokens_covered, self.stats.book_rows_written, self.stats.ws_messages,
                     self.stats.wallet_trades_seen, self.stats.wallet_trades_matched, self.stats.shadow_rows_written,
                     self.stats.daily_disk_bytes_estimate / 1_000_000)
            await self._sleep(self.config.heartbeat_interval_seconds)

    async def market_refresh_loop(self) -> None:
        while self.running:
            try:
                self.discover_markets()
            except Exception:
                LOG.exception("market discovery failed")
            await self._sleep(15 * 60)

    async def run(self) -> None:
        self.discover_markets()
        self.rest_snapshot_once()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, self.stop)
            except NotImplementedError:
                pass
        await asyncio.gather(self.ws_loop(), self.snapshot_loop(), self.wallet_loop(), self.heartbeat_loop(), self.market_refresh_loop())

    def stop(self) -> None:
        LOG.info("stopping")
        self.running = False


def configure_logging() -> None:
    level = os.getenv("POLYMARKET_ARCHIVE_LOG_LEVEL", "INFO").upper()
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


def main() -> None:
    configure_logging()
    daemon = BookArchiveDaemon(ArchiveConfig.load())
    asyncio.run(daemon.run())


if __name__ == "__main__":
    main()
