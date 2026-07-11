from __future__ import annotations

import asyncio
import gzip
import json
import logging
import os
import signal
import time
from collections import deque
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


def trade_fill_context(trade: dict[str, Any]) -> dict[str, Any]:
    return {
        "fill_price": _num(trade.get("price")),
        "fill_side": trade.get("side"),
        "fill_size": _num(trade.get("size")),
        "fill_timestamp": trade.get("timestamp"),
        "fill_outcome": trade.get("outcome"),
    }


def token_ids_for_market(market: dict[str, Any]) -> list[str]:
    return [str(x) for x in (market.get("clob_token_ids") or []) if x not in (None, "")]


@dataclass
class ArchiveStats:
    markets_covered: int = 0
    tokens_covered: int = 0
    book_rows_written: int = 0
    ws_messages: int = 0
    rest_snapshots: int = 0
    gap_rows_written: int = 0
    wallet_trades_seen: int = 0
    wallet_trades_matched: int = 0
    shadow_rows_written: int = 0
    started_at: str = ""
    last_heartbeat_at: str = ""
    rolling_disk_bytes_per_day: int = 0
    retention_footprint_bytes: int = 0
    rolling_window_seconds: int = 3600


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
        self.compressed_write_samples: deque[tuple[float, int]] = deque()
        self.row_buffers: dict[str, list[str]] = {}
        self.buffered_row_counts: dict[str, int] = {}
        self.state = self._load_state()
        self.followup_queue = self._load_followup_queue()
        self.last_ws_message_ts: str | None = None
        self.ws_stale_timeout_seconds = float(os.getenv("POLYMARKET_WS_STALE_TIMEOUT_SECONDS", "60"))
        self.wallet_driven_tokens: set[str] = set()
        self.wallet_driven_condition_ids: set[str] = set()
        self.wallet_trade_seen_tokens: set[str] = set()
        self._mark_startup_missed_followups()

    def _load_state(self) -> dict[str, Any]:
        path = self.config.state_path
        if path.exists():
            try:
                data = json.loads(path.read_text())
                data.setdefault("seen_trade_ids", [])
                data.setdefault("journaled_trade_ids", self._load_journaled_trade_ids())
                return data
            except Exception:
                LOG.exception("failed to load state; starting fresh")
        return {"seen_trade_ids": [], "journaled_trade_ids": self._load_journaled_trade_ids()}

    def _load_journaled_trade_ids(self) -> list[str]:
        ids: list[str] = []
        try:
            for path in sorted(self.config.archive_dir.glob("shadow_*.jsonl.gz")):
                with gzip.open(path, "rt") as f:
                    for line in f:
                        try:
                            row = json.loads(line)
                        except Exception:
                            continue
                        if (row.get("type") or row.get("kind")) == "fill" and row.get("trade_id"):
                            ids.append(str(row["trade_id"]))
        except Exception:
            LOG.exception("failed to load journaled shadow trade ids")
        return list(dict.fromkeys(ids))[-10000:]

    def _save_state(self) -> None:
        seen = list(dict.fromkeys(self.state.get("seen_trade_ids", [])))[-10000:]
        journaled = list(dict.fromkeys(self.state.get("journaled_trade_ids", [])))[-10000:]
        self.state = {"seen_trade_ids": seen, "journaled_trade_ids": journaled}
        self.config.state_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.config.state_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.state, indent=2, sort_keys=True))
        tmp.replace(self.config.state_path)

    def _load_followup_queue(self) -> list[dict[str, Any]]:
        path = self.config.followup_queue_path
        if path.exists():
            try:
                data = json.loads(path.read_text())
                return data if isinstance(data, list) else []
            except Exception:
                LOG.exception("failed to load followup queue; starting empty")
        # One-time migration from old combined state.
        old_pending = self.state.get("pending_observations", [])
        return old_pending if isinstance(old_pending, list) else []

    def _save_followup_queue(self) -> None:
        self.config.followup_queue_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.config.followup_queue_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.followup_queue[-10000:], indent=2, sort_keys=True, default=str))
        tmp.replace(self.config.followup_queue_path)

    def _mark_startup_missed_followups(self) -> None:
        if not self.followup_queue:
            return
        now = time.time()
        remaining: list[dict[str, Any]] = []
        missed_by_fill: dict[str, dict[str, Any]] = {}
        for item in self.followup_queue:
            if float(item.get("due_ts") or 0) <= now:
                fid = str(item.get("trade_id"))
                bucket = missed_by_fill.setdefault(fid, {"offsets_missed": [], "wallet": item.get("wallet"), "trade": item.get("trade") or {}})
                bucket["offsets_missed"].append(item.get("offset_seconds"))
            else:
                remaining.append(item)
        for fid, bucket in missed_by_fill.items():
            row = {
                "ts": iso_now(),
                "type": "followup_missed",
                "kind": "followup_missed",
                "fill_id": fid,
                "trade_id": fid,
                "wallet": bucket.get("wallet"),
                "offsets_missed": sorted(bucket.get("offsets_missed") or []),
                **trade_fill_context(bucket.get("trade") or {}),
            }
            self.append_row("shadow", row)
            self.stats.shadow_rows_written += 1
        self.flush_prefix("shadow")
        if missed_by_fill:
            LOG.warning("marked missed followups fills=%s", len(missed_by_fill))
        self.followup_queue = remaining
        self._save_followup_queue()

    def archive_path(self, prefix: str, day: datetime | None = None) -> Path:
        # Hourly rotation bounds crash-loss blast radius and makes file reads cheap.
        d = (day or utc_now()).strftime("%Y-%m-%d_%H")
        return self.config.archive_dir / f"{prefix}_{d}.jsonl.gz"

    def _track_compressed_bytes(self, delta: int) -> None:
        if delta <= 0:
            return
        now = time.time()
        self.compressed_write_samples.append((now, delta))
        cutoff = now - 3600
        while self.compressed_write_samples and self.compressed_write_samples[0][0] < cutoff:
            self.compressed_write_samples.popleft()
        window_seconds = max(1.0, min(3600.0, now - self.compressed_write_samples[0][0] if self.compressed_write_samples else 1.0))
        total = sum(b for _, b in self.compressed_write_samples)
        self.stats.rolling_window_seconds = int(window_seconds)
        self.stats.rolling_disk_bytes_per_day = int(total * 86400 / window_seconds)
        self.stats.retention_footprint_bytes = int(self.stats.rolling_disk_bytes_per_day * self.config.retention_days)

    def append_row(self, prefix: str, row: dict[str, Any], *, flush: bool = False) -> int:
        payload = json.dumps(row, separators=(",", ":"), sort_keys=True, default=str) + "\n"
        self.row_buffers.setdefault(prefix, []).append(payload)
        self.buffered_row_counts[prefix] = self.buffered_row_counts.get(prefix, 0) + 1
        if flush or len(self.row_buffers[prefix]) >= 100:
            return self.flush_prefix(prefix)
        return 0

    def flush_prefix(self, prefix: str) -> int:
        rows = self.row_buffers.get(prefix) or []
        if not rows:
            return 0
        path = self.archive_path(prefix)
        payload = "".join(rows).encode("utf-8")
        before = path.stat().st_size if path.exists() else 0
        # Write one gzip member per batch. If the process dies mid-member,
        # prior members remain readable; at worst we lose one batch (<=5s or <=100 rows).
        with path.open("ab") as f:
            f.write(gzip.compress(payload, compresslevel=6))
            f.flush()
            os.fsync(f.fileno())
        after = path.stat().st_size
        delta = max(0, after - before)
        self.row_buffers[prefix] = []
        self.buffered_row_counts[prefix] = 0
        self._track_compressed_bytes(delta)
        return delta

    def flush_all(self) -> int:
        return sum(self.flush_prefix(prefix) for prefix in list(self.row_buffers))

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
        # Restore wallet-driven tokens post refresh
        self._restore_wallet_driven_tokens()
        # Apply token ceiling
        self._evict_excess_tokens()
        self.stats.markets_covered = len(selected) + len(self.wallet_driven_condition_ids)
        self.stats.tokens_covered = len(self.token_meta)
        write_json(self.config.archive_dir / "markets_latest.json", {"ts": iso_now(), "markets": selected, "tokens": self.token_meta})
        LOG.info("discovered markets=%s tokens=%s wallet_driven_tokens=%s", len(selected), self.stats.tokens_covered, len(self.wallet_driven_tokens))

    def _add_wallet_driven_token(self, token_id: str, condition_id: str | None = None) -> None:
        """Add a token from wallet activity to the archive universe."""
        if token_id in self.token_meta or token_id in self.wallet_driven_tokens:
            return
        self.wallet_driven_tokens.add(token_id)
        if condition_id:
            self.wallet_driven_condition_ids.add(str(condition_id).lower())
        self.token_meta[token_id] = {"token_id": token_id, "wallet_driven": True}
        LOG.info("universe_add token=%s wallet_driven=%s condition_id=%s tokens_total=%s", token_id[:12], True, condition_id, len(self.token_meta))

    def _restore_wallet_driven_tokens(self) -> None:
        """Re-add wallet-driven tokens that were cleared by discover_markets()."""
        for token_id in self.wallet_driven_tokens:
            if token_id not in self.token_meta:
                self.token_meta[token_id] = {"token_id": token_id, "wallet_driven": True}

    def _evict_excess_tokens(self) -> None:
        """Keep total tokens under max_tokens ceiling.
        Evict oldest non-wallet-driven (top-50 baseline) tokens first, never wallet-driven ones."""
        max_tokens = self.config.max_tokens
        if len(self.token_meta) <= max_tokens:
            return
        evict_candidates = [tid for tid in self.token_meta if tid not in self.wallet_driven_tokens]
        if not evict_candidates:
            return
        evict_count = len(self.token_meta) - max_tokens
        to_evict = sorted(evict_candidates)[:evict_count]
        for tid in to_evict:
            meta = self.token_meta.pop(tid, None)
            LOG.info("universe_evict token=%s wallet_driven=False tokens_after=%s", tid[:12], len(self.token_meta))
        self.stats.tokens_covered = len(self.token_meta)

    def _ensure_wallet_trade_tokens(self, trade: dict[str, Any]) -> None:
        """If a wallet trade references tokens not in the archive, add them dynamically."""
        token = str(trade.get("asset") or trade.get("assetId") or trade.get("token_id") or trade.get("tokenId") or trade.get("clobTokenId") or "")
        if token and token not in self.token_meta:
            condition = str(trade.get("conditionId") or trade.get("condition_id") or "")
            self._add_wallet_driven_token(token, condition)

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
            "type": "book",
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
        self.append_row("book", row)
        self.stats.book_rows_written += 1

    def record_gap(self, start_ts: str, end_ts: str, tokens_affected: list[str], reason: str) -> dict[str, Any]:
        row = {
            "ts": end_ts,
            "type": "gap",
            "start_ts": start_ts,
            "end_ts": end_ts,
            "tokens_affected": tokens_affected,
            "tokens_affected_count": len(tokens_affected),
            "reason": reason,
        }
        self.append_row("book", row, flush=True)
        self.stats.gap_rows_written += 1
        LOG.warning("gap marker reason=%s tokens=%s start=%s end=%s", reason, len(tokens_affected), start_ts, end_ts)
        return row

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
        self.last_ws_message_ts = iso_now()
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
        last_disconnect_ts: str | None = None
        while self.running:
            tokens = list(self.token_meta)
            if not tokens:
                await self._sleep(5)
                continue
            try:
                async with websockets.connect(WS_MARKET_URL, ping_interval=20, open_timeout=20) as ws:
                    if last_disconnect_ts:
                        self.record_gap(last_disconnect_ts, iso_now(), tokens, "websocket_reconnect")
                        last_disconnect_ts = None
                    connect_ts = iso_now()
                    self.last_ws_message_ts = self.last_ws_message_ts or connect_ts
                    await ws.send(json.dumps({"assets_ids": tokens, "type": "market", "custom_feature_enabled": True}))
                    LOG.info("websocket subscribed tokens=%s", len(tokens))
                    while self.running:
                        raw = await asyncio.wait_for(ws.recv(), timeout=self.ws_stale_timeout_seconds)
                        if isinstance(raw, bytes):
                            raw = raw.decode("utf-8", "ignore")
                        self.handle_ws_message(raw)
            except asyncio.TimeoutError:
                end_ts = iso_now()
                start_ts = self.last_ws_message_ts or last_disconnect_ts or end_ts
                self.record_gap(start_ts, end_ts, list(self.token_meta), "ws_stale")
                LOG.warning("websocket stale for %.1fs; reconnecting/resubscribing", self.ws_stale_timeout_seconds)
                last_disconnect_ts = None
            except Exception:
                LOG.exception("websocket loop failed; reconnecting")
                last_disconnect_ts = last_disconnect_ts or iso_now()
                await self._sleep(5)

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
        slug = trade.get("marketSlug") or trade.get("slug")
        candidates = [state for _tid, state in self.book_state.items() if not slug or state.get("market", {}).get("market_slug") == slug]
        return {"tokens": candidates, "ts": iso_now()} if candidates else None

    def _trade_timestamp(self, trade: dict[str, Any]) -> float | None:
        raw = trade.get("timestamp") or trade.get("createdAt") or trade.get("created_at")
        try:
            if isinstance(raw, (int, float)):
                return float(raw)
            if isinstance(raw, str):
                dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
                return dt.timestamp()
        except Exception:
            return None
        return None

    def _mark_followups_missed_for_trade(self, wallet: str, tid: str, trade: dict[str, Any], offsets: list[int]) -> None:
        row = {
            "ts": iso_now(),
            "type": "followup_missed",
            "kind": "followup_missed",
            "fill_id": tid,
            "trade_id": tid,
            "wallet": wallet,
            "offsets_missed": sorted(offsets),
            **trade_fill_context(trade),
        }
        self.append_row("shadow", row)
        self.stats.shadow_rows_written += 1

    def poll_wallets_once(self) -> None:
        seen: set[str] = set(self.state.get("seen_trade_ids", []))
        journaled: set[str] = set(self.state.get("journaled_trade_ids", []))
        for wallet in self._configured_wallets():
            try:
                trades = user_trades(wallet, self.config.trade_poll_limit)
            except Exception:
                LOG.exception("wallet trade poll failed wallet=%s", wallet)
                continue
            for trade in trades:
                self.stats.wallet_trades_seen += 1
                tid = trade_id(trade)
                seen.add(tid)
                # Ensure wallet trade tokens are in the archive universe
                self._ensure_wallet_trade_tokens(trade)
                if tid in journaled:
                    continue
                matched = self._trade_matches_archive(trade)
                row = {
                    "ts": iso_now(),
                    "type": "fill",
                    "kind": "fill",
                    "wallet": wallet,
                    "trade_id": tid,
                    "archive_matched": matched,
                    "trade": trade,
                    "book_at_detection": self._current_book_for_trade(trade) if matched else None,
                    **trade_fill_context(trade),
                }
                self.append_row("shadow", row)
                self.stats.shadow_rows_written += 1
                journaled.add(tid)
                if matched:
                    self.stats.wallet_trades_matched += 1
                    now = time.time()
                    trade_ts = self._trade_timestamp(trade)
                    missed_offsets = [offset for offset in self.config.followup_offsets_seconds if trade_ts and trade_ts + offset <= now]
                    pending_offsets = [offset for offset in self.config.followup_offsets_seconds if offset not in missed_offsets]
                    if missed_offsets:
                        self._mark_followups_missed_for_trade(wallet, tid, trade, missed_offsets)
                    for offset in pending_offsets:
                        self.followup_queue.append({"due_ts": now + offset, "offset_seconds": offset, "wallet": wallet, "trade_id": tid, "trade": trade})
        self.state["seen_trade_ids"] = list(seen)
        self.state["journaled_trade_ids"] = list(journaled)
        self._process_due_followups()
        self.flush_prefix("shadow")
        self._save_state()
        self._save_followup_queue()

    def _process_due_followups(self) -> None:
        now = time.time()
        remaining: list[dict[str, Any]] = []
        for item in self.followup_queue:
            if float(item.get("due_ts") or 0) > now:
                remaining.append(item)
                continue
            trade = item.get("trade") or {}
            row = {
                "ts": iso_now(),
                "type": "followup_book",
                "kind": "followup_book",
                "wallet": item.get("wallet"),
                "trade_id": item.get("trade_id"),
                "offset_seconds": item.get("offset_seconds"),
                "book": self._current_book_for_trade(trade),
                **trade_fill_context(trade),
            }
            self.append_row("shadow", row)
            self.stats.shadow_rows_written += 1
        self.followup_queue = remaining

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

    async def flush_loop(self) -> None:
        while self.running:
            await self._sleep(5)
            self.flush_all()

    async def heartbeat_loop(self) -> None:
        while self.running:
            self.flush_all()
            self.stats.last_heartbeat_at = iso_now()
            report = {
                "ts": self.stats.last_heartbeat_at,
                "config": self.config.to_jsonable(),
                "stats": self.stats.__dict__,
                "disk_estimate": {
                    "rolling_window_seconds": self.stats.rolling_window_seconds,
                    "compressed_bytes_per_day": self.stats.rolling_disk_bytes_per_day,
                    "compressed_mb_per_day": round(self.stats.rolling_disk_bytes_per_day / 1_000_000, 3),
                    "retention_days": self.config.retention_days,
                    "retention_bytes": self.stats.retention_footprint_bytes,
                    "retention_gb": round(self.stats.retention_footprint_bytes / 1_000_000_000, 3),
                },
                "wallets_tracked": len(self._configured_wallets()),
                "wallet_driven_tokens": len(self.wallet_driven_tokens),
                "pending_followups": len(self.followup_queue),
                "archive_dir": str(self.config.archive_dir),
            }
            write_json(self.config.archive_dir / "heartbeat_latest.json", report)
            LOG.info(
                "heartbeat markets=%s tokens=%s book_rows=%s gaps=%s ws=%s wallet_seen=%s matched=%s shadow_rows=%s compressed_mb_day=%.3f retention_gb=%.3f",
                self.stats.markets_covered, self.stats.tokens_covered, self.stats.book_rows_written, self.stats.gap_rows_written,
                self.stats.ws_messages, self.stats.wallet_trades_seen, self.stats.wallet_trades_matched, self.stats.shadow_rows_written,
                self.stats.rolling_disk_bytes_per_day / 1_000_000, self.stats.retention_footprint_bytes / 1_000_000_000,
            )
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
        try:
            await asyncio.gather(self.ws_loop(), self.snapshot_loop(), self.wallet_loop(), self.flush_loop(), self.heartbeat_loop(), self.market_refresh_loop())
        finally:
            self.flush_all()

    def stop(self) -> None:
        LOG.info("stopping")
        self.running = False
        self.flush_all()


def configure_logging() -> None:
    level = os.getenv("POLYMARKET_ARCHIVE_LOG_LEVEL", "INFO").upper()
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


def main() -> None:
    configure_logging()
    daemon = BookArchiveDaemon(ArchiveConfig.load())
    asyncio.run(daemon.run())


if __name__ == "__main__":
    main()
