from __future__ import annotations

"""Isolated Polygon on-chain measurement worker.

The worker reads Polygon, the archive metadata, the existing allowlist, and the
existing Data API shadow files.  Its only writes are its own shadow event log
and heartbeat.  It has no follower import and no execution path.
"""

import asyncio
import json
import logging
import os
import signal
import time
from collections import Counter
from pathlib import Path
from typing import Any

import websockets

from .onchain_measurement import (
    ApiShadowReader,
    MeasurementLog,
    PolygonHttpRpc,
    lane_detection_row,
    load_tracked_wallets,
    parse_epoch,
    reconcile_data_api_row,
    utc_iso,
)
from .onchain_shadow import (
    EXCHANGE_ADDRESSES,
    ORDER_FILLED_TOPIC,
    ConfirmationBuffer,
    MetadataResolver,
    OnchainShadowConfig,
    decode_order_filled,
    tracked_wallet_role,
)
from .paper import write_json

LOG = logging.getLogger("polymarket_onchain_shadow")


class OnchainShadowWorker:
    def __init__(
        self,
        config: OnchainShadowConfig | None = None,
        *,
        rpc: PolygonHttpRpc | Any | None = None,
    ) -> None:
        self.config = config or OnchainShadowConfig.from_env()
        self.config.validate()
        self.log = MeasurementLog(self.config.output_path)
        self.rpc = rpc or PolygonHttpRpc(
            self.config.http_rpc_url,
            fallback_urls=self.config.fallback_http_rpc_urls,
        )
        self.metadata = MetadataResolver(self.config.markets_path)
        self.confirmations = ConfirmationBuffer(self.config.confirmations)
        self.stats: Counter[str] = Counter()
        self._tracked_wallets_cache: set[str] = set()
        self._allowlist_mtime_ns: int | None = None
        self.running = True
        self.current_head: int | None = self._load_last_head()
        self.last_wss_message_epoch: float | None = None
        self.last_head_message_epoch: float | None = None
        self.connected_since_epoch: float | None = None
        self.disconnected_since_epoch: float | None = None
        self._wss_provider_index = 0
        self.started_at_epoch = self._load_started_at() or time.time()
        self.api_reader = ApiShadowReader(
            self.config.archive_dir, started_at_epoch=self.started_at_epoch
        )

    def _load_started_at(self) -> float | None:
        path = self.config.output_path
        if not path.exists():
            return None
        try:
            with path.open() as handle:
                for line in handle:
                    try:
                        row = json.loads(line)
                    except (ValueError, TypeError):
                        continue
                    if row.get("type") == "worker_started":
                        return parse_epoch(row.get("started_at") or row.get("ts"))
        except OSError:
            return None
        return None

    def _load_last_head(self) -> int | None:
        try:
            row = json.loads(self.config.heartbeat_path.read_text())
            value = row.get("current_head")
            return int(value) if value is not None else None
        except (OSError, ValueError, TypeError):
            return None

    def tracked_wallets(self) -> set[str]:
        try:
            mtime_ns = self.config.allowlist_path.stat().st_mtime_ns
        except OSError:
            self._tracked_wallets_cache = set()
            self._allowlist_mtime_ns = None
            return set()
        if mtime_ns != self._allowlist_mtime_ns:
            self._tracked_wallets_cache = load_tracked_wallets(
                self.config.allowlist_path
            )
            self._allowlist_mtime_ns = mtime_ns
        return set(self._tracked_wallets_cache)

    def ingest_log(self, row: dict[str, Any], *, origin: str) -> None:
        try:
            fill = decode_order_filled(row)
        except ValueError as exc:
            self.stats["decode_failures"] += 1
            self.log.append(
                {
                    "type": "decode_failure",
                    "origin": origin,
                    "transaction_hash": row.get("transactionHash"),
                    "log_index": row.get("logIndex"),
                    "reason": str(exc),
                }
            )
            return
        role = tracked_wallet_role(fill, self.tracked_wallets())
        if fill.removed:
            pending = self.confirmations.remove(fill.durable_trade_id)
            if pending or ("polygon_onchain", fill.durable_trade_id) in self.log.seen_lane_ids:
                self.log.append(
                    {
                        "type": "reorg_removed",
                        "source": "polygon_onchain",
                        "durable_trade_id": fill.durable_trade_id,
                        "transaction_hash": fill.transaction_hash,
                        "log_index": fill.log_index,
                        "block_number": fill.block_number,
                        "block_hash": fill.block_hash,
                        "reason": "rpc_removed_log",
                        "was_pending": bool(pending),
                        "was_finalized": (
                            "polygon_onchain", fill.durable_trade_id
                        )
                        in self.log.seen_lane_ids,
                    }
                )
                self.stats["reorg_removed"] += 1
            return
        if role is None:
            return
        if role == "counterparty_only":
            # The event side/token belongs to topic[2] (the order owner), not
            # this tracked counterparty.  A separate event represents the
            # tracked wallet's own order and is the only copyable record.
            self.stats["counterparty_only"] += 1
            return
        if fill.durable_trade_id in self.log.seen_onchain_event_ids:
            self.stats["duplicate_polygon_onchain"] += 1
            self.log.append(
                {
                    "type": "duplicate_detection",
                    "source": "polygon_onchain",
                    "durable_trade_id": fill.durable_trade_id,
                    "origin": origin,
                }
            )
            return
        self.confirmations.add(fill, origin=origin)
        self.stats["owner_events_pending"] += 1

    async def finalize_ready(
        self, head_block: int, *, detection_epoch: float | None = None
    ) -> None:
        detected_at = detection_epoch if detection_epoch is not None else time.time()
        ready = self.confirmations.finalizable(head_block)
        for fill, origin in ready:
            try:
                block = await asyncio.to_thread(self.rpc.block, fill.block_number)
            except Exception as exc:
                self.stats["rpc_errors"] += 1
                self.log.append(
                    {
                        "type": "rpc_error",
                        "operation": "canonical_block_check",
                        "durable_trade_id": fill.durable_trade_id,
                        "reason": type(exc).__name__,
                    }
                )
                # Put it back so a transient HTTP failure cannot create a miss.
                self.confirmations.add(fill, origin=origin)
                continue
            canonical_hash = str((block or {}).get("hash") or "").lower()
            if not block or canonical_hash != fill.block_hash:
                self.stats["reorg_removed"] += 1
                self.log.append(
                    {
                        "type": "reorg_removed",
                        "source": "polygon_onchain",
                        "durable_trade_id": fill.durable_trade_id,
                        "transaction_hash": fill.transaction_hash,
                        "log_index": fill.log_index,
                        "block_number": fill.block_number,
                        "block_hash": fill.block_hash,
                        "canonical_block_hash": canonical_hash or None,
                        "reason": "canonical_block_hash_mismatch",
                        "was_pending": True,
                        "was_finalized": False,
                    }
                )
                continue
            try:
                ground_truth_epoch = int(block["timestamp"], 16)
            except (KeyError, TypeError, ValueError):
                self.stats["rpc_errors"] += 1
                self.log.append(
                    {
                        "type": "rpc_error",
                        "operation": "block_timestamp",
                        "durable_trade_id": fill.durable_trade_id,
                        "reason": "missing_block_timestamp",
                    }
                )
                self.confirmations.add(fill, origin=origin)
                continue
            if ground_truth_epoch < self.started_at_epoch:
                self.log.append(
                    {
                        "type": "pre_window_event",
                        "source": "polygon_onchain",
                        "durable_trade_id": fill.durable_trade_id,
                        "transaction_hash": fill.transaction_hash,
                        "log_index": fill.log_index,
                        "ground_truth_epoch": ground_truth_epoch,
                        "measurement_started_epoch": self.started_at_epoch,
                    }
                )
                self.stats["pre_window_polygon_onchain"] += 1
                continue
            row = lane_detection_row(
                source="polygon_onchain",
                fill=fill,
                detection_epoch=detected_at,
                ground_truth_epoch=ground_truth_epoch,
                wallet=fill.maker,
                market=self.metadata.resolve(fill.token_id),
                confirmations=self.config.confirmations,
                origin=origin,
            )
            self.log.append(row)
            self.stats["polygon_onchain_detections"] += 1

    async def process_data_api_row(self, row: dict[str, Any]) -> None:
        key = str(row.get("api_observation_key") or "")
        if key and key in self.log.seen_api_observations:
            return
        detection_epoch = parse_epoch(row.get("ts"))
        if detection_epoch is None:
            self.log.append(
                {
                    "type": "cross_source_mismatch",
                    "source": "data_api",
                    "status": "invalid_detection_timestamp",
                    "api_observation_key": key or None,
                }
            )
            self.stats["cross_source_mismatches"] += 1
            return
        now_epoch = time.time()
        if now_epoch - detection_epoch > self.config.api_max_observation_age_seconds:
            # The Data API lane is a fallback/reconciliation feed.  Rows this
            # old can never be tradeable (the follower's stale gate is 120s)
            # and only exist when the archive is re-scanned after a restart —
            # emitting them floods the follower with phantom stale signals.
            self.log.append(
                {
                    "type": "stale_api_observation",
                    "source": "data_api",
                    "api_observation_key": key or None,
                    "detection_epoch": detection_epoch,
                    "age_seconds": now_epoch - detection_epoch,
                }
            )
            self.stats["stale_api_observations"] += 1
            return
        try:
            match, ground_truth_epoch, _fills = await asyncio.to_thread(
                reconcile_data_api_row, row, self.rpc
            )
        except Exception as exc:
            self.stats["rpc_errors"] += 1
            self.log.append(
                {
                    "type": "rpc_error",
                    "operation": "data_api_reconcile",
                    "api_observation_key": key or None,
                    "reason": type(exc).__name__,
                }
            )
            return
        if match.status != "matched" or match.fill is None or ground_truth_epoch is None:
            trade = row.get("trade") or {}
            self.log.append(
                {
                    "type": "cross_source_mismatch",
                    "source": "data_api",
                    "status": match.status,
                    "transaction_hash": trade.get("transactionHash")
                    or row.get("trade_id"),
                    "wallet": row.get("wallet"),
                    "candidate_count": match.candidate_count,
                    "api_observation_key": key or None,
                }
            )
            self.stats["cross_source_mismatches"] += 1
            return
        fill = match.fill
        if float(ground_truth_epoch) < self.started_at_epoch:
            self.log.append(
                {
                    "type": "pre_window_event",
                    "source": "data_api",
                    "durable_trade_id": fill.durable_trade_id,
                    "transaction_hash": fill.transaction_hash,
                    "log_index": fill.log_index,
                    "ground_truth_epoch": ground_truth_epoch,
                    "measurement_started_epoch": self.started_at_epoch,
                    "api_observation_key": key or None,
                }
            )
            self.stats["pre_window_data_api"] += 1
            return
        lane_key = ("data_api", fill.durable_trade_id)
        if lane_key in self.log.seen_lane_ids:
            self.log.append(
                {
                    "type": "duplicate_detection",
                    "source": "data_api",
                    "durable_trade_id": fill.durable_trade_id,
                    "api_observation_key": key or None,
                }
            )
            self.stats["duplicate_data_api"] += 1
            return
        self.log.append(
            lane_detection_row(
                source="data_api",
                fill=fill,
                detection_epoch=detection_epoch,
                ground_truth_epoch=float(ground_truth_epoch),
                wallet=fill.maker,
                market=self.metadata.resolve(fill.token_id),
                api_observation_key=key or None,
            )
        )
        self.stats["data_api_detections"] += 1

    async def api_tail_loop(self) -> None:
        while self.running:
            for row in await asyncio.to_thread(self.api_reader.read_new):
                if not self.running:
                    break
                await self.process_data_api_row(row)
            await self._sleep(self.config.api_tail_seconds)

    async def _fetch_logs_resilient(
        self, start_block: int, end_block: int
    ) -> list[dict[str, Any]]:
        """Fetch logs while adapting to provider response-size limits."""
        try:
            return await asyncio.to_thread(self.rpc.logs, start_block, end_block)
        except Exception:
            if start_block >= end_block:
                raise
            middle = (start_block + end_block) // 2
            left = await self._fetch_logs_resilient(start_block, middle)
            right = await self._fetch_logs_resilient(middle + 1, end_block)
            return left + right

    async def _backfill(self, latest: int, *, initial: bool) -> None:
        previous = self.current_head
        desired_start = (
            max(0, latest - self.config.initial_backfill_blocks + 1)
            if previous is None
            else previous + 1
        )
        if desired_start > latest:
            self.current_head = latest
            return
        requested_blocks = latest - desired_start + 1
        start = desired_start
        dropped_blocks = 0
        if requested_blocks > self.config.max_backfill_blocks:
            dropped_blocks = requested_blocks - self.config.max_backfill_blocks
            start = latest - self.config.max_backfill_blocks + 1
        recovered = dropped_blocks == 0
        logs_replayed = 0
        caught_error: Exception | None = None
        try:
            for chunk_start in range(
                start, latest + 1, self.config.backfill_chunk_blocks
            ):
                chunk_end = min(
                    latest, chunk_start + self.config.backfill_chunk_blocks - 1
                )
                rows = await self._fetch_logs_resilient(chunk_start, chunk_end)
                for row in rows:
                    self.ingest_log(row, origin="initial_backfill" if initial else "gap_backfill")
                    logs_replayed += 1
            await self.finalize_ready(latest)
        except Exception as exc:
            recovered = False
            caught_error = exc
            self.stats["rpc_errors"] += 1
            self.log.append(
                {
                    "type": "rpc_error",
                    "operation": "gap_backfill",
                    "reason": type(exc).__name__,
                    "from_block": start,
                    "to_block": latest,
                }
            )
        self.log.append(
            {
                "type": "rpc_gap",
                "reason": "startup_backfill" if initial else "websocket_reconnect",
                "from_block": desired_start,
                "to_block": latest,
                "missing_blocks": requested_blocks,
                "replayed_from_block": start,
                "dropped_blocks": dropped_blocks,
                "logs_replayed": logs_replayed,
                "recovered": recovered,
            }
        )
        self.stats["rpc_gap_events"] += 1
        self.stats["rpc_gap_blocks"] += requested_blocks
        if caught_error is not None:
            # Do not advance past a failed range.  Reconnect and retry from
            # the previous durable head so transient provider failures cannot
            # create a permanent blind spot.
            self.current_head = previous
            raise caught_error
        self.current_head = latest

    async def _handle_subscription(self, payload: dict[str, Any]) -> None:
        params = payload.get("params") or {}
        result = params.get("result") if isinstance(params, dict) else None
        if not isinstance(result, dict):
            return
        self.last_wss_message_epoch = time.time()
        if result.get("topics") is not None:
            self.ingest_log(result, origin="live")
            return
        if result.get("number") is not None:
            self.last_head_message_epoch = time.time()
            head = int(result["number"], 16)
            self.current_head = max(head, self.current_head or head)
            await self.finalize_ready(head)

    async def connection_loop(self) -> None:
        initial = self.current_head is None
        while self.running:
            connected_at: float | None = None
            wss_urls = (self.config.wss_rpc_url, *self.config.fallback_wss_rpc_urls)
            provider_index = self._wss_provider_index % len(wss_urls)
            wss_url = wss_urls[provider_index]
            try:
                async with websockets.connect(
                    wss_url,
                    ping_interval=20,
                    ping_timeout=20,
                    open_timeout=20,
                    close_timeout=5,
                    max_queue=4096,
                ) as websocket:
                    connected_at = time.time()
                    self.connected_since_epoch = connected_at
                    downtime = (
                        connected_at - self.disconnected_since_epoch
                        if self.disconnected_since_epoch is not None
                        else 0.0
                    )
                    self.disconnected_since_epoch = None
                    self.log.append(
                        {
                            "type": "rpc_connected",
                            "downtime_seconds": downtime,
                            "reconnect": not initial,
                            "provider_index": provider_index,
                        }
                    )
                    requests_by_id = {
                        1: [
                            "logs",
                            {
                                "address": list(EXCHANGE_ADDRESSES),
                                "topics": [ORDER_FILLED_TOPIC],
                            },
                        ],
                        2: ["newHeads"],
                    }
                    for request_id, params in requests_by_id.items():
                        await websocket.send(
                            json.dumps(
                                {
                                    "jsonrpc": "2.0",
                                    "id": request_id,
                                    "method": "eth_subscribe",
                                    "params": params,
                                }
                            )
                        )
                    acknowledged: set[int] = set()
                    early_notifications: list[dict[str, Any]] = []
                    while len(acknowledged) < 2:
                        message = json.loads(await asyncio.wait_for(websocket.recv(), 30))
                        if message.get("id") in requests_by_id:
                            if message.get("error"):
                                raise RuntimeError("Polygon subscription rejected")
                            acknowledged.add(int(message["id"]))
                        elif message.get("method") == "eth_subscription":
                            early_notifications.append(message)
                    latest = await asyncio.to_thread(self.rpc.latest_block_number)
                    await self._backfill(latest, initial=initial)
                    self.last_head_message_epoch = time.time()
                    initial = False
                    for message in early_notifications:
                        await self._handle_subscription(message)
                    while self.running:
                        raw = await asyncio.wait_for(
                            websocket.recv(),
                            timeout=min(45.0, self.config.head_stale_seconds),
                        )
                        message = json.loads(raw)
                        if message.get("method") == "eth_subscription":
                            await self._handle_subscription(message)
                        if (
                            self.last_head_message_epoch is not None
                            and time.time() - self.last_head_message_epoch
                            > self.config.head_stale_seconds
                        ):
                            self.stats["head_stream_stale_disconnects"] += 1
                            raise TimeoutError("Polygon newHeads subscription stale")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.stats["rpc_disconnects"] += 1
                if len(wss_urls) > 1:
                    self._wss_provider_index = (provider_index + 1) % len(wss_urls)
                    self.stats["rpc_provider_failovers"] += 1
                self.disconnected_since_epoch = time.time()
                self.connected_since_epoch = None
                self.last_head_message_epoch = None
                self.log.append(
                    {
                        "type": "rpc_disconnected",
                        "reason": type(exc).__name__,
                        "connected_seconds": (
                            self.disconnected_since_epoch - connected_at
                            if connected_at is not None
                            else 0.0
                        ),
                        "provider_index": provider_index,
                        "next_provider_index": self._wss_provider_index,
                    }
                )
                LOG.exception("Polygon WSS disconnected; retrying")
                await self._sleep(self.config.reconnect_seconds)

    async def heartbeat_loop(self) -> None:
        while self.running:
            now = time.time()
            payload = {
                "ts": utc_iso(now),
                "service": "polymarket-onchain-shadow",
                "mode": (
                    "paper_signal_source"
                    if self.config.paper_follower_integrated
                    else "measurement_only"
                ),
                "paper_follower_integrated": self.config.paper_follower_integrated,
                "running": self.running,
                "started_at": utc_iso(self.started_at_epoch),
                "current_head": self.current_head,
                "confirmations": self.config.confirmations,
                "pending_confirmations": len(self.confirmations),
                "wallets_tracked": len(self.tracked_wallets()),
                "last_wss_message_ts": (
                    utc_iso(self.last_wss_message_epoch)
                    if self.last_wss_message_epoch is not None
                    else None
                ),
                "last_head_message_ts": (
                    utc_iso(self.last_head_message_epoch)
                    if self.last_head_message_epoch is not None
                    else None
                ),
                "wss_connected": self.connected_since_epoch is not None,
                "wss_provider_index": self._wss_provider_index,
                "wss_provider_count": 1 + len(self.config.fallback_wss_rpc_urls),
                "stats": dict(self.stats),
                "output_path": str(self.config.output_path),
            }
            write_json(self.config.heartbeat_path, payload)
            await self._sleep(self.config.heartbeat_seconds)

    async def _sleep(self, seconds: float) -> None:
        deadline = time.time() + seconds
        while self.running and time.time() < deadline:
            await asyncio.sleep(min(1.0, max(0.0, deadline - time.time())))

    async def run(self) -> None:
        if not self.tracked_wallets():
            raise RuntimeError("tracked-wallet allowlist is empty or unavailable")
        if self._load_started_at() is None:
            self.log.append(
                {
                    "type": "worker_started",
                    "started_at": utc_iso(self.started_at_epoch),
                    "mode": (
                        "paper_signal_source"
                        if self.config.paper_follower_integrated
                        else "measurement_only"
                    ),
                    "paper_follower_integrated": self.config.paper_follower_integrated,
                    "confirmations": self.config.confirmations,
                    "contracts": list(EXCHANGE_ADDRESSES),
                }
            )
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, self.stop)
            except NotImplementedError:
                pass
        await asyncio.gather(
            self.connection_loop(), self.api_tail_loop(), self.heartbeat_loop()
        )

    def stop(self) -> None:
        self.running = False


def configure_logging() -> None:
    level = os.getenv("ONCHAIN_SHADOW_LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def main() -> None:
    configure_logging()
    worker = OnchainShadowWorker()
    asyncio.run(worker.run())


if __name__ == "__main__":
    main()
