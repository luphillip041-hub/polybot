from __future__ import annotations

"""Cross-lane measurement utilities for the isolated Polygon shadow worker."""

import gzip
import json
import os
import threading
from collections import Counter, OrderedDict, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable

import requests

from .onchain_shadow import (
    EXCHANGE_ADDRESSES,
    ORDER_FILLED_TOPIC,
    DataApiMatch,
    DecodedFill,
    decode_order_filled,
    match_data_api_trade,
)

GROUND_TRUTH_DEFINITION = (
    "canonical Polygon block timestamp for the transaction after configured confirmations"
)


def utc_iso(epoch: float | None = None) -> str:
    dt = datetime.fromtimestamp(epoch, UTC) if epoch is not None else datetime.now(UTC)
    return dt.isoformat(timespec="milliseconds")


def parse_epoch(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str) and value:
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
        except ValueError:
            return None
    return None


def percentile(values: Iterable[float], quantile: float) -> float | None:
    rows = sorted(float(value) for value in values)
    if not rows:
        return None
    if not 0 <= quantile <= 1:
        raise ValueError("quantile must be between zero and one")
    position = (len(rows) - 1) * quantile
    lower = int(position)
    upper = min(lower + 1, len(rows) - 1)
    return rows[lower] + (rows[upper] - rows[lower]) * (position - lower)


def load_tracked_wallets(path: Path) -> set[str]:
    """Read the existing allowlist without ever modifying it."""
    try:
        raw = json.loads(path.read_text())
    except (OSError, ValueError, TypeError):
        return set()
    wallets = raw.get("wallets") if isinstance(raw, dict) else None
    if not isinstance(wallets, list):
        return set()
    return {
        str(wallet).strip().lower()
        for wallet in wallets
        if isinstance(wallet, str) and str(wallet).strip().lower().startswith("0x")
    }


class MeasurementLog:
    """Append-only shadow log with restart-time dedup reconstruction."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self.seen_lane_ids: set[tuple[str, str]] = set()
        self.seen_onchain_event_ids: set[str] = set()
        self.seen_api_observations: set[str] = set()
        self._load_seen()

    def _load_seen(self) -> None:
        if not self.path.exists():
            return
        try:
            with self.path.open() as handle:
                for line in handle:
                    try:
                        row = json.loads(line)
                    except (ValueError, TypeError):
                        continue
                    if row.get("type") == "lane_detection":
                        source = str(row.get("source") or "")
                        trade_id = str(row.get("durable_trade_id") or "")
                        if source and trade_id:
                            self.seen_lane_ids.add((source, trade_id))
                            if source == "polygon_onchain":
                                self.seen_onchain_event_ids.add(trade_id)
                    if row.get("type") == "pre_window_event" and row.get(
                        "source"
                    ) == "polygon_onchain":
                        trade_id = str(row.get("durable_trade_id") or "")
                        if trade_id:
                            self.seen_onchain_event_ids.add(trade_id)
                    api_key = row.get("api_observation_key")
                    if api_key:
                        self.seen_api_observations.add(str(api_key))
        except OSError:
            return

    def append(self, row: dict[str, Any]) -> None:
        payload = dict(row)
        payload.setdefault("ts", utc_iso())
        encoded = (json.dumps(payload, separators=(",", ":"), sort_keys=True, default=str) + "\n").encode()
        with self._lock:
            fd = os.open(self.path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o640)
            try:
                os.write(fd, encoded)
                os.fsync(fd)
            finally:
                os.close(fd)
            if payload.get("type") == "lane_detection":
                source = str(payload.get("source") or "")
                trade_id = str(payload.get("durable_trade_id") or "")
                if source and trade_id:
                    self.seen_lane_ids.add((source, trade_id))
                    if source == "polygon_onchain":
                        self.seen_onchain_event_ids.add(trade_id)
            if (
                payload.get("type") == "pre_window_event"
                and payload.get("source") == "polygon_onchain"
                and payload.get("durable_trade_id")
            ):
                self.seen_onchain_event_ids.add(str(payload["durable_trade_id"]))
            if payload.get("api_observation_key"):
                self.seen_api_observations.add(str(payload["api_observation_key"]))


class ApiShadowReader:
    """Read new Data API fill rows from the existing compressed archive.

    Files are never opened for writing.  A content key deduplicates repeated
    scans and survives rotation within the process.
    """

    def __init__(self, archive_dir: Path, started_at_epoch: float) -> None:
        self.archive_dir = archive_dir
        self.started_at_epoch = started_at_epoch
        self._seen: set[str] = set()

    @staticmethod
    def observation_key(row: dict[str, Any]) -> str:
        trade = row.get("trade") if isinstance(row.get("trade"), dict) else {}
        tx_hash = str(
            trade.get("transactionHash")
            or trade.get("transaction_hash")
            or row.get("trade_id")
            or ""
        ).lower()
        wallet = str(row.get("wallet") or trade.get("proxyWallet") or "").lower()
        token = str(trade.get("asset") or trade.get("token_id") or "")
        side = str(trade.get("side") or "").upper()
        return "|".join((tx_hash, wallet, token, side, str(trade.get("size") or "")))

    def read_new(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        # Only files that could contain detections from this measurement run
        # are relevant.  Avoid re-inflating the full 45-day archive every 15s.
        earliest_mtime = self.started_at_epoch - 3600
        for path in sorted(self.archive_dir.glob("shadow_*.jsonl.gz")):
            try:
                if path.stat().st_mtime < earliest_mtime:
                    continue
            except OSError:
                continue
            try:
                with gzip.open(path, "rt") as handle:
                    for line in handle:
                        try:
                            row = json.loads(line)
                        except (ValueError, TypeError):
                            continue
                        if (row.get("type") or row.get("kind")) != "fill":
                            continue
                        detected = parse_epoch(row.get("ts"))
                        if detected is None or detected < self.started_at_epoch:
                            continue
                        key = self.observation_key(row)
                        if key in self._seen:
                            continue
                        self._seen.add(key)
                        normalized = dict(row)
                        normalized["source"] = "data_api"
                        normalized["api_observation_key"] = key
                        rows.append(normalized)
            except (OSError, EOFError, gzip.BadGzipFile):
                # The active gzip member can be observed between append/fsync;
                # the next scan retries it without changing the source file.
                continue
        rows.sort(key=lambda row: str(row.get("ts") or ""))
        return rows


class PolygonHttpRpc:
    def __init__(
        self,
        url: str,
        timeout_seconds: float = 10.0,
        fallback_urls: tuple[str, ...] = (),
    ) -> None:
        self.urls = (url, *fallback_urls)
        self.url = url
        self.timeout_seconds = timeout_seconds
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "Hermes-Polymarket-Onchain-Shadow/0.1"})
        self._request_id = 0
        self._provider_index = 0
        self._id_lock = threading.Lock()
        self._cache_lock = threading.Lock()
        self._block_cache: OrderedDict[int, dict[str, Any]] = OrderedDict()
        self._block_cache_limit = 4096

    def call(self, method: str, params: list[Any]) -> Any:
        last_error: Exception | None = None
        for attempt in range(len(self.urls)):
            provider_index = (self._provider_index + attempt) % len(self.urls)
            url = self.urls[provider_index]
            try:
                with self._id_lock:
                    self._request_id += 1
                    request_id = self._request_id
                response = self.session.post(
                    url,
                    json={
                        "jsonrpc": "2.0",
                        "id": request_id,
                        "method": method,
                        "params": params,
                    },
                    timeout=self.timeout_seconds,
                )
                response.raise_for_status()
                body = response.json()
                if body.get("error"):
                    raise RuntimeError(
                        f"Polygon RPC {method} error: {body['error']}"
                    )
                self._provider_index = provider_index
                self.url = url
                return body.get("result")
            except Exception as exc:
                last_error = exc
        if last_error is not None:
            raise last_error
        raise RuntimeError("no Polygon HTTP RPC providers configured")

    def latest_block_number(self) -> int:
        return int(self.call("eth_blockNumber", []), 16)

    def receipt(self, transaction_hash: str) -> dict[str, Any] | None:
        value = self.call("eth_getTransactionReceipt", [transaction_hash])
        return value if isinstance(value, dict) else None

    def block(self, block_number: int) -> dict[str, Any] | None:
        with self._cache_lock:
            cached = self._block_cache.get(block_number)
            if cached is not None:
                self._block_cache.move_to_end(block_number)
                return dict(cached)
        value = self.call("eth_getBlockByNumber", [hex(block_number), False])
        if not isinstance(value, dict):
            return None
        with self._cache_lock:
            self._block_cache[block_number] = dict(value)
            self._block_cache.move_to_end(block_number)
            while len(self._block_cache) > self._block_cache_limit:
                self._block_cache.popitem(last=False)
        return value

    def logs(self, start_block: int, end_block: int) -> list[dict[str, Any]]:
        if end_block < start_block:
            return []
        value = self.call(
            "eth_getLogs",
            [{"fromBlock": hex(start_block), "toBlock": hex(end_block), "address": list(EXCHANGE_ADDRESSES), "topics": [ORDER_FILLED_TOPIC]}],
        )
        return [row for row in value or [] if isinstance(row, dict)]


def decode_receipt_fills(receipt: dict[str, Any]) -> list[DecodedFill]:
    fills: list[DecodedFill] = []
    for row in receipt.get("logs") or []:
        if not isinstance(row, dict):
            continue
        try:
            fills.append(decode_order_filled(row))
        except ValueError:
            continue
    return fills


def reconcile_data_api_row(
    row: dict[str, Any], rpc: PolygonHttpRpc
) -> tuple[DataApiMatch, int | None, list[DecodedFill]]:
    trade = dict(row.get("trade") or {})
    trade.setdefault("wallet", row.get("wallet"))
    tx_hash = str(
        trade.get("transactionHash")
        or trade.get("transaction_hash")
        or row.get("trade_id")
        or ""
    ).lower()
    if not tx_hash.startswith("0x"):
        return DataApiMatch("missing_transaction_hash", None, 0), None, []
    receipt = rpc.receipt(tx_hash)
    if not receipt:
        return DataApiMatch("receipt_unavailable", None, 0), None, []
    fills = decode_receipt_fills(receipt)
    match = match_data_api_trade(trade, fills)
    block_number = int(receipt.get("blockNumber"), 16)
    block = rpc.block(block_number)
    ground_truth_epoch = int(block.get("timestamp"), 16) if block else None
    return match, ground_truth_epoch, fills


def lane_detection_row(
    *,
    source: str,
    fill: DecodedFill,
    detection_epoch: float,
    ground_truth_epoch: float,
    wallet: str,
    market: dict[str, Any] | None,
    confirmations: int | None = None,
    api_observation_key: str | None = None,
) -> dict[str, Any]:
    row: dict[str, Any] = {
        "type": "lane_detection",
        "source": source,
        "durable_trade_id": fill.durable_trade_id,
        "transaction_hash": fill.transaction_hash,
        "log_index": fill.log_index,
        "block_number": fill.block_number,
        "block_hash": fill.block_hash,
        "ground_truth": "polygon_block_timestamp",
        "ground_truth_definition": GROUND_TRUTH_DEFINITION,
        "ground_truth_epoch": ground_truth_epoch,
        "ground_truth_ts": utc_iso(ground_truth_epoch),
        "detection_epoch": detection_epoch,
        "detection_ts": utc_iso(detection_epoch),
        "detection_latency_seconds": detection_epoch - ground_truth_epoch,
        "wallet": wallet.lower(),
        "side": fill.side,
        "token_id": fill.token_id,
        "size": fill.size,
        "price": fill.price,
        "contract": fill.contract,
        "market": market,
    }
    if confirmations is not None:
        row["confirmations"] = confirmations
    if api_observation_key:
        row["api_observation_key"] = api_observation_key
    return row


def coverage_report(
    path: Path,
    *,
    as_of_epoch: float | None = None,
    miss_grace_seconds: float = 900.0,
) -> dict[str, Any]:
    as_of_epoch = float(as_of_epoch if as_of_epoch is not None else datetime.now(UTC).timestamp())
    detections: dict[str, dict[str, list[dict[str, Any]]]] = defaultdict(
        lambda: defaultdict(list)
    )
    type_counts: Counter[str] = Counter()
    mismatch_count = 0
    reorg_count = 0
    gap_events = gap_blocks = recovered_gaps = 0
    duplicate_events: Counter[str] = Counter()
    rpc_connections = rpc_disconnects = 0
    connected_seconds = downtime_seconds = 0.0
    started_at: str | None = None
    ended_at: str | None = None
    if path.exists():
        with path.open() as handle:
            for line in handle:
                try:
                    row = json.loads(line)
                except (ValueError, TypeError):
                    continue
                row_type = str(row.get("type") or "unknown")
                type_counts[row_type] += 1
                ts = str(row.get("ts") or row.get("detection_ts") or "")
                if ts:
                    started_at = min(started_at, ts) if started_at else ts
                    ended_at = max(ended_at, ts) if ended_at else ts
                if row_type == "lane_detection":
                    source = str(row.get("source") or "")
                    trade_id = str(row.get("durable_trade_id") or "")
                    if source and trade_id:
                        detections[trade_id][source].append(row)
                elif row_type == "cross_source_mismatch":
                    mismatch_count += 1
                elif row_type == "reorg_removed":
                    reorg_count += 1
                elif row_type == "rpc_gap":
                    gap_events += 1
                    gap_blocks += int(row.get("missing_blocks") or 0)
                    recovered_gaps += int(bool(row.get("recovered")))
                elif row_type == "duplicate_detection":
                    duplicate_events[str(row.get("source") or "unknown")] += 1
                elif row_type == "rpc_connected":
                    rpc_connections += 1
                    downtime_seconds += float(row.get("downtime_seconds") or 0)
                elif row_type == "rpc_disconnected":
                    rpc_disconnects += 1
                    connected_seconds += float(row.get("connected_seconds") or 0)

    sources = ("polygon_onchain", "data_api")
    coverage = {"seen_by_both": 0, "onchain_only": 0, "data_api_only": 0}
    mature_coverage = {"seen_by_both": 0, "onchain_only": 0, "data_api_only": 0}
    mature_ids: set[str] = set()
    metadata_resolved = metadata_unresolved = 0
    latencies: dict[str, list[float]] = {source: [] for source in sources}
    duplicates: dict[str, int] = {source: 0 for source in sources}
    first_seen = {"polygon_onchain": 0, "data_api": 0, "tie": 0}
    first_seen_deltas: list[float] = []

    for trade_id, by_source in detections.items():
        chain = by_source.get("polygon_onchain") or []
        api = by_source.get("data_api") or []
        category = "seen_by_both" if chain and api else "onchain_only" if chain else "data_api_only"
        coverage[category] += 1
        all_rows = chain + api
        ground_truth_epoch = min(float(row["ground_truth_epoch"]) for row in all_rows)
        if ground_truth_epoch <= as_of_epoch - miss_grace_seconds:
            mature_ids.add(trade_id)
            mature_coverage[category] += 1
        if chain:
            first_chain = min(chain, key=lambda row: float(row["detection_epoch"]))
            if first_chain.get("market") is not None:
                metadata_resolved += 1
            else:
                metadata_unresolved += 1
        if chain and api:
            chain_time = min(float(row["detection_epoch"]) for row in chain)
            api_time = min(float(row["detection_epoch"]) for row in api)
            delta = api_time - chain_time
            first_seen_deltas.append(delta)
            if delta > 0:
                first_seen["polygon_onchain"] += 1
            elif delta < 0:
                first_seen["data_api"] += 1
            else:
                first_seen["tie"] += 1
        for source in sources:
            source_rows = by_source.get(source) or []
            duplicates[source] += max(0, len(source_rows) - 1)
            if source_rows:
                first = min(source_rows, key=lambda row: float(row["detection_epoch"]))
                latency = first.get("detection_latency_seconds")
                if latency is None:
                    latency = float(first["detection_epoch"]) - float(
                        first["ground_truth_epoch"]
                    )
                latencies[source].append(float(latency))

    lane_report: dict[str, dict[str, Any]] = {}
    total = len(detections)
    for source in sources:
        duplicates[source] += duplicate_events[source]
        event_count = sum(bool(rows.get(source)) for rows in detections.values())
        mature_event_count = sum(
            trade_id in mature_ids and bool(rows.get(source))
            for trade_id, rows in detections.items()
        )
        lane_report[source] = {
            "event_count": event_count,
            "miss_count": total - event_count,
            "mature_event_count": mature_event_count,
            "mature_miss_count": len(mature_ids) - mature_event_count,
            "p50_latency_seconds": percentile(latencies[source], 0.5),
            "p90_latency_seconds": percentile(latencies[source], 0.9),
        }
    return {
        "ground_truth": "polygon_block_timestamp",
        "ground_truth_definition": GROUND_TRUTH_DEFINITION,
        "started_at": started_at,
        "ended_at": ended_at,
        "lanes": lane_report,
        "coverage": coverage,
        "coverage_mature": {
            **mature_coverage,
            "grace_seconds": miss_grace_seconds,
            "total_mature_events": len(mature_ids),
        },
        "metadata_resolution": {
            "resolved": metadata_resolved,
            "unresolved": metadata_unresolved,
            "resolved_percent": (
                100.0 * metadata_resolved / (metadata_resolved + metadata_unresolved)
                if metadata_resolved + metadata_unresolved > 0
                else None
            ),
        },
        "first_seen": first_seen,
        "first_seen_delta_seconds": {
            "p50": percentile(first_seen_deltas, 0.5),
            "p90": percentile(first_seen_deltas, 0.9),
        },
        "duplicates": duplicates,
        "cross_source_mismatches": mismatch_count,
        "reorg_events": reorg_count,
        "rpc_gaps": {
            "events": gap_events,
            "missing_blocks": gap_blocks,
            "recovered_events": recovered_gaps,
        },
        "rpc_uptime": {
            "connections": rpc_connections,
            "disconnects": rpc_disconnects,
            "connected_seconds_reported": connected_seconds,
            "downtime_seconds_reported": downtime_seconds,
            "uptime_percent_reported": (
                100.0
                if rpc_connections > 0 and rpc_disconnects == 0
                else 100.0 * connected_seconds / (connected_seconds + downtime_seconds)
                if connected_seconds + downtime_seconds > 0
                else None
            ),
        },
        "row_types": dict(type_counts),
    }
