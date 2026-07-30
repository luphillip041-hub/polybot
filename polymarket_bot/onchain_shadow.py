from __future__ import annotations

"""Measurement-only Polygon fill decoder for Polybot.

This module has no import of ``paper_follower`` and no order, ledger, paper
state, or allowlist write path.  It is the isolated Stage One measurement lane.
"""

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from eth_abi import decode as abi_decode
from eth_utils import keccak

from .config import CONFIG

# Official Polygon mainnet addresses from https://docs.polymarket.com/resources/contracts
# (CTF Exchange V2 deployments current as of 2026-07-30).
CTF_EXCHANGE_V2 = "0xe111180000d2663c0091e4f400237545b87b996b"
NEG_RISK_CTF_EXCHANGE_V2 = "0xe2222d279d744050d28e00520010520000310f59"
EXCHANGE_ADDRESSES = (CTF_EXCHANGE_V2, NEG_RISK_CTF_EXCHANGE_V2)

ORDER_FILLED_SIGNATURE = (
    "OrderFilled(bytes32,address,address,uint8,uint256,uint256,uint256,uint256,bytes32,bytes32)"
)
ORDER_FILLED_TOPIC = "0x" + keccak(text=ORDER_FILLED_SIGNATURE).hex()


def _hex_int(value: Any, *, field: str) -> int:
    if isinstance(value, int):
        return value
    if not isinstance(value, str) or not value:
        raise ValueError(f"missing {field}")
    try:
        return int(value, 16 if value.startswith("0x") else 10)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"invalid {field}") from exc


def _topic_address(topic: Any) -> str:
    if not isinstance(topic, str) or not topic.startswith("0x") or len(topic) != 66:
        raise ValueError("invalid indexed address topic")
    return "0x" + topic[-40:].lower()


def _norm_address(value: Any) -> str:
    text = str(value or "").strip().lower()
    if len(text) != 42 or not text.startswith("0x"):
        return ""
    return text


@dataclass(frozen=True)
class DecodedFill:
    durable_trade_id: str
    transaction_hash: str
    log_index: int
    block_number: int
    block_hash: str
    contract: str
    order_hash: str
    maker: str
    taker: str
    side: str
    token_id: str
    maker_amount_raw: int
    taker_amount_raw: int
    fee_raw: int
    builder: str
    metadata: str
    size: float
    price: float
    removed: bool = False

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "durable_trade_id": self.durable_trade_id,
            "transaction_hash": self.transaction_hash,
            "log_index": self.log_index,
            "block_number": self.block_number,
            "block_hash": self.block_hash,
            "contract": self.contract,
            "order_hash": self.order_hash,
            "maker": self.maker,
            "taker": self.taker,
            "side": self.side,
            "token_id": self.token_id,
            "maker_amount_raw": self.maker_amount_raw,
            "taker_amount_raw": self.taker_amount_raw,
            "fee_raw": self.fee_raw,
            "builder": self.builder,
            "metadata": self.metadata,
            "size": self.size,
            "price": self.price,
            "removed": self.removed,
        }


def decode_order_filled(log: dict[str, Any]) -> DecodedFill:
    """Decode one official CTF Exchange V2 ``OrderFilled`` log.

    ``maker`` in topic[2] is the owner of the order represented by this event.
    ``taker`` in topic[3] is the counterparty.  The event's side belongs to
    ``maker``; it must never be assigned to a tracked wallet found only in
    ``taker``.
    """

    address = _norm_address(log.get("address"))
    if address not in EXCHANGE_ADDRESSES:
        raise ValueError("unsupported exchange contract")
    topics = log.get("topics")
    if not isinstance(topics, list) or len(topics) != 4:
        raise ValueError("OrderFilled requires four topics")
    if str(topics[0]).lower() != ORDER_FILLED_TOPIC:
        raise ValueError("unexpected event topic")
    data_hex = log.get("data")
    if not isinstance(data_hex, str) or not data_hex.startswith("0x"):
        raise ValueError("missing event data")
    try:
        values = abi_decode(
            ["uint8", "uint256", "uint256", "uint256", "uint256", "bytes32", "bytes32"],
            bytes.fromhex(data_hex[2:]),
        )
    except Exception as exc:
        raise ValueError("invalid OrderFilled data") from exc
    side_num = int(values[0])
    if side_num not in (0, 1):
        raise ValueError(f"unsupported side {side_num}")
    side = "BUY" if side_num == 0 else "SELL"
    maker_amount = int(values[2])
    taker_amount = int(values[3])
    # All current exchange assets use six decimal units.  For BUY orders the
    # owner makes collateral and takes shares; SELL is the reverse.
    collateral_raw = maker_amount if side == "BUY" else taker_amount
    shares_raw = taker_amount if side == "BUY" else maker_amount
    if shares_raw <= 0:
        raise ValueError("zero share amount")
    tx_hash = str(log.get("transactionHash") or "").lower()
    if not tx_hash.startswith("0x") or len(tx_hash) != 66:
        raise ValueError("invalid transaction hash")
    log_index = _hex_int(log.get("logIndex"), field="logIndex")
    return DecodedFill(
        durable_trade_id=f"{tx_hash}:{log_index}",
        transaction_hash=tx_hash,
        log_index=log_index,
        block_number=_hex_int(log.get("blockNumber"), field="blockNumber"),
        block_hash=str(log.get("blockHash") or "").lower(),
        contract=address,
        order_hash=str(topics[1]).lower(),
        maker=_topic_address(topics[2]),
        taker=_topic_address(topics[3]),
        side=side,
        token_id=str(int(values[1])),
        maker_amount_raw=maker_amount,
        taker_amount_raw=taker_amount,
        fee_raw=int(values[4]),
        builder="0x" + bytes(values[5]).hex(),
        metadata="0x" + bytes(values[6]).hex(),
        size=shares_raw / 1_000_000,
        price=collateral_raw / shares_raw,
        removed=bool(log.get("removed")),
    )


def tracked_wallet_role(fill: DecodedFill, wallets: Iterable[str]) -> str | None:
    normalized = {_norm_address(wallet) for wallet in wallets}
    normalized.discard("")
    if fill.maker in normalized:
        return "order_owner"
    if fill.taker in normalized:
        return "counterparty_only"
    return None


class ConfirmationBuffer:
    def __init__(self, confirmations: int) -> None:
        if confirmations < 0:
            raise ValueError("confirmations must be non-negative")
        self.confirmations = confirmations
        self._pending: dict[str, DecodedFill] = {}

    def add(self, fill: DecodedFill) -> None:
        self._pending[fill.durable_trade_id] = fill

    def remove(self, durable_trade_id: str) -> DecodedFill | None:
        return self._pending.pop(durable_trade_id, None)

    def finalizable(self, head_block: int) -> list[DecodedFill]:
        ready = [
            fill
            for fill in self._pending.values()
            if head_block >= fill.block_number + self.confirmations
        ]
        for fill in ready:
            self._pending.pop(fill.durable_trade_id, None)
        return sorted(ready, key=lambda fill: (fill.block_number, fill.log_index))

    def __len__(self) -> int:
        return len(self._pending)


class MetadataResolver:
    """Read token metadata from the existing archive; never fetch or mutate it."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._mtime_ns: int | None = None
        self._tokens: dict[str, dict[str, Any]] = {}

    def _refresh(self) -> None:
        try:
            mtime_ns = self.path.stat().st_mtime_ns
        except OSError:
            self._tokens = {}
            self._mtime_ns = None
            return
        if mtime_ns == self._mtime_ns:
            return
        try:
            raw = json.loads(self.path.read_text())
            tokens = raw.get("tokens") if isinstance(raw, dict) else None
            self._tokens = tokens if isinstance(tokens, dict) else {}
            self._mtime_ns = mtime_ns
        except (OSError, ValueError, TypeError):
            self._tokens = {}
            self._mtime_ns = mtime_ns

    def resolve(self, token_id: str) -> dict[str, Any] | None:
        self._refresh()
        meta = self._tokens.get(str(token_id))
        return dict(meta) if isinstance(meta, dict) else None


@dataclass(frozen=True)
class DataApiMatch:
    status: str
    fill: DecodedFill | None
    candidate_count: int


def match_data_api_trade(
    trade: dict[str, Any], decoded_fills: Iterable[DecodedFill]
) -> DataApiMatch:
    """Map a Data API trade to one canonical txHash+logIndex without guessing."""

    tx_hash = str(
        trade.get("transactionHash") or trade.get("transaction_hash") or ""
    ).lower()
    wallet = _norm_address(
        trade.get("proxyWallet") or trade.get("proxy_wallet") or trade.get("wallet")
    )
    token = str(trade.get("asset") or trade.get("token_id") or trade.get("tokenId") or "")
    side = str(trade.get("side") or "").upper()
    try:
        size = float(trade.get("size"))
        price = float(trade.get("price"))
    except (TypeError, ValueError):
        return DataApiMatch("invalid_data_api_trade", None, 0)

    owner_candidates = [
        fill
        for fill in decoded_fills
        if fill.transaction_hash == tx_hash and fill.maker == wallet
    ]
    exact = [
        fill
        for fill in owner_candidates
        if fill.token_id == token
        and fill.side == side
        and abs(fill.size - size) <= max(0.000001, abs(size) * 0.000001)
        and abs(fill.price - price) <= 0.000001
    ]
    if len(exact) == 1:
        return DataApiMatch("matched", exact[0], 1)
    if len(exact) > 1:
        return DataApiMatch("ambiguous", None, len(exact))
    return DataApiMatch("no_exact_match", None, len(owner_candidates))


@dataclass(frozen=True)
class OnchainShadowConfig:
    wss_rpc_url: str
    http_rpc_url: str
    confirmations: int
    reconnect_seconds: float
    initial_backfill_blocks: int
    max_backfill_blocks: int
    heartbeat_seconds: int
    api_tail_seconds: int
    output_path: Path
    heartbeat_path: Path
    allowlist_path: Path
    markets_path: Path
    archive_dir: Path
    backfill_chunk_blocks: int = 50

    @classmethod
    def from_env(cls) -> "OnchainShadowConfig":
        paper_dir = CONFIG.runs_dir / "paper"
        shadow_dir = CONFIG.runs_dir / "onchain_shadow"
        archive_dir = CONFIG.runs_dir / "book_archive"
        return cls(
            wss_rpc_url=os.getenv(
                "POLYGON_WSS_RPC_URL", "wss://polygon-bor-rpc.publicnode.com"
            ),
            http_rpc_url=os.getenv(
                "POLYGON_HTTP_RPC_URL", "https://polygon-bor-rpc.publicnode.com"
            ),
            confirmations=int(os.getenv("ONCHAIN_CONFIRMATIONS", "6")),
            reconnect_seconds=float(os.getenv("ONCHAIN_RECONNECT_SECONDS", "5")),
            initial_backfill_blocks=int(os.getenv("ONCHAIN_INITIAL_BACKFILL_BLOCKS", "256")),
            max_backfill_blocks=int(os.getenv("ONCHAIN_MAX_BACKFILL_BLOCKS", "2048")),
            heartbeat_seconds=int(os.getenv("ONCHAIN_HEARTBEAT_SECONDS", "60")),
            api_tail_seconds=int(os.getenv("ONCHAIN_API_TAIL_SECONDS", "15")),
            output_path=Path(
                os.getenv(
                    "ONCHAIN_SHADOW_LOG", str(shadow_dir / "shadow_onchain.jsonl")
                )
            ),
            heartbeat_path=Path(
                os.getenv(
                    "ONCHAIN_SHADOW_HEARTBEAT",
                    str(shadow_dir / "heartbeat.json"),
                )
            ),
            allowlist_path=Path(
                os.getenv("ONCHAIN_ALLOWLIST_PATH", str(paper_dir / "allowlist.json"))
            ),
            markets_path=Path(
                os.getenv(
                    "ONCHAIN_MARKETS_PATH", str(archive_dir / "markets_latest.json")
                )
            ),
            archive_dir=Path(
                os.getenv("ONCHAIN_ARCHIVE_DIR", str(archive_dir))
            ),
            backfill_chunk_blocks=int(
                os.getenv("ONCHAIN_BACKFILL_CHUNK_BLOCKS", "50")
            ),
        )

    def validate(self) -> None:
        if not self.wss_rpc_url.startswith(("wss://", "ws://")):
            raise ValueError("POLYGON_WSS_RPC_URL must be a websocket URL")
        if not self.http_rpc_url.startswith(("https://", "http://")):
            raise ValueError("POLYGON_HTTP_RPC_URL must be an HTTP URL")
        if self.confirmations < 1:
            raise ValueError("ONCHAIN_CONFIRMATIONS must be at least 1")
        if self.max_backfill_blocks < self.initial_backfill_blocks:
            raise ValueError("max backfill must cover initial backfill")
        if self.backfill_chunk_blocks < 1:
            raise ValueError("backfill chunk size must be positive")
