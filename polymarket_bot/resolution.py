from __future__ import annotations

"""On-chain settlement resolver for Polymarket paper positions.

Polymarket does not expose per-token resolution status through any public REST endpoint:
  - Gamma /markets?condition_id= returns a curated 20-row page that ignores the filter
  - Gamma /markets?clob_token_ids= returns empty for archived tokens
  - Gamma /markets-by-token/{token} returns 404 (despite OpenAPI docs)
  - CLOB /markets-by-token/{token} returns the condition_id but no outcome data

The only reliable ground truth is the **ConditionalTokens contract on Polygon
(0x4D97DCd97eC945f40cF65F87097ACe5EA0476045)**. Two view functions are enough:

  - payoutDenominator(conditionId) -> uint256
        > 0 means the market is settled. Returns the denominator in /
          (typically 1, but a few multi-outcome markets use other values).
  - payoutNumerators(conditionId, i) -> uint256
        Returns the per-index payout numerator in /.
        Index 0 is PRIMARY (the YES-equivalent side, typically what we hold
        when we long via paper follower). Index 1 is SECONDARY (NO-equivalent).

For multi-outcome markets (rare for our paper follower scope), use
getOutcomeSlotCount(conditionId) to discover the array length.

This module wraps:
  - Mapping a CLOB token_id -> {condition_id, primary, secondary} via CLOB REST.
    Cached on disk because token/condition pairings never change once issued.
  - Batch payoutDenominator + payoutNumerators(0) + payoutNumerators(1) reads
    via Multicall3 (0xcA11bde05977b3631167028862bE2a173976CA11) on Polygon.
  - Exposes resolved_outcome_for_token() with the same dict shape used elsewhere.

RPC endpoints are public, no keys required, read-only.
"""

import json
import logging
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from eth_abi.abi import decode, encode
from eth_utils.crypto import keccak

from .config import CONFIG, BotConfig
from .http import ApiError, get_json

LOG = logging.getLogger("polymarket_resolution")

# On-chain ground truth: Polymarket ConditionalTokens (CTF v1)
CTF_ADDRESS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
MULTICALL3_ADDRESS = "0xcA11bde05977b3631167028862bE2a173976CA11"

# Polygon RPCs — free public endpoints, no keys required. Tested in 2026-07.
# Triage order: first one to succeed wins; fall back on timeout/401/403/5xx.
DEFAULT_RPCS = [
    "https://polygon.gateway.tenderly.co",
    "https://polygon-rpc.com",
    "https://polygon-bor-rpc.publicnode.com",
    "https://polygon.drpc.org",
    "https://polygon.llamarpc.com",
]

# Function selectors (computed via keccak256 of canonical signature).
def _selector(sig: str) -> str:
    return keccak(text=sig)[:4].hex()


SEL_PAYOUT_DENOM = _selector("payoutDenominator(bytes32)")
SEL_PAYOUT_NUMS = _selector("payoutNumerators(bytes32,uint256)")
SEL_TRY_AGG = _selector("tryAggregate(bool,(address,bytes)[])")


# ---------------------------------------------------------------------------
# Token -> ConditionId mapping (cached on disk)
# ---------------------------------------------------------------------------

def map_token_to_condition(token_id: str, *, config: BotConfig = CONFIG) -> dict[str, Any] | None:
    """Resolve a CLOB token_id to {condition_id, primary_token_id, secondary_token_id}.

    Returns None if the CLOB endpoint doesn't recognize the token.

    These pairings are immutable on issuance — cache to disk per token.
    """
    if not token_id:
        return None
    url = f"{config.clob_base}/markets-by-token/{urllib.parse.quote(token_id)}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": config.user_agent, "Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=10) as r:
            data = json.loads(r.read())
    except Exception as exc:
        LOG.debug("clob markets-by-token failed for %s: %s", token_id[:24], exc)
        return None
    if not isinstance(data, dict):
        return None
    cid = data.get("condition_id")
    primary = data.get("primary_token_id")
    secondary = data.get("secondary_token_id")
    if not (cid and primary and secondary):
        return None
    return {
        "condition_id": str(cid),
        "primary_token_id": str(primary),
        "secondary_token_id": str(secondary),
    }


def token_maps_cache_path(paper_dir: Path) -> Path:
    return Path(paper_dir) / "token_maps.json"


@dataclass
class TokenMap:
    """Persistent cache of {token_id: {condition_id, primary_token_id, secondary_token_id}}.

    Immutable per issue; safe to keep on disk forever.
    """
    paper_dir: Path
    _cache: dict[str, dict[str, Any]]

    @classmethod
    def load(cls, paper_dir: Path) -> "TokenMap":
        path = token_maps_cache_path(paper_dir)
        data: dict[str, dict[str, Any]] = {}
        if path.exists():
            try:
                data = json.loads(path.read_text())
            except Exception:
                LOG.warning("token_maps cache unreadable; rebuilding")
                data = {}
        return cls(paper_dir=paper_dir, _cache=data)

    def save(self) -> None:
        self.paper_dir.mkdir(parents=True, exist_ok=True)
        tmp = token_maps_cache_path(self.paper_dir).with_suffix(".tmp")
        tmp.write_text(json.dumps(self._cache, indent=2, sort_keys=True))
        tmp.replace(self._cache_path())

    def _cache_path(self) -> Path:
        return token_maps_cache_path(self.paper_dir)

    def get(self, token_id: str) -> dict[str, Any] | None:
        return self._cache.get(token_id)

    def put(self, token_id: str, info: dict[str, Any]) -> None:
        self._cache[token_id] = info

    def items(self) -> Iterable[tuple[str, dict[str, Any]]]:
        return self._cache.items()

    def ensure_filled(self, token_ids: Iterable[str], *, config: BotConfig = CONFIG) -> tuple[int, int]:
        """Fill cache for any missing token_ids via the CLOB endpoint. Returns (filled, failed)."""
        missing = [t for t in token_ids if t and t not in self._cache]
        filled = 0
        failed = 0
        for tok in missing:
            info = map_token_to_condition(tok, config=config)
            if info is None:
                failed += 1
                continue
            self._cache[tok] = info
            filled += 1
        if filled or failed:
            self.save()
        return filled, failed


# ---------------------------------------------------------------------------
# Polygon RPC client with endpoint failover
# ---------------------------------------------------------------------------

class RpcClient:
    """Minimal JSON-RPC client with endpoint failover. Read-only."""

    def __init__(self, endpoints: list[str] | None = None, *, timeout: float = 12.0, user_agent: str | None = None) -> None:
        self.endpoints = list(endpoints or DEFAULT_RPCS)
        self.timeout = timeout
        self.user_agent = user_agent or CONFIG.user_agent
        self._endpoint_idx = 0

    def _next(self) -> str:
        return self.endpoints[self._endpoint_idx % len(self.endpoints)]

    def rotate(self) -> None:
        self._endpoint_idx += 1

    def eth_call(self, to: str, data: str) -> str:
        """Returns the result hex string, or raises on failure (after rotating)."""
        payload = {
            "jsonrpc": "2.0",
            "method": "eth_call",
            "params": [{"to": to, "data": data}, "latest"],
            "id": 1,
        }
        body = json.dumps(payload).encode("utf-8")
        attempts = 0
        while attempts < len(self.endpoints):
            ep = self._next()
            try:
                req = urllib.request.Request(ep, data=body, headers={"Content-Type": "application/json", "User-Agent": self.user_agent})
                with urllib.request.urlopen(req, timeout=self.timeout) as r:
                    resp = json.loads(r.read())
                if "result" in resp:
                    return resp["result"]
                err = resp.get("error") or {}
                LOG.warning("RPC error from %s: %s", ep, str(err)[:120])
                self.rotate()
            except urllib.error.HTTPError as e:
                LOG.warning("RPC HTTP %s from %s", e.code, ep)
                self.rotate()
            except Exception as e:
                LOG.warning("RPC failure from %s: %s", ep, str(e)[:80])
                self.rotate()
            attempts += 1
        raise ApiError("all Polygon RPC endpoints exhausted")


# ---------------------------------------------------------------------------
# Calldata helpers
# ---------------------------------------------------------------------------

def encode_payout_denominator_call(condition_id: str) -> bytes:
    cb = bytes.fromhex(condition_id[2:].lower())
    return bytes.fromhex(SEL_PAYOUT_DENOM) + encode(["bytes32"], [cb])


def encode_payout_numerators_call(condition_id: str, index: int) -> bytes:
    cb = bytes.fromhex(condition_id[2:].lower())
    return bytes.fromhex(SEL_PAYOUT_NUMS) + encode(["bytes32", "uint256"], [cb, index])


def encode_try_aggregate(calls: list[tuple[bytes, bytes]]) -> bytes:
    """Build calldata for Multicall3.tryAggregate(true, calls).

    Each call is (address_bytes20, calldata_bytes).
    The 1-byte 'true' bool is appended as a 32-byte word.
    """
    return bytes.fromhex(SEL_TRY_AGG) + encode(
        ["bool", "(address,bytes)[]"],
        [True, [(a, c) for a, c in calls]],
    )


def decode_uint256(calldata_response: str) -> int:
    """Decode a single uint256 return — string of 32-byte padded hex (optionally 0x-prefixed)."""
    h = calldata_response[2:] if calldata_response.startswith("0x") else calldata_response
    raw = bytes.fromhex(h)
    if len(raw) != 32:
        # tryAggregate returns ABI-encoded (bool, bytes) — unwrap if needed
        if len(raw) > 32 and raw[31] == 0 and len(raw) >= 64:
            # naive: take last 32 bytes
            raw = raw[-32:]
        else:
            raise ValueError(f"unexpected uint256 length {len(raw)}")
    return int.from_bytes(raw, "big", signed=False)


def decode_try_aggregate_result(response: str) -> list[tuple[bool, bytes]]:
    """Decode `tryAggregate(true, [...])` -> list of (success, returnData).

    The Multicall3 contract returns `(bool success, bytes returnData)[]] — no blockNumber.
    """
    raw = bytes.fromhex(response[2:] if response.startswith("0x") else response)
    decoded = decode(["(bool,bytes)[]"], raw)
    return [(bool(ok), bytes(data)) for ok, data in decoded[0]]


# ---------------------------------------------------------------------------
# Batch reads (multicall)
# ---------------------------------------------------------------------------

def batch_payout_denominators(condition_ids: list[str], *, rpc: RpcClient | None = None) -> dict[str, int]:
    """Multicall payoutDenominator for many condition_ids. Returns {cid: denom}."""
    if not condition_ids:
        return {}
    rpc = rpc or RpcClient()
    # Cap at 200 per round to keep calldata reasonable
    out: dict[str, int] = {}
    chunk_size = 200
    for i in range(0, len(condition_ids), chunk_size):
        chunk = condition_ids[i:i + chunk_size]
        calls = [(bytes.fromhex(CTF_ADDRESS[2:].lower()), encode_payout_denominator_call(c)) for c in chunk]
        calldata = encode_try_aggregate(calls)
        result = rpc.eth_call(MULTICALL3_ADDRESS, "0x" + calldata.hex())
        rows = decode_try_aggregate_result(result)
        for cid, (success, return_data) in zip(chunk, rows):
            if not success or not return_data:
                out[cid] = 0
                continue
            try:
                out[cid] = decode_uint256("0x" + return_data.hex())
            except Exception:
                out[cid] = 0
    return out


def batch_payout_numerators(condition_ids: list[str], index: int, *, rpc: RpcClient | None = None) -> dict[str, int]:
    """Multicall payoutNumerators(conditionId, index). Returns {cid: numerator}."""
    if not condition_ids:
        return {}
    rpc = rpc or RpcClient()
    out: dict[str, int] = {}
    chunk_size = 200
    for i in range(0, len(condition_ids), chunk_size):
        chunk = condition_ids[i:i + chunk_size]
        calls = [(bytes.fromhex(CTF_ADDRESS[2:].lower()), encode_payout_numerators_call(c, index)) for c in chunk]
        calldata = encode_try_aggregate(calls)
        result = rpc.eth_call(MULTICALL3_ADDRESS, "0x" + calldata.hex())
        rows = decode_try_aggregate_result(result)
        for cid, (success, return_data) in zip(chunk, rows):
            if not success or not return_data:
                out[cid] = 0
                continue
            try:
                out[cid] = decode_uint256("0x" + return_data.hex())
            except Exception:
                out[cid] = 0
    return out


# ---------------------------------------------------------------------------
# High-level: token -> resolved outcome dict (matches API of gamma.resolved_outcome_for_token)
# ---------------------------------------------------------------------------

def resolved_outcome_for_token(
    token_id: str,
    *,
    token_map: TokenMap | None = None,
    rpc: RpcClient | None = None,
    config: BotConfig = CONFIG,
) -> dict[str, Any] | None:
    """On-chain resolution outcome for a CLOB token id.

    Returns a dict with the same shape as the Gamma resolver:
        {"resolved": bool, "resolution_status": "YES"|"NO"|<num_string>,
         "side": "PRIMARY"|"SECONDARY"|"UNKNOWN", "question": None,
         "market_id": None, "condition_id": str,
         "closed": bool, "active": None, "raw": {}}
    """
    if not token_id:
        return None

    # Step 1: Get the token -> condition_id mapping (cached)
    if token_map is not None:
        info = token_map.get(token_id)
        if info is None:
            info = map_token_to_condition(token_id, config=config)
            if info is None:
                return None
            token_map.put(token_id, info)
            token_map.save()
    else:
        info = map_token_to_condition(token_id, config=config)
        if info is None:
            return None

    cid = info["condition_id"]
    primary = info["primary_token_id"]
    secondary = info["secondary_token_id"]

    # Step 2: Batch on-chain reads
    rpc = rpc or RpcClient()
    denom_map = batch_payout_denominators([cid], rpc=rpc)
    denom = denom_map.get(cid, 0)
    if denom == 0:
        return {
            "resolved": False,
            "resolution_status": None,
            "side": _token_side(token_id, primary, secondary),
            "question": None,
            "market_id": None,
            "condition_id": cid,
            "closed": False,
            "active": True,
            "raw": info,
        }

    nums = batch_payout_numerators([cid], 0, rpc=rpc)
    nums_1 = batch_payout_numerators([cid], 1, rpc=rpc)
    n0 = nums.get(cid, 0)
    n1 = nums_1.get(cid, 0)

    # Determine which side won
    side_primary_won = n0 > 0
    side_secondary_won = n1 > 0
    status_str = "PRIMARY" if side_primary_won else "SECONDARY" if side_secondary_won else f"0/{denom}"

    return {
        "resolved": True,
        "resolution_status": status_str,
        "side": _token_side(token_id, primary, secondary),
        "question": None,
        "market_id": None,
        "condition_id": cid,
        "closed": True,
        "active": False,
        "raw": {**info, "denom": denom, "n0": n0, "n1": n1},
    }


def _token_side(token_id: str, primary: str, secondary: str) -> str:
    t = token_id.lower()
    if t == primary.lower():
        return "PRIMARY"
    if t == secondary.lower():
        return "SECONDARY"
    return "UNKNOWN"


# ---------------------------------------------------------------------------
# Standalone driver — settle all known positions in batch
# ---------------------------------------------------------------------------

def settle_open_positions(
    state: dict[str, Any],
    *,
    paper_dir: Path,
    config: BotConfig = CONFIG,
    rpc: RpcClient | None = None,
) -> dict[str, Any]:
    """Walk all open positions in state, mark any resolved positions in-place.

    Returns a summary with keys: filled, failed, resolved_open, by_token [{...}, ...].
    - Fills token_map cache (writes to disk).
    - Settles positions: writes a 'resolution' row-able dict to summary
      instead of mutating state directly. Caller can build ledger rows.

    Does NOT modify state.json or ledger.jsonl — caller is responsible for that.
    """
    paper_dir = Path(paper_dir)
    token_map = TokenMap.load(paper_dir)
    rpc = rpc or RpcClient()
    positions = state.get("positions", {}) if isinstance(state.get("positions"), dict) else {}

    distinct_tokens = sorted({str(p.get("token")) for p in positions.values() if isinstance(p, dict) and p.get("token")})

    if not distinct_tokens:
        return {"checked": 0, "resolved": 0, "skipped": 0, "by_token": []}

    # Step 1: ensure token -> condition_id cache
    missing = [t for t in distinct_tokens if not token_map.get(t)]
    if missing:
        LOG.info("filling token_map for %d new tokens", len(missing))
        for tok in missing:
            info = map_token_to_condition(tok, config=config)
            if info:
                token_map.put(tok, info)
            # tiny throttle so we don't hammer the CLOB endpoint
            time.sleep(0.05)
        token_map.save()

    # Step 2: gather all condition_ids we need to query
    cid_to_tokens: dict[str, list[str]] = {}
    for tok in distinct_tokens:
        info = token_map.get(tok)
        if not info:
            continue
        cid_to_tokens.setdefault(info["condition_id"], []).append(tok)

    cids = sorted(cid_to_tokens.keys())
    LOG.info("settling %d open positions across %d condition_ids", sum(len(v) for v in cid_to_tokens.values()), len(cids))

    # Step 3: read all denominators + numerators(0) + numerators(1) via multicall
    denom_map = batch_payout_denominators(cids, rpc=rpc)
    # Only fetch numerators for those with denom > 0 (resolved)
    resolved_cids = [c for c, d in denom_map.items() if d > 0]
    LOG.info("resolved condition_ids: %d / %d", len(resolved_cids), len(cids))

    num0_map = batch_payout_numerators(resolved_cids, 0, rpc=rpc) if resolved_cids else {}
    num1_map = batch_payout_numerators(resolved_cids, 1, rpc=rpc) if resolved_cids else {}

    # Step 4: walk positions and emit outcomes
    by_token = []
    resolved_count = 0
    skipped = 0
    for pos_id, pos in positions.items():
        if not isinstance(pos, dict):
            continue
        token = str(pos.get("token") or "")
        if not token:
            continue
        info = token_map.get(token)
        if not info:
            skipped += 1
            by_token.append({"pos_id": pos_id, "token": token, "action": "skip", "reason": "no_condition_id"})
            continue
        cid = info["condition_id"]
        denom = denom_map.get(cid, 0)
        if denom == 0:
            skipped += 1
            by_token.append({"pos_id": pos_id, "token": token, "action": "skip", "reason": "not_resolved", "condition_id": cid})
            continue

        primary = info["primary_token_id"]
        n0 = num0_map.get(cid, 0)
        n1 = num1_map.get(cid, 0)
        idx_primary_won = n0 > 0
        idx_secondary_won = n1 > 0
        our_side = _token_side(token, primary, info["secondary_token_id"])

        if our_side == "PRIMARY":
            payout = float(n0) / float(denom)
            won = idx_primary_won
        elif our_side == "SECONDARY":
            payout = float(n1) / float(denom)
            won = idx_secondary_won
        else:
            payout = 0.0
            won = False

        entry_price = float(pos.get("entry_price") or 0.0)
        shares = float(pos.get("shares") or 0.0)
        cost_usd = float(pos.get("cost_usd") or 0.0)
        proceeds = payout * shares * 1.0  # CTF payout is 1 USD per share if won
        pnl = proceeds - cost_usd

        by_token.append({
            "pos_id": pos_id,
            "token": token,
            "wallet": pos.get("wallet"),
            "condition_id": cid,
            "side": our_side,
            "won": won,
            "payout_per_share": payout,
            "denom": denom,
            "n0": n0,
            "n1": n1,
            "entry_price": entry_price,
            "shares": shares,
            "cost_usd": cost_usd,
            "proceeds": proceeds,
            "pnl": pnl,
            "action": "resolve",
        })
        resolved_count += 1

    summary = {
        "checked": sum(1 for r in by_token if r["action"] != "skip"),
        "resolved": resolved_count,
        "skipped": skipped,
        "by_token": by_token,
        "rpc_endpoints_attempted": len(rpc.endpoints),
        "last_checked_at": time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime()),
    }
    return summary
