from __future__ import annotations

import json
from pathlib import Path

import pytest

from polymarket_bot.onchain_shadow import (
    CTF_EXCHANGE_V2,
    ORDER_FILLED_TOPIC,
    ConfirmationBuffer,
    MetadataResolver,
    decode_order_filled,
    match_data_api_trade,
    tracked_wallet_role,
)

TRACKED = "0xfe787d2da716d60e8acff57fb87eb13cd4d10319"
TX_HASH = "0xa695db094e5c603e1e8d65d3dac1fe119475260ad8cffcbc31434ff752be4d99"
TOKEN_ID = "14276748550157131128264970654641660984562839966146774962941183022286746886582"

# Real CTF Exchange V2 logs from Polygon block 91,145,435.  These fixtures
# pin the official ABI and, critically, the difference between an order owner
# in topic[2] and a counterparty in topic[3].
TRACKED_MAKER_LOG = {
    "address": CTF_EXCHANGE_V2,
    "blockHash": "0x96e1620543203676bfd9f4c737f2ccbc959da558e31b91423c068153912219d4",
    "blockNumber": "0x56ec4db",
    "blockTimestamp": "0x6a6b6545",
    "data": "0x00000000000000000000000000000000000000000000000000000000000000001f905a735731ed23581f810122da24c492a5f1c7bc7284617094c3be495b21b6000000000000000000000000000000000000000000000000000000010e6a882f00000000000000000000000000000000000000000000000000000002335df10d000000000000000000000000000000000000000000000000000000000707e3c200000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
    "logIndex": "0x346",
    "removed": False,
    "topics": [
        ORDER_FILLED_TOPIC,
        "0x1e3ed4dd848b0b98c3485cd2c6691c9a08a771e90d89bf2c71ffc9b448233e59",
        "0x000000000000000000000000fe787d2da716d60e8acff57fb87eb13cd4d10319",
        "0x000000000000000000000000e111180000d2663c0091e4f400237545b87b996b",
    ],
    "transactionHash": TX_HASH,
    "transactionIndex": "0x51",
}

COUNTERPARTY_LOG = {
    "address": CTF_EXCHANGE_V2,
    "blockHash": TRACKED_MAKER_LOG["blockHash"],
    "blockNumber": TRACKED_MAKER_LOG["blockNumber"],
    "blockTimestamp": TRACKED_MAKER_LOG["blockTimestamp"],
    "data": "0x000000000000000000000000000000000000000000000000000000000000000079c15007afe0e0fc7639b9028a99005ea3f3fbee98d22284773a80a45759566c000000000000000000000000000000000000000000000000000000000b020ac000000000000000000000000000000000000000000000000000000000152b4fc0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
    "logIndex": "0x32c",
    "removed": False,
    "topics": [
        ORDER_FILLED_TOPIC,
        "0xa573161887d38b2bf4c10cc53308457da6347041963ec82c0c904d1cc6d3f375",
        "0x000000000000000000000000a697d0b3fff7d285a0f92d6ee03a7f97809e59d5",
        "0x000000000000000000000000fe787d2da716d60e8acff57fb87eb13cd4d10319",
    ],
    "transactionHash": TX_HASH,
    "transactionIndex": "0x51",
}


def test_official_v2_order_filled_topic_is_pinned() -> None:
    assert ORDER_FILLED_TOPIC == "0xd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee"


def test_decode_known_historical_fill_and_buy_side() -> None:
    fill = decode_order_filled(TRACKED_MAKER_LOG)
    assert fill.durable_trade_id == f"{TX_HASH}:838"
    assert fill.maker == TRACKED
    assert fill.side == "BUY"
    assert fill.token_id == TOKEN_ID
    assert fill.size == pytest.approx(9451.729165)
    assert fill.price == pytest.approx(0.48)
    assert fill.block_number == 91_145_435


def test_taker_topic_does_not_invert_leader_side() -> None:
    fill = decode_order_filled(COUNTERPARTY_LOG)
    role = tracked_wallet_role(fill, {TRACKED})
    assert role == "counterparty_only"
    assert fill.maker != TRACKED
    # The leader's own OrderFilled event is a separate log and is the only
    # event whose side/token may be treated as the leader's trade.
    assert tracked_wallet_role(decode_order_filled(TRACKED_MAKER_LOG), {TRACKED}) == "order_owner"


def test_decode_rejects_unknown_side() -> None:
    bad = dict(TRACKED_MAKER_LOG)
    words = [bad["data"][2 + i : 2 + i + 64] for i in range(0, len(bad["data"]) - 2, 64)]
    words[0] = f"{2:064x}"
    bad["data"] = "0x" + "".join(words)
    with pytest.raises(ValueError, match="side"):
        decode_order_filled(bad)


def test_confirmation_buffer_finalizes_and_records_removed_reorg() -> None:
    fill = decode_order_filled(TRACKED_MAKER_LOG)
    buf = ConfirmationBuffer(confirmations=3)
    buf.add(fill)
    assert buf.finalizable(fill.block_number + 2) == []
    assert buf.finalizable(fill.block_number + 3) == [(fill, "live")]
    buf.add(fill)
    removed = buf.remove(fill.durable_trade_id)
    assert removed == fill
    assert buf.remove(fill.durable_trade_id) is None


def test_metadata_resolver_reads_existing_archive_only(tmp_path: Path) -> None:
    path = tmp_path / "markets_latest.json"
    path.write_text(json.dumps({"tokens": {TOKEN_ID: {"question": "Known?", "outcome": "Yes"}}}))
    resolver = MetadataResolver(path)
    assert resolver.resolve(TOKEN_ID) == {"question": "Known?", "outcome": "Yes"}
    assert resolver.resolve("999") is None


def test_data_api_trade_matches_exact_owner_event() -> None:
    owner = decode_order_filled(TRACKED_MAKER_LOG)
    counterparty = decode_order_filled(COUNTERPARTY_LOG)
    trade = {
        "transactionHash": TX_HASH,
        "proxyWallet": TRACKED,
        "asset": TOKEN_ID,
        "side": "BUY",
        "size": 9451.729165,
        "price": 0.48,
    }
    result = match_data_api_trade(trade, [counterparty, owner])
    assert result.status == "matched"
    assert result.fill == owner
    assert result.candidate_count == 1


def test_data_api_trade_reports_mismatch_instead_of_guessing() -> None:
    trade = {
        "transactionHash": TX_HASH,
        "proxyWallet": TRACKED,
        "asset": "999",
        "side": "SELL",
        "size": 1,
        "price": 0.01,
    }
    result = match_data_api_trade(trade, [decode_order_filled(TRACKED_MAKER_LOG)])
    assert result.status == "no_exact_match"
    assert result.fill is None
