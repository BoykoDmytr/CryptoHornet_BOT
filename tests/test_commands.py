from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys

import pytest

# Ensure the project root is importable when running tests locally.
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from crypto_hornet.commands import (
    LATEST_EXCHANGES,
    LATEST_MARKETS,
    format_latest_summary,
    parse_simulate_listing,
    _select_latest,
)
from crypto_hornet.state import PostedRecord


def make_record(exchange: str, market: str, pair: str, offset_minutes: int) -> PostedRecord:
    return PostedRecord(
        exchange=exchange,
        market=market,
        pair=pair,
        message_id=1,
        chat_id=2,
        posted_at=datetime(2024, 1, 1, tzinfo=timezone.utc) + timedelta(minutes=offset_minutes),
    )


def test_parse_simulate_listing_allows_bot_suffix() -> None:
    exchange, market, pair = parse_simulate_listing("/simulate_listing@hornet_bot binance futures btc/usdt")
    assert exchange == "binance"
    assert market == "futures"
    assert pair == "BTC/USDT"


def test_parse_simulate_listing_requires_arguments() -> None:
    with pytest.raises(ValueError):
        parse_simulate_listing("/simulate_listing binance")


def test_format_latest_summary_includes_all_exchanges() -> None:
    records = {
        ("bingx", "spot"): make_record("bingx", "spot", "BTC/USDT", 0),
        ("bitget", "futures"): make_record("bitget", "futures", "ETH/USDT", 5),
    }
    summary = format_latest_summary(records)
    lines = summary.splitlines()

    assert lines[0] == "📊 Latest listings overview"

    expected_sections = [f"{exchange.upper()}:" for exchange in LATEST_EXCHANGES]
    for section in expected_sections:
        assert section in lines

    assert "  • SPOT: BTC/USDT (2024-01-01 00:00 UTC)" in lines
    assert "  • FUTURES: ETH/USDT (2024-01-01 00:05 UTC)" in lines

    for exchange in LATEST_EXCHANGES:
        for market in LATEST_MARKETS:
            if (exchange, market) not in records:
                marker = f"  • {market.upper()}: no records"
                assert marker in lines


def test_select_latest_prefers_newer_records() -> None:
    older = make_record("gate", "spot", "ABC/USDT", 0)
    newer = make_record("gate", "spot", "XYZ/USDT", 10)
    latest = _select_latest([older, newer])
    assert latest[("gate", "spot")].pair == "XYZ/USDT"