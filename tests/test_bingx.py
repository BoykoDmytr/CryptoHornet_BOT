"""Tests for the BingX exchange helpers."""

import asyncio
from pathlib import Path
import sys

import httpx
import pytest

# Ensure the project root is importable when running tests locally.
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from crypto_hornet.exchanges import bingx


def test_spot_parses_multiple_symbol_formats(monkeypatch):
    payloads = [
        {
            "code": 0,
            "data": [
                {"symbol": "BTC-USDT"},
                {"baseAsset": "eth", "quoteAsset": "usdt"},
                {"baseCurrency": "xrp", "quoteCurrency": "usdt"},
                {"symbols": [{"symbolName": "DOGE_USDT"}]},
            ],
        }
    ]

    async def fake_get_json(client, url, *, params=None, headers=None):
        return payloads.pop(0)

    monkeypatch.setattr(bingx, "get_json", fake_get_json)

    async def _run() -> dict[str, str]:
        async with httpx.AsyncClient() as client:
            return await bingx.spot(client)

    snapshot = asyncio.run(_run())

    assert snapshot == {
        "BTC/USDT": "https://bingx.com/en-us/spot/BTC_USDT",
        "ETH/USDT": "https://bingx.com/en-us/spot/ETH_USDT",
        "XRP/USDT": "https://bingx.com/en-us/spot/XRP_USDT",
        "DOGE/USDT": "https://bingx.com/en-us/spot/DOGE_USDT",
    }