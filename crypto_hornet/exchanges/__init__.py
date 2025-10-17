"""Exchange registry."""
from __future__ import annotations

from typing import Iterable

from . import base
from . import binance, bingx, bitget, bybit, gate, mexc, okx

__all__ = ["build_feeds", "base"]


def build_feeds(default_interval: int) -> Iterable[base.Feed]:
    interval = max(30, default_interval)
    return [
        base.Feed("binance", "spot", interval, binance.spot),
        base.Feed("binance", "futures", interval, binance.futures),
        base.Feed("okx", "spot", interval, okx.spot),
        base.Feed("okx", "futures", interval, okx.futures),
        base.Feed("gate", "spot", interval, gate.spot),
        base.Feed("gate", "futures", interval, gate.futures),
        base.Feed("bitget", "spot", interval, bitget.spot),
        base.Feed("bitget", "futures", interval, bitget.futures),
        base.Feed("mexc", "futures", interval, mexc.futures),
        base.Feed("bingx", "spot", interval, bingx.spot),
        base.Feed("bingx", "futures", interval, bingx.futures),
        base.Feed("bybit", "spot", interval, bybit.spot),
        base.Feed("bybit", "futures", interval, bybit.futures),
    ]