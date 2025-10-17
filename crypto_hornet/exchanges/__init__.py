"""Exchange registry."""
from __future__ import annotations

from typing import Iterable

from . import base
from . import binance, bingx, bitget, bybit, gate, kucoin, mexc, okx

__all__ = ["build_feeds", "base"]


def _speed_tier(exchange: str, market: str) -> str:
    tier_map = {
        ("binance", "spot"): "Tier 1 — Binance announcements mirror",
        ("binance", "futures"): "Tier 1 — Binance announcements mirror",
        ("mexc", "futures"): "Tier 4 — Scraping/API fallback",
    }
    return tier_map.get((exchange, market), "Tier 2 — Fast API polling")


def _source_hint(exchange: str, market: str) -> str:
    if exchange == "binance" and market == "spot":
        return "Binance exchangeInfo (REST)"
    if exchange == "binance" and market == "futures":
        return "Binance futures exchangeInfo"
    if exchange == "okx":
        inst_type = "SPOT" if market == "spot" else "SWAP"
        return f"OKX instruments API ({inst_type})"
    if exchange == "gate" and market == "spot":
        return "Gate.io currency_pairs API"
    if exchange == "gate":
        return "Gate.io futures contracts API"
    if exchange == "bitget" and market == "spot":
        return "Bitget products API"
    if exchange == "bitget":
        return "Bitget mix contracts API"
    if exchange == "kucoin" and market == "spot":
        return "KuCoin symbols API"
    if exchange == "kucoin":
        return "KuCoin futures contracts API"
    if exchange == "mexc":
        return "MEXC contract detail/list API"
    if exchange == "bingx" and market == "spot":
        return "BingX spot symbols API"
    if exchange == "bingx":
        return "BingX swap contracts API"
    if exchange == "bybit" and market == "spot":
        return "Bybit spot instruments API"
    if exchange == "bybit":
        return "Bybit futures instruments API"
    return "Exchange API"

def build_feeds(default_interval: int) -> Iterable[base.Feed]:
    interval = max(10, default_interval)
    raw_feeds: Iterable[tuple[str, str, base.Fetcher]] = [
        ("binance", "spot", binance.spot),
        ("binance", "futures", binance.futures),
        ("okx", "spot", okx.spot),
        ("okx", "futures", okx.futures),
        ("gate", "spot", gate.spot),
        ("gate", "futures", gate.futures),
        ("bitget", "spot", bitget.spot),
        ("bitget", "futures", bitget.futures),
        ("kucoin", "spot", kucoin.spot),
        ("kucoin", "futures", kucoin.futures),
        ("mexc", "futures", mexc.futures),
        ("bingx", "spot", bingx.spot),
        ("bingx", "futures", bingx.futures),
        ("bybit", "spot", bybit.spot),
        ("bybit", "futures", bybit.futures),
    ]
    return [
        base.Feed(
            exchange,
            market,
            interval,
            fetch,
            _source_hint(exchange, market),
            _speed_tier(exchange, market),
        )
        for exchange, market, fetch in raw_feeds
    ]