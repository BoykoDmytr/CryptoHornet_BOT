"""KuCoin exchange fetchers."""
from __future__ import annotations

from typing import Dict

import httpx

from .base import Snapshot, get_json


def _is_true(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() == "true"
    return bool(value)


def _format_pair(base: str | None, quote: str | None) -> str | None:
    if not base or not quote:
        return None
    base = base.upper()
    quote = quote.upper()
    return f"{base}/{quote}"


async def spot(client: httpx.AsyncClient) -> Snapshot:
    payload = await get_json(client, "https://api.kucoin.com/api/v2/symbols")
    data = payload.get("data", []) if isinstance(payload, dict) else []
    out: Dict[str, str] = {}
    for entry in data:
        if not isinstance(entry, dict):
            continue
        if not _is_true(entry.get("enableTrading")):
            continue
        pair = _format_pair(entry.get("baseCurrency"), entry.get("quoteCurrency"))
        if not pair:
            continue
        base, quote = pair.split("/")
        out[pair] = f"https://www.kucoin.com/trade/{base}-{quote}"
    return out


async def futures(client: httpx.AsyncClient) -> Snapshot:
    payload = await get_json(client, "https://api-futures.kucoin.com/api/v1/contracts/active")
    data = payload.get("data", []) if isinstance(payload, dict) else []
    out: Dict[str, str] = {}
    for entry in data:
        if not isinstance(entry, dict):
            continue
        if not _is_true(entry.get("isActive")):
            continue
        pair = _format_pair(entry.get("baseCurrency"), entry.get("quoteCurrency"))
        if not pair:
            continue
        symbol = entry.get("symbol")
        if isinstance(symbol, str) and symbol:
            url = f"https://futures.kucoin.com/trade/{symbol}"
        else:
            base, quote = pair.split("/")
            url = f"https://futures.kucoin.com/trade/{base}-{quote}"
        out[pair] = url
    return out