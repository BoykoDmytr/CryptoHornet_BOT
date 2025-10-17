"""Binance exchange fetchers."""
from __future__ import annotations

from typing import Dict

import httpx

from .base import Snapshot, get_json


async def spot(client: httpx.AsyncClient) -> Snapshot:
    payload = await get_json(client, "https://api.binance.com/api/v3/exchangeInfo")
    data = payload if isinstance(payload, dict) else {}
    out: Dict[str, str] = {}
    for symbol in data.get("symbols", []):
        if not isinstance(symbol, dict):
            continue
        if symbol.get("status") != "TRADING":
            continue
        base = (symbol.get("baseAsset") or "").upper()
        quote = (symbol.get("quoteAsset") or "").upper()
        if not base or not quote:
            continue
        out[f"{base}/{quote}"] = f"https://www.binance.com/en/trade/{base}_{quote}"
    return out


async def futures(client: httpx.AsyncClient) -> Snapshot:
    payload = await get_json(client, "https://fapi.binance.com/fapi/v1/exchangeInfo")
    data = payload if isinstance(payload, dict) else {}
    out: Dict[str, str] = {}
    for symbol in data.get("symbols", []):
        if not isinstance(symbol, dict):
            continue
        if symbol.get("status") != "TRADING":
            continue
        if symbol.get("contractType") != "PERPETUAL":
            continue
        base = (symbol.get("baseAsset") or "").upper()
        quote = (symbol.get("quoteAsset") or "").upper()
        if not base or not quote:
            continue
        out[f"{base}/{quote}"] = f"https://www.binance.com/en/futures/{base}{quote}"
    return out