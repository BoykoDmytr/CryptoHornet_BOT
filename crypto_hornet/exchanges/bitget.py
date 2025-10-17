"""Bitget exchange fetchers."""
from __future__ import annotations

from typing import Dict

import httpx

from .base import Snapshot, get_json


async def spot(client: httpx.AsyncClient) -> Snapshot:
    payload = await get_json(client, "https://api.bitget.com/api/spot/v1/public/products")
    data = payload.get("data", []) if isinstance(payload, dict) else []
    out: Dict[str, str] = {}
    for item in data:
        if not isinstance(item, dict):
            continue
        base = (item.get("baseCoin") or "").upper()
        quote = (item.get("quoteCoin") or "").upper()
        if not base or not quote:
            continue
        out[f"{base}/{quote}"] = f"https://www.bitget.com/spot/{base}{quote}"
    return out


async def futures(client: httpx.AsyncClient) -> Snapshot:
    payload = await get_json(client, "https://api.bitget.com/api/mix/v1/market/contracts", params={"productType": "umcbl"})
    data = payload.get("data", []) if isinstance(payload, dict) else []
    out: Dict[str, str] = {}
    for item in data:
        if not isinstance(item, dict):
            continue
        symbol = (item.get("symbol") or "").upper()
        if "USDT" not in symbol:
            continue
        base = symbol.split("USDT")[0]
        out[f"{base}/USDT"] = f"https://www.bitget.com/mix/usdt/{base}USDT"
    return out