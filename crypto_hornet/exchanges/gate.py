"""Gate.io exchange fetchers."""
from __future__ import annotations

from typing import Dict

import httpx

from .base import Snapshot, get_json


async def spot(client: httpx.AsyncClient) -> Snapshot:
    payload = await get_json(client, "https://api.gateio.ws/api/v4/spot/currency_pairs")
    data = payload if isinstance(payload, list) else []
    out: Dict[str, str] = {}
    for item in data:
        if not isinstance(item, dict):
            continue
        base = (item.get("base") or "").upper()
        quote = (item.get("quote") or "").upper()
        if not base or not quote:
            continue
        out[f"{base}/{quote}"] = f"https://www.gate.io/trade/{base}_{quote}"
    return out


async def futures(client: httpx.AsyncClient) -> Snapshot:
    payload = await get_json(client, "https://api.gateio.ws/api/v4/futures/usdt/contracts")
    data = payload if isinstance(payload, list) else []
    out: Dict[str, str] = {}
    for item in data:
        if not isinstance(item, dict):
            continue
        name = (item.get("name") or "").upper()
        if "_" not in name:
            continue
        base, quote = name.split("_", 1)
        out[f"{base}/{quote}"] = f"https://www.gate.io/futures_trade/USDT/{base}_{quote}"
    return out
