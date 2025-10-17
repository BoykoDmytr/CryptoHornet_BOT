"""Bybit exchange fetchers."""
from __future__ import annotations

from typing import Dict

import httpx

from .base import Snapshot, get_json


async def spot(client: httpx.AsyncClient) -> Snapshot:
    payload = await get_json(client, "https://api.bybit.com/v5/market/instruments-info", params={"category": "spot"})
    data = payload.get("result", {}).get("list", []) if isinstance(payload, dict) else []
    out: Dict[str, str] = {}
    for item in data:
        if not isinstance(item, dict):
            continue
        symbol = (item.get("symbol") or "").upper()
        if not symbol:
            continue
        if not symbol.endswith("USDT"):
            continue
        base = symbol[:-4]
        out[f"{base}/USDT"] = f"https://www.bybit.com/en/trade/spot/{base}/USDT"
    return out


async def futures(client: httpx.AsyncClient) -> Snapshot:
    payload = await get_json(client, "https://api.bybit.com/v5/market/instruments-info", params={"category": "linear"})
    data = payload.get("result", {}).get("list", []) if isinstance(payload, dict) else []
    out: Dict[str, str] = {}
    for item in data:
        if not isinstance(item, dict):
            continue
        symbol = (item.get("symbol") or "").upper()
        if not symbol.endswith("USDT"):
            continue
        base = symbol[:-4]
        out[f"{base}/USDT"] = f"https://www.bybit.com/en/trade/usdt/{base}USDT"
    return out