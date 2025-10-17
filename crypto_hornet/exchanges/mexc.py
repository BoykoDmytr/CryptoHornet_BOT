"""MEXC exchange fetchers."""
from __future__ import annotations

from typing import Dict

import httpx

from .base import Snapshot, get_json


async def futures(client: httpx.AsyncClient) -> Snapshot:
    endpoints = [
        "https://contract.mexc.com/api/v1/contract/detail",
        "https://contract.mexc.com/api/v1/contract/list",
    ]
    out: Dict[str, str] = {}
    for url in endpoints:
        payload = await get_json(client, url)
        data = []
        if isinstance(payload, dict):
            data = payload.get("data") or payload.get("result") or []
            if isinstance(data, dict):
                data = data.get("symbols") or data.get("list") or []
        elif isinstance(payload, list):
            data = payload
        for item in data or []:
            if not isinstance(item, dict):
                continue
            symbol = (item.get("symbol") or item.get("instrument_id") or item.get("contractId") or "").upper()
            if not symbol:
                continue
            if "_" in symbol:
                base, quote = symbol.split("_", 1)
            elif symbol.endswith("USDT"):
                base, quote = symbol[:-4], "USDT"
            else:
                continue
            out[f"{base}/{quote}"] = f"https://www.mexc.com/futures/{base}_{quote}"
        if out:
            break
    return out
