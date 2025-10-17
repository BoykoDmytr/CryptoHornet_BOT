"""OKX exchange fetchers."""
from __future__ import annotations

from typing import Dict

import httpx

from .base import Snapshot, get_json


async def spot(client: httpx.AsyncClient) -> Snapshot:
    payload = await get_json(client, "https://www.okx.com/api/v5/public/instruments", params={"instType": "SPOT"})
    data = payload if isinstance(payload, dict) else {}
    out: Dict[str, str] = {}
    for item in data.get("data", []):
        if not isinstance(item, dict):
            continue
        inst_id = item.get("instId")
        if not inst_id or "-" not in inst_id:
            continue
        base, quote, *_ = inst_id.upper().split("-")
        state = (item.get("state") or item.get("status") or "").lower()
        if state and state not in {"live", "listed", "trading"}:
            continue
        out[f"{base}/{quote}"] = f"https://www.okx.com/trade-spot/{base}-{quote}"
    return out


async def futures(client: httpx.AsyncClient) -> Snapshot:
    payload = await get_json(client, "https://www.okx.com/api/v5/public/instruments", params={"instType": "SWAP"})
    data = payload if isinstance(payload, dict) else {}
    out: Dict[str, str] = {}
    for item in data.get("data", []):
        if not isinstance(item, dict):
            continue
        inst_id = item.get("instId")
        if not inst_id or inst_id.count("-") < 1:
            continue
        parts = inst_id.upper().split("-")
        base = parts[0]
        quote = parts[1]
        out[f"{base}/{quote}"] = f"https://www.okx.com/trade-swap/{base}-{quote}-SWAP"
    return out
