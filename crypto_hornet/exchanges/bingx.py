"""BingX exchange fetchers."""
from __future__ import annotations

from typing import Dict

import httpx

from .base import Snapshot, get_json


def _headers(client: httpx.AsyncClient) -> dict[str, str]:
    api_key = client.headers.get("X-BX-APIKEY") or client.headers.get("x-bx-apikey") or ""
    return {"X-BX-APIKEY": api_key} if api_key else {}


async def spot(client: httpx.AsyncClient) -> Snapshot:
    hosts = [
        "https://open-api.bingx.com",
        "https://api-swap-rest.bingx.com",
    ]
    out: Dict[str, str] = {}
    headers = _headers(client)
    for host in hosts:
        payload = await get_json(client, f"{host}/openApi/spot/v1/common/symbols", headers=headers)
        data = []
        if isinstance(payload, dict):
            code = str(payload.get("code") or "")
            if code and code not in {"0", "200000"}:
                continue
            data = payload.get("data") or []
        elif isinstance(payload, list):
            data = payload
        for item in data or []:
            if not isinstance(item, dict):
                continue
            base = (item.get("baseAsset") or "").upper()
            quote = (item.get("quoteAsset") or "").upper()
            if not base or not quote:
                continue
            out[f"{base}/{quote}"] = f"https://bingx.com/en-us/spot/{base}_{quote}"
        if out:
            break
    return out


async def futures(client: httpx.AsyncClient) -> Snapshot:
    headers = _headers(client)
    payload = await get_json(client, "https://open-api.bingx.com/openApi/swap/v2/quote/contracts", headers=headers)
    data = []
    if isinstance(payload, dict):
        data = payload.get("data") or {}
        if isinstance(data, dict):
            data = data.get("contracts") or data.get("list") or []
    elif isinstance(payload, list):
        data = payload
    out: Dict[str, str] = {}
    for item in data or []:
        symbol = ""
        if isinstance(item, dict):
            symbol = (item.get("symbol") or item.get("contractName") or "").upper()
        elif isinstance(item, str):
            symbol = item.upper()
        if not symbol:
            continue
        symbol = symbol.replace("-", "")
        if not symbol.endswith("USDT"):
            continue
        base = symbol[:-4]
        out[f"{base}/USDT"] = f"https://bingx.com/en-us/futures/{base}USDT"
    return out