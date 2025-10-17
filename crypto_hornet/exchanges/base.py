"""Common helpers for exchange polling."""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Awaitable, Callable, Dict, Mapping

import httpx

log = logging.getLogger(__name__)

Snapshot = Dict[str, str]
Fetcher = Callable[[httpx.AsyncClient], Awaitable[Snapshot]]


@dataclass(slots=True)
class Feed:
    exchange: str
    market: str
    interval: int
    fetch: Fetcher
    source: str | None = None
    speed_tier: str | None = None

    @property
    def key(self) -> str:
        return f"{self.exchange}|{self.market}".lower()


async def get_json(client: httpx.AsyncClient, url: str, *, params: Mapping[str, str] | None = None, headers: Mapping[str, str] | None = None) -> object:
    try:
        response = await client.get(url, params=params, headers=headers)
        response.raise_for_status()
    except httpx.HTTPError as exc:
        log.warning("Failed to fetch %s: %s", url, exc)
        return {}
    content_type = response.headers.get("content-type", "").lower()
    text = response.text
    if "application/json" in content_type or text.strip().startswith("{") or text.strip().startswith("["):
        try:
            return response.json()
        except json.JSONDecodeError:
            pass
    try:
        return json.loads(text)
    except Exception:
        return text


def diff_snapshot(previous: Mapping[str, str] | None, current: Mapping[str, str]) -> Dict[str, str]:
    prev_keys = set(previous.keys()) if previous else set()
    return {pair: url for pair, url in current.items() if pair not in prev_keys}


def now_utc() -> datetime:
    return datetime.now(timezone.utc)