# app/exchanges/bitget_spot.py
import os
import httpx
from typing import AsyncIterator
from app.exchanges.base import Listing
from app.utils.time import now_utc

name = "BITGET"

ENDPOINT = os.getenv(
    "BITGET_SYMBOLS_ENDPOINT",
    "https://api.bitget.com/api/v2/spot/public/symbols",
)
TRADE_URL = os.getenv(
    "BITGET_TRADE_URL",
    "https://www.bitget.com/spot/{base}USDT",
)

class BitgetSpot:
    def __init__(self, poll_seconds: float = 2.0):
        self.poll_seconds = poll_seconds
        self._known: set[str] = set()

    async def _fetch(self) -> list[dict]:
        async with httpx.AsyncClient(timeout=10) as cx:
            r = await cx.get(ENDPOINT)
            r.raise_for_status()
            data = r.json()
            # v2 returns {"code":"00000","msg":"success","requestTime":..., "data":[...]}
            return data.get("data", [])

    async def stream(self) -> AsyncIterator[Listing]:
        import asyncio
        while True:
            try:
                for it in await self._fetch():
                    # ⬇️ THIS is the bit you asked about (mapping + USDT filter)
                    base = (
                        it.get("baseCoin")
                        or it.get("baseCoinName")
                        or it.get("symbol", "").replace("USDT", "")
                    )
                    quote = it.get("quoteCoin") or "USDT"
                    if not base or quote != "USDT":
                        continue

                    # (Optional) if you want to require tradable status, check it here:
                    # status = (it.get("status") or "").lower()
                    # if status not in {"online", "listed", "trading"}:
                    #     continue

                    dedupe_key = f"BITGET:SPOT:{base}"
                    if dedupe_key in self._known:
                        continue
                    self._known.add(dedupe_key)

                    # Phase A: post fast; no official time yet → source_time=None, provisional=True
                    yield Listing(
                        exchange=name,
                        market_type="SPOT",
                        symbol=base,
                        source_time=None,               # filled later by announcements parser
                        provisional=True,               # marks Start time as "~" when shown
                        source_name="Bitget symbols API",
                        source_url=TRADE_URL.format(base=base),
                        speed_tier=2,
                        dedupe_key=dedupe_key,
                    )
            except Exception:
                await asyncio.sleep(1)
            await asyncio.sleep(self.poll_seconds)

Adapter = BitgetSpot
