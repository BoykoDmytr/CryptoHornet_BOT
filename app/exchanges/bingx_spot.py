import httpx
from typing import AsyncIterator
from app.exchanges.base import Listing
from app.utils.time import now_utc
import os
API_KEY = os.getenv("BINGX_API_KEY", "")
HEADERS = {"X-BX-APIKEY": API_KEY} if API_KEY else {}

SPOT_ENDPOINT = os.getenv("BINGX_SPOT_SYMBOLS_ENDPOINT",
                          "https://open-api.bingx.com/openApi/spot/v1/common/symbols")
FUT_ENDPOINT  = os.getenv("BINGX_FUTURES_CONTRACTS_ENDPOINT",
                          "https://api-swap-rest.bingx.com/api/v1/contract/symbols")


name = "BINGX"
# NOTE: Some BingX spot endpoints may require API keys; adjust if needed.
ENDPOINT = "https://open-api.bingx.com/openApi/spot/v1/common/symbols"  # verify in production
TRADE_URL = "https://bingx.com/en-us/spot/{base}USDT"

class BingXSpot:
    def __init__(self, poll_seconds: float = 2.0):
        self.poll_seconds = poll_seconds
        self._known: set[str] = set()

    async def _fetch(self) -> list[dict]:
        async with httpx.AsyncClient(timeout=10, headers=HEADERS) as cx:
            r = await cx.get(ENDPOINT)
            r.raise_for_status()
            data = r.json()
            return data.get("data", [])

    async def stream(self) -> AsyncIterator[Listing]:
        import asyncio
        while True:
            try:
                for it in await self._fetch():
                    sym = it.get("symbol") or it.get("s") or ""
                    if not sym.endswith("USDT"):
                        continue
                    base = sym.replace("USDT", "")
                    dedupe_key = f"BINGX:SPOT:{base}"
                    if dedupe_key in self._known:
                        continue
                    self._known.add(dedupe_key)
                    start = now_utc()
                    yield Listing(
                        exchange=name,
                        market_type="SPOT",
                        symbol=base,
                        source_time=start,
                        source_name="BingX spot symbols API",
                        source_url=TRADE_URL.format(base=base),
                        speed_tier=2,
                        dedupe_key=dedupe_key,
                    )
            except Exception:
                await asyncio.sleep(1)
            await asyncio.sleep(self.poll_seconds)

Adapter = BingXSpot