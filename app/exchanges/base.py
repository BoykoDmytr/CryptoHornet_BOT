# app/exchanges/base.py
from typing import AsyncIterator, Protocol, Optional
from datetime import datetime
from pydantic import BaseModel

class Listing(BaseModel):
    exchange: str                # e.g., KUCOIN
    market_type: str             # "SPOT" | "FUTURES"
    symbol: str                  # e.g., RVV
    source_time: Optional[datetime] = None
    provisional: bool = True
    source_name: str
    source_url: str
    speed_tier: int              # 1/2/3
    dedupe_key: str              # stable unique key across polls

class ExchangeAdapter(Protocol):
    name: str
    async def stream(self) -> AsyncIterator[Listing]:
        ...

# Optional: for your “phase B” time-filler worker
class Announcement(BaseModel):
    exchange: str
    market_type: str
    symbol: str
    official_time: datetime
    notice_url: str
    dedupe_hint: str | None = None

class AnnouncementAdapter(Protocol):
    name: str
    async def stream(self) -> AsyncIterator[Announcement]:
        ...
