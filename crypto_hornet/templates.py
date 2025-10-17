"""Message formatting utilities."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone


@dataclass(slots=True)
class ListingEvent:
    exchange: str
    market: str
    pair: str
    url: str | None
    discovered_at: datetime

    @property
    def base(self) -> str:
        return self.pair.split("/", 1)[0]

    @property
    def quote(self) -> str:
        return self.pair.split("/", 1)[1] if "/" in self.pair else ""


def format_listing(event: ListingEvent) -> str:
    exchange = event.exchange.upper()
    market = event.market.upper()
    discovered = event.discovered_at.replace(tzinfo=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    url = event.url or ""

    lines = [
        f"🚀 {exchange} — {market} listing detected",
        f"Pair: {event.pair}",
        f"Detected: {discovered}",
    ]
    if url:
        lines.append(f"Link: {url}")
    return "\n".join(lines)
