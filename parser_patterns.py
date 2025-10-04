# parser_patterns.py
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

@dataclass
class ListingEvent:
    exchange: str
    market_type: str  # "spot" | "futures" | "alpha" | "unknown"
    symbol: Optional[str] = None
    open_time: Optional[datetime] = None
    network: Optional[str] = None
    contract: Optional[str] = None
    price: Optional[float] = None

def parse_any(text: str, tz: str = "Europe/Kyiv") -> List[ListingEvent]:
    """Порожня заглушка: повертає [].
    Якщо захочеш, тут можна реалізувати розбір текстів із тг-каналів."""
    return []
