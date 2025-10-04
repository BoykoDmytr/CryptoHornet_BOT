# -*- coding: utf-8 -*-
"""
Парсери текстів із каналів‑джерел у структуру подій.
"""
from __future__ import annotations
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List, Dict, Any, Tuple
import pytz

# --- Моделі ---
@dataclass
class ListingEvent:
    exchange: str                # 'mexc', 'gate', 'binance', 'kucoin', ...
    market_type: str             # 'spot' | 'futures' | 'alpha' | 'unknown'
    symbol: Optional[str] = None # 'P_USDT' або 'P'
    open_time: Optional[datetime] = None
    network: Optional[str] = None
    contract: Optional[str] = None
    price: Optional[float] = None
    source_msg_link: Optional[str] = None
    meta: Dict[str, Any] = None

# --- Хелпери ---
EXCH_ALIASES = {
    'mexc': 'mexc',
    'mexс': 'mexc',  # на випадок кирилиці "с"
    'gate': 'gate',
    'kucoin': 'kucoin',
    'bitget': 'bitget',
    'bingx': 'bingx',
    'okx': 'okx',
    'bybit': 'bybit',
    'binance': 'binance',
    'bithumb': 'bithumb',
    'upbit': 'upbit',
}

def norm_exchange(s: str) -> str:
    s = s.lower().strip()
    s = s.replace('bing x', 'bingx')
    return EXCH_ALIASES.get(s, s)

def parse_time_any(s: str, tz: str = "Europe/Kyiv") -> Optional[datetime]:
    s = s.strip()
    # Підтримка різних форматів дат/часів, прикл.: "2025-10-03 14:30", "14:00"
    for fmt in ["%Y-%m-%d %H:%M", "%d.%m.%Y %H:%M", "%H:%M"]:
        try:
            dt = datetime.strptime(s, fmt)
            if fmt == "%H:%M":
                # Якщо дата відсутня — підставимо сьогодні
                today = datetime.now(pytz.timezone(tz))
                dt = today.replace(hour=dt.hour, minute=dt.minute, second=0, microsecond=0)
            return pytz.timezone(tz).localize(dt) if dt.tzinfo is None else dt
        except Exception:
            pass
    return None

# --- Регулярки ---
RE_ADDR = re.compile(r"0x[a-fA-F0-9]{40}")
RE_PRICE = re.compile(r"(?:Цена|Price|Ціна)\s*:\s*\$?\s*([0-9]*\.?[0-9]+)")
RE_NETWORK = re.compile(r"(?:Сеть|Network|Мережа)\s*:\s*([A-Za-z0-9_-]+)", re.I)
RE_OPEN = re.compile(r"(?:Открытие|Open|Відкриття|Откритие)\s*:\s*([0-9:.\-\s]+)")
RE_START_TRADING = re.compile(r"(?:Start trading|Пара|Торговая пара)\s*:\s*([A-Za-z0-9_./-]+)")

# Лінії типу: "binance (alpha) 14:00"
RE_LINE_SCHEDULE = re.compile(
    r"(?i)\\b(binance|gate|mexc|kucoin|bitget|bingx|okx|bybit|bithumb|upbit)\\b\\s*\\(\\s*(alpha|spot|futures)\\s*\\)\\s*([0-2]?\\d:[0-5]\\d)"
)

def parse_mexc_futures_block(text: str, tz: str = "Europe/Kyiv") -> Optional[ListingEvent]:
    # Орієнтуємося на блоки з "Start trading: P_USDT" і деталями
    m_pair = RE_START_TRADING.search(text)
    m_open = RE_OPEN.search(text)
    m_net = RE_NETWORK.search(text)
    m_price = RE_PRICE.search(text)
    m_addr = RE_ADDR.search(text)

    if any([m_pair, m_open, m_net, m_addr]):
        return ListingEvent(
            exchange="mexc",
            market_type="futures",
            symbol=m_pair.group(1) if m_pair else None,
            open_time=parse_time_any(m_open.group(1), tz) if m_open else None,
            network=m_net.group(1).lower() if m_net else None,
            contract=m_addr.group(0) if m_addr else None,
            price=float(m_price.group(1)) if m_price else None,
            meta={"raw": text},
        )
    return None

def parse_schedule_block(text: str, tz: str = "Europe/Kyiv") -> List[ListingEvent]:
    events: List[ListingEvent] = []
    for exch, kind, hhmm in RE_LINE_SCHEDULE.findall(text):
        events.append(
            ListingEvent(
                exchange=norm_exchange(exch),
                market_type=kind.lower(),
                symbol=None,
                open_time=parse_time_any(hhmm, tz),
                meta={"from_block": True},
            )
        )
    return events

def parse_any(text: str, tz: str = "Europe/Kyiv") -> List[ListingEvent]:
    text_norm = text.replace("\\u200b", " ").replace("\\xa0", " ")
    out: List[ListingEvent] = []
    e1 = parse_mexc_futures_block(text_norm, tz)
    if e1:
        out.append(e1)
    out.extend(parse_schedule_block(text_norm, tz))
    return out
