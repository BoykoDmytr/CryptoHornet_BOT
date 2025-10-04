# announcements_mexc.py
import re
from datetime import datetime
from typing import List, Dict, Any, Optional
import requests
from bs4 import BeautifulSoup
import pytz

UA_TZ = pytz.timezone("Europe/Kyiv")

MONTHS = {
    "січня":1, "лютого":2, "березня":3, "квітня":4, "травня":5, "червня":6,
    "липня":7, "серпня":8, "вересня":9, "жовтня":10, "листопада":11, "грудня":12,
    "января":1, "февраля":2, "марта":3, "апреля":4, "мая":5, "июня":6,
    "июля":7, "августа":8, "сентября":9, "октября":10, "ноября":11, "декабря":12,
    "january":1, "february":2, "march":3, "april":4, "may":5, "june":6,
    "july":7, "august":8, "september":9, "october":10, "november":11, "december":12,
}

RE_DT = re.compile(r"(?P<h>\d{1,2}):(?P<m>\d{2})\s*,?\s*(?P<d>\d{1,2})\s+(?P<mon>[А-Яа-яA-Za-z]+)\s+(?P<y>\d{4})")
RE_SYMBOL = re.compile(r"\b([A-Z0-9]{2,})USDT(?:-M)?\b")

def _get(url: str) -> str:
    r = requests.get(url, headers={
        "User-Agent":"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36"
    }, timeout=20)
    r.raise_for_status()
    return r.text

def _abs(locale: str, href: str) -> str:
    if href.startswith("http"):
        return href
    base = f"https://www.mexc.com/{locale}".rstrip("/")
    if href.startswith("/"):
        return base + href
    return base + "/" + href

def _parse_dt_kiev(text: str) -> Optional[datetime]:
    m = RE_DT.search(text)
    if not m:
        return None
    h, mnt = int(m.group("h")), int(m.group("m"))
    d, y = int(m.group("d")), int(m.group("y"))
    mon = MONTHS.get(m.group("mon").lower())
    if not mon:
        return None
    return UA_TZ.localize(datetime(y, mon, d, h, mnt))

def fetch_article(url: str, locale: str = "uk-UA") -> Optional[Dict[str, Any]]:
    html = _get(_abs(locale, url))
    soup = BeautifulSoup(html, "html.parser")
    title = (soup.find("h1").get_text(" ", strip=True)) if soup.find("h1") else ""
    plain = soup.get_text(" ", strip=True)

    symbols = sorted(set(RE_SYMBOL.findall(plain)))
    if not symbols:
        # запасний варіант — інколи символи тільки в таблицях
        for td in soup.select("td"):
            symbols += RE_SYMBOL.findall(td.get_text(" ", strip=True))
        symbols = sorted(set(symbols))

    market = "futures" if ("USDT-M" in plain.upper() or "ф'ючер" in plain.lower() or "futures" in plain.lower()) else "spot"
    start_dt = _parse_dt_kiev(plain)

    return {
        "exchange": "mexc",
        "market": market,
        "title": title,
        "symbols": symbols,
        "start_dt": start_dt,
        "url": _abs(locale, url),
    }

def fetch_latest(locale: str = "uk-UA", pages: int = 1) -> List[Dict[str, Any]]:
    """Збирає посилання зі сторінки 'Нові лістинги', потім парсить кожну статтю."""
    out: List[Dict[str, Any]] = []
    seen = set()
    for p in range(1, pages + 1):
        url = f"https://www.mexc.com/{locale}/announcements/new-listings" + (f"?page={p}" if p > 1 else "")
        soup = BeautifulSoup(_get(url), "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "/announcements/article" in href:
                full = _abs(locale, href)
                if full not in seen:
                    seen.add(full)
                    art = fetch_article(full, locale=locale)
                    if art: out.append(art)
    return out
