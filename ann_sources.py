from __future__ import annotations
import re
from typing import List, Dict, Any, Iterable, Optional
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import pytz

UA_TZ = pytz.timezone("Europe/Kyiv")
HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/126 Safari/537.36"
}

# ------ утиліти ---------------------------------------------------------------
# 2 шаблони: "час → дата" і "дата → час" (OKX), враховує UTC/GMT
RE_TIME_FIRST = re.compile(
    r"(?P<h>\d{1,2}):(?P<m>\d{2})\s*(?P<tz>UTC|GMT)?\s*,?\s*(?P<d>\d{1,2})\s+(?P<mon>[A-Za-zА-Яа-яІіЄєЇї]+)\s+(?P<y>\d{4})",
    re.I,
)
RE_DATE_FIRST = re.compile(
    r"(?P<d>\d{1,2})\s+(?P<mon>[A-Za-zА-Яа-яІіЄєЇї]+)\s+(?P<y>\d{4}).{0,80}?"
    r"(?P<h>\d{1,2}):(?P<m>\d{2})\s*(?P<tz>UTC|GMT)?",
    re.I | re.S,
)

MONTHS = {
    # uk
    "січня":1,"лютого":2,"березня":3,"квітня":4,"травня":5,"червня":6,"липня":7,"серпня":8,"вересня":9,"жовтня":10,"листопада":11,"грудня":12,
    # ru
    "января":1,"февраля":2,"марта":3,"апреля":4,"мая":5,"июня":6,"июля":7,"августа":8,"сентября":9,"октября":10,"ноября":11,"декабря":12,
    # en
    "january":1,"february":2,"march":3,"april":4,"may":5,"june":6,"july":7,"august":8,"september":9,"october":10,"november":11,"december":12,
}

RE_PAIR = re.compile(r"\b([A-Z0-9]{2,})\s*/\s*USDT\b|\b([A-Z0-9]{2,})USDT(?:-M)?\b")

def get_html(url: str) -> str:
    r = requests.get(url, headers=HEADERS, timeout=25)
    r.raise_for_status()
    return r.text

def uniq(seq: Iterable[str]) -> List[str]:
    s, out = set(), []
    for x in seq:
        if x not in s:
            s.add(x); out.append(x)
    return out

def extract_symbols(text: str) -> List[str]:
    out = set()
    up = text.upper()
    for m in RE_PAIR.finditer(up):
        sym = (m.group(1) or m.group(2) or "").strip()
        if not sym:
            continue
        if sym.endswith("USDT"):
            sym = sym[:-4]
        out.add(sym)
    return sorted(out)

def _mk_dt(d: int, mon_name: str, y: int, h: int, m: int, tz_token: str | None):
    mon = MONTHS.get(mon_name.lower())
    if not mon:
        return None
    naive = datetime(y, mon, d, h, m)
    if tz_token and tz_token.upper() in ("UTC", "GMT"):
        return pytz.utc.localize(naive).astimezone(UA_TZ)
    return UA_TZ.localize(naive)

def parse_dt_kiev(text: str) -> Optional[datetime]:
    t = " ".join(text.split())
    m = RE_TIME_FIRST.search(t)
    if m:
        return _mk_dt(int(m["d"]), m["mon"], int(m["y"]), int(m["h"]), int(m["m"]), m.group("tz"))
    m2 = RE_DATE_FIRST.search(t)
    if m2:
        return _mk_dt(int(m2["d"]), m2["mon"], int(m2["y"]), int(m2["h"]), int(m2["m"]), m2.group("tz"))
    return None

# ------ MEXC: ЛИШЕ FUTURES ----------------------------------------------------
def mexc_futures_latest(locale: str = "uk-UA") -> List[Dict[str, Any]]:
    base = f"https://www.mexc.com/{locale}/announcements/new-listings/19"
    soup = BeautifulSoup(get_html(base), "html.parser")
    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/announcements/article" in href:
            if not href.startswith("http"):
                href = f"https://www.mexc.com/{locale}{href if href.startswith('/') else '/' + href}"
            links.append(href)
    links = uniq(links)[:20]
    out: List[Dict[str, Any]] = []
    for u in links:
        s = BeautifulSoup(get_html(u), "html.parser")
        title = s.find("h1").get_text(" ", strip=True) if s.find("h1") else ""
        plain = s.get_text(" ", strip=True)
        syms = extract_symbols(plain)
        dt = parse_dt_kiev(plain)
        out.append({"exchange":"mexc","market":"futures","title":title,"symbols":syms,"start_dt":dt,"url":u})
    return out

# ------ GATE: spot / futures (ru) ---------------------------------------------
def gate_collect(list_url: str, market: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(get_html(list_url), "html.parser")
    arts = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/announcements/detail" in href or "/announcements/article" in href:
            if not href.startswith("http"):
                href = "https://www.gate.com" + href
            arts.append(href)
    arts = uniq(arts)[:20]
    out = []
    for u in arts:
        s = BeautifulSoup(get_html(u), "html.parser")
        title = s.find("h1").get_text(" ", strip=True) if s.find("h1") else ""
        plain = s.get_text(" ", strip=True)
        syms = extract_symbols(plain)
        dt = parse_dt_kiev(plain)
        out.append({"exchange":"gate","market":market,"title":title,"symbols":syms,"start_dt":dt,"url":u})
    return out

def gate_spot_latest() -> List[Dict[str, Any]]:
    return gate_collect("https://www.gate.com/ru/announcements/newspotlistings", "spot")

def gate_futures_latest() -> List[Dict[str, Any]]:
    return gate_collect("https://www.gate.com/ru/announcements/newfutureslistings", "futures")

# ------ BINGX: spot / futures --------------------------------------------------
def bingx_collect(section_url: str, market: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(get_html(section_url), "html.parser")
    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/support/announcements/" in href or "/support/notice-center/" in href:
            if not href.startswith("http"):
                href = "https://bingx.com" + href
            links.append(href)
    links = uniq(links)[:20]
    out = []
    for u in links:
        s = BeautifulSoup(get_html(u), "html.parser")
        title = s.find(["h1","h2"]).get_text(" ", strip=True) if s.find(["h1","h2"]) else ""
        plain = s.get_text(" ", strip=True)
        syms = extract_symbols(plain)
        dt = parse_dt_kiev(plain)
        out.append({"exchange":"bingx","market":market,"title":title,"symbols":syms,"start_dt":dt,"url":u})
    return out

def bingx_spot_latest() -> List[Dict[str, Any]]:
    return bingx_collect("https://bingx.com/en/support/notice-center/11257060005007", "spot")

def bingx_futures_latest() -> List[Dict[str, Any]]:
    return bingx_collect("https://bingx.com/en/support/notice-center/11257015822991", "futures")

# ------ BITGET: spot / futures -------------------------------------------------
def bitget_collect(section_url: str, market: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(get_html(section_url), "html.parser")
    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/support/articles/" in href or "/support/announcements/" in href:
            if not href.startswith("http"):
                href = "https://www.bitget.com" + href
            links.append(href)
    links = uniq(links)[:20]
    out = []
    for u in links:
        s = BeautifulSoup(get_html(u), "html.parser")
        title = s.find(["h1","h2"]).get_text(" ", strip=True) if s.find(["h1","h2"]) else ""
        plain = s.get_text(" ", strip=True)
        syms = extract_symbols(plain)
        dt = parse_dt_kiev(plain)
        out.append({"exchange":"bitget","market":market,"title":title,"symbols":syms,"start_dt":dt,"url":u})
    return out

def bitget_spot_latest() -> List[Dict[str, Any]]:
    return bitget_collect("https://www.bitget.com/support/sections/5955813039257", "spot")

def bitget_futures_latest() -> List[Dict[str, Any]]:
    return bitget_collect("https://www.bitget.com/support/sections/12508313405000", "futures")

# ------ OKX: spot + futures (з фільтром за префіксом 'Лістинг/Листинг/Listing') ----
def _title_is_listing(title: str) -> bool:
    if not title:
        return False
    t = title.strip().lstrip("[]()【】—-–·* ").lower()
    return t.startswith(("лістинг", "листинг", "listing"))

def okx_latest() -> List[Dict[str, Any]]:
    url = "https://www.okx.com/ua/help/section/announcements-new-listings"
    soup = BeautifulSoup(get_html(url), "html.parser")
    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/help/articles/" in href or "/help/announcement" in href:
            if not href.startswith("http"):
                href = "https://www.okx.com" + href
            links.append(href)
    links = uniq(links)[:20]
    out = []
    for u in links:
        s = BeautifulSoup(get_html(u), "html.parser")
        title = s.find(["h1","h2"]).get_text(" ", strip=True) if s.find(["h1","h2"]) else ""
        if not _title_is_listing(title):
            continue
        plain = s.get_text(" ", strip=True)
        syms = extract_symbols(plain)
        market = "futures" if any(k in plain.lower() for k in ["swap", "perpetual", "ф'ючер"]) else "spot"
        dt = parse_dt_kiev(plain)
        out.append({"exchange":"okx","market":market,"title":title,"symbols":syms,"start_dt":dt,"url":u})
    return out

# ------ BINANCE: spot + futures + alpha ---------------------------------------
def binance_latest(rows: int = 20) -> List[Dict[str, Any]]:
    catalogs = [48, 251, 137]  # 48=new listings, 251=futures, 137=alpha/research
    out: List[Dict[str, Any]] = []
    for cat in catalogs:
        try:
            j = requests.get(
                "https://www.binance.com/bapi/composite/v1/public/cms/article/list",
                params={"type": 1, "page": 1, "rows": rows, "catalogId": cat},
                headers=HEADERS, timeout=25
            ).json()
            items = j.get("data", {}).get("articles", []) or j.get("data", {}).get("items", [])
            for it in items:
                aid = it.get("code") or it.get("id") or ""
                if not aid:
                    continue
                url = f"https://www.binance.com/en/support/announcement/{aid}"
                s = BeautifulSoup(get_html(url), "html.parser")
                title = s.find("h1").get_text(" ", strip=True) if s.find("h1") else (it.get("title") or "")
                plain = s.get_text(" ", strip=True)
                syms = extract_symbols(plain)
                dt = parse_dt_kiev(plain)
                market = "spot"
                t = (title or "").lower()
                if "futures" in t or "perpetual" in plain.lower():
                    market = "futures"
                if cat == 137 or "alpha" in t:
                    market = "alpha"
                out.append({"exchange":"binance","market":market,"title":title,"symbols":syms,"start_dt":dt,"url":url})
        except Exception:
            continue
    return out

# ------ реєстр джерел під твій список -----------------------------------------
def sources_matrix() -> List:
    return [
        mexc_futures_latest,      # MEXC only futures
        gate_spot_latest,
        gate_futures_latest,
        bingx_spot_latest,
        bingx_futures_latest,
        bitget_spot_latest,
        bitget_futures_latest,
        okx_latest,               # spot + futures (із префікс-фільтром)
        binance_latest,           # spot + futures + alpha
    ]
