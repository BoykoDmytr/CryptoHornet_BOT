# ann_sources.py
from __future__ import annotations

# на самому верху
import os
import re
from typing import List, Dict, Any, Iterable, Optional
from datetime import datetime
from urllib.parse import urlencode, quote

import pytz
import requests
from bs4 import BeautifulSoup

# ⚠️ нове: для TLS-імперсонації Chrome
try:
    from curl_cffi import requests as curl_requests
    HAS_CURL = True
except Exception:
    HAS_CURL = False

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/126.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,uk-UA;q=0.8,ru-RU;q=0.7",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Upgrade-Insecure-Requests": "1",
    # “хромові” заголовки — часто вирішують саме 403 на MEXC/Gate
    "Sec-Ch-Ua": '"Chromium";v="126", "Not.A/Brand";v="24", "Google Chrome";v="126"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-User": "?1",
    "Sec-Fetch-Dest": "document",
    "Referer": "https://www.google.com/",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)

HTTP_PROXY = os.getenv("HTTP_PROXY") or os.getenv("http_proxy")
HTTPS_PROXY = os.getenv("HTTPS_PROXY") or os.getenv("https_proxy")
PROXIES = {"http": HTTP_PROXY, "https": HTTPS_PROXY} if (HTTP_PROXY or HTTPS_PROXY) else None

# опційно, якщо потім схочеш додати платний скрейпер: https://api.scraperapi.com?api_key=KEY&url
SCRAPER_URL = os.getenv("SCRAPER_URL", "").rstrip("/")



def _fetch(url: str, params: Dict[str, Any] | None = None):
    # 1) прямий запит звичайним requests
    try:
        r = SESSION.get(url, params=params, timeout=25, allow_redirects=True, proxies=PROXIES)
        r.raise_for_status()
        return r
    except requests.HTTPError as e:
        code = getattr(e.response, "status_code", None)

        # 2) якщо 403/503 — пробуємо TLS-імперсонацію реального Chrome
        if code in (403, 503) and HAS_CURL:
            try:
                r2 = curl_requests.get(
                    url,
                    params=params,
                    headers=HEADERS,
                    impersonate="chrome124",   # або "chrome120"/"chrome110"
                    timeout=25,
                    proxies=PROXIES,
                    http2=True,                # важливо — багато сайтів очікують h2
                )
                if r2.status_code == 200:
                    return r2
                r2.raise_for_status()
            except Exception:
                pass

        # 3) останній шанс — зовнішній скрейпер (якщо заданий)
        if code in (403, 503) and SCRAPER_URL:
            q = url
            if params:
                q = url + ("&" if "?" in url else "?") + urlencode(params)
            final = f"{SCRAPER_URL}={quote(q, safe='')}"
            r3 = SESSION.get(final, timeout=25, allow_redirects=True)
            r3.raise_for_status()
            return r3

        raise  # повертаємо початкову помилку

def get_html(url: str, params: Dict[str, Any] | None = None) -> str:
    return _fetch(url, params=params).text



# ---------- загальні утиліти --------------------------------------------------

UA_TZ = pytz.timezone("Europe/Kyiv")

# 2 шаблони часу: "час → дата" та "дата → час" (OKX), враховує UTC/GMT
RE_TIME_FIRST = re.compile(
    r"(?P<h>\d{1,2}):(?P<m>\d{2})\s*(?P<tz>UTC|GMT)?\s*,?\s*"
    r"(?P<d>\d{1,2})\s+(?P<mon>[A-Za-zА-Яа-яІіЄєЇї]+)\s+(?P<y>\d{4})",
    re.I,
)
RE_DATE_FIRST = re.compile(
    r"(?P<d>\d{1,2})\s+(?P<mon>[A-Za-zА-Яа-яІіЄєЇї]+)\s+(?P<y>\d{4}).{0,80}?"
    r"(?P<h>\d{1,2}):(?P<m>\d{2})\s*(?P<tz>UTC|GMT)?",
    re.I | re.S,
)

MONTHS = {
    # uk
    "січня": 1, "лютого": 2, "березня": 3, "квітня": 4, "травня": 5, "червня": 6,
    "липня": 7, "серпня": 8, "вересня": 9, "жовтня": 10, "листопада": 11, "грудня": 12,
    # ru
    "января": 1, "февраля": 2, "марта": 3, "апреля": 4, "мая": 5, "июня": 6,
    "июля": 7, "августа": 8, "сентября": 9, "октября": 10, "ноября": 11, "декабря": 12,
    # en
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11, "december": 12,
}

RE_PAIR = re.compile(r"\b([A-Z0-9]{2,})\s*/\s*USDT\b|\b([A-Z0-9]{2,})USDT(?:-M)?\b")


def uniq(seq: Iterable[str]) -> List[str]:
    seen, out = set(), []
    for x in seq:
        if x not in seen:
            seen.add(x)
            out.append(x)
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


def _mk_dt(d: int, mon_name: str, y: int, h: int, m: int, tz_token: Optional[str]):
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


# ---------- MEXC: лише FUTURES ------------------------------------------------

def mexc_futures_latest(locale: Optional[str] = None) -> List[Dict[str, Any]]:
    locale = locale or os.getenv("MEXC_LOCALE", "en-US")  # було uk-UA
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
        out.append({"exchange": "mexc", "market": "futures", "title": title, "symbols": syms, "start_dt": dt, "url": u})
    return out


# ---------- GATE: spot / futures (fallback-и .io/.com і ru/en) ----------------

def gate_collect(market: str) -> List[Dict[str, Any]]:
    # Спробуємо кілька сторінок — що перша віддасть 200, ту й парсимо
    candidates = [
        ("https://www.gate.io/ru/announcements/newspotlistings", "https://www.gate.io") if market == "spot"
        else ("https://www.gate.io/ru/announcements/newfutureslistings", "https://www.gate.io"),
        ("https://www.gate.io/en/announcements/newspotlistings", "https://www.gate.io") if market == "spot"
        else ("https://www.gate.io/en/announcements/newfutureslistings", "https://www.gate.io"),
        ("https://www.gate.com/ru/announcements/newspotlistings", "https://www.gate.com") if market == "spot"
        else ("https://www.gate.com/ru/announcements/newfutureslistings", "https://www.gate.com"),
        ("https://www.gate.com/en/announcements/newspotlistings", "https://www.gate.com") if market == "spot"
        else ("https://www.gate.com/en/announcements/newfutureslistings", "https://www.gate.com"),
    ]
    html = None
    base_domain = None
    for u, dom in candidates:
        try:
            html = get_html(u)
            base_domain = dom
            break
        except requests.HTTPError as e:
            if getattr(e.response, "status_code", None) == 403:
                continue
            raise
    if html is None:
        raise requests.HTTPError("403/blocked on all Gate endpoints")  # підхопить верхній try/except

    soup = BeautifulSoup(html, "html.parser")
    arts = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/announcements/detail" in href or "/announcements/article" in href:
            if not href.startswith("http"):
                href = base_domain + href
            arts.append(href)
    arts = uniq(arts)[:20]
    out = []
    for u in arts:
        s = BeautifulSoup(get_html(u), "html.parser")
        title = s.find("h1").get_text(" ", strip=True) if s.find("h1") else ""
        plain = s.get_text(" ", strip=True)
        syms = extract_symbols(plain)
        dt = parse_dt_kiev(plain)
        out.append({"exchange": "gate", "market": market, "title": title, "symbols": syms, "start_dt": dt, "url": u})
    return out


def gate_spot_latest() -> List[Dict[str, Any]]:
    return gate_collect("spot")


def gate_futures_latest() -> List[Dict[str, Any]]:
    return gate_collect("futures")


# ---------- BINGX: spot / futures --------------------------------------------

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
        title = s.find(["h1", "h2"]).get_text(" ", strip=True) if s.find(["h1", "h2"]) else ""
        plain = s.get_text(" ", strip=True)
        syms = extract_symbols(plain)
        dt = parse_dt_kiev(plain)  # якщо буде явно UTC — теж підхопиться
        out.append({"exchange": "bingx", "market": market, "title": title, "symbols": syms, "start_dt": dt, "url": u})
    return out


def bingx_spot_latest() -> List[Dict[str, Any]]:
    return bingx_collect("https://bingx.com/en/support/notice-center/11257060005007", "spot")


def bingx_futures_latest() -> List[Dict[str, Any]]:
    return bingx_collect("https://bingx.com/en/support/notice-center/11257015822991", "futures")


# ---------- BITGET: spot / futures (EN) ---------------------------------------

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
        title = s.find(["h1", "h2"]).get_text(" ", strip=True) if s.find(["h1", "h2"]) else ""
        plain = s.get_text(" ", strip=True)
        syms = extract_symbols(plain)
        dt = parse_dt_kiev(plain)
        out.append({"exchange": "bitget", "market": market, "title": title, "symbols": syms, "start_dt": dt, "url": u})
    return out


def bitget_spot_latest() -> List[Dict[str, Any]]:
    return bitget_collect("https://www.bitget.com/en/support/sections/5955813039257", "spot")


def bitget_futures_latest() -> List[Dict[str, Any]]:
    return bitget_collect("https://www.bitget.com/en/support/sections/12508313405000", "futures")


# ---------- OKX: spot + futures (тільки заголовки, що починаються з "Лістинг/Listing") --

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
        title = s.find(["h1", "h2"]).get_text(" ", strip=True) if s.find(["h1", "h2"]) else ""
        if not _title_is_listing(title):
            continue
        plain = s.get_text(" ", strip=True)
        syms = extract_symbols(plain)
        market = "futures" if any(k in plain.lower() for k in ["swap", "perpetual", "ф'ючер"]) else "spot"
        dt = parse_dt_kiev(plain)
        out.append({"exchange": "okx", "market": market, "title": title, "symbols": syms, "start_dt": dt, "url": u})
    return out


# ---------- BINANCE: spot + futures + alpha (CMS API) -------------------------

def binance_latest(rows: int = 20) -> List[Dict[str, Any]]:
    catalogs = [48, 251, 137]  # 48=new listings, 251=futures, 137=alpha/research
    out: List[Dict[str, Any]] = []
    for cat in catalogs:
        try:
            j = _fetch(
                "https://www.binance.com/bapi/composite/v1/public/cms/article/list",
                params={"type": 1, "page": 1, "rows": rows, "catalogId": cat},
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
                out.append({"exchange": "binance", "market": market, "title": title, "symbols": syms, "start_dt": dt, "url": url})
        except Exception:
            continue
    return out


# ---------- реєстр джерел -----------------------------------------------------

def sources_matrix() -> List:
    return [
        mexc_futures_latest,      # MEXC only futures
        gate_spot_latest,
        gate_futures_latest,
        bingx_spot_latest,
        bingx_futures_latest,
        bitget_spot_latest,
        bitget_futures_latest,
        okx_latest,               # spot + futures (із префікс-фільтром на "Лістинг/Listing")
        binance_latest,           # spot + futures + alpha
    ]
