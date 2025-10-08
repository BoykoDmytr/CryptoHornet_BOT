# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import re
import json
from typing import List, Dict, Any, Iterable, Optional, Tuple
from datetime import datetime
from urllib.parse import urlencode, quote

import pytz
import requests
from bs4 import BeautifulSoup

# ---- TLS-імперсонація Chrome (для обходу 403) ----
try:
    from curl_cffi import requests as curl_requests
    HAS_CURL = True
except Exception:
    HAS_CURL = False

# ---------- HTTP session / headers / proxy / scraper -------------------------

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

SCRAPER_URL = (os.getenv("SCRAPER_URL", "") or "").rstrip("/")

# локальні/мовні кукі
try:
    SESSION.cookies.set("locale", os.getenv("MEXC_LOCALE", "en-US"), domain=".mexc.com")
    SESSION.cookies.set("lang", "en_US", domain=".gate.io")
    SESSION.cookies.set("lang", "en_US", domain=".gate.com")
except Exception:
    pass


def _wrap_scraper_target(q: str) -> str:
    if not SCRAPER_URL:
        return q
    if "{url}" in SCRAPER_URL:
        return SCRAPER_URL.replace("{url}", quote(q, safe=""))
    if SCRAPER_URL.endswith("="):
        return SCRAPER_URL + quote(q, safe="")
    if "?" in SCRAPER_URL:
        return SCRAPER_URL + "&url=" + quote(q, safe="")
    return SCRAPER_URL + "?url=" + quote(q, safe="")


def _fetch(url: str, params: Dict[str, Any] | None = None):
    # 1) прямий запит
    try:
        r = SESSION.get(url, params=params, timeout=25, allow_redirects=True, proxies=PROXIES)
        r.raise_for_status()
        return r
    except requests.HTTPError as e:
        code = getattr(e.response, "status_code", None)

        # 2) TLS-імперсонація
        if code in (403, 503) and HAS_CURL:
            for imp in ("chrome124", "chrome120", "chrome110"):
                try:
                    r2 = curl_requests.get(
                        url,
                        params=params,
                        headers=HEADERS,
                        impersonate=imp,
                        timeout=25,
                        proxies=PROXIES,
                        http2=True,
                    )
                    if r2.status_code == 200:
                        return r2
                    r2.raise_for_status()
                except Exception:
                    continue

        # 3) зовнішній скрейпер
        if code in (403, 503) and SCRAPER_URL:
            q = url
            if params:
                q = url + ("&" if "?" in url else "?") + urlencode(params)
            final = _wrap_scraper_target(q)
            r3 = SESSION.get(final, timeout=25, allow_redirects=True)
            r3.raise_for_status()
            return r3

        raise


def get_html(url: str, params: Dict[str, Any] | None = None) -> str:
    return _fetch(url, params=params).text


# ---------- утиліти ----------------------------------------------------------

UA_TZ = pytz.timezone("Europe/Kyiv")

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

RE_PAIR = re.compile(r"\b([A-Z0-9]{2,})\s*(?:/|_|-)?\s*USDT(?:-M)?\b")

# час у форматах:
# 1) 2025-10-08 06:29:01 UTC+0 / UTC+8 / GMT+8 / (секунди опціональні)
RE_ISO_TZ = re.compile(
    r"\b(?P<d>\d{4})[-/.](?P<m>\d{1,2})[-/.](?P<y>\d{1,2})[ T]"
    r"(?P<h>\d{1,2}):(?P<min>\d{2})(?::(?P<s>\d{2}))?\s*"
    r"(?P<tz>(?:UTC|GMT)(?:[+\-]\d{1,2})?|UTC[ ]?[+\-]?\d{1,2}|UTC\+0|UTC-0)?\b",
    re.I,
)

# 2) «Открытие|Start|Listing time|Trading starts: YYYY-MM-DD HH:MM(:SS) ...»
RE_LABELLED = re.compile(
    r"(?:(Открытие|Старт|Start|Listing\s*Time|Trading\s*(?:Starts|Opens)|Go-?Live)\s*[:\-–]\s*)"
    r"(?P<date>\d{4}[-/.]\d{1,2}[-/.]\d{1,2})[ T]"
    r"(?P<time>\d{1,2}:\d{2}(?::\d{2})?)\s*(?P<tz>(?:UTC|GMT)(?:[+\-]\d{1,2})?|UTC[ ]?[+\-]?\d{1,2}|UTC\+0|UTC-0)?",
    re.I,
)

# 3) Time-first «15:00 UTC, 8 October 2025»
RE_TIME_FIRST = re.compile(
    r"(?P<h>\d{1,2}):(?P<m>\d{2})\s*(?P<tz>UTC|GMT)?\s*,?\s*"
    r"(?P<d>\d{1,2})\s+(?P<mon>[A-Za-zА-Яа-яІіЄєЇї]+)\s+(?P<y>\d{4})",
    re.I,
)

# 4) Date-first «8 October 2025 ... 15:00 UTC»
RE_DATE_FIRST = re.compile(
    r"(?P<d>\d{1,2})\s+(?P<mon>[A-Za-zА-Яа-яІіЄєЇї]+)\s+(?P<y>\d{4}).{0,120}?"
    r"(?P<h>\d{1,2}):(?P<m>\d{2})\s*(?P<tz>UTC|GMT)?",
    re.I | re.S,
)


def uniq_keep(seq: Iterable[str]) -> List[str]:
    seen, out = set(), []
    for x in seq:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


def extract_symbols(text: str) -> List[str]:
    out = set()
    up = (text or "").upper()
    for m in RE_PAIR.finditer(up):
        sym = (m.group(1) or "").strip()
        if not sym:
            continue
        if sym.endswith("USDT"):
            sym = sym[:-4]
        out.add(sym)
    return sorted(out)


def _month_num(name: str) -> Optional[int]:
    return MONTHS.get(name.lower())


def parse_all_times(text: str) -> List[str]:
    """
    Витягає УСІ помічені часи з тексту і повертає масив рядків, як вони мають відображатися.
    Нічого не конвертуємо в Київ — показуємо "як є".
    """
    t = " ".join((text or "").split())
    found: List[str] = []

    # 1) ISO + TZ
    for m in RE_ISO_TZ.finditer(t):
        dd = f"{m.group('d')}-{int(m.group('m')):02d}-{int(m.group('y')):02d}"
        tt = f"{int(m.group('h')):02d}:{int(m.group('min')):02d}"
        if m.group('s'):
            tt = f"{tt}:{int(m.group('s')):02d}"
        tz = (m.group('tz') or "").upper().replace("  ", " ").strip()
        disp = f"{dd} {tt}" + (f" {tz}" if tz else "")
        found.append(disp)

    # 2) Labelled
    for m in RE_LABELLED.finditer(t):
        dd = m.group('date')
        tt = m.group('time')
        tz = (m.group('tz') or "").upper().strip()
        disp = f"{dd} {tt}" + (f" {tz}" if tz else "")
        found.append(disp)

    # 3) Time-first
    for m in RE_TIME_FIRST.finditer(t):
        d = int(m.group('d')); y = int(m.group('y'))
        mon_n = _month_num(m.group('mon'))
        if not mon_n:
            continue
        tt = f"{int(m.group('h')):02d}:{int(m.group('m')):02d}"
        tz = (m.group('tz') or "").upper().strip()
        disp = f"{y}-{mon_n:02d}-{d:02d} {tt}" + (f" {tz}" if tz else "")
        found.append(disp)

    # 4) Date-first
    for m in RE_DATE_FIRST.finditer(t):
        d = int(m.group('d')); y = int(m.group('y'))
        mon_n = _month_num(m.group('mon'))
        if not mon_n:
            continue
        tt = f"{int(m.group('h')):02d}:{int(m.group('m')):02d}"
        tz = (m.group('tz') or "").upper().strip()
        disp = f"{y}-{mon_n:02d}-{d:02d} {tt}" + (f" {tz}" if tz else "")
        found.append(disp)

    # Нормалізація дрібних варіацій (UTC+0 vs UTC+00:00 ми не торкаємось — показуємо як є)
    # Але прибираємо дублікати, зберігаючи порядок:
    return uniq_keep(found)


def extract_meta_dt(soup: BeautifulSoup) -> Optional[str]:
    """
    ЧИСТО як додатковий кандидат (не завжди це час старту торгів) — час публікації.
    Повертаємо дисплей-рядок у UTC (ISO), або None.
    """
    cands = []
    for sel in [
        ("meta", {"property": "article:published_time"}),
        ("meta", {"property": "og:article:published_time"}),
        ("meta", {"name": "pubdate"}),
        ("meta", {"itemprop": "datePublished"}),
        ("time", {"datetime": True}),
    ]:
        tag = soup.find(sel[0], attrs=sel[1])
        if tag:
            val = tag.get("content") or tag.get("datetime") or tag.get("value")
            if val:
                cands.append(val)

    for js in soup.find_all("script", attrs={"type": "application/ld+json"}):
        try:
            data = json.loads(js.string or "")
            def walk(x):
                if isinstance(x, dict):
                    v = x.get("datePublished") or x.get("dateModified")
                    if isinstance(v, str):
                        cands.append(v)
                    for vv in x.values():
                        walk(vv)
                elif isinstance(x, list):
                    for vv in x:
                        walk(vv)
            walk(data)
        except Exception:
            pass

    # вертаємо перший валідний ISO
    for s in cands:
        try:
            s2 = s.strip()
            if s2.endswith("Z"):
                s2 = s2[:-1] + "+00:00"
            dt = datetime.fromisoformat(s2)
            # показуємо як UTC ISO без перетворень
            if dt.tzinfo is None:
                return dt.strftime("%Y-%m-%d %H:%M")
            return dt.astimezone(pytz.utc).strftime("%Y-%m-%d %H:%M UTC")
        except Exception:
            continue
    return None


# ---------- Колектори з ЖОРСТКО фіксованих сторінок --------------------------
# ПАРСИМО ТІЛЬКИ з цих URL (як ти просив):

URLS = {
    "mexc_futures": "https://www.mexc.com/uk-UA/announcements/new-listings/19",
    "gate_spot":    "https://www.gate.com/ru/announcements/newspotlistings",
    "gate_futures": "https://www.gate.com/ru/announcements/newfutureslistings",
    "bingx_spot":   "https://bingx.com/en/support/notice-center/11257060005007",
    "bingx_futures":"https://bingx.com/en/support/notice-center/11257015822991",
    "bitget_spot":  "https://www.bitget.com/support/sections/5955813039257",
    "bitget_futures":"https://www.bitget.com/support/sections/12508313405000",
    "okx_all":      "https://www.okx.com/ua/help/section/announcements-new-listings",
    "binance_all":  "https://www.binance.com/en/support/announcement/list/48",
}


def _collect_generic(section_url: str, base_domain: str | None = None) -> List[str]:
    html = get_html(section_url)
    soup = BeautifulSoup(html, "html.parser")
    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if any(k in href for k in ("/announcements/", "/support/articles/", "/help/articles/", "/support/announcement")):
            if base_domain and not href.startswith("http"):
                href = base_domain + (href if href.startswith("/") else "/" + href)
            elif not href.startswith("http"):
                # на деяких сайтах посилання відносні
                if "bingx.com" in section_url:
                    href = "https://bingx.com" + (href if href.startswith("/") else "/" + href)
                elif "bitget.com" in section_url:
                    href = "https://www.bitget.com" + (href if href.startswith("/") else "/" + href)
                elif "okx.com" in section_url:
                    href = "https://www.okx.com" + (href if href.startswith("/") else "/" + href)
                elif "binance.com" in section_url:
                    href = "https://www.binance.com" + (href if href.startswith("/") else "/" + href)
            links.append(href)
    # трохи усікти шум
    # (залишаємо перші ~20 для продуктивності)
    out = []
    seen = set()
    for u in links:
        if u not in seen:
            seen.add(u)
            out.append(u)
        if len(out) >= 20:
            break
    return out


def _article_to_record(u: str, exchange: str, market: str) -> Dict[str, Any]:
    s = BeautifulSoup(get_html(u), "html.parser")
    title = s.find(["h1", "h2"]).get_text(" ", strip=True) if s.find(["h1", "h2"]) else ""
    plain = s.get_text(" ", strip=True)
    syms = extract_symbols(plain)
    cands = parse_all_times(plain)

    # як fallback — час публікації (може бути корисним як орієнтир)
    meta_iso = extract_meta_dt(s)
    if meta_iso and meta_iso not in cands:
        cands.append(meta_iso)

    return {
        "exchange": exchange,
        "market": market,
        "title": title,
        "symbols": syms,
        "time_candidates": cands,           # <— ВСІ знайдені часи
        "start_text": cands[0] if cands else None,  # перший кандидат
        "url": u,
    }


def mexc_futures_latest() -> List[Dict[str, Any]]:
    links = _collect_generic(URLS["mexc_futures"], base_domain="https://www.mexc.com")
    return [_article_to_record(u, "mexc", "futures") for u in links]


def gate_spot_latest() -> List[Dict[str, Any]]:
    links = _collect_generic(URLS["gate_spot"], base_domain="https://www.gate.com")
    return [_article_to_record(u, "gate", "spot") for u in links]


def gate_futures_latest() -> List[Dict[str, Any]]:
    links = _collect_generic(URLS["gate_futures"], base_domain="https://www.gate.com")
    return [_article_to_record(u, "gate", "futures") for u in links]


def bingx_spot_latest() -> List[Dict[str, Any]]:
    links = _collect_generic(URLS["bingx_spot"])
    return [_article_to_record(u, "bingx", "spot") for u in links]


def bingx_futures_latest() -> List[Dict[str, Any]]:
    links = _collect_generic(URLS["bingx_futures"])
    return [_article_to_record(u, "bingx", "futures") for u in links]


def bitget_spot_latest() -> List[Dict[str, Any]]:
    links = _collect_generic(URLS["bitget_spot"])
    return [_article_to_record(u, "bitget", "spot") for u in links]


def bitget_futures_latest() -> List[Dict[str, Any]]:
    links = _collect_generic(URLS["bitget_futures"])
    return [_article_to_record(u, "bitget", "futures") for u in links]


def okx_latest() -> List[Dict[str, Any]]:
    links = _collect_generic(URLS["okx_all"])
    out = []
    for u in links:
        rec = _article_to_record(u, "okx", "spot")  # дефолт spot
        # якщо у тексті є ознаки ф’ючерсів — переключаємо
        txt = (rec["title"] + " " + get_html(u)).lower()
        if any(k in txt for k in ["swap", "perpetual", "ф'ючер", "фьючер"]):
            rec["market"] = "futures"
        out.append(rec)
    return out


def binance_latest(rows: int = 20) -> List[Dict[str, Any]]:
    """
    Через CMS ліст отримуємо id статей, але сам текст і час тягнемо зі сторінок:
    посилання на список ти дав, тому парсимо так само зі сторінок.
    """
    # просто парсимо саму сторінку розділу (у Binance там теж є посилання на статті)
    links = _collect_generic(URLS["binance_all"])
    return [_article_to_record(u, "binance", "spot") for u in links]


def sources_matrix() -> List:
    # реєстр колекторів анонсів
    return [
        mexc_futures_latest,
        gate_spot_latest,
        gate_futures_latest,
        bingx_spot_latest,
        bingx_futures_latest,
        bitget_spot_latest,
        bitget_futures_latest,
        okx_latest,
        binance_latest,
    ]

# Індексні сторінки (тільки ті, що ти попросив)
_INDEX_URLS = {
    ("mexc", "futures"):  ["https://www.mexc.com/uk-UA/announcements/new-listings/19"],
    ("gate", "spot"):     ["https://www.gate.com/ru/announcements/newspotlistings"],
    ("gate", "futures"):  ["https://www.gate.com/ru/announcements/newfutureslistings"],
    ("bingx", "spot"):    ["https://bingx.com/en/support/notice-center/11257060005007"],
    ("bingx", "futures"): ["https://bingx.com/en/support/notice-center/11257015822991"],
    ("bitget", "spot"):   ["https://www.bitget.com/support/sections/5955813039257"],
    ("bitget", "futures"):["https://www.bitget.com/support/sections/12508313405000"],
    ("okx", "spot"):      ["https://www.okx.com/ua/help/section/announcements-new-listings"],
    ("okx", "futures"):   ["https://www.okx.com/ua/help/section/announcements-new-listings"],
    ("binance", "spot"):  ["https://www.binance.com/en/support/announcement/list/48"],
    ("binance", "futures"):["https://www.binance.com/en/support/announcement/list/48"],
}

# Які посилання на детальні статті вважати релевантними для кожної біржі
def _is_detail_link(href: str, exchange: str) -> bool:
    if not href:
        return False
    if exchange == "mexc":
        return "/announcements/article/" in href
    if exchange == "gate":
        return ("/announcements/detail" in href) or ("/announcements/article" in href)
    if exchange == "bingx":
        return ("/support/announcements/" in href) or ("/support/notice-center/" in href)
    if exchange == "bitget":
        return "/support/articles/" in href
    if exchange == "okx":
        return "/help/articles/" in href
    if exchange == "binance":
        return "/support/announcement/" in href
    return False

def _abs_url(href: str, exchange: str) -> str:
    if href.startswith("http"):
        return href
    base = {
        "mexc":   "https://www.mexc.com",
        "gate":   "https://www.gate.com",
        "bingx":  "https://bingx.com",
        "bitget": "https://www.bitget.com",
        "okx":    "https://www.okx.com",
        "binance":"https://www.binance.com",
    }.get(exchange, "")
    if not href.startswith("/"):
        href = "/" + href
    return base + href

# Витягнути максимум варіантів часу з тексту
def _extract_time_candidates_from_text(text: str) -> list[str]:
    cands = []
    # 1) Наш головний парсер (повертає один «гарний» дисплей)
    dt, disp = parse_dt_and_display(text)
    if disp:
        cands.append(disp)

    # 2) Усі згадки «HH:MM TZ»
    for m in RE_TZ_LABEL.finditer(text):
        hh = m.group("h")
        mm = m.group("m")
        lb = (m.group("label") or "").upper()
        cands.append(f"{hh}:{mm} {lb}")

    # 3) Простий ISO-подібний формат дати/часу без TZ
    for m in re.finditer(r"\b(20\d{2}-\d{2}-\d{2})\s+(\d{1,2}:\d{2})\b", text):
        cands.append(f"{m.group(1)} {m.group(2)}")

    # 4) Локальні формати типу «HH:MM (за Києвом)»
    for m in RE_KYIV_LABEL.finditer(text):
        t = m.group("t")
        cands.append(f"{t} (за Києвом)")

    # унікалізація з порядком
    seen, out = set(), []
    for t in cands:
        t = (t or "").strip()
        if t and t not in seen:
            seen.add(t)
            out.append(t)
    return out

def _looks_like_market(text: str, market: str) -> bool:
    """
    Дуже проста евристика: якщо futures — шукаємо згадки futures/perpetual/swap;
    якщо spot — відкидаємо явні futures-згадки.
    """
    t = (text or "").lower()
    fut = any(k in t for k in ["futures", "perpetual", "swap", "фьючер", "ф'ючер", "usdt-m"])
    if market == "futures":
        return fut
    # spot:
    return not fut

def _find_article_for_pair(exchange: str, market: str, base: str, quote: str) -> tuple[list[str], str | None]:
    """
    Повертає (time_candidates, source_url) для конкретної пари з оголошень біржі.
    Якщо не знайдено — ([], None).
    """
    ex = exchange.lower()
    mk = market.lower()
    idx_urls = _INDEX_URLS.get((ex, mk)) or []
    if not idx_urls:
        return [], None

    base_u = (base or "").upper()
    quote_u = (quote or "").upper()

    for idx in idx_urls:
        try:
            html = get_html(idx)
        except Exception:
            continue

        soup = BeautifulSoup(html, "html.parser")
        # з індексної — збираємо посилання на статті
        links = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if _is_detail_link(href, ex):
                links.append(_abs_url(href, ex))
        links = uniq(links)[:30]

        # проходимо статті й шукаємо ту, де є наша пара та збігається ринок
        for u in links:
            try:
                s = BeautifulSoup(get_html(u), "html.parser")
            except Exception:
                continue

            title = s.find(["h1", "h2"])
            title_text = title.get_text(" ", strip=True) if title else ""
            plain = s.get_text(" ", strip=True)
            syms = extract_symbols(plain)  # повертає список BASE із контексту .../USDT
            # Фільтр по парі (base/quote)
            if base_u not in syms:
                # іноді символ у заголовку, підстрахуємось:
                if base_u not in extract_symbols(title_text):
                    continue

            # Фільтр по ринку
            blob = f"{title_text}\n{plain}"
            if not _looks_like_market(blob, mk):
                continue

            # Виймаємо усі можливі варіанти часу з тексту
            cands = _extract_time_candidates_from_text(blob)
            if cands:
                return cands, u

    return [], None

def ann_lookup_listing_time(exchange: str, market: str, base: str, quote: str) -> dict:
    """
    Публічний API для app.py:
      - Повертає dict з полями:
          {
            "time_candidates": [ "2025-10-08 07:40 UTC+8", "07:40 (за Києвом)", ... ],
            "source_url": "https://.../article/...."
          }
      - Якщо нічого не знайшли: {"time_candidates": [], "source_url": None}

    Якщо твій app.py очікує інтерфейс виду (list[str], Optional[str]) —
    просто заміни return на: `return cands, url`
    """
    cands, url = _find_article_for_pair(exchange, market, base, quote)
    return {"time_candidates": cands, "source_url": url}
# === END ADDITION =============================================================
