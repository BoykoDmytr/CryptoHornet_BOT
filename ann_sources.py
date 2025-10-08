# ann_sources.py
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

# ---- TLS-імперсонація Chrome (для обходу 403, якщо потрібно) ----
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
    # “хромові” заголовки — інколи допомагають на MEXC/Gate
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

# Приклад: "https://<your-worker>.workers.dev?url=" або "https://.../fetch?url=" або "https://.../proxy/"
SCRAPER_URL = (os.getenv("SCRAPER_URL", "") or "").rstrip("/")

# локальні/мовні кукі (інколи прибирають 403)
try:
    SESSION.cookies.set("locale", os.getenv("MEXC_LOCALE", "uk-UA"), domain=".mexc.com")
    SESSION.cookies.set("lang", "ru_RU", domain=".gate.com")
except Exception:
    pass


def _wrap_scraper_target(q: str) -> str:
    """
    Гнучко формуємо URL до зовнішнього скрейпера:
    - якщо SCRAPER_URL містить '{url}' -> підставляємо
    - якщо закінчується '=' -> просто додаємо закодований q
    - якщо вже є '?', але нема '=' в кінці -> додаємо '&url=' + q
    - інакше -> '?url=' + q
    """
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
    # 1) прямий запит звичайним requests
    try:
        r = SESSION.get(url, params=params, timeout=25, allow_redirects=True, proxies=PROXIES)
        r.raise_for_status()
        return r
    except requests.HTTPError as e:
        code = getattr(e.response, "status_code", None)

        # 2) якщо 403/503 — пробуємо TLS-імперсонацію реального Chrome
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

        # 3) останній шанс — зовнішній скрейпер (якщо заданий)
        if code in (403, 503) and SCRAPER_URL:
            q = url
            if params:
                q = url + ("&" if "?" in url else "?") + urlencode(params)
            final = _wrap_scraper_target(q)
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

# ширший патерн пар: /, _, -, з/без суфікса -M
RE_PAIR = re.compile(r"\b([A-Z0-9]{2,})\s*(?:/|_|-)?\s*USDT(?:-M)?\b")


def uniq(seq: Iterable[str]) -> List[str]:
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


def _mk_dt(d: int, mon_name: str, y: int, h: int, m: int, tz_token: Optional[str]):
    mon = MONTHS.get(mon_name.lower())
    if not mon:
        return None
    naive = datetime(y, mon, d, h, m)
    if tz_token and tz_token.upper() in ("UTC", "GMT"):
        return pytz.utc.localize(naive).astimezone(UA_TZ)
    return UA_TZ.localize(naive)


def parse_dt_kiev(text: str) -> Optional[datetime]:
    t = " ".join((text or "").split())
    m = RE_TIME_FIRST.search(t)
    if m:
        return _mk_dt(int(m["d"]), m["mon"], int(m["y"]), int(m["h"]), int(m["m"]), m.group("tz"))
    m2 = RE_DATE_FIRST.search(t)
    if m2:
        return _mk_dt(int(m2["d"]), m2["mon"], int(m2["y"]), int(m2["h"]), int(m2["m"]), m2.group("tz"))
    return None


# ----------- display-час як у статті + dt у Києві ----------------------------

RE_TZ_LABEL = re.compile(
    r"(?P<h>\d{1,2}):(?P<m>\d{2})\s*(?P<label>"
    r"UTC(?:[+\-]\d{1,2})?|GMT|MSK|MSD|HKT|SGT|KST|JST|CET|CEST|EET|EEST|PST|PDT|EST|EDT"
    r")\b",
    re.I,
)
RE_KYIV_LABEL = re.compile(r"(?P<t>\d{1,2}:\d{2})\s*\((?:за|по)\s*києвом\)", re.I)


def _valid_hm(t: str) -> bool:
    try:
        h, m = t.split(":")
        h = int(h); m = int(m)
        return 0 <= h < 24 and 0 <= m < 60
    except Exception:
        return False

def parse_dt_and_display(text: str) -> tuple[Optional[datetime], Optional[str]]:
    t = " ".join(text.split())

    m = RE_TZ_LABEL.search(t)
    if m and _valid_hm(m.group('t')):
        disp = f"{m.group('t')} {m.group('label').upper()}"
        dt = parse_dt_kiev(t)
        return dt, disp

    m2 = RE_KYIV_LABEL.search(t)
    if m2 and _valid_hm(m2.group('t')):
        disp = f"{m2.group('t')} (за Києвом)"
        dt = parse_dt_kiev(t)
        return dt, disp

    dt = parse_dt_kiev(t)
    if dt:
        # якщо в тексті згаданий UTC/GMT — виведемо його, інакше локальний
        if " utc" in t.lower() or " gmt" in t.lower():
            return dt, f"{dt.astimezone(pytz.utc).strftime('%Y-%m-%d %H:%M')} UTC"
        else:
            return dt, dt.strftime("%Y-%m-%d %H:%M")
    return None, None


def _dt_from_iso(s: str) -> Optional[datetime]:
    if not s:
        return None
    try:
        s2 = s.strip()
        if s2.endswith("Z"):
            s2 = s2[:-1] + "+00:00"
        dt = datetime.fromisoformat(s2)
        if dt.tzinfo is None:
            dt = pytz.utc.localize(dt)
        return dt.astimezone(UA_TZ)
    except Exception:
        return None


def extract_meta_dt(soup: BeautifulSoup) -> Optional[datetime]:
    """Пробуємо витягнути дату публікації зі стандартних метаданих/ld+json."""
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

    for s in cands:
        dt = _dt_from_iso(s)
        if dt:
            return dt
    return None


# ---------------- MEXC Futures (тільки цей URL) ------------------------------

def mexc_futures_latest(locale: Optional[str] = None) -> List[Dict[str, Any]]:
    # Тільки з: https://www.mexc.com/uk-UA/announcements/new-listings/19
    url = "https://www.mexc.com/uk-UA/announcements/new-listings/19"
    soup = BeautifulSoup(get_html(url), "html.parser")
    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/announcements/article/" in href:
            if not href.startswith("http"):
                href = "https://www.mexc.com" + (href if href.startswith("/") else "/" + href)
            links.append(href)
    links = uniq(links)[:20]

    out: List[Dict[str, Any]] = []
    for u in links:
        s = BeautifulSoup(get_html(u), "html.parser")
        title = s.find("h1").get_text(" ", strip=True) if s.find("h1") else ""
        plain = s.get_text(" ", strip=True)
        syms = extract_symbols(plain)
        dt, disp = parse_dt_and_display(plain)
        out.append({"exchange": "mexc", "market": "futures", "title": title, "symbols": syms,
                    "start_dt": dt, "start_text": disp, "url": u})
    return out


# ---------------- Gate: Spot / Futures (тільки gate.com RU) ------------------

def _gate_collect_ru(market: str) -> List[Dict[str, Any]]:
    if market == "spot":
        url = "https://www.gate.com/ru/announcements/newspotlistings"
    else:
        url = "https://www.gate.com/ru/announcements/newfutureslistings"

    html = get_html(url)
    soup = BeautifulSoup(html, "html.parser")
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
        dt, disp = parse_dt_and_display(plain)
        out.append({"exchange": "gate", "market": market, "title": title, "symbols": syms,
                    "start_dt": dt, "start_text": disp, "url": u})
    return out

def gate_spot_latest() -> List[Dict[str, Any]]:
    return _gate_collect_ru("spot")

def gate_futures_latest() -> List[Dict[str, Any]]:
    return _gate_collect_ru("futures")


# ---------------- BingX: Spot / Futures (тільки ці URL) ----------------------

def _bingx_collect(section_url: str, market: str) -> List[Dict[str, Any]]:
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
        dt, disp = parse_dt_and_display(plain)
        if dt is None:
            dt = extract_meta_dt(s)
        out.append({"exchange": "bingx", "market": market, "title": title, "symbols": syms,
                    "start_dt": dt, "start_text": disp, "url": u})
    return out

def bingx_spot_latest() -> List[Dict[str, Any]]:
    return _bingx_collect("https://bingx.com/en/support/notice-center/11257060005007", "spot")

def bingx_futures_latest() -> List[Dict[str, Any]]:
    return _bingx_collect("https://bingx.com/en/support/notice-center/11257015822991", "futures")


# ---------------- Bitget: Spot / Futures (тільки ці URL) ---------------------

def _bitget_collect(section_url: str, market: str) -> List[Dict[str, Any]]:
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
        dt, disp = parse_dt_and_display(plain)
        if dt is None:
            dt = extract_meta_dt(s)
        out.append({"exchange": "bitget", "market": market, "title": title, "symbols": syms,
                    "start_dt": dt, "start_text": disp, "url": u})
    return out

def bitget_spot_latest() -> List[Dict[str, Any]]:
    return _bitget_collect("https://www.bitget.com/support/sections/5955813039257", "spot")

def bitget_futures_latest() -> List[Dict[str, Any]]:
    return _bitget_collect("https://www.bitget.com/support/sections/12508313405000", "futures")


# ---------------- OKX: Spot + Futures (лише ця секція) -----------------------

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
        market = "futures" if any(k in plain.lower() for k in ["swap", "perpetual", "ф'ючер", "фьючер"]) else "spot"
        dt, disp = parse_dt_and_display(plain)
        if dt is None:
            dt = extract_meta_dt(s)
        out.append({"exchange": "okx", "market": market, "title": title, "symbols": syms,
                    "start_dt": dt, "start_text": disp, "url": u})
    return out


# ---------------- Binance: Spot (+Futures/Alpha якщо сторінка це містить) ----
# Вимога: парсити ТІЛЬКИ звідси: https://www.binance.com/en/support/announcement/list/48
# (це список оголошень, далі переходимо в самі статті й тягнемо час)

def binance_latest(rows: int = 30) -> List[Dict[str, Any]]:
    list_url = "https://www.binance.com/en/support/announcement/list/48"
    html = get_html(list_url)
    soup = BeautifulSoup(html, "html.parser")

    # зберемо посилання на окремі анонси
    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        # посилання на сторінку анонсу
        if "/en/support/announcement/" in href:
            if not href.startswith("http"):
                href = "https://www.binance.com" + href
            links.append(href)
    links = uniq(links)[:rows] if links else []

    out: List[Dict[str, Any]] = []
    for u in links:
        s = BeautifulSoup(get_html(u), "html.parser")
        title = s.find("h1").get_text(" ", strip=True) if s.find("h1") else ""
        plain = s.get_text(" ", strip=True)
        syms = extract_symbols(plain)
        dt, disp = parse_dt_and_display(plain)
        if dt is None:
            dt = extract_meta_dt(s)

        # Маркет: цю сторінку ми трактуємо як SPOT-листинги.
        # Якщо в заголовку/тексті явно згаданий futures/perpetual — позначимо "futures".
        market = "spot"
        tl = (title or "").lower()
        pl = (plain or "").lower()
        if "futures" in tl or "futures" in pl or "perpetual" in pl:
            market = "futures"
        if "alpha" in tl or "alpha" in pl:
            # за потреби ти відфільтруєш (у основному коді)
            market = "alpha"

        out.append({
            "exchange": "binance",
            "market": market,
            "title": title,
            "symbols": syms,
            "start_dt": dt,
            "start_text": disp,
            "url": u
        })
    return out


# ------ реєстр джерел (парсимо тільки з наданих URL) -------------------------

def sources_matrix() -> List:
    """
    Використовується лише якщо десь потрібно пройтися по всіх джерелах.
    Важливо: тільки ті URL, які надав користувач.
    """
    out = []
    out.append(mexc_futures_latest)               # MEXC Futures (uk-UA)
    out.append(gate_spot_latest)                  # Gate Spot (gate.com/ru)
    out.append(gate_futures_latest)               # Gate Futures (gate.com/ru)
    out.append(bingx_spot_latest)                 # BingX Spot
    out.append(bingx_futures_latest)              # BingX Futures
    out.append(bitget_spot_latest)                # Bitget Spot
    out.append(bitget_futures_latest)             # Bitget Futures
    out.append(okx_latest)                        # OKX Spot+Futures (секція new listings)
    out.append(binance_latest)                    # Binance Spot (+futures/alpha якщо є у пості)
    return out
