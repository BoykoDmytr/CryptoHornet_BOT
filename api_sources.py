# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import time
import json
import logging
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Optional
import datetime as _dt
import pytz as _pytz
import requests

log = logging.getLogger("hornet.api")

# Таймзони для довідки/логів (видруковуємо "detected:")
EXCHANGE_TZ = {
    "binance": _pytz.utc,
    "okx": _pytz.utc,
    "gate": _pytz.timezone("Asia/Shanghai"),
    "bitget": _pytz.timezone("Asia/Singapore"),
    "mexc": _pytz.timezone("Asia/Singapore"),
    "bingx": _pytz.timezone("Asia/Singapore"),
    "bybit": _pytz.timezone("Asia/Singapore"),
    "bithumb": _pytz.timezone("Asia/Seoul"),
    "upbit": _pytz.timezone("Asia/Seoul"),
}

ONLY_USDT = os.getenv("API_ONLY_USDT", "1") == "1"

# ------------------ HTTP session ------------------
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/126.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json,text/plain,*/*",
    "Accept-Language": "en-US,en;q=0.9,uk-UA;q=0.8,ru-RU;q=0.7",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
})

# --- time formatting helpers (показуємо час у ТЗ біржі, як є) ---
def _fmt_tzlabel(dt: _dt.datetime) -> str:
    off = dt.utcoffset() or _dt.timedelta(0)
    hours = int(off.total_seconds() // 3600)
    return "UTC" if hours == 0 else f"UTC{hours:+d}"

def _fmt_ts_for_exchange(ts_ms: int, exchange: str) -> str:
    tz = EXCHANGE_TZ.get(exchange.lower(), _pytz.utc)
    if ts_ms < 10**12:  # сек -> мс
        ts_ms *= 1000
    dt = _dt.datetime.fromtimestamp(ts_ms / 1000.0, tz)
    return dt.strftime("%Y-%m-%d %H:%M ") + _fmt_tzlabel(dt)

# чи фільтрувати лише USDT-коти
ONLY_USDT = (os.getenv("API_ONLY_USDT", "1").lower() not in ("0", "false", "no"))
HTTP_PROXY = os.getenv("HTTP_PROXY") or os.getenv("http_proxy")
HTTPS_PROXY = os.getenv("HTTPS_PROXY") or os.getenv("https_proxy")
PROXIES = {"http": HTTP_PROXY, "https": HTTPS_PROXY} if (HTTP_PROXY or HTTPS_PROXY) else None

def _parse_json_text(text: str):
    try:
        obj = json.loads(text)
        # інколи API повертає JSON-рядок всередині JSON-рядка
        for _ in range(2):
            if isinstance(obj, str):
                obj = json.loads(obj)
            else:
                break
        return obj
    except Exception:
        return text

def _get_json(url: str, params: dict | None = None, headers: dict | None = None, timeout: int = 25):
    h = dict(SESSION.headers)
    if headers:
        h.update(headers)
    r = SESSION.get(url, params=params, headers=h, timeout=timeout, proxies=PROXIES)
    r.raise_for_status()
    ct = (r.headers.get("content-type") or "").lower()
    if "application/json" in ct:
        try:
            return r.json()
        except Exception:
            return _parse_json_text(r.text)
    return _parse_json_text(r.text)



def _now_utc_text() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

def api_now_exchange_iso(exchange: str) -> str:
    tz = EXCHANGE_TZ.get((exchange or "").lower(), _pytz.utc)
    return _dt.datetime.now(tz).strftime("%Y-%m-%d %H:%M %Z")

SUPPORTED: Dict[str, Tuple[bool, bool]] = {
    "binance": (True, True),
    "okx":     (True, True),
    "gate":    (True, True),
    "bitget":  (True, True),
    "mexc":    (False, True),
    "bingx":   (True, True),
    "bybit":   (True, True),
    "bithumb": (True, False),
    "upbit":   (True, False),
}

def api_lookup_listing_time(exchange: str, market: str, base: str, quote: str) -> tuple[Optional[str], Optional[int]]:
    """
    Повертає (start_text, start_ts_ms) якщо біржа дає час лістингу у своєму API.
    Якщо немає — (None, None). НІЧОГО не конвертуємо в Київ; показуємо у ТЗ біржі.
    """
    ex = exchange.lower()
    mk = market.lower()

    try:
        # ---------- BINANCE ----------
        if ex == "binance":
            if mk == "spot":
                j = _get_json("https://api.binance.com/api/v3/exchangeInfo", params={"symbol": f"{base}{quote}"})
                syms = j.get("symbols", [])
                if syms:
                    onboard = syms[0].get("onboardDate")
                    if onboard:
                        return _fmt_ts_for_exchange(int(onboard), ex), int(onboard)
            else:  # futures
                j = _get_json("https://fapi.binance.com/fapi/v1/exchangeInfo", params={"symbol": f"{base}{quote}"})
                syms = j.get("symbols", [])
                if syms:
                    onboard = syms[0].get("onboardDate") or syms[0].get("onboardTs")
                    if onboard:
                        return _fmt_ts_for_exchange(int(onboard), ex), int(onboard)
            return None, None

        # ---------- OKX ----------
        if ex == "okx":
            if mk == "spot":
                j = _get_json("https://www.okx.com/api/v5/public/instruments",
                              params={"instType": "SPOT", "instId": f"{base}-{quote}"})
            else:
                j = _get_json("https://www.okx.com/api/v5/public/instruments",
                              params={"instType": "SWAP", "instId": f"{base}-{quote}-SWAP"})
            data = (j or {}).get("data") or []
            if data:
                it = data[0]
                ts = it.get("listTime") or it.get("listTs") or it.get("launchTime")
                if ts:
                    ts = int(ts)
                    return _fmt_ts_for_exchange(ts, ex), ts
            return None, None

        # ---------- GATE ----------
        if ex == "gate":
            if mk == "futures":
                # деталь контракту
                j = _get_json(f"https://api.gateio.ws/api/v4/futures/usdt/contracts/{base}_{quote}")
                # шукаємо кандидатні поля часу
                for key in ("launch_time", "inited_at", "create_time", "start_time", "listing_time"):
                    v = j.get(key)
                    if v:
                        # у Gate часто секунди (int/str)
                        v = int(v)
                        ts_ms = v * 1000 if v < 10**12 else v
                        return _fmt_ts_for_exchange(ts_ms, ex), ts_ms
            # spot у Gate часу не дає
            return None, None

        # ---------- BITGET ----------
        if ex == "bitget":
            if mk == "spot":
                j = _get_json("https://api.bitget.com/api/spot/v1/public/products")
                for it in j.get("data", []):
                    if (it.get("baseCoin") or "").upper() == base and (it.get("quoteCoin") or "").upper() == quote:
                        # інколи є listTime
                        ts = it.get("listTime") or it.get("onlineTime")
                        if ts:
                            ts = int(ts)
                            return _fmt_ts_for_exchange(ts, ex), ts
                return None, None
            else:
                j = _get_json("https://api.bitget.com/api/mix/v1/market/contracts", params={"productType": "umcbl"})
                target = f"{base}{quote}_UMCBL"
                for it in j.get("data", []):
                    if (it.get("symbol") or "").upper() == target:
                        ts = it.get("listTime") or it.get("launchTime") or it.get("onLineTime")
                        if ts:
                            ts = int(ts)
                            return _fmt_ts_for_exchange(ts, ex), ts
                return None, None

        # ---------- MEXC (тільки futures) ----------
        if ex == "mexc" and mk == "futures":
            # спроба через detail
            for url in (
                f"https://contract.mexc.com/api/v1/contract/detail",
                f"https://contract.mexc.com/api/v1/contract/list",
            ):
                try:
                    j = _get_json(url)
                except Exception:
                    continue
                data = j.get("data", j.get("result", []))
                if isinstance(data, dict):
                    data = data.get("symbols") or data.get("list") or []
                target1 = f"{base}_{quote}"
                target2 = f"{base}{quote}"
                # шукаємо контракт
                for it in data or []:
                    sym = (it.get("symbol") or it.get("instrument_id") or it.get("contractId") or "")
                    s = str(sym).upper()
                    if s not in (target1, target2):
                        continue
                    # кандидатні поля часу
                    for key in ("onlineTime", "stateTime", "startTime", "launchTime", "listingTime"):
                        v = it.get(key)
                        if v:
                            v = int(v)
                            ts_ms = v if v >= 10**12 else v * 1000
                            return _fmt_ts_for_exchange(ts_ms, ex), ts_ms
                # якщо не знайшли на цьому URL — пробуємо наступний
            return None, None

        # ---------- BYBIT ----------
        if ex == "bybit":
            cat = "spot" if mk == "spot" else "linear"
            j = _get_json("https://api.bybit.com/v5/market/instruments-info", params={"category": cat})
            lst = (j.get("result") or {}).get("list") or []
            # в API буває подвійний список
            seq = []
            for item in lst:
                if isinstance(item, dict) and "list" in item:
                    seq += item.get("list") or []
                else:
                    seq.append(item)
            for it in seq:
                if (it.get("symbol") or "").upper() == f"{base}{quote}":
                    for key in ("launchTime", "listTime", "createdTime"):
                        v = it.get(key)
                        if v:
                            v = int(v)
                            ts_ms = v if v >= 10**12 else v * 1000
                            return _fmt_ts_for_exchange(ts_ms, ex), ts_ms
            return None, None

        # ---------- BINGX (futures) ----------
        if ex == "bingx" and mk == "futures":
            j = _get_json("https://open-api.bingx.com/openApi/swap/v2/quote/contracts")
            data = (j.get("data") or {}).get("contracts") or j.get("data") or []
            target1 = f"{base}-{quote}".upper()
            target2 = f"{base}{quote}".upper()
            for it in data:
                s = (it.get("symbol") or it.get("contractName") or "").upper()
                if s not in (target1, target2):
                    continue
                for key in ("listingTime", "launchTime", "onlineTime", "createTime"):
                    v = it.get(key)
                    if v:
                        v = int(v)
                        ts_ms = v if v >= 10**12 else v * 1000
                        return _fmt_ts_for_exchange(ts_ms, ex), ts_ms
            return None, None

    except Exception:
        return None, None

    return None, None



# ------------------ BINANCE ------------------
def _binance_spot() -> Dict[str, str]:
    j = _get_json("https://api.binance.com/api/v3/exchangeInfo")
    out: Dict[str, str] = {}
    for s in j.get("symbols", []):
        if s.get("status") != "TRADING":
            continue
        base, quote = (s.get("baseAsset") or "").upper(), (s.get("quoteAsset") or "").upper()
        if not base or not quote:
            continue
        if ONLY_USDT and quote != "USDT":
            continue
        out[f"{base}/{quote}"] = f"https://www.binance.com/en/trade/{base}_{quote}"
    return out

def _binance_futures() -> Dict[str, str]:
    j = _get_json("https://fapi.binance.com/fapi/v1/exchangeInfo")
    out: Dict[str, str] = {}
    for s in j.get("symbols", []):
        if s.get("status") != "TRADING":
            continue
        if s.get("contractType") != "PERPETUAL":
            continue
        base, quote = (s.get("baseAsset") or "").upper(), (s.get("quoteAsset") or "").upper()
        if not base or not quote:
            continue
        if ONLY_USDT and quote != "USDT":
            continue
        out[f"{base}/{quote}"] = f"https://www.binance.com/en/futures/{base}{quote}"
    return out

# ------------------ OKX ------------------
def _okx_spot() -> Dict[str, str]:
    j = _get_json("https://www.okx.com/api/v5/public/instruments", params={"instType": "SPOT"})
    out: Dict[str, str] = {}
    for it in j.get("data", []):
        inst_id = it.get("instId")  # BTC-USDT
        if not inst_id or "-" not in inst_id:
            continue
        base, quote = inst_id.split("-")[0].upper(), inst_id.split("-")[1].upper()
        if ONLY_USDT and quote != "USDT":
            continue
        out[f"{base}/{quote}"] = f"https://www.okx.com/trade-spot/{base}-{quote}"
    return out

def _okx_futures() -> Dict[str, str]:
    j = _get_json("https://www.okx.com/api/v5/public/instruments", params={"instType": "SWAP"})
    out: Dict[str, str] = {}
    for it in j.get("data", []):
        inst_id = it.get("instId")  # BTC-USDT-SWAP
        if not inst_id or "-" not in inst_id:
            continue
        parts = inst_id.split("-")
        if len(parts) < 2:
            continue
        base, quote = parts[0].upper(), parts[1].upper()
        if ONLY_USDT and quote != "USDT":
            continue
        out[f"{base}/{quote}"] = f"https://www.okx.com/trade-swap/{base}-{quote}-SWAP"
    return out

# ------------------ GATE ------------------
def _gate_spot() -> Dict[str, str]:
    j = _get_json("https://api.gateio.ws/api/v4/spot/currency_pairs")
    out: Dict[str, str] = {}
    for it in j:
        base, quote = (it.get("base") or "").upper(), (it.get("quote") or "").upper()
        if not base or not quote:
            continue
        if ONLY_USDT and quote != "USDT":
            continue
        out[f"{base}/{quote}"] = f"https://www.gate.io/trade/{base}_{quote}"
    return out

def _gate_futures() -> Dict[str, str]:
    j = _get_json("https://api.gateio.ws/api/v4/futures/usdt/contracts")
    out: Dict[str, str] = {}
    for it in j:
        name = (it.get("name") or "").upper()  # BTC_USDT
        if "_" not in name:
            continue
        base, quote = name.split("_", 1)[0], name.split("_", 1)[1]
        if ONLY_USDT and quote != "USDT":
            continue
        out[f"{base}/{quote}"] = f"https://www.gate.io/futures_trade/USDT/{base}_{quote}"
    return out

# ------------------ BITGET ------------------
def _bitget_spot() -> Dict[str, str]:
    # правильний ендпоїнт: /api/spot/v1/public/products
    j = _get_json("https://api.bitget.com/api/spot/v1/public/products")
    out: Dict[str, str] = {}
    for it in j.get("data", []) or []:
        base = (it.get("baseCoin") or "").upper()
        quote = (it.get("quoteCoin") or "").upper()
        sym = (it.get("symbol") or "").upper()  # BTCUSDT
        if not base or not quote or not sym:
            continue
        if ONLY_USDT and quote != "USDT":
            continue
        out[f"{base}/{quote}"] = f"https://www.bitget.com/spot/{base}{quote}"
    return out

def _bitget_futures() -> Dict[str, str]:
    # USDT-M perpetuals (productType=umcbl)
    j = _get_json("https://api.bitget.com/api/mix/v1/market/contracts", params={"productType": "umcbl"})
    out: Dict[str, str] = {}
    for it in j.get("data", []) or []:
        sym = (it.get("symbol") or "").upper()  # BTCUSDT_UMCBL
        if "USDT" not in sym:
            continue
        base = sym.split("USDT")[0]
        quote = "USDT"
        if ONLY_USDT and quote != "USDT":
            continue
        out[f"{base}/{quote}"] = f"https://www.bitget.com/mix/usdt/{base}{quote}"
    return out

# ------------------ MEXC ------------------
#def _mexc_spot() -> Dict[str, str]:
    """
    Основний: /api/v3/exchangeInfo
    Фолбек:   /api/v3/ticker/price  (будуємо пари з символів)
    """
    out: Dict[str, str] = {}

    def _add(base: str, quote: str):
        base, quote = (base or "").upper(), (quote or "").upper()
        if not base or not quote:
            return
        if ONLY_USDT and quote != "USDT":
            return
        out[f"{base}/{quote}"] = f"https://www.mexc.com/exchange/{base}_{quote}"

    # primary: v3/exchangeInfo
    try:
        j = _get_json("https://api.mexc.com/api/v3/exchangeInfo")
        symbols = j.get("symbols", []) if isinstance(j, dict) else []
        if symbols:
            ok_status = {"TRADING", "ENABLED", "PENDING_TRADING", "PRE_TRADING"}
            for it in symbols:
                base = it.get("baseAsset")
                quote = it.get("quoteAsset")
                status = (it.get("status") or it.get("state") or "").upper()
                if status and status not in ok_status:
                    continue
                _add(base, quote)
    except Exception as e:
        log.warning("mexc spot v3 error: %s", e)

    # fallback: v3/ticker/price (повертає масив символів типу BTCUSDT)
    if not out:
        try:
            j2 = _get_json("https://api.mexc.com/api/v3/ticker/price")
            arr = j2 if isinstance(j2, list) else (j2.get("data") or j2.get("symbols") or [])
            for it in arr:
                sym = (it.get("symbol") if isinstance(it, dict) else it) or ""
                sym = str(sym).upper()
                if not sym:
                    continue

                base, quote = None, None
                if sym.endswith("USDT"):
                    base, quote = sym[:-4], "USDT"
                elif not ONLY_USDT:
                    # спробуємо інші типові коти, якщо не фільтруємо тільки USDT
                    for q in ("USDC", "BTC", "ETH", "MX", "TRY", "BRL", "EUR", "GBP", "RUB", "UAH"):
                        if sym.endswith(q):
                            base, quote = sym[:-len(q)], q
                            break
                if base and quote:
                    _add(base, quote)
        except Exception as e:
            log.warning("mexc spot ticker fallback error: %s", e)

    if not out:
        log.info("mexc/spot: 0 symbols (v3 exchangeInfo і ticker/price порожньо/заблоковано)")
    return out




def _mexc_futures() -> Dict[str, str]:
    out: Dict[str, str] = {}
    for url in [
        "https://contract.mexc.com/api/v1/contract/detail",
        "https://contract.mexc.com/api/v1/contract/list",
    ]:
        try:
            j = _get_json(url)
            data = j.get("data", j.get("result", []))
            if isinstance(data, dict):
                data = data.get("symbols") or data.get("list") or []
            for it in data or []:
                sym = (it.get("symbol") or it.get("instrument_id") or it.get("contractId") or "")
                if not sym:
                    continue
                s = str(sym).upper()  # BTC_USDT або BTCUSDT
                if "_" in s:
                    base, quote = s.split("_", 1)[0], s.split("_", 1)[1]
                elif s.endswith("USDT"):
                    base, quote = s[:-4], "USDT"
                else:
                    continue
                if ONLY_USDT and quote != "USDT":
                    continue
                out[f"{base}/{quote}"] = f"https://www.mexc.com/futures/{base}_{quote}"
            if out:
                return out
        except Exception:
            continue
    return out

# ------------------ BINGX ------------------
def _bingx_headers() -> dict:
    api_key = os.getenv("BINGX_API_KEY", "").strip()
    return {"X-BX-APIKEY": api_key} if api_key else {}

def _bingx_spot() -> Dict[str, str]:
    """
    Основний хост:   https://open-api.bingx.com
    Альтернативний:  https://api-swap-rest.bingx.com
    БЕЗ BINGX_API_KEY часто повертає code != 0 (порожньо).
    """
    hosts = [
        "https://open-api.bingx.com",
        "https://api-swap-rest.bingx.com",
    ]
    out: Dict[str, str] = {}

    for host in hosts:
        try:
            j = _get_json(f"{host}/openApi/spot/v1/common/symbols", headers=_bingx_headers())
            if isinstance(j, str):
                j = _parse_json_text(j)

            data = None
            if isinstance(j, dict):
                code = j.get("code")
                if code not in (0, "0", None):
                    log.warning("bingx/spot(%s): non-success code=%s msg=%s", host, code, j.get("msg"))
                    continue
                data = j.get("data") or []
            elif isinstance(j, list):
                data = j
            else:
                log.warning("bingx/spot(%s): unexpected payload type=%s", host, type(j).__name__)
                continue

            for it in data:
                if not isinstance(it, dict):
                    continue
                base = (it.get("baseAsset") or "").upper()
                quote = (it.get("quoteAsset") or "").upper()
                if not base or not quote:
                    continue
                if ONLY_USDT and quote != "USDT":
                    continue
                out[f"{base}/{quote}"] = f"https://bingx.com/en-us/spot/{base}_{quote}"

            if out:
                return out
        except Exception as e:
            log.warning("bingx/spot host %s error: %s", host, e)

    if not out:
        log.info("bingx/spot: 0 symbols (ймовірно потрібен BINGX_API_KEY або хости обмежені по IP)")
    return out




def _bingx_futures() -> Dict[str, str]:
    j = _get_json("https://open-api.bingx.com/openApi/swap/v2/quote/contracts", headers=_bingx_headers())
    if isinstance(j, str):
        j = _parse_json_text(j)

    # Може бути:
    # { "data": {"contracts": [...] } } або { "data": [...] } або просто [...]
    data = []
    if isinstance(j, dict):
        data = (j.get("data") or {})
        if isinstance(data, dict):
            data = data.get("contracts") or data.get("list") or []
    elif isinstance(j, list):
        data = j

    out: Dict[str, str] = {}
    for it in data or []:
        s = ""
        if isinstance(it, dict):
            s = (it.get("symbol") or it.get("contractName") or "").upper()
        elif isinstance(it, str):
            s = it.upper()
        if not s:
            continue
        s = s.replace("-", "")  # BTC-USDT -> BTCUSDT
        if ONLY_USDT and not s.endswith("USDT"):
            continue
        base, quote = s[:-4], "USDT"
        out[f"{base}/{quote}"] = f"https://bingx.com/en-us/futures/{base}{quote}"
    return out

# ------------------ BYBIT ------------------
def _bybit_spot() -> Dict[str, str]:
    j = _get_json("https://api.bybit.com/v5/market/instruments-info", params={"category": "spot"})
    items = j.get("result", {}).get("list", []) or []
    out: Dict[str, str] = {}
    for it in items:
        sym = (it.get("symbol") or "").upper()  # BTCUSDT
        if not sym:
            continue
        if ONLY_USDT and not sym.endswith("USDT"):
            continue
        base, quote = sym[:-4], "USDT" if sym.endswith("USDT") else sym[-3:]
        out[f"{base}/{quote}"] = f"https://www.bybit.com/en/trade/spot/{base}/USDT"
    return out

def _bybit_futures() -> Dict[str, str]:
    j = _get_json("https://api.bybit.com/v5/market/instruments-info", params={"category": "linear"})
    items = j.get("result", {}).get("list", []) or []
    out: Dict[str, str] = {}
    for it in items:
        sym = (it.get("symbol") or "").upper()
        if not sym:
            continue
        if ONLY_USDT and not sym.endswith("USDT"):
            continue
        base, quote = sym[:-4], "USDT" if sym.endswith("USDT") else sym[-3:]
        out[f"{base}/{quote}"] = f"https://www.bybit.com/en/trade/usdt/{base}USDT"
    return out

# ------------------ BITHUMB (spot) ------------------
def _bithumb_spot() -> Dict[str, str]:
    try:
        j = _get_json("https://api.bithumb.com/public/ticker/ALL")
    except Exception:
        return {}
    data = j.get("data", {})
    out: Dict[str, str] = {}
    for sym, payload in data.items():
        if not isinstance(payload, dict):
            continue
        # формуємо декілька ринків, USDT залишимо обов'язково
        quotes = ("USDT",) if ONLY_USDT else ("USDT", "KRW", "BTC")
        for quote in quotes:
            out[f"{sym}/{quote}"] = f"https://www.bithumb.com/trade/order/{sym}_{quote}"
    return out

# ------------------ UPBIT (spot) ------------------
def _upbit_spot() -> Dict[str, str]:
    j = _get_json("https://api.upbit.com/v1/market/all")
    out: Dict[str, str] = {}
    for it in j:
        market = it.get("market")  # KRW-BTC, USDT-BTC
        if not market or "-" not in market:
            continue
        quote, base = market.split("-")[0].upper(), market.split("-")[1].upper()
        if ONLY_USDT and quote != "USDT":
            continue
        out[f"{base}/{quote}"] = f"https://upbit.com/exchange?code=CRIX.UPBIT.{quote}-{base}"
    return out

# ------------------ API entrypoints ------------------
def api_fetch_snapshot(exchange: str, market: str) -> Dict[str, str]:
    ex = exchange.lower().strip()
    mk = market.lower().strip()

    if ex not in SUPPORTED:
        raise ValueError(f"Unsupported exchange: {exchange}")
    has_spot, has_fut = SUPPORTED[ex]
    if mk == "spot" and not has_spot:
        return {}
    if mk == "futures" and not has_fut:
        return {}

    try:
        if ex == "binance":
            return _binance_spot() if mk == "spot" else _binance_futures()
        if ex == "okx":
            return _okx_spot() if mk == "spot" else _okx_futures()
        if ex == "gate":
            return _gate_spot() if mk == "spot" else _gate_futures()
        if ex == "bitget":
            return _bitget_spot() if mk == "spot" else _bitget_futures()
        if ex == "mexc":
            return _mexc_futures()
        if ex == "bingx":
            return _bingx_spot() if mk == "spot" else _bingx_futures()
        if ex == "bybit":
            return _bybit_spot() if mk == "spot" else _bybit_futures()
        if ex == "bithumb":
            return _bithumb_spot()
        if ex == "upbit":
            return _upbit_spot()
    except requests.HTTPError as e:
        log.warning("api %s/%s HTTP %s for %s: %s",
                    ex, mk, getattr(e.response, "status_code", "?"), exchange, e)
        return {}
    except Exception as e:
        log.warning("api %s/%s error: %s", ex, mk, e)
        return {}

    return {}

def api_build_events_from_diff(
    exchange: str,
    market: str,
    prev_pairs: Dict[str, str] | None,
    cur_pairs: Dict[str, str],
) -> List[dict]:
    prev_keys = set(prev_pairs.keys()) if prev_pairs else set()
    events: List[dict] = []

    for pair, url in cur_pairs.items():
        if pair in prev_keys:
            continue
        base, quote = (pair.split("/", 1) + [""])[:2]

        start_text, start_ts = api_lookup_listing_time(exchange, market, base, quote)

        events.append({
            "exchange": exchange.lower(),
            "market": market.lower(),
            "pair": pair,
            "base": base,
            "quote": quote,
            "url": url,
            "title": "нова пара (API)",
            "start_text": start_text,   # <— якщо None, просто не виведемо рядок
            "start_dt": None,           # для сумісності з існ. кодом
            "start_ts": start_ts,       # опційно (не обов’язково)
        })
    return events


def api_preview(exchange: str, market: str, limit: int = 5) -> List[dict]:
    snap = api_fetch_snapshot(exchange, market)
    events = []
    i = 0
    for pair, url in snap.items():
        base, quote = (pair.split("/", 1) + [""])[:2]
        events.append({
            "exchange": exchange.lower(),
            "market": market.lower(),
            "pair": pair,
            "base": base,
            "quote": quote,
            "url": url,
            "title": "тестова пара (API preview)",
            "start_text": f"detected: {_now_utc_text()}",
            "start_dt": None,
        })
        i += 1
        if i >= max(1, int(limit)):
            break
    return events

ALL_EXCHANGES: List[Tuple[str, str]] = []
for ex, (has_spot, has_fut) in SUPPORTED.items():
    if has_spot:
        ALL_EXCHANGES.append((ex, "spot"))
    if has_fut:
        ALL_EXCHANGES.append((ex, "futures"))

def api_seed_all() -> Dict[Tuple[str, str], Dict[str, str]]:
    out: Dict[Tuple[str, str], Dict[str, str]] = {}
    for ex, mk in ALL_EXCHANGES:
        snap = api_fetch_snapshot(ex, mk)
        out[(ex, mk)] = snap
        log.info("api seed %s/%s: %d symbols", ex, mk, len(snap))
        time.sleep(0.2)
    return out
