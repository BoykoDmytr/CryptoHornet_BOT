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
from ann_sources import ann_lookup_listing_time, binance_upcoming_announcements  


# --- optional time fallback from announcements (only for time) ---
try:
    from ann_sources import (
        mexc_futures_latest,
        bitget_spot_latest, bitget_futures_latest,
        gate_spot_latest, gate_futures_latest,
        bingx_spot_latest, bingx_futures_latest,
        okx_latest, binance_latest,
    )
    _HAS_ANN = True
except Exception:
    _HAS_ANN = False
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

# чи фільтрувати лише USDT-коти
ONLY_USDT = (os.getenv("API_ONLY_USDT", "1").lower() not in ("0", "false", "no"))

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

HTTP_PROXY = os.getenv("HTTP_PROXY") or os.getenv("http_proxy")
HTTPS_PROXY = os.getenv("HTTPS_PROXY") or os.getenv("https_proxy")
PROXIES = {"http": HTTP_PROXY, "https": HTTPS_PROXY} if (HTTP_PROXY or HTTPS_PROXY) else None

def _ann_time_fallback(exchange: str, market: str, base: str, quote: str) -> tuple[Optional[str], Optional[int]]:
    """
    Легкий фолбек: йдемо в оголошення, дістаємо ТІЛЬКИ час.
    Повертає (start_text, start_ts_ms) або (None, None).
    """
    if not _HAS_ANN or os.getenv("ANN_TIME_FALLBACK", "1") not in ("1", "true", "True"):
        return None, None

    ex, mk = exchange.lower(), market.lower()
    B, Q = base.upper(), quote.upper()

    # вибір колектора
    funcs = []
    if ex == "mexc" and mk == "futures":
        funcs = [mexc_futures_latest]
    elif ex == "bitget":
        funcs = [bitget_futures_latest] if mk == "futures" else [bitget_spot_latest]
    elif ex == "gate":
        funcs = [gate_futures_latest] if mk == "futures" else [gate_spot_latest]
    elif ex == "bingx":
        funcs = [bingx_futures_latest] if mk == "futures" else [bingx_spot_latest]
    elif ex == "okx":
        funcs = [okx_latest]
    elif ex == "binance":
        funcs = [binance_latest]
    else:
        funcs = []

    for f in funcs:
        try:
            items = f() or []
        except Exception:
            continue
        # шукаємо збіг по символу в останніх 15–20 постах
        for it in items[:20]:
            syms = [s.upper() for s in (it.get("symbols") or [])]
            if B in syms or f"{B}{Q}" in syms:
                # якщо є dt — приводимо до TZ біржі та з лейблом
                dt = it.get("start_dt")
                if dt is not None:
                    tz = EXCHANGE_TZ.get(ex, _pytz.utc)
                    dt_ex = dt.astimezone(tz)
                    label = _fmt_tzlabel(dt_ex)
                    txt = dt_ex.strftime("%Y-%m-%d %H:%M ") + label
                    return txt, int(dt_ex.timestamp() * 1000)
                # якщо немає dt, але є готовий текст — віддамо його як є
                stxt = it.get("start_text")
                if stxt:
                    return stxt, None
    return None, None


def _parse_json_text(text: str):
    try:
        obj = json.loads(text)
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

def _fmt_tzlabel(dt: _dt.datetime) -> str:
    off = dt.utcoffset() or _dt.timedelta(0)
    hours = int(off.total_seconds() // 3600)
    return "UTC" if hours == 0 else f"UTC{hours:+d}"

def _fmt_ts_for_exchange(ts_ms: int, exchange: str) -> str:
    tz = EXCHANGE_TZ.get(exchange.lower(), _pytz.utc)
    if ts_ms < 10**12:  # sec -> ms
        ts_ms *= 1000
    dt = _dt.datetime.fromtimestamp(ts_ms / 1000.0, tz)
    return dt.strftime("%Y-%m-%d %H:%M ") + _fmt_tzlabel(dt)

def api_now_exchange_iso(exchange: str) -> str:
    tz = EXCHANGE_TZ.get((exchange or "").lower(), _pytz.utc)
    return _dt.datetime.now(tz).strftime("%Y-%m-%d %H:%M %Z")

SUPPORTED: Dict[str, Tuple[bool, bool]] = {
    "binance": (True, True),
    "okx":     (True, True),
    "gate":    (True, True),
    "bitget":  (True, True),
    "mexc":    (False, True),  # MEXC spot вимкнено "на корені"
    "bingx":   (False, True),
    "bybit":   (True, True),
    "bithumb": (True, False),
    "upbit":   (True, False),
}

# =======================
#  LOOKUP ЧАСУ (API)
# =======================
def api_lookup_listing_time(exchange: str, market: str, base: str, quote: str) -> tuple[Optional[str], Optional[int]]:
    """
    Повертає (start_text, start_ts_ms) якщо біржа дає час лістингу у своєму API.
    Якщо немає — (None, None). НІЧОГО не конвертуємо у «Київ»; показуємо у ТЗ біржі.
    """

    def _to_ts_ms(v) -> Optional[int]:
        """Підтримує int секунд/мс та ISO-рядки типу '2024-09-01T12:00:00Z'."""
        if v is None:
            return None
        try:
            # ISO?
            if isinstance(v, str):
                s = v.strip()
                if s.endswith("Z") or "T" in s or "+" in s or ":" in s:
                    try:
                        dt = _dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
                        return int(dt.timestamp() * 1000)
                    except Exception:
                        pass
                # число рядком
                v = int(float(s))
            # int/float
            v = int(v)
            return v * 1000 if v < 10**12 else v
        except Exception:
            return None

    def _pick_ts_ms(obj: dict, *keys) -> Optional[int]:
        for k in keys:
            if k in obj and obj[k] is not None:
                ts = _to_ts_ms(obj[k])
                if ts:
                    return ts
        return None

    ex = (exchange or "").lower()
    mk = (market or "").lower()
    B, Q = (base or "").upper(), (quote or "").upper()

    try:
        # ---------- BINANCE ----------
        if ex == "binance":
            if mk == "spot":
                j = _get_json("https://api.binance.com/api/v3/exchangeInfo", params={"symbol": f"{B}{Q}"})
                syms = j.get("symbols", [])
                if syms:
                    ts = _pick_ts_ms(syms[0], "onboardDate", "onboardTs", "listTime", "launchTime")
                    if ts: return _fmt_ts_for_exchange(ts, ex), ts
            else:
                j = _get_json("https://fapi.binance.com/fapi/v1/exchangeInfo", params={"symbol": f"{B}{Q}"})
                syms = j.get("symbols", [])
                if syms:
                    ts = _pick_ts_ms(syms[0], "onboardDate", "onboardTs", "listTime", "launchTime")
                    if ts: return _fmt_ts_for_exchange(ts, ex), ts
            return None, None

        # ---------- OKX ----------
        if ex == "okx":
            if mk == "spot":
                j = _get_json("https://www.okx.com/api/v5/public/instruments",
                              params={"instType": "SPOT", "instId": f"{B}-{Q}"})
            else:
                j = _get_json("https://www.okx.com/api/v5/public/instruments",
                              params={"instType": "SWAP", "instId": f"{B}-{Q}-SWAP"})
            data = (j or {}).get("data") or []
            if data:
                it = data[0]
                ts = _pick_ts_ms(it, "listTime", "listTs", "launchTime", "onlineTime")
                if ts: return _fmt_ts_for_exchange(ts, ex), ts
            return None, None

        # ---------- GATE (час є для futures per-symbol) ----------
        if ex == "gate" and mk == "futures":
            # точковий ендпоїнт по контракту
            j = _get_json(f"https://api.gateio.ws/api/v4/futures/usdt/contracts/{B}_{Q}")
            # можливі ключі часу у Gate:
            ts = _pick_ts_ms(
                j,
                "launch_time", "inited_at", "init_time",
                "create_time", "start_time", "listing_time", "first_open_time"
            )
            if ts: return _fmt_ts_for_exchange(ts, ex), ts
            return None, None
        # Gate spot не дає часу
        if ex == "gate":
            return None, None

        # ---------- BITGET ----------
        if ex == "bitget":
            if mk == "spot":
                j = _get_json("https://api.bitget.com/api/spot/v1/public/products")
                for it in j.get("data", []) or []:
                    if (it.get("baseCoin") or "").upper() == B and (it.get("quoteCoin") or "").upper() == Q:
                        ts = _pick_ts_ms(it, "listTime", "onlineTime", "launchTime")
                        if ts: return _fmt_ts_for_exchange(ts, ex), ts
                return None, None
            else:
                j = _get_json("https://api.bitget.com/api/mix/v1/market/contracts", params={"productType": "umcbl"})
                target = f"{B}{Q}_UMCBL"
                for it in j.get("data", []) or []:
                    if (it.get("symbol") or "").upper() == target:
                        ts = _pick_ts_ms(it, "listTime", "launchTime", "onLineTime", "onlineTime")
                        if ts: return _fmt_ts_for_exchange(ts, ex), ts
                return None, None

        # ---------- MEXC (futures, per-symbol detail ПРІОРИТЕТНО) ----------
        if ex == "mexc" and mk == "futures":
            # 1) спробувати точковий detail із параметром symbol=BASE_USDT (найстабільніше)
            for sym in (f"{B}_{Q}", f"{B}{Q}"):
                try:
                    j = _get_json("https://contract.mexc.com/api/v1/contract/detail", params={"symbol": sym})
                    data = j.get("data")
                    if isinstance(data, dict):
                        ts = _pick_ts_ms(
                            data,
                            "onlineTime", "onLineTime", "stateTime", "startTime", "launchTime", "listingTime", "initTime"
                        )
                        if ts: return _fmt_ts_for_exchange(ts, ex), ts
                except Exception:
                    pass
            # 2) фолбек: загальні списки
            for url in ("https://contract.mexc.com/api/v1/contract/detail",
                        "https://contract.mexc.com/api/v1/contract/list"):
                try:
                    j = _get_json(url)
                    data = j.get("data", j.get("result", []))
                    if isinstance(data, dict):
                        data = data.get("symbols") or data.get("list") or []
                    tgt = {f"{B}_{Q}", f"{B}{Q}"}
                    for it in data or []:
                        s = str(it.get("symbol") or it.get("instrument_id") or it.get("contractId") or "").upper()
                        if s in tgt:
                            ts = _pick_ts_ms(
                                it,
                                "onlineTime", "onLineTime", "stateTime", "startTime", "launchTime", "listingTime", "initTime"
                            )
                            if ts: return _fmt_ts_for_exchange(ts, ex), ts
                except Exception:
                    continue
            return None, None

        # ---------- BYBIT ----------
        if ex == "bybit":
            cat = "spot" if mk == "spot" else "linear"
            j = _get_json("https://api.bybit.com/v5/market/instruments-info", params={"category": cat})
            lst = (j.get("result") or {}).get("list") or []
            seq = []
            for item in lst:
                if isinstance(item, dict) and "list" in item:
                    seq += item.get("list") or []
                else:
                    seq.append(item)
            for it in seq:
                if (it.get("symbol") or "").upper() == f"{B}{Q}":
                    ts = _pick_ts_ms(it, "launchTime", "listTime", "createdTime", "onlineTime")
                    if ts: return _fmt_ts_for_exchange(ts, ex), ts
            return None, None

        # ---------- BINGX (futures): публічний контракт-лист часто БЕЗ часу ----------
        if ex == "bingx" and mk == "futures":
            j = _get_json("https://open-api.bingx.com/openApi/swap/v2/quote/contracts")
            data = (j.get("data") or {}).get("contracts") or j.get("data") or []
            tgt = {f"{B}-{Q}", f"{B}{Q}"}
            for it in data or []:
                s = (it.get("symbol") or it.get("contractName") or "").upper()
                if s in tgt:
                    ts = _pick_ts_ms(it, "listingTime", "launchTime", "onlineTime", "createTime")
                    if ts: return _fmt_ts_for_exchange(ts, ex), ts
            st, ts = _ann_time_fallback(ex, mk, B, Q)
            if st:
                return st, ts
            

    except Exception:
        # тихо віддаємо «без часу»
        return None, None

    return None, None


# =======================
#   СПИСКИ ПАР (API)
# =======================
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

def _okx_spot() -> Dict[str, str]:
    j = _get_json("https://www.okx.com/api/v5/public/instruments", params={"instType": "SPOT"})
    out: Dict[str, str] = {}
    for it in j.get("data", []):
        inst_id = it.get("instId")  # BTC-USDT
        if not inst_id or "-" not in inst_id:
            continue
        base, quote = inst_id.split("-")[0].upper(), inst_id.split("-")[1].upper()

        # 💡 ключова правка: пропускати все, що ще не live/listed/trading
        state = (it.get("state") or it.get("status") or "").lower()
        if state and state not in ("live", "listed", "trading"):
            continue

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

def _bitget_spot() -> Dict[str, str]:
    j = _get_json("https://api.bitget.com/api/spot/v1/public/products")
    out: Dict[str, str] = {}
    for it in j.get("data", []) or []:
        base = (it.get("baseCoin") or "").upper()
        quote = (it.get("quoteCoin") or "").upper()
        sym = (it.get("symbol") or "").upper()
        if not base or not quote or not sym:
            continue
        if ONLY_USDT and quote != "USDT":
            continue
        out[f"{base}/{quote}"] = f"https://www.bitget.com/spot/{base}{quote}"
    return out

def _bitget_futures() -> Dict[str, str]:
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
                s = str(sym).upper()
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

def _bingx_headers() -> dict:
    api_key = os.getenv("BINGX_API_KEY", "").strip()
    return {"X-BX-APIKEY": api_key} if api_key else {}

def _bingx_spot() -> Dict[str, str]:
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
        s = s.replace("-", "")
        if ONLY_USDT and not s.endswith("USDT"):
            continue
        base, quote = s[:-4], "USDT"
        out[f"{base}/{quote}"] = f"https://bingx.com/en-us/futures/{base}{quote}"
    return out

def _bybit_spot() -> Dict[str, str]:
    j = _get_json("https://api.bybit.com/v5/market/instruments-info", params={"category": "spot"})
    items = j.get("result", {}).get("list", []) or []
    out: Dict[str, str] = {}
    for it in items:
        sym = (it.get("symbol") or "").upper()
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
        quotes = ("USDT",) if ONLY_USDT else ("USDT", "KRW", "BTC")
        for quote in quotes:
            out[f"{sym}/{quote}"] = f"https://www.bithumb.com/trade/order/{sym}_{quote}"
    return out

def _upbit_spot() -> Dict[str, str]:
    j = _get_json("https://api.upbit.com/v1/market/all")
    out: Dict[str, str] = {}
    for it in j:
        market = it.get("market")
        if not market or "-" not in market:
            continue
        quote, base = market.split("-")[0].upper(), market.split("-")[1].upper()
        if ONLY_USDT and quote != "USDT":
            continue
        out[f"{base}/{quote}"] = f"https://upbit.com/exchange?code=CRIX.UPBIT.{quote}-{base}"
    return out

# =======================
#  API entrypoints
# =======================
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

        # 1) час із API (де є): binance/okx/gate/bybit/... (ми вже реалізували)
        start_text, start_ts = api_lookup_listing_time(exchange, market, base, quote)

        ev = {
            "exchange": exchange.lower(),
            "market": market.lower(),
            "pair": pair,
            "base": base,
            "quote": quote,
            "url": url,
            "title": "нова пара (API)",
            "start_text": start_text,     # може бути None
            "start_dt": None,
            "start_ts": start_ts,         # може бути None
        }

        # 2) якщо API не дав час — одразу беремо кандидати з анонсів
        if not start_text:
            try:
                enrich = ann_lookup_listing_time(exchange, market, base, quote) or {}
                # enrich = {"time_candidates": [...], "best": "...", "best_ts": 123456}
                tlist = enrich.get("time_candidates") or []
                if tlist:
                    ev["time_candidates"] = tlist
                if not start_ts and enrich.get("best_ts"):
                    ev["ann_ts"] = int(enrich["best_ts"])  # для фільтра «старі/свіжі»
            except Exception:
                pass

        events.append(ev)

    return events


def api_preview(exchange: str, market: str, limit: int = 5) -> List[dict]:
    snap = api_fetch_snapshot(exchange, market)
    events = []
    for pair, url in snap.items():
        base, quote = (pair.split("/", 1) + [""])[:2]
        start_text, start_ts = api_lookup_listing_time(exchange, market, base, quote)

        ev = {
            "exchange": exchange.lower(),
            "market": market.lower(),
            "pair": pair,
            "base": base,
            "quote": quote,
            "url": url,
            "title": "тестова пара (API preview)",
            "start_text": start_text,
            "start_dt": None,
            "start_ts": start_ts,
        }

        if not start_text:
            try:
                enrich = ann_lookup_listing_time(exchange, market, base, quote) or {}
                tlist = enrich.get("time_candidates") or []
                if tlist:
                    ev["time_candidates"] = tlist
                if not start_ts and enrich.get("best_ts"):
                    ev["ann_ts"] = int(enrich["best_ts"])
            except Exception:
                pass

        events.append(ev)
        if len(events) >= max(1, int(limit)):
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
