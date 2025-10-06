# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import time
import json
import logging
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Optional

import requests

log = logging.getLogger("hornet.api")

# -------------------------------------------------------
# HTTP session (стабільні заголовки + необов'язковий проксі)
# -------------------------------------------------------
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

def _get_json(url: str, params: dict | None = None, headers: dict | None = None, timeout: int = 25):
    h = dict(SESSION.headers)
    if headers:
        h.update(headers)
    r = SESSION.get(url, params=params, headers=h, timeout=timeout, proxies=PROXIES)
    r.raise_for_status()
    ct = r.headers.get("content-type", "")
    if "application/json" not in ct and not r.text.strip().startswith(("{", "[")):
        # деякі API віддають text/plain
        try:
            return json.loads(r.text)
        except Exception:
            pass
    return r.json()

def _now_utc_text() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

# ---------------------------------------------------------------------
# УНІФІКОВАНИЙ ІНТЕРФЕЙС
# ---------------------------------------------------------------------
# api_fetch_snapshot(exchange, market) -> Dict["BASE/QUOTE", "url-to-ticker"]
#   exchange ∈ {"binance","okx","gate","bitget","mexc","bingx","bybit","bithumb","upbit"}
#   market   ∈ {"spot","futures"}
#
# api_build_events_from_diff(exchange, market, prev_pairs, cur_pairs) -> List[event dict]
#   перетворює Δ у список подій для публікації
#
# api_preview(exchange, market, limit=5) -> List[event dict]
#   повертає "фейкові нові" події для швидкого тесту бота (без очікувань)

SUPPORTED: Dict[str, Tuple[bool, bool]] = {
    # exchange: (has_spot, has_futures)
    "binance": (True, True),
    "okx":     (True, True),
    "gate":    (True, True),
    "bitget":  (True, True),
    "mexc":    (True, True),   # spot + USDT-M contracts
    "bingx":   (True, True),   # публічні ендпоїнти можуть вимагати ключ (опційно)
    "bybit":   (True, True),
    "bithumb": (True, False),  # лише spot
    "upbit":   (True, False),  # лише spot (у т.ч. USDT-ринок)
}

# --------------------------------------
# BINANCE
# --------------------------------------
def _binance_spot() -> Dict[str, str]:
    # https://api.binance.com/api/v3/exchangeInfo
    j = _get_json("https://api.binance.com/api/v3/exchangeInfo")
    out: Dict[str, str] = {}
    for s in j.get("symbols", []):
        if s.get("status") != "TRADING":
            continue
        base, quote = s.get("baseAsset"), s.get("quoteAsset")
        if not base or not quote:
            continue
        pair = f"{base}/{quote}"
        url  = f"https://www.binance.com/en/trade/{base}_{quote}"
        out[pair] = url
    return out

def _binance_futures() -> Dict[str, str]:
    # USDT-M perpetual
    j = _get_json("https://fapi.binance.com/fapi/v1/exchangeInfo")
    out: Dict[str, str] = {}
    for s in j.get("symbols", []):
        if s.get("status") != "TRADING":
            continue
        if s.get("contractType") != "PERPETUAL":
            continue
        base, quote = s.get("baseAsset"), s.get("quoteAsset")
        if not base or not quote:
            continue
        pair = f"{base}/{quote}"
        url  = f"https://www.binance.com/en/futures/{base}{quote}"
        out[pair] = url
    return out

# --------------------------------------
# OKX
# --------------------------------------
def _okx_spot() -> Dict[str, str]:
    # https://www.okx.com/api/v5/public/instruments?instType=SPOT
    j = _get_json("https://www.okx.com/api/v5/public/instruments", params={"instType": "SPOT"})
    out: Dict[str, str] = {}
    for it in j.get("data", []):
        inst_id = it.get("instId")  # e.g. BTC-USDT
        if not inst_id or "-" not in inst_id:
            continue
        base, quote = inst_id.split("-")[0], inst_id.split("-")[1]
        url  = f"https://www.okx.com/trade-spot/{base}-{quote}"
        out[f"{base}/{quote}"] = url
    return out

def _okx_futures() -> Dict[str, str]:
    # беремо SWAP (perpetual)
    j = _get_json("https://www.okx.com/api/v5/public/instruments", params={"instType": "SWAP"})
    out: Dict[str, str] = {}
    for it in j.get("data", []):
        inst_id = it.get("instId")  # e.g. BTC-USDT-SWAP
        if not inst_id or "-" not in inst_id:
            continue
        parts = inst_id.split("-")
        if len(parts) < 2:
            continue
        base, quote = parts[0], parts[1]
        url  = f"https://www.okx.com/trade-swap/{base}-{quote}-SWAP"
        out[f"{base}/{quote}"] = url
    return out

# --------------------------------------
# GATE
# --------------------------------------
def _gate_spot() -> Dict[str, str]:
    # https://api.gateio.ws/api/v4/spot/currency_pairs
    j = _get_json("https://api.gateio.ws/api/v4/spot/currency_pairs")
    out: Dict[str, str] = {}
    for it in j:
        base, quote = it.get("base"), it.get("quote")
        if not base or not quote:
            continue
        pair = f"{base}/{quote}"
        url  = f"https://www.gate.io/trade/{base}_{quote}"
        out[pair] = url
    return out

def _gate_futures() -> Dict[str, str]:
    # https://api.gateio.ws/api/v4/futures/usdt/contracts
    j = _get_json("https://api.gateio.ws/api/v4/futures/usdt/contracts")
    out: Dict[str, str] = {}
    for it in j:
        name = it.get("name")  # e.g. BTC_USDT
        if not name or "_" not in name:
            continue
        base, quote = name.split("_")[0], name.split("_")[1]
        pair = f"{base}/{quote}"
        url  = f"https://www.gate.io/futures_trade/USDT/{base}_{quote}"
        out[pair] = url
    return out

# --------------------------------------
# BITGET
# --------------------------------------
def _bitget_spot() -> Dict[str, str]:
    # https://api.bitget.com/api/spot/v1/public/symbols
    j = _get_json("https://api.bitget.com/api/spot/v1/public/symbols")
    out: Dict[str, str] = {}
    for it in j.get("data", []):
        sym = it.get("symbol")  # e.g. BTCUSDT
        if not sym or "USDT" not in sym:
            continue
        base, quote = sym[:-4], "USDT"
        pair = f"{base}/{quote}"
        url  = f"https://www.bitget.com/spot/{base}{quote}"
        out[pair] = url
    return out

def _bitget_futures() -> Dict[str, str]:
    # USDT-M perpetuals (productType=umcbl)
    j = _get_json("https://api.bitget.com/api/mix/v1/market/contracts", params={"productType": "umcbl"})
    out: Dict[str, str] = {}
    for it in j.get("data", []):
        sym = it.get("symbol")  # e.g. BTCUSDT_UMCBL
        if not sym or "USDT" not in sym:
            continue
        base = sym.split("USDT")[0]
        quote = "USDT"
        pair = f"{base}/{quote}"
        url  = f"https://www.bitget.com/mix/usdt/{base}{quote}"
        out[pair] = url
    return out

# --------------------------------------
# MEXC
# --------------------------------------
def _mexc_spot() -> Dict[str, str]:
    # https://api.mexc.com/api/v3/exchangeInfo (аналог binance)
    j = _get_json("https://api.mexc.com/api/v3/exchangeInfo")
    out: Dict[str, str] = {}
    for s in j.get("symbols", []):
        if s.get("status") != "TRADING":
            continue
        base, quote = s.get("baseAsset"), s.get("quoteAsset")
        if not base or not quote:
            continue
        pair = f"{base}/{quote}"
        url  = f"https://www.mexc.com/exchange/{base}_{quote}"
        out[pair] = url
    return out

def _mexc_futures() -> Dict[str, str]:
    # Список контрактів USDT-M (перпетуали).
    # Документація MEXC контрактів: https://mxcdevelop.github.io/apidocs/contract_v1_en/#public-api
    # Працює ендпоїнт /api/v1/contract/detail або /api/v1/contract/list — спробуємо обидва.
    out: Dict[str, str] = {}
    tried = []
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
                # name / symbol варіюється. Часто: "symbol": "BTC_USDT"
                sym = it.get("symbol") or it.get("instrument_id") or it.get("contractId")
                if not sym:
                    continue
                if isinstance(sym, int):
                    continue
                s = str(sym)
                base, quote = None, None
                if "_" in s:
                    base, quote = s.split("_", 1)[0], s.split("_", 1)[1]
                elif s.endswith("USDT"):
                    base, quote = s[:-4], "USDT"
                if not base or not quote:
                    continue
                pair = f"{base}/{quote}"
                urlp = f"https://www.mexc.com/futures/{base}_{quote}"
                out[pair] = urlp
            if out:
                return out
            tried.append(url)
        except Exception as e:
            tried.append(f"{url} ({e})")
            continue
    if not out:
        log.info("mexc futures: empty (tried: %s)", "; ".join(tried))
    return out

# --------------------------------------
# BINGX  (може вимагати API-ключ; якщо є — ставимо в заголовок)
# --------------------------------------
def _bingx_headers() -> dict:
    api_key = os.getenv("BINGX_API_KEY", "").strip()
    return {"X-BX-APIKEY": api_key} if api_key else {}

def _bingx_spot() -> Dict[str, str]:
    # https://open-api.bingx.com/openApi/spot/v1/common/symbols
    j = _get_json("https://open-api.bingx.com/openApi/spot/v1/common/symbols", headers=_bingx_headers())
    out: Dict[str, str] = {}
    data = j.get("data") or j.get("result") or []
    for it in data:
        base = it.get("baseAsset") or it.get("baseCurrency")
        quote = it.get("quoteAsset") or it.get("quoteCurrency")
        if not base or not quote:
            continue
        pair = f"{base}/{quote}"
        url  = f"https://bingx.com/spot/{base}{quote}"
        out[pair] = url
    return out

def _bingx_futures() -> Dict[str, str]:
    # https://open-api.bingx.com/openApi/swap/v2/common/symbols
    j = _get_json("https://open-api.bingx.com/openApi/swap/v2/common/symbols", headers=_bingx_headers())
    out: Dict[str, str] = {}
    data = j.get("data") or j.get("result") or []
    for it in data:
        sym = it.get("symbol")  # BTC-USDT
        if not sym:
            continue
        if "-" in sym:
            base, quote = sym.split("-")[0], sym.split("-")[1]
        else:
            base = sym.replace("USDT", "")
            quote = "USDT"
        pair = f"{base}/{quote}"
        url  = f"https://bingx.com/futures/{base}{quote}"
        out[pair] = url
    return out

# --------------------------------------
# BYBIT
# --------------------------------------
def _bybit_spot() -> Dict[str, str]:
    # https://api.bybit.com/v5/market/instruments-info?category=spot
    j = _get_json("https://api.bybit.com/v5/market/instruments-info", params={"category": "spot"})
    out: Dict[str, str] = {}
    for page in j.get("result", {}).get("list", []):
        for it in page.get("list", []) if "list" in page else [page]:
            sym = it.get("symbol")  # e.g. BTCUSDT
            if not sym or "USDT" not in sym:
                continue
            base = sym.split("USDT")[0]
            pair = f"{base}/USDT"
            url  = f"https://www.bybit.com/en/trade/spot/{base}/USDT"
            out[pair] = url
    return out

def _bybit_futures() -> Dict[str, str]:
    # linear USDT perpetual
    j = _get_json("https://api.bybit.com/v5/market/instruments-info", params={"category": "linear"})
    out: Dict[str, str] = {}
    for page in j.get("result", {}).get("list", []):
        for it in page.get("list", []) if "list" in page else [page]:
            sym = it.get("symbol")  # e.g. BTCUSDT
            if not sym or "USDT" not in sym:
                continue
            base = sym.split("USDT")[0]
            pair = f"{base}/USDT"
            url  = f"https://www.bybit.com/en/trade/usdt/{base}USDT"
            out[pair] = url
    return out

# --------------------------------------
# BITHUMB (spot тільки; в основному KRW ринки, але залишимо всі)
# --------------------------------------
def _bithumb_spot() -> Dict[str, str]:
    # Візьмемо список торгових пар — побудуємо URL до web-тікера.
    # Публічний ендпоїнт (неофіційний) зі списком: https://api.bithumb.com/public/ticker/ALL
    # Дає tickers по ринках (KRW, BTC, USDT). Візьмемо ключі з "data".
    try:
        j = _get_json("https://api.bithumb.com/public/ticker/ALL")
    except Exception:
        return {}
    data = j.get("data", {})
    out: Dict[str, str] = {}
    for sym, payload in data.items():
        if not isinstance(payload, dict):
            continue
        # побудуємо кілька можливих ринків
        for quote in ("USDT", "KRW", "BTC"):
            pair = f"{sym}/{quote}"
            url  = f"https://www.bithumb.com/trade/order/{sym}_{quote}"
            out[pair] = url
    return out

# --------------------------------------
# UPBIT (spot тільки)
# --------------------------------------
def _upbit_spot() -> Dict[str, str]:
    # https://api.upbit.com/v1/market/all
    j = _get_json("https://api.upbit.com/v1/market/all")
    out: Dict[str, str] = {}
    for it in j:
        market = it.get("market")  # e.g. KRW-BTC, USDT-BTC
        if not market or "-" not in market:
            continue
        quote, base = market.split("-")[0], market.split("-")[1]
        pair = f"{base}/{quote}"
        url  = f"https://upbit.com/exchange?code=CRIX.UPBIT.{quote}-{base}"
        out[pair] = url
    return out

# ---------------------------------------------------------------------
# Головна точка: знімки для біржі/ринку
# ---------------------------------------------------------------------
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
            return _mexc_spot() if mk == "spot" else _mexc_futures()
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

# ---------------------------------------------------------------------
# Побудова подій з різниці (Δ) між знімками
# ---------------------------------------------------------------------
def api_build_events_from_diff(
    exchange: str,
    market: str,
    prev_pairs: Dict[str, str] | None,
    cur_pairs: Dict[str, str],
) -> List[dict]:
    """
    prev_pairs / cur_pairs: dict pair -> url
    Повертає список подій (тільки нові пари, яких не було у prev_pairs).
    """
    prev_keys = set(prev_pairs.keys()) if prev_pairs else set()
    events: List[dict] = []

    for pair, url in cur_pairs.items():
        if pair in prev_keys:
            continue
        base, quote = (pair.split("/", 1) + [""])[:2]
        events.append({
            "exchange": exchange.lower(),
            "market": market.lower(),
            "pair": pair,
            "base": base,
            "quote": quote,
            "url": url,
            "title": "нова пара (API)",
            "start_text": f"detected: {_now_utc_text()}",
            "start_dt": None,  # немає «часу запуску» від API
        })
    return events

# ---------------------------------------------------------------------
# Прев’ю для тестів (без реальних лістингів)
# ---------------------------------------------------------------------
def api_preview(exchange: str, market: str, limit: int = 5) -> List[dict]:
    """
    Повертає перші N пар як "фейкові нові" — зручно для перевірки формату постів.
    """
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

# ---------------------------------------------------------------------
# Масив для ініціалізації/обходу у твоєму app.py
# ---------------------------------------------------------------------
ALL_EXCHANGES: List[Tuple[str, str]] = []
for ex, (has_spot, has_fut) in SUPPORTED.items():
    if has_spot:
        ALL_EXCHANGES.append((ex, "spot"))
    if has_fut:
        ALL_EXCHANGES.append((ex, "futures"))

def api_seed_all() -> Dict[Tuple[str, str], Dict[str, str]]:
    """
    Зручно викликати при старті бота:
      - повертає знімки для всіх доступних (exchange, market)
      - у лог пише кількість
    """
    out: Dict[Tuple[str, str], Dict[str, str]] = {}
    for ex, mk in ALL_EXCHANGES:
        snap = api_fetch_snapshot(ex, mk)
        out[(ex, mk)] = snap
        log.info("api seed %s/%s: %d symbols", ex, mk, len(snap))
        time.sleep(0.2)
    return out
