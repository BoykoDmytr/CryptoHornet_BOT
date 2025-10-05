# -*- coding: utf-8 -*-
# api_sources.py — офіційні/публічні API бірж для списків пар (spot + futures)

from __future__ import annotations
import os
import re
from typing import Dict, Set, Tuple
import requests

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": UA, "Accept": "application/json"})
TIMEOUT = float(os.getenv("API_HTTP_TIMEOUT", "15"))

ONLY_USDT = os.getenv("API_ONLY_USDT", "1") == "1"

def _get_json(url: str, params: dict | None = None) -> dict:
    r = SESSION.get(url, params=params, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()

def _pair_id(base: str, quote: str) -> str:
    # нормалізований ідентифікатор пари, щоб зручно зберігати у БД
    return f"{base.upper()}_{quote.upper()}"

# --------------------- BINANCE ------------------------

def binance_spot_pairs() -> Set[str]:
    j = _get_json("https://api.binance.com/api/v3/exchangeInfo")
    out = set()
    for s in j.get("symbols", []):
        if s.get("status") != "TRADING":
            continue
        base, quote = s.get("baseAsset", ""), s.get("quoteAsset", "")
        if ONLY_USDT and quote.upper() != "USDT":
            continue
        out.add(_pair_id(base, quote))
    return out

def binance_futures_pairs() -> Set[str]:
    j = _get_json("https://fapi.binance.com/fapi/v1/exchangeInfo")
    out = set()
    for s in j.get("symbols", []):
        if s.get("status") != "TRADING":
            continue
        if s.get("contractType") != "PERPETUAL":
            continue
        base, quote = s.get("baseAsset", ""), s.get("quoteAsset", "")
        if ONLY_USDT and quote.upper() != "USDT":
            continue
        out.add(_pair_id(base, quote))
    return out

# ----------------------- OKX -------------------------

def okx_spot_pairs() -> Set[str]:
    j = _get_json("https://www.okx.com/api/v5/public/instruments", {"instType": "SPOT"})
    out = set()
    for it in j.get("data", []):
        inst_id = it.get("instId", "")  # BTC-USDT
        parts = inst_id.split("-")
        if len(parts) < 2:
            continue
        base, quote = parts[0], parts[1]
        if ONLY_USDT and quote.upper() != "USDT":
            continue
        out.add(_pair_id(base, quote))
    return out

def okx_futures_pairs() -> Set[str]:
    j = _get_json("https://www.okx.com/api/v5/public/instruments", {"instType": "SWAP"})
    out = set()
    for it in j.get("data", []):
        inst_id = it.get("instId", "")  # BTC-USDT-SWAP
        parts = inst_id.split("-")
        if len(parts) < 2:
            continue
        base, quote = parts[0], parts[1]
        if ONLY_USDT and quote.upper() != "USDT":
            continue
        out.add(_pair_id(base, quote))
    return out

# ----------------------- GATE ------------------------

def gate_spot_pairs() -> Set[str]:
    j = _get_json("https://api.gateio.ws/api/v4/spot/currency_pairs")
    out = set()
    for it in j:
        base, quote = it.get("base"), it.get("quote")
        if not base or not quote:
            continue
        if it.get("trade_status") and it.get("trade_status") != "tradable":
            continue
        if ONLY_USDT and quote.upper() != "USDT":
            continue
        out.add(_pair_id(base, quote))
    return out

def gate_futures_pairs() -> Set[str]:
    # USDT-маржинальні перпетуали
    j = _get_json("https://api.gateio.ws/api/v4/futures/usdt/contracts")
    out = set()
    for it in j:
        # name/contract вигляду BTC_USDT
        contract = (it.get("name") or it.get("contract") or "").upper()
        m = re.match(r"^([A-Z0-9]+)[_\-/]USDT$", contract)
        if not m:
            continue
        base = m.group(1)
        out.add(_pair_id(base, "USDT"))
    return out

# ---------------------- BITGET -----------------------

def bitget_spot_pairs() -> Set[str]:
    j = _get_json("https://api.bitget.com/api/spot/v1/public/symbols")
    out = set()
    for it in (j.get("data") or []):
        base, quote = it.get("baseCoin"), it.get("quoteCoin")
        if not base or not quote:
            continue
        if ONLY_USDT and quote.upper() != "USDT":
            continue
        out.add(_pair_id(base, quote))
    return out

def bitget_futures_pairs() -> Set[str]:
    # USDT-M perpetual contracts (umcbl)
    j = _get_json("https://api.bitget.com/api/mix/v1/market/contracts", {"productType": "umcbl"})
    out = set()
    for it in (j.get("data") or []):
        sym = (it.get("symbol") or "").upper()  # напр. BTCUSDT_UMCBL
        m = re.match(r"^([A-Z0-9]+)USDT", sym)
        if not m:
            continue
        base = m.group(1)
        out.add(_pair_id(base, "USDT"))
    return out

# ----------------------- MEXC -----------------------

def mexc_spot_pairs() -> Set[str]:
    j = _get_json("https://api.mexc.com/api/v3/exchangeInfo")
    out = set()
    for s in j.get("symbols", []):
        if s.get("status") != "TRADING":
            continue
        base, quote = s.get("baseAsset", ""), s.get("quoteAsset", "")
        if ONLY_USDT and quote.upper() != "USDT":
            continue
        out.add(_pair_id(base, quote))
    return out

def mexc_futures_pairs() -> Set[str]:
    # Публічний список контрактів
    j = _get_json("https://contract.mexc.com/api/v1/contract/detail")
    out = set()
    for it in (j.get("data") or []):
        # symbol/name може бути BTC_USDT
        sym = (it.get("symbol") or it.get("name") or "").upper()
        m = re.match(r"^([A-Z0-9]+)[_\-/]USDT$", sym)
        if not m:
            continue
        base = m.group(1)
        out.add(_pair_id(base, "USDT"))
    return out

# ----------------------- BINGX ----------------------

def bingx_spot_pairs() -> Set[str]:
    # публічна точка для споту
    j = _get_json("https://open-api.bingx.com/openApi/spot/v1/common/symbols")
    out = set()
    for it in (j.get("data") or []):
        base, quote = it.get("baseAsset"), it.get("quoteAsset")
        if not base or not quote:
            continue
        if ONLY_USDT and (quote.upper() != "USDT"):
            continue
        out.add(_pair_id(base, quote))
    return out

# деякі ф’ючерсні ендпойнти BingX вимагають ключ/підпис — тут їх не чіпаємо

# ------------------- Збірка по всіх ------------------

def fetch_all_pairs() -> Dict[Tuple[str, str], Set[str]]:
    """
    Повертає мапу {(exchange, market): {BASE_QUOTE, ...}}.
    market ∈ {"spot","futures"} (futures — перпетуальні USDT)
    """
    out: Dict[Tuple[str, str], Set[str]] = {}

    def put(ex: str, mk: str, data: Set[str]):
        if data:
            out[(ex, mk)] = data

    try: put("binance", "spot",    binance_spot_pairs())
    except Exception: pass
    try: put("binance", "futures", binance_futures_pairs())
    except Exception: pass

    try: put("okx", "spot",    okx_spot_pairs())
    except Exception: pass
    try: put("okx", "futures", okx_futures_pairs())
    except Exception: pass

    try: put("gate", "spot",    gate_spot_pairs())
    except Exception: pass
    try: put("gate", "futures", gate_futures_pairs())
    except Exception: pass

    try: put("bitget", "spot",    bitget_spot_pairs())
    except Exception: pass
    try: put("bitget", "futures", bitget_futures_pairs())
    except Exception: pass

    try: put("mexc", "spot",    mexc_spot_pairs())
    except Exception: pass
    try: put("mexc", "futures", mexc_futures_pairs())
    except Exception: pass

    try: put("bingx", "spot",   bingx_spot_pairs())
    except Exception: pass
    # bingx futures — пропускаємо (може вимагати ключ/підпис)

    return out
