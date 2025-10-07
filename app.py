# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sys
import time
import json
import random
import asyncio
import logging
from html import escape as html_escape
from typing import Dict, List, Tuple, Optional

from dotenv import load_dotenv

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from telegram.constants import ParseMode

# --- API-шари (жодного HTML-парсингу) ---
from api_sources import (
    api_fetch_snapshot,
    api_build_events_from_diff,
    api_preview,
    ALL_EXCHANGES,
)

# ----------------------- LOGGING -----------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("hornet")

# ----------------------- ENV --------------------------
load_dotenv()

BOT_TOKEN        = os.getenv("BOT_TOKEN", "")
TARGET_CHAT_ID   = os.getenv("TARGET_CHAT_ID", "")
OWNER_CHAT_ID    = os.getenv("OWNER_CHAT_ID", "")

ENABLE_POLLING   = os.getenv("ENABLE_POLLING", "1").strip() == "1"
API_PAIRS_INTERVAL_SEC = int(os.getenv("API_PAIRS_INTERVAL_SEC", "300"))

# Список вимкнених джерел моніторингу у форматі: "mexc/spot, bingx/spot"
DISABLE_API = {
    p.strip().lower()
    for p in os.getenv("DISABLE_API", "mexc/spot").split(",")
    if p.strip()
}

STATE_PATH = os.getenv("DB_PATH", "state.json")  # JSON-файл зі станом (seen + snapshots)

# --------------------- STATE (JSON) --------------------
# Структура:
# {
#   "seen": {
#       "<dedupe_key>": 1730792100,  # unixtime
#       ...
#   },
#   "snapshots": {
#       "binance/spot": {"BTC/USDT": "https://...", ...},
#       "gate/futures": {"ETH/USDT": "https://...", ...},
#       ...
#   }
# }

_state_cache: Dict[str, dict] | None = None

def _state_load() -> Dict[str, dict]:
    global _state_cache
    if _state_cache is not None:
        return _state_cache
    if not os.path.exists(STATE_PATH):
        _state_cache = {"seen": {}, "snapshots": {}}
        return _state_cache
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            _state_cache = json.load(f)
            if not isinstance(_state_cache, dict):
                _state_cache = {"seen": {}, "snapshots": {}}
    except Exception:
        _state_cache = {"seen": {}, "snapshots": {}}
    # захист від поламаних структур
    _state_cache.setdefault("seen", {})
    _state_cache.setdefault("snapshots", {})
    return _state_cache

def _state_save():
    st = _state_load()
    tmp = STATE_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(st, f, ensure_ascii=False, indent=2)
    os.replace(tmp, STATE_PATH)

def db_mark_new(dedupe_key: str) -> bool:
    """
    Повертає True, якщо ключ бачимо вперше (тобто можна постити).
    Dedupe-ключ повинен стабільно ідентифікувати подію (наприклад: api://binance/futures/BTC/USDT).
    """
    st = _state_load()
    seen = st["seen"]
    if dedupe_key in seen:
        return False
    seen[dedupe_key] = int(time.time())
    _state_save()
    return True

def get_prev_snapshot_key(ex: str, mk: str) -> str:
    return f"{ex.lower().strip()}/{mk.lower().strip()}"

def get_prev_snapshot(ex: str, mk: str) -> Dict[str, str]:
    st = _state_load()
    return dict(st["snapshots"].get(get_prev_snapshot_key(ex, mk), {}))

def set_prev_snapshot(ex: str, mk: str, snapshot: Dict[str, str]):
    st = _state_load()
    st["snapshots"][get_prev_snapshot_key(ex, mk)] = snapshot or {}
    _state_save()

# ----------------------- TELEGRAM ----------------------
_last_send_ts = 0.0

def send_owner(text: str):
    if not BOT_TOKEN or not OWNER_CHAT_ID:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            json={"chat_id": OWNER_CHAT_ID, "text": text},
            timeout=15
        )
    except Exception:
        pass

def send_bot_message(text_html: str, chat_id: Optional[str] = None, disable_preview: bool = True, max_retries: int = 3):
    """
    Надійна відправка в канал/чат:
    - HTML розмітка (використовуй html_escape для всього динамічного),
    - тротлінг між повідомленнями,
    - повага до 429 retry_after,
    - до 3 спроб.
    """
    import requests  # локальний імпорт, щоб не тягнути завжди

    global _last_send_ts
    if not BOT_TOKEN:
        log.warning("BOT_TOKEN порожній — пропускаю send.")
        return

    cid = chat_id or TARGET_CHAT_ID
    if not cid:
        log.warning("TARGET_CHAT_ID не заданий — пропускаю send.")
        return

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": cid,
        "text": text_html,
        "parse_mode": "HTML",
        "disable_web_page_preview": disable_preview,
    }

    # простий throttle ~1.2s
    now = time.time()
    gap = now - _last_send_ts
    min_gap = 1.2
    if gap < min_gap:
        time.sleep(min_gap - gap)

    for attempt in range(1, max_retries + 1):
        try:
            r = requests.post(url, json=payload, timeout=25)
            if r.status_code == 200:
                _last_send_ts = time.time()
                return
            if r.status_code == 429:
                # поважаємо retry_after
                try:
                    j = r.json()
                    wait = int(j.get("parameters", {}).get("retry_after", 3))
                except Exception:
                    wait = 3
                wait += 1
                log.error("Bot send 429. Waiting %ss (attempt %d/%d)", wait, attempt, max_retries)
                time.sleep(wait)
                continue
            # інші коди — лог і вихід
            log.error("Bot send error: %s %s", r.status_code, r.text[:500])
            return
        except Exception as e:
            log.exception("Bot send failed (attempt %d/%d): %s", attempt, max_retries, e)
            time.sleep(1 + attempt * 0.5 + random.random())

# ----------------------- HELPERS ----------------------
def format_event_html(ev: dict) -> str:
    """
    Єдиний формат для всіх бірж (API).
    ev очікується з api_build_events_from_diff / api_preview:
      - exchange, market, pair, base, quote, url, title
      - start_text (за наявності; для API зазвичай 'detected: ...')
      - start_dt (може бути None)
    """
    ex = html_escape((ev.get("exchange") or "").upper())
    mk = html_escape(ev.get("market") or "")
    title = html_escape(ev.get("title") or "нова пара (API)")
    pair = html_escape(ev.get("pair") or "")
    url  = html_escape(ev.get("url") or "")

    # текст часу: якщо api вміє — відобразимо, інакше показуємо detected
    start_txt = ev.get("start_text")
    if start_txt:
        start_txt = html_escape(start_txt)

    lines = [
        f"✅ <b>{ex}</b> — <b>{mk}</b> {title}",
        f"Пара: <code>{pair}</code>",
        f"🔗 Тікер: {url}",
    ]
    # час — окремим рядком, якщо є
    if start_txt:
        lines.insert(2, f"🕒 Час: {start_txt}")

    return "\n".join(lines)

def monitored_pairs() -> List[Tuple[str, str]]:
    """
    Вибірка пар (exchange/market) для моніторингу з урахуванням DISABLE_API.
    За замовчуванням у DISABLE_API вже є 'mexc/spot'.
    """
    all_pairs: List[Tuple[str, str]] = list(ALL_EXCHANGES)
    out: List[Tuple[str, str]] = []
    for ex, mk in all_pairs:
        key = f"{ex}/{mk}".lower()
        if key in DISABLE_API:
            continue
        out.append((ex, mk))
    return out

# -------------------- BOT (команди) -------------------
async def cmd_ping(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("pong")

async def cmd_testpost(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # простий тест у канал
    msg = "✅ Test publish from Crypto Hornet bot."
    send_bot_message(html_escape(msg))
    await update.message.reply_text("Відправив тест у TARGET_CHAT_ID.")

def _parse_preview_args(args: List[str]) -> Tuple[str, str, int, bool]:
    """
    /preview <exchange|all> <limit:int> [channel]
    Приклади:
      /preview all 2
      /preview gate 3 channel
      /preview binance 5
    """
    ex = (args[0] if args else "all").lower()
    limit = 3
    to_channel = False

    market = "spot"  # для preview ми беремо і spot, і futures по черзі (див. нижче)

    if len(args) >= 2 and args[1].isdigit():
        limit = max(1, int(args[1]))

    if len(args) >= 3 and args[2].lower() in ("chan", "channel", "c"):
        to_channel = True

    return ex, market, limit, to_channel

async def cmd_preview(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Швидкий прев’ю постів без очікувань реальних лістингів.
    Забирає перші N із кожної вибраної (exchange,market) як "ніби нові".
    """
    if not update.message:
        return
    ex, market, limit, to_channel = _parse_preview_args(context.args or [])

    pairs = monitored_pairs()
    if ex != "all":
        pairs = [(e, m) for (e, m) in pairs if e == ex]

    if not pairs:
        await update.message.reply_text("Немає джерел для прев’ю (можливо, все вимкнено через DISABLE_API).")
        return

    total = 0
    for e, m in pairs:
        # Для прев’ю: візьмемо spot і futures якщо є обидва й не фільтрували
        if ex != "all":
            targets = [(e, m)]
        else:
            # «all» — пройдемось по самих (e,m), що у monitored_pairs()
            targets = [(e, m)]

        for te, tm in targets:
            events = api_preview(te, tm, limit=limit)
            if not events:
                txt = f"ℹ️ {te}/{tm}: порожньо."
                if to_channel:
                    send_bot_message(html_escape(txt))
                else:
                    await update.message.reply_text(txt)
                continue

            for ev in events:
                html = format_event_html(ev)
                if to_channel:
                    send_bot_message(html)
                else:
                    await update.message.reply_html(html)
                await asyncio.sleep(0.4)  # невеликий тротлінг
                total += 1

    if not to_channel:
        await update.message.reply_text(f"✅ Готово. Відправлено {total} прев’ю.")

def _owner_only(update: Update) -> bool:
    try:
        uid = update.effective_user.id if update.effective_user else None
        return OWNER_CHAT_ID and str(uid) == str(OWNER_CHAT_ID)
    except Exception:
        return False

async def cmd_inject(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Ручна ін’єкція події (для тестів), тільки для OWNER.
    Формат:
      /inject <exchange> <spot|futures> <BASE/QUOTE> <url?>
    Приклад:
      /inject gate futures LYN/USDT https://www.gate.io/futures_trade/USDT/LYN_USDT
    """
    if not _owner_only(update):
        await update.message.reply_text("Доступно лише власнику.")
        return

    args = context.args or []
    if len(args) < 3:
        await update.message.reply_text("Формат: /inject <exchange> <spot|futures> <BASE/QUOTE> <url?>")
        return

    ex, mk, pair = args[0].lower(), args[1].lower(), args[2].upper()
    url = args[3] if len(args) >= 4 else f"https://example.com/{ex}/{mk}/{pair.replace('/', '')}"

    ev = {
        "exchange": ex,
        "market": mk,
        "pair": pair,
        "base": pair.split("/", 1)[0],
        "quote": pair.split("/", 1)[1] if "/" in pair else "",
        "url": url,
        "title": "тестова пара (INJECT)",
        # показуємо чітко, що це штучний івент
        "start_text": "manual inject",
        "start_dt": None,
    }
    dedupe = f"api://{ex}/{mk}/{pair}"
    if not db_mark_new(dedupe):
        await update.message.reply_text("Вже ін’єктовано раніше (дедуп).")
        return

    send_bot_message(format_event_html(ev))
    await update.message.reply_text("✅ Ін’єкцію відправлено.")

def build_bot_app():
    if not BOT_TOKEN:
        return None
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("ping", cmd_ping))
    app.add_handler(CommandHandler("testpost", cmd_testpost))
    app.add_handler(CommandHandler("preview", cmd_preview))
    app.add_handler(CommandHandler("inject", cmd_inject))
    return app

# -------------------- API POLLING LOOP ----------------
async def poll_api_loop():
    """
    Основний цикл: періодично тягнемо знімки з API бірж,
    порівнюємо з попередніми, публікуємо нові пари.
    """
    # 1) початкові знімки (щоб не було «бурсту» при першому запуску)
    pairs = monitored_pairs()
    for ex, mk in pairs:
        try:
            snapshot = api_fetch_snapshot(ex, mk)
            set_prev_snapshot(ex, mk, snapshot)
            log.info("api seed %s/%s: %d symbols", ex, mk, len(snapshot))
            await asyncio.sleep(0.2)
        except Exception as e:
            log.warning("seed %s/%s error: %s", ex, mk, e)

    # 2) цикл моніторингу
    while True:
        try:
            for ex, mk in pairs:
                try:
                    cur = api_fetch_snapshot(ex, mk)
                    prev = get_prev_snapshot(ex, mk)
                    if not isinstance(cur, dict):
                        cur = {}
                    evs = api_build_events_from_diff(ex, mk, prev, cur)

                    # оновлюємо snapshot відразу, щоб не дублювати при наступних ітераціях
                    set_prev_snapshot(ex, mk, cur)

                    # публікація тільки нових (через dedupe ключ)
                    for ev in evs:
                        pair = ev.get("pair") or ""
                        dedupe = f"api://{ex}/{mk}/{pair}"
                        if not db_mark_new(dedupe):
                            continue
                        send_bot_message(format_event_html(ev))
                        await asyncio.sleep(0.5)  # маленький тротлінг
                except Exception as e:
                    log.warning("api loop %s/%s error: %s", ex, mk, e)

            await asyncio.sleep(API_PAIRS_INTERVAL_SEC)
        except Exception as e:
            log.exception("api poll loop error: %s", e)
            await asyncio.sleep(5)

# ---------------------- MAIN --------------------------
async def main():
    app = build_bot_app()
    api_task = None
    try:
        if app and ENABLE_POLLING:
            # запуск TG-команд (polling)
            try:
                await app.initialize()
                await app.start()
                try:
                    await app.updater.start_polling(drop_pending_updates=True)
                except Exception as e:
                    if "Conflict" in str(e):
                        log.warning("Updater conflict: уже є інстанс, продовжую без команд.")
                    else:
                        raise
            except Exception as e:
                log.exception("Bot init failed: %s", e)

        # цикл API-моніторингу
        api_task = asyncio.create_task(poll_api_loop())

        wait_tasks = [t for t in (api_task,) if t]
        if wait_tasks:
            await asyncio.wait(wait_tasks, return_when=asyncio.FIRST_EXCEPTION)
        else:
            while True:
                await asyncio.sleep(3600)
    finally:
        if api_task:
            try:
                api_task.cancel()
            except Exception:
                pass
        if app:
            try: await app.updater.stop()
            except Exception: pass
            try: await app.stop()
            except Exception: pass
            try: await app.shutdown()
            except Exception: pass

if __name__ == "__main__":
    import requests  # для send_* локального імпорту
    asyncio.run(main())
