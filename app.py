# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sys
import time
import random
import yaml
import pytz
import asyncio
import sqlite3
import logging
import requests
from typing import List, Optional, Tuple, Dict
from datetime import datetime

from dotenv import load_dotenv

# Telegram
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from telegram.error import Conflict

# ---- наші джерела через API бірж (окремий модуль) ----
from api_sources import (
    ALL_EXCHANGES,            # List[Tuple[str, str]]: [("binance","spot"), ("gate","futures"), ...]
    api_seed_all,             # () -> Dict[(ex, mk) -> Dict[pair->url, ...]]
    api_fetch_snapshot,       # (ex, mk) -> Dict[pair->url, ...]
    api_build_events_from_diff,  # (ex, mk, prev, cur) -> List[Dict(event...)]
    api_now_exchange_iso,     # (ex, mk) -> str | None  (час з біржі у вигляді ISO або людейно)
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

BOT_TOKEN       = os.getenv("BOT_TOKEN", "")
TARGET_CHAT_ID  = os.getenv("TARGET_CHAT_ID", "")
OWNER_CHAT_ID   = os.getenv("OWNER_CHAT_ID", "")

# Таймзона залишається для сумісності (може знадобитися в майбутньому)
TIMEZONE        = os.getenv("TIMEZONE", "Europe/Kyiv")
TZ              = pytz.timezone(TIMEZONE)

# Інтервал опитування API (сек)
API_POLL_SEC    = int(os.getenv("API_POLL_SEC", "180"))

# Скільки сек чекати між sendMessage (щоб не ловити 429)
MIN_GAP_BETWEEN_MESSAGES = float(os.getenv("TG_MIN_GAP_SEC", "1.2"))

# ----------------------- DB ---------------------------
DB_PATH = os.getenv("DB_PATH", "state.db")  # на хостингу краще /data/state.db
conn = sqlite3.connect(DB_PATH, check_same_thread=False)
cur  = conn.cursor()

# Таблиця для старої логіки оголошень (залишаємо, раптом повернешся до HTML)
cur.execute("""
CREATE TABLE IF NOT EXISTS seen_announcements(
  url TEXT PRIMARY KEY,
  exchange TEXT,
  market TEXT,
  title TEXT,
  symbols TEXT,
  start_ts INTEGER
)""")

# Нова таблиця: щоб не дублювати API події між рестартами
cur.execute("""
CREATE TABLE IF NOT EXISTS seen_pairs(
  exchange TEXT NOT NULL,
  market   TEXT NOT NULL,
  pair     TEXT NOT NULL,
  url      TEXT,
  first_seen_ts INTEGER,
  PRIMARY KEY(exchange, market, pair)
)""")
conn.commit()

# ----------------------- UTILS ------------------------
_last_send_ts = 0.0

def _html(msg: str) -> str:
    """Проста екрануюча функція для HTML parse_mode."""
    import html
    return html.escape(msg, quote=False)

def _send_telegram(chat_id: str, text: str, parse_mode: str = "HTML",
                   disable_preview: bool = True, max_retries: int = 3):
    """
    Безпечна відправка:
    - HTML parse_mode (щоб не ламався Markdown),
    - повага до 429 retry_after,
    - глобальний тротлінг між повідомленнями.
    """
    global _last_send_ts

    if not BOT_TOKEN or not chat_id:
        log.warning("BOT_TOKEN або chat_id порожні — пропускаю send.")
        return

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": parse_mode,
        "disable_web_page_preview": disable_preview,
    }

    # пер-чат ліміт
    now = time.time()
    gap = now - _last_send_ts
    if gap < MIN_GAP_BETWEEN_MESSAGES:
        time.sleep(MIN_GAP_BETWEEN_MESSAGES - gap)

    for attempt in range(1, max_retries + 1):
        try:
            r = requests.post(url, json=payload, timeout=25)
            if r.status_code == 200:
                _last_send_ts = time.time()
                return
            if r.status_code == 429:
                try:
                    j = r.json()
                    wait = int(j.get("parameters", {}).get("retry_after", 3))
                except Exception:
                    wait = 3
                wait += 1
                log.error("Bot send 429. Waiting %ss (attempt %d/%d)", wait, attempt, max_retries)
                time.sleep(wait)
                continue
            log.error("Bot send error: %s %s", r.status_code, r.text[:500])
            return
        except Exception as e:
            log.exception("Bot send failed (attempt %d/%d): %s", attempt, max_retries, e)
            time.sleep(1 + attempt * 0.5 + random.random())

def send_bot_message(text_html: str, disable_preview: bool = True):
    if TARGET_CHAT_ID:
        _send_telegram(TARGET_CHAT_ID, text_html, "HTML", disable_preview)

def send_owner(text: str):
    if OWNER_CHAT_ID:
        _send_telegram(OWNER_CHAT_ID, _html(text), "HTML", True)

def _fmt_pair_line(pair: str) -> str:
    return f"<code>{_html(pair)}</code>"

def _upsert_seen_pair(exchange: str, market: str, pair: str, url: Optional[str]) -> bool:
    """
    Повертає True, якщо запис новий (тобто треба постити).
    Якщо вже бачили — повертає False.
    """
    ts = int(time.time())
    try:
        cur.execute(
            "INSERT OR IGNORE INTO seen_pairs(exchange, market, pair, url, first_seen_ts) VALUES (?,?,?,?,?)",
            (exchange, market, pair, url or None, ts),
        )
        conn.commit()
        cur.execute("SELECT changes()")
        return cur.fetchone()[0] > 0
    except Exception as e:
        log.exception("seen_pairs insert error: %s", e)
        return True  # на всяк випадок не блокуємо постинг

# -------------------- BOT (команди) -------------------
# --- TEST: /inject <exchange> <spot|futures> <BASE/QUOTE> [start] [end] [channel]
async def cmd_inject(update, context):
    args = (context.args or [])
    if len(args) < 3:
        return await update.message.reply_text(
            "usage:\n"
            "/inject <exchange> <spot|futures> <BASE/QUOTE> [start_text] [end_text] [channel]\n"
            "example:\n"
            "/inject gate spot BTC/USDT \"2025-10-07 13:00 UTC+8\" \"2025-10-07 15:00 UTC+8\" channel"
        )
    ex = args[0].lower()
    mk = args[1].lower()
    pair = args[2].upper()
    start_text = args[3] if len(args) >= 4 else ""
    end_text   = args[4] if len(args) >= 5 else ""
    to_channel = (len(args) >= 6 and args[5].lower() == "channel")

    base, quote = (pair.split("/", 1) + [""])[:2]
    ev = {
        "exchange": ex,
        "market": mk,
        "pair": pair,
        "base": base,
        "quote": quote,
        "url": "",
        "title": "тестова пара (API inject)",
        "start_text": start_text,
        "end_text": end_text,
        "start_dt": None,
    }

    # той самий рендер, що і для реальних API-івентів
    lines = []
    title_line = f"✅ <b>{ex.upper()}</b> — {mk} нова пара (API)"
    lines.append(title_line)
    lines.append(f"Пара: <code>{pair}</code>")

    t_lines = []
    if start_text and end_text:
        t_lines.append(f"🕒 {start_text} → {end_text}")
    elif start_text:
        t_lines.append(f"🕒 {start_text}")
    if t_lines:
        lines.extend(t_lines)

    text = "\n".join(lines)
    if to_channel:
        send_bot_message(text, disable_preview=False)
        await update.message.reply_text("✅ injected to channel")
    else:
        await update.message.reply_html(text, disable_web_page_preview=False)

# реєстрація
app.add_handler(CommandHandler("inject", cmd_inject))



async def cmd_ping(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("pong")

async def cmd_sources(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Відображає список API-джерел, які бот опитує (за ALL_EXCHANGES).
    """
    try:
        lines = ["Джерела API:"]
        for ex, mk in ALL_EXCHANGES:
            lines.append(f"• {ex}/{mk}")
        await update.message.reply_text("\n".join(lines))
    except Exception as e:
        await update.message.reply_text(f"Помилка виводу джерел: {e}")

async def cmd_testpost(update: Update, context: ContextTypes.DEFAULT_TYPE):
    send_bot_message("<b>✅ Test publish from Crypto Hornet bot.</b>")
    await update.message.reply_text("Відправив тест у TARGET_CHAT_ID.")

def _parse_preview_args(args: List[str]) -> Tuple[str, Optional[str], int]:
    """
    Підтримка:
      /preview all 2
      /preview binance 5
      /preview gate futures 3
    """
    ex = "all"
    mk: Optional[str] = None
    limit = 3
    if not args:
        return ex, mk, limit
    # перше — ex|all
    ex = args[0].lower()
    # друге — market або limit
    if len(args) >= 2:
        if args[1].isdigit():
            limit = int(args[1])
        else:
            mk = args[1].lower()
    # третє — limit (якщо було mk)
    if len(args) >= 3 and args[2].isdigit():
        limit = int(args[2])
    return ex, mk, limit

async def cmd_preview(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /preview all 2
    /preview binance 5
    /preview gate futures 3
    Показує «останній знімок» від API без публікації.
    """
    ex, mk, limit = _parse_preview_args(context.args or [])
    lines: List[str] = []

    def add_block(title: str, items: Dict[str, str]):
        lines.append(f"<b>{_html(title)}</b>")
        if not items:
            lines.append("—")
            return
        c = 0
        for pair, url in items.items():
            if c >= limit:
                break
            url = url or ""
            if url:
                lines.append(f"• {_fmt_pair_line(pair)} — <a href=\"{url}\">тикер</a>")
            else:
                lines.append(f"• {_fmt_pair_line(pair)}")
            c += 1
        lines.append("")

    if ex == "all":
        for _ex, _mk in ALL_EXCHANGES:
            snap = api_fetch_snapshot(_ex, _mk)
            add_block(f"{_ex}/{_mk}", snap)
    else:
        # фільтр по біржі (і, опційно, ринку)
        pairs = [t for t in ALL_EXCHANGES if t[0] == ex and (mk is None or t[1] == mk)]
        if not pairs:
            await update.message.reply_text("Невірні аргументи. Приклади: /preview all 2 | /preview binance 5 | /preview gate futures 3")
            return
        for _ex, _mk in pairs:
            snap = api_fetch_snapshot(_ex, _mk)
            add_block(f"{_ex}/{_mk}", snap)

    # відправляємо приватно
    try:
        await update.message.reply_html("\n".join(lines), disable_web_page_preview=True)
    except Exception:
        # якщо прив'язаний до каналу — шлемо власнику
        send_owner("\n".join(lines))

def build_bot_app():
    if not BOT_TOKEN:
        return None
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("ping",     cmd_ping))
    app.add_handler(CommandHandler("sources",  cmd_sources))
    app.add_handler(CommandHandler("testpost", cmd_testpost))
    app.add_handler(CommandHandler("preview",  cmd_preview))
    return app

# -------------------- API LOOP ------------------------
async def poll_api_loop():
    """
    Основний цикл: опитування API всіх бірж, порівняння з попереднім знімком,
    фільтр через БД (антидубль між рестартами), форматований пост у канал.
    """
    # 1) початкові «знімки» всіх бірж/ринків
    snapshots: Dict[Tuple[str, str], Dict[str, str]] = api_seed_all()

    # 2) головний цикл
    while True:
        try:
            for ex, mk in ALL_EXCHANGES:
                # поточний знімок
                cur_snap = api_fetch_snapshot(ex, mk)  # {pair->url}
                prev_snap = snapshots.get((ex, mk), {})

                # будуємо події (нові пари, delist і т.д. — залежить від реалізації api_build_events_from_diff)
                events = api_build_events_from_diff(ex, mk, prev_snap, cur_snap)

                # постимо тільки нові (через БД антидубль)
                for ev in events:
                    pair = ev.get("pair") or ""
                    url  = ev.get("url")  or ""
                    if not pair:
                        continue
                    # якщо вже було — не постимо
                    if not _upsert_seen_pair(ex, mk, pair, url):
                        continue

                    # час як на біржі (без локалізації), якщо є
                    start_text = ev.get("start_text") or ""   # те саме, що прийшло з біржі
                    end_text   = ev.get("end_text") or ""     # кінець, якщо API його дає

                    time_lines = []
                    if start_text and end_text:
                        time_lines.append(f"🕒 {_html(start_text)} → {_html(end_text)}")
                    elif start_text:
                        time_lines.append(f"🕒 {_html(start_text)}")

                    # заголовок + пара
                    title_line = f"✅ <b>{_html(ex.upper())}</b> — {_html(mk)} нова пара (API)"
                    lines = [title_line, f"Пара: {_fmt_pair_line(pair)}"]

                    # додаємо час, якщо є
                    if time_lines:
                        lines.extend(time_lines)

                    # посилання на тікер
                    if url:
                        lines.append(f"🔗 Тікер: <a href=\"{url}\">{_html(url)}</a>")

                    send_bot_message("\n".join(lines), disable_preview=False)


                # оновлюємо кеш знімків у пам'яті
                snapshots[(ex, mk)] = cur_snap

                # невелика пауза між біржами
                await asyncio.sleep(0.3 + random.random() * 0.3)

            await asyncio.sleep(API_POLL_SEC)

        except Exception as e:
            log.exception("api loop error: %s", e)
            await asyncio.sleep(5)

# -------------------- MAIN ---------------------------
async def main():
    app = build_bot_app()
    api_task = None
    try:
        if app:
            try:
                await app.initialize()
                await app.start()
                try:
                    # drop_pending_updates=True — щоб не тягнути старі команди
                    await app.updater.start_polling(drop_pending_updates=True)
                except Conflict:
                    # Якщо інший інстанс уже поллінгить (наприклад, локальний) — продовжуємо без команд
                    log.warning("Updater conflict: already polling elsewhere, continue without commands.")
                except Exception as e:
                    raise e
            except Exception as e:
                log.exception("Bot init failed: %s", e)

        # запускаємо API-цикл
        api_task = asyncio.create_task(poll_api_loop())

        # чекаємо задачі
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
    asyncio.run(main())
