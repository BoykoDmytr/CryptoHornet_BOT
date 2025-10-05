# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sys
import time
import random
import asyncio
import sqlite3
import logging
from html import escape as html_escape
from typing import Optional, Dict, Tuple, Set

import pytz
import requests
from dotenv import load_dotenv

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# ----------------------- LOGGING -----------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("hornet")

# ----------------------- ENV --------------------------
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
TARGET_CHAT_ID = os.getenv("TARGET_CHAT_ID", "")
OWNER_CHAT_ID = os.getenv("OWNER_CHAT_ID", "")

TIMEZONE = os.getenv("TIMEZONE", "Europe/Kyiv")
TZ = pytz.timezone(TIMEZONE)

# API-режим (через офіційні ендпойнти бірж)
ENABLE_API_PAIRS = os.getenv("ENABLE_API_PAIRS", "1") == "1"
API_PAIRS_INTERVAL_SEC = int(os.getenv("API_PAIRS_INTERVAL_SEC", "60"))
API_SEED_ON_START = os.getenv("API_SEED_ON_START", "1") == "1"  # перший запуск без постингу
API_ONLY_USDT = os.getenv("API_ONLY_USDT", "1") == "1"  # тільки USDT-пари

# ----------------------- DB ---------------------------
DB_PATH = os.getenv("DB_PATH", "state.db")  # на Railway/Render краще: /data/state.db
conn = sqlite3.connect(DB_PATH, check_same_thread=False)
cur = conn.cursor()

# Таблиця для API-пар (нові появи)
cur.execute("""
CREATE TABLE IF NOT EXISTS seen_pairs(
  exchange   TEXT NOT NULL,
  market     TEXT NOT NULL,   -- 'spot' | 'futures'
  symbol_id  TEXT NOT NULL,   -- нормалізовано, напр. 'BTC_USDT'
  first_seen_ts INTEGER DEFAULT (strftime('%s','now')),
  PRIMARY KEY(exchange, market, symbol_id)
)
""")
conn.commit()

# ----------------------- HTTP -------------------------
_session = requests.Session()

# ----------------------- UTILS ------------------------
_last_send_ts = 0.0

def _fmt_dt(dt) -> str:
    try:
        return dt.astimezone(TZ).strftime("%Y-%m-%d %H:%M %Z")
    except Exception:
        return "—"

def send_bot_message(text: str, disable_preview: bool = True, max_retries: int = 3):
    """
    Безпечна відправка в канал:
    - глобальний тротлінг (~1.2s між повідомленнями),
    - повага до 429 retry_after,
    - до 3 спроб.
    Формат: HTML (динамічний текст екранується перед побудовою).
    """
    global _last_send_ts
    if not BOT_TOKEN or not TARGET_CHAT_ID:
        log.warning("BOT_TOKEN або TARGET_CHAT_ID порожні — пропускаю send.")
        return

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TARGET_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": disable_preview,
    }

    # простий пер-чат ліміт ~1.2s
    now = time.time()
    gap = now - _last_send_ts
    min_gap = 1.2
    if gap < min_gap:
        time.sleep(min_gap - gap)

    for attempt in range(1, max_retries + 1):
        try:
            r = _session.post(url, json=payload, timeout=25)
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

def send_chat_message(chat_id: str | int, text: str, disable_preview: bool = True):
    """Відправка саме у чат (для команд /preview_api), формат HTML."""
    if not BOT_TOKEN:
        return
    try:
        _session.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            json={
                "chat_id": chat_id,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": disable_preview,
            },
            timeout=20
        )
    except Exception as e:
        log.warning("send_chat_message failed: %s", e)

def trade_url(exchange: str, market: str, base: str) -> str | None:
    b = base.upper()
    if exchange == "binance":
        return f"https://www.binance.com/en/{'futures' if market=='futures' else 'trade'}/{b}{'USDT' if market=='futures' else '_USDT'}"
    if exchange == "okx":
        if market == "futures":
            return f"https://www.okx.com/trade-swap/{b}-USDT"
        return f"https://www.okx.com/trade-spot/{b}-USDT"
    if exchange == "gate":
        if market == "futures":
            return f"https://www.gate.io/futures_trade/USDT/{b}_USDT"
        return f"https://www.gate.io/trade/{b}_USDT"
    if exchange == "bitget":
        if market == "futures":
            return f"https://www.bitget.com/futures/usdt/{b}USDT"
        return f"https://www.bitget.com/spot/{b}USDT"
    if exchange == "mexc":
        if market == "futures":
            return f"https://futures.mexc.com/exchange/{b}_USDT"
        return f"https://www.mexc.com/exchange/{b}_USDT"
    if exchange == "bingx" and market == "spot":
        return f"https://bingx.com/spot/{b}_USDT"
    return None

# -------------------- API SOURCES --------------------
# окремий модуль з логікою офіційних ендпойнтів
from api_sources import fetch_all_pairs

# -------------------- BOT (команди) -------------------
async def cmd_ping(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("pong")

async def cmd_testpost(update: Update, context: ContextTypes.DEFAULT_TYPE):
    send_bot_message("✅ Test publish from Crypto Hornet bot.")
    await update.message.reply_text("Відправив тест у TARGET_CHAT_ID.")

async def cmd_preview_api(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /preview_api [exchange] [market] [limit]
    exchange: binance|okx|gate|bitget|mexc|bingx|all (за замовчуванням all)
    market: spot|futures (за замовчуванням spot)
    limit: скільки показати (за замовчуванням 10)
    """
    args = context.args or []
    ex = (args[0].lower() if len(args) >= 1 else "all")
    mk = (args[1].lower() if len(args) >= 2 else "spot")
    try:
        lim = int(args[2]) if len(args) >= 3 else 10
    except Exception:
        lim = 10

    data = fetch_all_pairs()  # {(ex, mk): {BASE_QUOTE,...}}
    lines = []
    for (e, m), sset in sorted(data.items()):
        if ex != "all" and e != ex:
            continue
        if m != mk:
            continue
        pairs = sorted(sset)
        if not pairs:
            continue
        show = pairs[:lim]
        lines.append(f"<b>{html_escape(e.upper())} / {html_escape(m)}</b> ({len(pairs)}):")
        for p in show:
            base = p.split("_")[0]
            link = trade_url(e, m, base)
            if link:
                lines.append(f"• <code>{html_escape(base)}/USDT</code> — {html_escape(link)}")
            else:
                lines.append(f"• <code>{html_escape(base)}/USDT</code>")
    if not lines:
        lines = ["(порожньо)"]
    send_chat_message(update.effective_chat.id, "\n".join(lines))

def build_bot_app():
    if not BOT_TOKEN:
        return None
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("ping", cmd_ping))
    app.add_handler(CommandHandler("testpost", cmd_testpost))
    app.add_handler(CommandHandler("preview_api", cmd_preview_api))
    return app

# --------------- API loop: нові пари -----------------
async def poll_api_pairs_loop():
    """
    Відстежує появу нових USDT-пар через офіційні API.
    На першому запуску по кожному exchange/market — робить seed без постингу.
    """
    while True:
        try:
            all_pairs: Dict[Tuple[str, str], Set[str]] = fetch_all_pairs()
            for (ex, mk), pairset in all_pairs.items():
                # чи є щось у БД для цієї біржі/ринку?
                cur.execute("SELECT COUNT(1) FROM seen_pairs WHERE exchange=? AND market=?", (ex, mk))
                exists_any = (cur.fetchone() or (0,))[0] > 0

                # наявні в БД:
                cur.execute("SELECT symbol_id FROM seen_pairs WHERE exchange=? AND market=?", (ex, mk))
                seen_now = {row[0] for row in cur.fetchall()}

                new_symbols = pairset - seen_now
                if not exists_any and API_SEED_ON_START:
                    if new_symbols:
                        cur.executemany(
                            "INSERT OR IGNORE INTO seen_pairs(exchange,market,symbol_id) VALUES (?,?,?)",
                            [(ex, mk, s) for s in new_symbols]
                        )
                        conn.commit()
                        log.info("api seed %s/%s: %d symbols", ex, mk, len(new_symbols))
                    # невеличка пауза між біржами
                    await asyncio.sleep(0.3 + random.random()*0.3)
                    continue

                # додаємо нові у БД і постимо
                for sym in sorted(new_symbols):
                    cur.execute(
                        "INSERT OR IGNORE INTO seen_pairs(exchange,market,symbol_id) VALUES (?,?,?)",
                        (ex, mk, sym)
                    )
                    conn.commit()

                    base = sym.split("_")[0]
                    link = trade_url(ex, mk, base)

                    ex_html = html_escape(ex.upper())
                    mk_html = html_escape(mk)
                    base_html = html_escape(base)
                    lines = [
                        f"✅ <b>{ex_html}</b> — <b>{mk_html}</b> нова пара (API)",
                        f"Пара: <code>{base_html}/USDT</code>",
                    ]
                    if link:
                        lines.append(f"🔗 Тікер: {html_escape(link)}")
                    send_bot_message("\n".join(lines))

                await asyncio.sleep(0.3 + random.random()*0.3)

            await asyncio.sleep(API_PAIRS_INTERVAL_SEC)

        except Exception as e:
            log.exception("api pairs loop error: %s", e)
            await asyncio.sleep(3)

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
                    # може кинути 409, якщо ще один інстанс у поллінгу — тоді просто працюємо без команд
                    await app.updater.start_polling(drop_pending_updates=True)
                except Exception as e:
                    if "Conflict" in str(e):
                        log.warning("Updater conflict: already polling elsewhere, continue without commands.")
                    else:
                        raise
            except Exception as e:
                log.exception("Bot init failed: %s", e)

        if ENABLE_API_PAIRS:
            api_task = asyncio.create_task(poll_api_pairs_loop())

        # очікуємо задачі
        wait_tasks = [t for t in (api_task,) if t]
        if wait_tasks:
            await asyncio.wait(wait_tasks, return_when=asyncio.FIRST_EXCEPTION)
        else:
            # якщо все вимкнено — просто не завершуємось
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
