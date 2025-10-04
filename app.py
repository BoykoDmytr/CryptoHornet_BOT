# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import asyncio
import sqlite3
import logging
import sys
from dataclasses import asdict
from typing import List
from datetime import datetime

import pytz
import yaml
import requests

from dotenv import load_dotenv
from telethon import TelegramClient, events
from telethon.sessions import StringSession  # <-- для headless деплою

from parser_patterns import parse_any, ListingEvent

# ----------------------- ЛОГІНГ -----------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

# ----------------------- ENV --------------------------
load_dotenv()

API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
SESSION_NAME = os.getenv("SESSION_NAME", "hornet_session")  # локально
SESSION_STRING = os.getenv("SESSION_STRING", "").strip()     # хмара

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
TARGET_CHAT_ID = os.getenv("TARGET_CHAT_ID", "")  # e.g. -100123...
OWNER_CHAT_ID = os.getenv("OWNER_CHAT_ID", "")

TIMEZONE = os.getenv("TIMEZONE", "Europe/Kyiv")

# ----------------------- DB DE-DUPE -------------------
DB_PATH = "state.db"
conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()
cur.execute(
    """
    CREATE TABLE IF NOT EXISTS seen_messages(
      channel_id INTEGER,
      msg_id INTEGER,
      PRIMARY KEY(channel_id, msg_id)
    )
    """
)
conn.commit()

# ----------------------- УТИЛІТИ ----------------------
def send_bot_message(text: str, disable_preview: bool = True):
    """Надіслати повідомлення у TARGET_CHAT_ID Bot API-методом."""
    if not BOT_TOKEN or not TARGET_CHAT_ID:
        logging.warning("BOT_TOKEN або TARGET_CHAT_ID порожні — пропускаю send.")
        return
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TARGET_CHAT_ID,
        "text": text,
        "parse_mode": "Markdown",
        "disable_web_page_preview": disable_preview,
    }
    try:
        r = requests.post(url, json=payload, timeout=20)
        if r.status_code != 200:
            logging.error("Bot send error: %s %s", r.status_code, r.text)
    except Exception as e:
        logging.exception("Bot send failed: %s", e)

def send_owner(text: str):
    """Сервісні алерти у приват власнику."""
    if not BOT_TOKEN or not OWNER_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": OWNER_CHAT_ID, "text": text}
    try:
        requests.post(url, json=payload, timeout=20)
    except Exception:
        pass

def format_events_daily(events: List[ListingEvent]) -> str:
    """Форматування блоку у стилі скрінів (біржа (тип) час)."""
    if not events:
        return "Немає нових подій."
    events_sorted = sorted(
        events,
        key=lambda e: (e.open_time.timestamp() if e.open_time else 0, e.exchange, e.market_type),
    )
    lines = []
    today = datetime.now(pytz.timezone(TIMEZONE)).strftime("%d.%m")
    lines.append(f"*Listing {today}*")
    by_kind = {"alpha": [], "spot": [], "futures": [], "unknown": []}
    for e in events_sorted:
        hhmm = e.open_time.strftime("%H:%M") if e.open_time else "--:--"
        kind = e.market_type if e.market_type in by_kind else "unknown"
        by_kind[kind].append(f"{e.exchange} ({kind}) {hhmm}")
    for k in ["alpha", "spot", "futures", "unknown"]:
        if by_kind[k]:
            lines.append("\n".join(f"• {row}" for row in by_kind[k]))
    return "\n\n".join(lines)

def format_event_verbose(e: ListingEvent) -> str:
    parts = [f"*{e.exchange.upper()}* ({e.market_type})"]
    if e.symbol:
        parts.append(f"Pair: `{e.symbol}`")
    if e.open_time:
        parts.append(f"Open: {e.open_time.strftime('%Y-%m-%d %H:%M %Z')}")
    if e.network:
        parts.append(f"Network: {e.network}")
    if e.contract:
        parts.append(f"Contract: `{e.contract}`")
    if e.price:
        parts.append(f"Price: ${e.price}")
    return "\n".join(parts)

# -------------------- BOT (команди) -------------------
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

async def cmd_ping(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("pong")

async def cmd_sources(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        with open("sources.yml", "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}
        srcs = cfg.get("sources", [])
        if not srcs:
            await update.message.reply_text("sources.yml порожній.")
        else:
            msg = "Джерела:\n" + "\n".join(f"• {s}" for s in srcs)
            await update.message.reply_text(msg, disable_web_page_preview=True)
    except Exception as e:
        await update.message.reply_text(f"Помилка читання sources.yml: {e}")

# опційна команда для швидкого тесту постингу
async def cmd_testpost(update: Update, context: ContextTypes.DEFAULT_TYPE):
    send_bot_message("✅ Test publish from Crypto Hornet bot.")
    await update.message.reply_text("Відправив тест у TARGET_CHAT_ID.")

def build_bot_app():
    if not BOT_TOKEN:
        return None
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("ping", cmd_ping))
    app.add_handler(CommandHandler("sources", cmd_sources))
    app.add_handler(CommandHandler("testpost", cmd_testpost))
    return app

# -------------------- TELETHON WATCHER ----------------
async def run_watcher():
    # 1) Завантажуємо джерела
    with open("sources.yml", "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    sources = cfg.get("sources", [])
    if not sources:
        logging.warning("sources.yml is empty.")

    # 2) Ініціалізація Telethon-клієнта
    if SESSION_STRING:
        logging.info("Using Telethon StringSession")
        client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
    else:
        logging.info("Using file session (dev). Will prompt for login.")
        client = TelegramClient(SESSION_NAME, API_ID, API_HASH)

    await client.connect()

    # 3) Перевірка авторизації (щоб не було input() у хмарі)
    if not await client.is_user_authorized():
        raise RuntimeError(
            "SESSION_STRING is missing/invalid. Згенеруй локально make_session.py "
            "та встанови змінну середовища SESSION_STRING."
        )

    logging.info("Telethon started. Listening: %s", sources)

    @client.on(events.NewMessage(chats=sources if sources else None))
    async def handler(event):
        try:
            ch_id = event.chat_id
            msg_id = event.id

            # дедуплікація
            cur.execute(
                "INSERT OR IGNORE INTO seen_messages(channel_id, msg_id) VALUES (?, ?)",
                (ch_id, msg_id),
            )
            conn.commit()
            cur.execute("SELECT changes()")
            if cur.fetchone()[0] == 0:
                return  # вже бачили

            text = (event.message.message or "").strip()
            if not text:
                return

            events_list = parse_any(text, tz=TIMEZONE)
            if not events_list:
                return

            # деталізований кейс
            if len(events_list) == 1 and (events_list[0].symbol or events_list[0].contract):
                msg = format_event_verbose(events_list[0])
                send_bot_message(msg)
            else:
                # агреговане зведення
                msg = format_events_daily(events_list)
                send_bot_message(msg)

        except Exception as e:
            logging.exception("Handler error: %s", e)
            send_owner(f"⚠️ Handler error: {e}")

    # 4) чекаємо поки клієнт не відключиться
    await client.run_until_disconnected()

# -------------------- ГОЛОВНИЙ ЦИКЛ ------------------
async def main():
    app = build_bot_app()
    watcher_task = None
    try:
        if app:
            # послідовний старт Application + polling
            await app.initialize()
            await app.start()
            await app.updater.start_polling(drop_pending_updates=True)

        # Telethon watcher працює паралельно
        watcher_task = asyncio.create_task(run_watcher())
        await watcher_task
    finally:
        # акуратне вимкнення
        if app:
            try:
                await app.updater.stop()
            except Exception:
                pass
            try:
                await app.stop()
            except Exception:
                pass
            try:
                await app.shutdown()
            except Exception:
                pass
        if watcher_task:
            try:
                watcher_task.cancel()
            except Exception:
                pass

# -------------------- ENTRYPOINT ----------------------
if __name__ == "__main__":
    asyncio.run(main())

