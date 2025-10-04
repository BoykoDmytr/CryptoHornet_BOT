# -*- coding: utf-8 -*-
from __future__ import annotations
import os, asyncio, sqlite3, time, logging, json
from dataclasses import asdict
from typing import List
from datetime import datetime
import pytz, yaml, requests

from telethon import TelegramClient, events
from dotenv import load_dotenv

from parser_patterns import parse_any, ListingEvent

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
load_dotenv()

API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
SESSION_NAME = os.getenv("SESSION_NAME", "hornet_session")

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
TARGET_CHAT_ID = os.getenv("TARGET_CHAT_ID", "")  # e.g. -100123...
OWNER_CHAT_ID = os.getenv("OWNER_CHAT_ID", "")

TIMEZONE = os.getenv("TIMEZONE", "Europe/Kyiv")

# --- DB for dedupe ---
DB_PATH = "state.db"
conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()
cur.execute("""
CREATE TABLE IF NOT EXISTS seen_messages(
  channel_id INTEGER,
  msg_id INTEGER,
  PRIMARY KEY(channel_id, msg_id)
)
""")
conn.commit()

async def cmd_testpost(update: Update, context: ContextTypes.DEFAULT_TYPE):
    send_bot_message("✅ Test publish from Crypto Hornet bot.")
    await update.message.reply_text("Відправив тест у TARGET_CHAT_ID.")


def send_bot_message(text: str, disable_preview: bool = True):
    if not BOT_TOKEN or not TARGET_CHAT_ID:
        logging.warning("BOT_TOKEN or TARGET_CHAT_ID not set; skipping send.")
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
    if not BOT_TOKEN or not OWNER_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": OWNER_CHAT_ID, "text": text}
    try:
        requests.post(url, json=payload, timeout=20)
    except Exception:
        pass

def format_events_daily(events: List[ListingEvent]) -> str:
    """Форматування у стилі ваших скрінів (список біржа (тип) час)."""
    if not events:
        return "Немає нових подій."
    # Сортуємо за часом/біржею
    events_sorted = sorted(
        events,
        key=lambda e: (e.open_time.timestamp() if e.open_time else 0, e.exchange, e.market_type)
    )
    lines = []
    today = datetime.now(pytz.timezone(TIMEZONE)).strftime("%d.%m")
    lines.append(f"*Listing {today}*")
    by_kind = {"alpha": [], "spot": [], "futures": [], "unknown": []}
    for e in events_sorted:
        hhmm = e.open_time.strftime("%H:%M") if e.open_time else "--:--"
        by_kind[e.market_type if e.market_type in by_kind else "unknown"].append(f"{e.exchange} ({e.market_type}) {hhmm}")
    # Порядок як на скрінах приблизно
    order = ["alpha", "spot", "futures", "unknown"]
    for k in order:
        if not by_kind[k]:
            continue
        block = "\n".join(f"• {row}" for row in by_kind[k])
        lines.append(block)
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

# --- Bot polling for minimal commands (/ping, /sources) ---
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
            await update.message.reply_text("Джерела:\n" + "\n".join(f"• {s}" for s in srcs))
    except Exception as e:
        await update.message.reply_text(f"Помилка читання sources.yml: {e}")

def build_bot_app():
    if not BOT_TOKEN:
        return None
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("ping", cmd_ping))
    app.add_handler(CommandHandler("sources", cmd_sources))
    app.add_handler(CommandHandler("testpost", cmd_testpost))

    return app

# --- Telethon watcher ---
async def run_watcher():
    # Завантажуємо джерела
    with open("sources.yml", "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    sources = cfg.get("sources", [])
    if not sources:
        logging.warning("sources.yml is empty.")
    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
    await client.start()
    logging.info("Telethon started. Listening: %s", sources)

    @client.on(events.NewMessage(chats=sources if sources else None))
    async def handler(event):
        try:
            ch_id = event.chat_id
            msg_id = event.id
            # dedupe
            cur.execute("INSERT OR IGNORE INTO seen_messages(channel_id, msg_id) VALUES (?, ?)", (ch_id, msg_id))
            conn.commit()
            # Якщо вже бачили — ігноруємо
            cur.execute("SELECT changes()")
            if cur.fetchone()[0] == 0:
                return
            text = event.message.message or ""
            if not text.strip():
                return
            events_list = parse_any(text, tz=TIMEZONE)
            if not events_list:
                return  # не схоже на лістинг
            # Якщо це один деталізований евент (типу MEXC futures) — шлемо детально
            if len(events_list) == 1 and (events_list[0].symbol or events_list[0].contract):
                msg = format_event_verbose(events_list[0])
                send_bot_message(msg)
            else:
                # Групове форматування
                msg = format_events_daily(events_list)
                send_bot_message(msg)
        except Exception as e:
            logging.exception("Handler error: %s", e)
            send_owner(f"⚠️ Handler error: {e}")

    await client.run_until_disconnected()

async def main():
    app = build_bot_app()
    watcher_task = None
    try:
        if app:
            # 1) послідовно ініціалізуємо та стартуємо Application
            await app.initialize()
            await app.start()
            # 2) запускаємо polling для команд /ping, /sources
            await app.updater.start_polling()

        # 3) Telethon-watcher працює паралельно (чекаємо, поки не від’єднається)
        watcher_task = asyncio.create_task(run_watcher())
        await watcher_task
    finally:
        # 4) акуратне вимкнення
        if app:
            try:
                await app.updater.stop()
            except Exception:
                pass
            await app.stop()
            try:
                await app.shutdown()
            except Exception:
                pass
        if watcher_task:
            watcher_task.cancel()



if __name__ == "__main__":
    asyncio.run(main())
