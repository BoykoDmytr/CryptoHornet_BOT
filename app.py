# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sys
import yaml
import time
import pytz
import asyncio
import sqlite3
import logging
from typing import List, Optional
from datetime import datetime

from dotenv import load_dotenv
import requests

from telethon import TelegramClient, events
from telethon.sessions import StringSession

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# --- твої парсери з тг-каналів (якщо користуєшся) ---
# якщо цього файлу нема, можеш закоментувати 2 рядки нижче (+ виклики з run_watcher)
from parser_patterns import parse_any, ListingEvent  # noqa: F401

# --- парсери анонсів бірж (новий модуль знизу) ---
from ann_sources import sources_matrix

# ----------------------- LOGGING -----------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("hornet")

# ----------------------- ENV --------------------------
load_dotenv()

API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
SESSION_NAME = os.getenv("SESSION_NAME", "hornet_session")  # локально
SESSION_STRING = os.getenv("SESSION_STRING", "").strip()     # для хмари

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
TARGET_CHAT_ID = os.getenv("TARGET_CHAT_ID", "")  # e.g. -100123...
OWNER_CHAT_ID = os.getenv("OWNER_CHAT_ID", "")

TIMEZONE = os.getenv("TIMEZONE", "Europe/Kyiv")
ANN_INTERVAL_SEC = int(os.getenv("ANN_INTERVAL_SEC", "180"))  # кожні 3 хв за замовч.

TZ = pytz.timezone(TIMEZONE)

# ----------------------- DB ---------------------------
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
cur.execute("""
CREATE TABLE IF NOT EXISTS seen_announcements(
  url TEXT PRIMARY KEY,
  exchange TEXT,
  market TEXT,
  title TEXT,
  symbols TEXT,
  start_ts INTEGER
)""")
conn.commit()

# ----------------------- UTILS ------------------------
def send_bot_message(text: str, disable_preview: bool = True):
    """Надіслати повідомлення у TARGET_CHAT_ID через Bot API."""
    if not BOT_TOKEN or not TARGET_CHAT_ID:
        log.warning("BOT_TOKEN або TARGET_CHAT_ID порожні — пропускаю send.")
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
            log.error("Bot send error: %s %s", r.status_code, r.text)
    except Exception as e:
        log.exception("Bot send failed: %s", e)

def send_owner(text: str):
    if not BOT_TOKEN or not OWNER_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    try:
        requests.post(url, json={"chat_id": OWNER_CHAT_ID, "text": text}, timeout=20)
    except Exception:
        pass

def _fmt_dt(dt: Optional[datetime]) -> str:
    return dt.strftime("%Y-%m-%d %H:%M %Z") if dt else "—"

# ----------- (опційно) форматування для тг-джерел ------------
def format_events_daily(events: List["ListingEvent"]) -> str:
    if not events:
        return "Немає нових подій."
    events_sorted = sorted(
        events,
        key=lambda e: (e.open_time.timestamp() if getattr(e, "open_time", None) else 0, e.exchange, e.market_type),
    )
    lines = []
    today = datetime.now(TZ).strftime("%d.%m")
    lines.append(f"*Listing {today}*")
    by_kind = {"alpha": [], "spot": [], "futures": [], "unknown": []}
    for e in events_sorted:
        hhmm = e.open_time.strftime("%H:%M") if getattr(e, "open_time", None) else "--:--"
        kind = e.market_type if getattr(e, "market_type", "unknown") in by_kind else "unknown"
        by_kind[kind].append(f"{e.exchange} ({kind}) {hhmm}")
    for k in ["alpha", "spot", "futures", "unknown"]:
        if by_kind[k]:
            lines.append("\n".join(f"• {row}" for row in by_kind[k]))
    return "\n\n".join(lines)

def format_event_verbose(e: "ListingEvent") -> str:
    parts = [f"*{e.exchange.upper()}* ({e.market_type})"]
    if getattr(e, "symbol", ""):
        parts.append(f"Pair: `{e.symbol}`")
    if getattr(e, "open_time", None):
        parts.append(f"Open: {e.open_time.strftime('%Y-%m-%d %H:%M %Z')}")
    if getattr(e, "network", ""):
        parts.append(f"Network: {e.network}")
    if getattr(e, "contract", ""):
        parts.append(f"Contract: `{e.contract}`")
    if getattr(e, "price", ""):
        parts.append(f"Price: ${e.price}")
    return "\n".join(parts)

# -------------------- BOT (команди) -------------------
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

# -------------------- TELETHON WATCHER (опційно) -----
async def run_watcher():
    # якщо не користуєшся джерелами з Telegram — просто не запускай цю таску у main()
    try:
        with open("sources.yml", "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}
        sources = cfg.get("sources", [])
    except Exception:
        sources = []

    if SESSION_STRING:
        log.info("Using Telethon StringSession")
        client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
    else:
        log.info("Using file session (dev). Will prompt for login.")
        client = TelegramClient(SESSION_NAME, API_ID, API_HASH)

    await client.connect()
    if not await client.is_user_authorized():
        raise RuntimeError("SESSION_STRING is missing/invalid. Згенеруй локально і встанови змінну середовища.")

    log.info("Telethon started. Listening: %s", sources)

    @client.on(events.NewMessage(chats=sources if sources else None))
    async def handler(event):
        try:
            ch_id = event.chat_id
            msg_id = event.id
            # anti-dup
            cur.execute(
                "INSERT OR IGNORE INTO seen_messages(channel_id, msg_id) VALUES (?, ?)",
                (ch_id, msg_id),
            )
            conn.commit()
            cur.execute("SELECT changes()")
            if cur.fetchone()[0] == 0:
                return

            text = (event.message.message or "").strip()
            if not text:
                return

            # якщо є файл parser_patterns.py
            try:
                events_list = parse_any(text, tz=TIMEZONE)  # type: ignore[name-defined]
            except Exception:
                events_list = []

            if not events_list:
                return

            if len(events_list) == 1 and (events_list[0].symbol or events_list[0].contract):
                msg = format_event_verbose(events_list[0])
                send_bot_message(msg)
            else:
                msg = format_events_daily(events_list)
                send_bot_message(msg)
        except Exception as e:
            log.exception("Handler error: %s", e)
            send_owner(f"⚠️ Handler error: {e}")

    await client.run_until_disconnected()

# -------------------- ANNOUNCEMENTS LOOP -------------
async def poll_announcements_loop():
    while True:
        try:
            for fetch in sources_matrix():
                try:
                    data = fetch()  # list[dict]
                    for a in data:
                        url = a["url"]
                        cur.execute("SELECT 1 FROM seen_announcements WHERE url=?", (url,))
                        if cur.fetchone():
                            continue

                        start_ts = int(a["start_dt"].timestamp()) if a.get("start_dt") else None
                        cur.execute(
                            "INSERT OR IGNORE INTO seen_announcements(url,exchange,market,title,symbols,start_ts) VALUES (?,?,?,?,?,?)",
                            (url, a.get("exchange"), a.get("market"), a.get("title"),
                             ",".join(a.get("symbols", [])), start_ts)
                        )
                        conn.commit()

                        lines = [
                            f"📣 *{a.get('exchange','').upper()}* — *{a.get('market','')}* listing announced",
                            f"📝 {a.get('title','')}",
                        ]
                        syms = a.get("symbols") or []
                        if syms:
                            lines.append("Пари:\n" + "\n".join(f"• `{s}/USDT`" for s in syms))
                        lines.append(f"🕒 Старт (Київ): {_fmt_dt(a.get('start_dt'))}")
                        lines.append(f"🔗 Джерело: {url}")
                        send_bot_message("\n".join(lines))
                except Exception as e:
                    log.exception("ann-source error for %s: %s", getattr(fetch, "__name__", "src"), e)
            await asyncio.sleep(ANN_INTERVAL_SEC)
        except Exception as e:
            log.exception("ann loop error: %s", e)
            await asyncio.sleep(5)

# -------------------- MAIN ---------------------------
async def main():
    app = build_bot_app()
    watcher_task = None
    ann_task = None
    try:
        if app:
            await app.initialize()
            await app.start()
            await app.updater.start_polling(drop_pending_updates=True)

        # запусти те, що потрібно; якщо тг-джерела не потрібні — не створюй watcher_task
        # watcher_task = asyncio.create_task(run_watcher())
        ann_task = asyncio.create_task(poll_announcements_loop())

        wait_tasks = [t for t in (watcher_task, ann_task) if t]
        if wait_tasks:
            await asyncio.wait(wait_tasks, return_when=asyncio.FIRST_EXCEPTION)
        else:
            # якщо нічого не запущено — просто не виходимо
            while True:
                await asyncio.sleep(3600)
    finally:
        for t in (watcher_task, ann_task):
            if t:
                try:
                    t.cancel()
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
