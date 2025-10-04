# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sys
import yaml
import asyncio
import sqlite3
import logging
from typing import List, Optional
from datetime import datetime

from dotenv import load_dotenv
import pytz
import requests

# --- Telegram Bot API (команди/сервісні) ---
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# --- Telethon (опційно: моніторинг тг-каналів) ---
from telethon import TelegramClient, events
from telethon.sessions import StringSession

# --- (опційно) локальний парсер для тг-постів ---
try:
    from parser_patterns import parse_any, ListingEvent  # type: ignore
except Exception:
    ListingEvent = object  # заглушка
    def parse_any(*args, **kwargs): return []

# --- парсери анонсів бірж ---
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
SESSION_NAME = os.getenv("SESSION_NAME", "hornet_session")
SESSION_STRING = os.getenv("SESSION_STRING", "").strip()

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
TARGET_CHAT_ID = os.getenv("TARGET_CHAT_ID", "")
OWNER_CHAT_ID = os.getenv("OWNER_CHAT_ID", "")

TIMEZONE = os.getenv("TIMEZONE", "Europe/Kyiv")
ANN_INTERVAL_SEC = int(os.getenv("ANN_INTERVAL_SEC", ""))

TZ = pytz.timezone(TIMEZONE)

# ----------------------- DB ---------------------------
DB_PATH = os.getenv("DB_PATH", "state.db")  # на Render: поставь /data/state.db (із підключеним Disk)
conn = sqlite3.connect(DB_PATH, check_same_thread=False)
cur = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS seen_messages(
  channel_id INTEGER,
  msg_id INTEGER,
  PRIMARY KEY(channel_id, msg_id)
)""")
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

# -------- форматування результатів parse_any (опційно) --------
def format_events_daily(events: List["ListingEvent"]) -> str:
    if not events:
        return "Немає нових подій."
    events_sorted = sorted(
        events,
        key=lambda e: (getattr(e, "open_time", datetime(1970,1,1)).timestamp(), getattr(e, "exchange", ""), getattr(e, "market_type", "")),
    )
    lines = []
    today = datetime.now(TZ).strftime("%d.%m")
    lines.append(f"*Listing {today}*")
    by_kind = {"alpha": [], "spot": [], "futures": [], "unknown": []}
    for e in events_sorted:
        ot = getattr(e, "open_time", None)
        hhmm = ot.strftime("%H:%M") if ot else "--:--"
        kind = getattr(e, "market_type", "unknown")
        if kind not in by_kind: kind = "unknown"
        by_kind[kind].append(f"{getattr(e,'exchange','?')} ({kind}) {hhmm}")
    for k in ["alpha", "spot", "futures", "unknown"]:
        if by_kind[k]:
            lines.append("\n".join(f"• {row}" for row in by_kind[k]))
    return "\n\n".join(lines)

def format_event_verbose(e: "ListingEvent") -> str:
    parts = [f"*{getattr(e,'exchange','').upper()}* ({getattr(e,'market_type','')})"]
    if getattr(e, "symbol", ""): parts.append(f"Pair: `{e.symbol}`")
    if getattr(e, "open_time", None): parts.append(f"Open: {e.open_time.strftime('%Y-%m-%d %H:%M %Z')}")
    if getattr(e, "network", ""): parts.append(f"Network: {e.network}")
    if getattr(e, "contract", ""): parts.append(f"Contract: `{e.contract}`")
    if getattr(e, "price", ""): parts.append(f"Price: ${e.price}")
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

# -------------------- TELETHON WATCHER (optional) ----
async def run_watcher():
    # якщо не треба слухати тг-канали — НЕ запускати цю таску в main()
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
            cur.execute("INSERT OR IGNORE INTO seen_messages(channel_id, msg_id) VALUES (?,?)", (ch_id, msg_id))
            conn.commit()
            cur.execute("SELECT changes()")
            if cur.fetchone()[0] == 0:
                return

            text = (event.message.message or "").strip()
            if not text:
                return

            events_list = []
            try:
                events_list = parse_any(text, tz=TIMEZONE)  # type: ignore
            except Exception:
                pass

            if not events_list:
                return

            if len(events_list) == 1 and (getattr(events_list[0], "symbol", "") or getattr(events_list[0], "contract", "")):
                send_bot_message(format_event_verbose(events_list[0]))
            else:
                send_bot_message(format_events_daily(events_list))
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
                        start_ts = int(a["start_dt"].timestamp()) if a.get("start_dt") else None

                        # запис і антидублі
                        cur.execute(
                            "INSERT OR IGNORE INTO seen_announcements(url,exchange,market,title,symbols,start_ts) "
                            "VALUES (?,?,?,?,?,?)",
                            (url, a.get("exchange"), a.get("market"), a.get("title"),
                             ",".join(a.get("symbols", [])), start_ts)
                        )
                        conn.commit()

                        # постимо ТІЛЬКИ якщо справді новий запис
                        cur.execute("SELECT changes()")
                        if cur.fetchone()[0] == 0:
                            continue

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

                except requests.exceptions.HTTPError as e:
                    code = getattr(getattr(e, "response", None), "status_code", None)
                    if code == 403 or "403" in str(e):
                        log.warning("ann-source http 403 for %s: %s",
                                    getattr(fetch, "__name__", "src"), e)
                        continue  # скіпаємо це джерело в цьому циклі
                    log.exception("ann-source HTTP error for %s: %s",
                                  getattr(fetch, "__name__", "src"), e)
                    continue

                except requests.exceptions.RequestException as e:
                    # таймаути, DNS, конекти, 5xx без raise_for_status, тощо
                    log.warning("ann-source network error for %s: %s",
                                getattr(fetch, "__name__", "src"), e)
                    continue

                except Exception as e:
                    log.exception("ann-source error for %s: %s",
                                  getattr(fetch, "__name__", "src"), e)
                    continue

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

        # TODO: якщо НЕ треба слухати тг-канали — залишай watcher_task закоментованим
        # watcher_task = asyncio.create_task(run_watcher())
        ann_task = asyncio.create_task(poll_announcements_loop())

        wait_tasks = [t for t in (watcher_task, ann_task) if t]
        if wait_tasks:
            await asyncio.wait(wait_tasks, return_when=asyncio.FIRST_EXCEPTION)
        else:
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
