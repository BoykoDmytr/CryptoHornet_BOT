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
from typing import List, Optional
from datetime import datetime

from dotenv import load_dotenv

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# парсери біржових анонсів
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

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
TARGET_CHAT_ID = os.getenv("TARGET_CHAT_ID", "")
OWNER_CHAT_ID = os.getenv("OWNER_CHAT_ID", "")

TIMEZONE = os.getenv("TIMEZONE", "Europe/Kyiv")
ANN_INTERVAL_SEC = int(os.getenv("ANN_INTERVAL_SEC", "180"))

TZ = pytz.timezone(TIMEZONE)

# ----------------------- DB ---------------------------
DB_PATH = os.getenv("DB_PATH", "state.db")  # на Render: підключи Disk і постав /data/state.db
conn = sqlite3.connect(DB_PATH, check_same_thread=False)
cur = conn.cursor()

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
_last_send_ts = 0.0

def send_bot_message(text: str, disable_preview: bool = True, max_retries: int = 3):
    """
    Безпечна відправка в канал:
    - глобальний тротлінг (~1.2s між повідомленнями),
    - повага до 429 retry_after,
    - до 3 спроб.
    """
    global _last_send_ts
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

    # простий пер-чат ліміт ~1.2s
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

def send_owner(text: str):
    if not BOT_TOKEN or not OWNER_CHAT_ID:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            json={"chat_id": OWNER_CHAT_ID, "text": text},
            timeout=20
        )
    except Exception:
        pass

def _fmt_dt(dt: Optional[datetime]) -> str:
    return dt.strftime("%Y-%m-%d %H:%M %Z") if dt else "—"

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

# -------------------- ANNOUNCEMENTS LOOP -------------
async def poll_announcements_loop():
    import requests as _rq  # локальний псевдонім для except

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

                        # час: спершу як у статті, інакше фолбек на Київ
                        start_text = a.get("start_text")
                        if start_text:
                            lines.append(f"🕒 Старт: {start_text}")
                        else:
                            lines.append(f"🕒 Старт (Київ): {_fmt_dt(a.get('start_dt'))}")

                        lines.append(f"🔗 Джерело: {url}")
                        send_bot_message("\n".join(lines))

                except _rq.exceptions.HTTPError as e:
                    code = getattr(getattr(e, "response", None), "status_code", None)
                    if code in (403, 503) or "403" in str(e):
                        log.warning("ann-source http %s for %s: %s",
                                    code or "HTTPError", getattr(fetch, "__name__", "src"), e)
                    else:
                        log.exception("ann-source HTTP error for %s: %s",
                                      getattr(fetch, "__name__", "src"), e)

                except _rq.exceptions.RequestException as e:
                    log.warning("ann-source network error for %s: %s",
                                getattr(fetch, "__name__", "src"), e)

                except Exception as e:
                    log.exception("ann-source error for %s: %s",
                                  getattr(fetch, "__name__", "src"), e)

                finally:
                    # невелика пауза між сайтами, щоб менше trigger’ити захисти
                    await asyncio.sleep(0.6 + random.random() * 0.7)

            await asyncio.sleep(ANN_INTERVAL_SEC)

        except Exception as e:
            log.exception("ann loop error: %s", e)
            await asyncio.sleep(5)

# -------------------- MAIN ---------------------------
async def main():
    app = build_bot_app()
    ann_task = None
    try:
        if app:
            # Ініціалізація і старт бота
            try:
                await app.initialize()
                await app.start()
                try:
                    await app.updater.start_polling(drop_pending_updates=True)
                except Exception as e:
                    # Якщо інший інстанс уже поллінгить (409) — працюємо без команд
                    if "Conflict" in str(e):
                        log.warning("Updater conflict: already polling elsewhere, continue without commands.")
                    else:
                        raise
            except Exception as e:
                log.exception("Bot init failed: %s", e)

        # цикл оголошень
        ann_task = asyncio.create_task(poll_announcements_loop())

        # очікуємо завершення задач
        wait_tasks = [t for t in (ann_task,) if t]
        if wait_tasks:
            await asyncio.wait(wait_tasks, return_when=asyncio.FIRST_EXCEPTION)
        else:
            while True:
                await asyncio.sleep(3600)
    finally:
        if ann_task:
            try:
                ann_task.cancel()
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
