# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import json
import asyncio
import logging
from typing import Dict, Tuple, List, Optional

from datetime import datetime

from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
)

# ---- Наші модулі ----
from api_sources import (
    api_seed_all,
    api_fetch_snapshot,
    api_build_events_from_diff,
    api_preview,
    api_lookup_listing_time,
    ALL_EXCHANGES,
)
from ann_sources import ann_lookup_listing_time

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("hornet")

# =========================
#   НАЛАШТУВАННЯ (ENV)
# =========================
BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
if not BOT_TOKEN:
    raise SystemExit("BOT_TOKEN is required")

OWNER_CHAT_ID = int(os.environ.get("OWNER_CHAT_ID", "0") or "0")
TARGET_CHAT_ID = int(os.environ.get("TARGET_CHAT_ID", "0") or "0")

API_SEED_ON_START = os.environ.get("API_SEED_ON_START", "1") not in ("0", "false", "no")
API_PAIRS_INTERVAL_SEC = int(os.environ.get("API_PAIRS_INTERVAL_SEC", "300") or "300")
ANN_INTERVAL_SEC = int(os.environ.get("ANN_INTERVAL_SEC", "450") or "450")
ENABLE_POLLING = os.environ.get("ENABLE_POLLING", "1") not in ("0", "false", "no")

STATE_FILE = os.environ.get("STATE_FILE", "./state.json")

# =========================
#   ЗБЕРЕЖЕННЯ СТАНУ
# =========================
_state_lock = asyncio.Lock()

# Структура:
# {
#   "snapshots": { "exchange|market": { "PAIR": "url" } },
#   "posted": { "exchange|market|pair": {
#       "exchange":..., "market":..., "pair":..., "base":..., "quote":..., "url":...,
#       "message_id": 123, "chat_id": -100..., "have_time": false,
#       "start_text": null, "source_url": null, "title": null
#   }}
# }
def _load_state() -> dict:
    if not os.path.exists(STATE_FILE):
        return {"snapshots": {}, "posted": {}}
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"snapshots": {}, "posted": {}}

def _save_state(data: dict) -> None:
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, STATE_FILE)

# =========================
#   ДОПОМОЖНІ ФУНКЦІЇ
# =========================
def _k(ex: str, mk: str) -> str:
    return f"{ex.lower()}|{mk.lower()}"

def _kp(ex: str, mk: str, pair: str) -> str:
    return f"{ex.lower()}|{mk.lower()}|{pair.upper()}"

def _display_exchange(ex: str) -> str:
    return ex.upper()

def _display_market(mk: str) -> str:
    return "spot" if mk.lower() == "spot" else "futures"

def _format_event_text(ev: dict) -> str:
    """
    Формує текст повідомлення без Markdown/HTML.
    Підтримує:
      - ev["time_candidates"]: список рядків часу (усі знайдені парсером варіанти)
      - ev["start_text"]: одиничний час (fallback, якщо кандидатів немає)
    Нічого не конвертуємо — друкуємо «як є» (UTC/UTC+8 тощо).
    """
    ex = _display_exchange(ev.get("exchange", ""))
    mk = _display_market(ev.get("market", ""))
    pair = ev.get("pair") or f"{ev.get('base','')}/{ev.get('quote','')}"
    url = ev.get("url", "")
    title = ev.get("title") or "нова пара (API)"

    # зібрати часи
    times = ev.get("time_candidates") or []
    if not times:
        st = ev.get("start_text")
        if st:
            times = [st]

    # унікалізація з збереженням порядку + тримінг
    seen = set()
    uniq_times = []
    for t in times:
        if not isinstance(t, str):
            continue
        t = t.strip()
        if not t or t in seen:
            continue
        seen.add(t)
        uniq_times.append(t)

    lines = []
    lines.append(f"✅ {ex} — {mk} {title}")
    lines.append(f"Пара: {pair}")

    if len(uniq_times) == 1:
        lines.append(f"🕒 Старт: {uniq_times[0]}")
    elif len(uniq_times) > 1:
        lines.append("🕒 Старт (кандидати):")
        for t in uniq_times[:6]:  # не більше 6 рядків, щоб не роздувати пост
            lines.append(f"• {t}")

    if url:
        lines.append(f"🔗 Тікер: {url}")

    return "\n".join(lines)


async def _post_event(ctx: ContextTypes.DEFAULT_TYPE, ev: dict) -> Optional[int]:
    chat_id = TARGET_CHAT_ID or OWNER_CHAT_ID
    if not chat_id:
        log.warning("TARGET_CHAT_ID/OWNER_CHAT_ID не задані — пропуск публікації")
        return None
    text = _format_event_text(ev)
    msg = await ctx.bot.send_message(
        chat_id=chat_id,
        text=text,
        disable_web_page_preview=True,
    )
    return msg.message_id

async def _edit_event(ctx: ContextTypes.DEFAULT_TYPE, posted_rec: dict) -> bool:
    chat_id = posted_rec.get("chat_id")
    msg_id = posted_rec.get("message_id")
    if not chat_id or not msg_id:
        return False
    text = _format_event_text(posted_rec)
    try:
        await ctx.bot.edit_message_text(
            chat_id=chat_id,
            message_id=msg_id,
            text=text,
            disable_web_page_preview=True,
        )
        return True
    except Exception as e:
        log.warning("edit_message_text failed: %s", e)
        return False

async def _enrich_with_times(ev: dict) -> dict:
    """
    Повертає ev з доповненими start_text/title/source_url,
    використовуючи спочатку API-біржі (api_lookup_listing_time),
    а якщо нема — парсер анонсів (ann_lookup_listing_time).
    """
    if ev.get("start_text"):
        return ev

    base = ev.get("base") or (ev.get("pair", "").split("/", 1)[0] if ev.get("pair") else "")
    quote = ev.get("quote") or (ev.get("pair", "").split("/", 1)[1] if ev.get("pair") else "")
    exchange = ev.get("exchange", "")
    market = ev.get("market", "")

    # 1) Спроба з API біржі
    try:
        st_text, _ts_ms = api_lookup_listing_time(exchange, market, base, quote)
        if st_text:
            ev["start_text"] = st_text
            return ev
    except Exception:
        pass

    # 2) Спроба через анонси (парсер)
    try:
        st_text2, src_url, title = ann_lookup_listing_time(exchange, market, base, quote)
        if st_text2:
            ev["start_text"] = st_text2
            if title:
                ev["title"] = title
            if src_url:
                ev["source_url"] = src_url
            return ev
    except Exception:
        pass

    return ev

# =========================
#   БЕКГРАУНД-ЦИКЛИ
# =========================
async def api_pairs_loop(app):
    """
    Основний цикл:
    - знімає свіжі снапшоти,
    - рахує дельту,
    - на нові пари робить пост; перед постом намагається підтягнути час (API/анонс),
    - записує в state.
    """
    await asyncio.sleep(2.0)  # коротка пауза після старту
    while True:
        try:
            async with _state_lock:
                state = _load_state()
                snapshots: Dict[str, Dict[str, str]] = state.get("snapshots", {})
                posted: Dict[str, dict] = state.get("posted", {})

            for ex, mk in ALL_EXCHANGES:
                key = _k(ex, mk)
                prev = snapshots.get(key, {}) or {}
                cur = api_fetch_snapshot(ex, mk)

                events = api_build_events_from_diff(ex, mk, prev, cur)
                # збагатити часом до публікації
                for ev in events:
                    ev = await _enrich_with_times(ev)
                    kk = _kp(ev["exchange"], ev["market"], ev["pair"])
                    if kk in posted:
                        continue  # на випадок рестартів/гонок
                    # пост
                    msg_id = await _post_event(app, ev)
                    if not msg_id:
                        continue
                    rec = dict(ev)
                    rec["message_id"] = msg_id
                    rec["chat_id"] = TARGET_CHAT_ID or OWNER_CHAT_ID
                    rec["have_time"] = bool(ev.get("start_text"))
                    async with _state_lock:
                        state = _load_state()
                        state.setdefault("posted", {})[kk] = rec
                        state.setdefault("snapshots", {})[key] = cur
                        _save_state(state)

                # оновити снапшот навіть без нових подій
                async with _state_lock:
                    state = _load_state()
                    state.setdefault("snapshots", {})[key] = cur
                    _save_state(state)

                await asyncio.sleep(0.2)

        except Exception as e:
            log.exception("api_pairs_loop error: %s", e)

        await asyncio.sleep(API_PAIRS_INTERVAL_SEC)


async def ann_enrich_loop(app):
    """
    Проходить по вже опублікованих без часу й намагається добрати start_text з анонсів/API.
    Якщо знайдено — редагує повідомлення.
    """
    await asyncio.sleep(5.0)
    while True:
        try:
            async with _state_lock:
                state = _load_state()
                posted: Dict[str, dict] = state.get("posted", {})

            changed = False
            for kk, rec in list(posted.items()):
                if rec.get("have_time"):
                    continue
                # збагачення
                rec2 = await _enrich_with_times(dict(rec))
                if rec2.get("start_text"):
                    # редагуємо пост
                    ok = await _edit_event(app, rec2)
                    if ok:
                        rec2["have_time"] = True
                        async with _state_lock:
                            state = _load_state()
                            state.setdefault("posted", {})[kk] = rec2
                            _save_state(state)
                        changed = True
                await asyncio.sleep(0.05)

            if not changed:
                log.info("ann_enrich_loop: немає що доповнювати (pending=0?)")

        except Exception as e:
            log.exception("ann_enrich_loop error: %s", e)

        await asyncio.sleep(ANN_INTERVAL_SEC)

# =========================
#   КОМАНДИ БОТА
# =========================
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Crypto Hornet API бот запущений. /preview, /inject, /seed")

async def cmd_seed(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    # ручний seed усіх бірж
    snaps = api_seed_all()
    async with _state_lock:
        state = _load_state()
        for (ex, mk), snap in snaps.items():
            state.setdefault("snapshots", {})[_k(ex, mk)] = snap
        _save_state(state)
    await update.message.reply_text("✅ Seed завершено.")

async def cmd_preview(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """
    /preview [exchange|all] [market|all] [limit]
    приклади:
      /preview all all 2
      /preview gate futures 3
      /preview binance spot
    """
    args = (ctx.args or [])
    ex = (args[0].lower() if len(args) >= 1 else "all")
    mk = (args[1].lower() if len(args) >= 2 else "all")
    limit = int(args[2]) if len(args) >= 3 and args[2].isdigit() else 2

    pairs = []
    if ex == "all":
        pairs = list(ALL_EXCHANGES)
    else:
        if mk == "all":
            pairs = [(ex, "spot"), (ex, "futures")]
        else:
            pairs = [(ex, mk)]

    count = 0
    for e, m in pairs:
        try:
            preview = api_preview(e, m, limit=limit)
            for ev in preview:
                ev = await _enrich_with_times(ev)
                text = _format_event_text(ev)
                await update.message.reply_text(text, disable_web_page_preview=True)
                count += 1
        except Exception as e:
            await update.message.reply_text(f"{e} for {e}/{m}")
        await asyncio.sleep(0.2)

    if count == 0:
        await update.message.reply_text("Нічого не знайдено для прев’ю.")

async def cmd_inject(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """
    Швидка перевірка ланцюга публікації.
    Варіанти:
      /inject                 -> візьме першу пару з gate/futures (умовно)
      /inject binance spot BTC USDT
    """
    args = ctx.args or []
    ex = args[0].lower() if len(args) >= 1 else "gate"
    mk = args[1].lower() if len(args) >= 2 else "futures"
    base = args[2].upper() if len(args) >= 3 else "BTC"
    quote = args[3].upper() if len(args) >= 4 else "USDT"

    # спробуємо знайти url в поточному снапшоті
    async with _state_lock:
        state = _load_state()
        cur = state.get("snapshots", {}).get(_k(ex, mk), {}) or {}
    url = cur.get(f"{base}/{quote}") or f"https://example.com/{ex}/{base}_{quote}"
    ev = {
        "exchange": ex,
        "market": mk,
        "pair": f"{base}/{quote}",
        "base": base,
        "quote": quote,
        "url": url,
        "title": "тестова пара (INJECT)",
        "start_text": None,
    }
    ev = await _enrich_with_times(ev)
    msg_id = await _post_event(ctx, ev)
    if not msg_id:
        await update.message.reply_text("❌ Не вдалося надіслати тест.")
        return
    # зберегти як опубліковане
    rec = dict(ev)
    rec["message_id"] = msg_id
    rec["chat_id"] = TARGET_CHAT_ID or OWNER_CHAT_ID
    rec["have_time"] = bool(ev.get("start_text"))
    kk = _kp(ex, mk, f"{base}/{quote}")
    async with _state_lock:
        state = _load_state()
        state.setdefault("posted", {})[kk] = rec
        _save_state(state)
    await update.message.reply_text(f"✅ Надіслано. message_id={msg_id}")

# =========================
#   MAIN
# =========================
async def main():
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("seed", cmd_seed))
    app.add_handler(CommandHandler("preview", cmd_preview))
    app.add_handler(CommandHandler("inject", cmd_inject))

    # Попередній seed (опційно)
    if API_SEED_ON_START:
        snaps = api_seed_all()
        async with _state_lock:
            state = _load_state()
            for (ex, mk), snap in snaps.items():
                state.setdefault("snapshots", {})[_k(ex, mk)] = snap
            _save_state(state)

    # Фонові цикли
    #app.job_queue.run_repeating(lambda *_: None, interval=3600, first=0)  # dummy, щоб job_queue існував
    asyncio.create_task(api_pairs_loop(app))
    asyncio.create_task(ann_enrich_loop(app))

    if ENABLE_POLLING:
        await app.initialize()
        await app.start()
        try:
            await app.updater.start_polling(allowed_updates=Update.ALL_TYPES)
            await asyncio.Event().wait()
        finally:
            await app.updater.stop()
            await app.stop()
            await app.shutdown()
    else:
        # Якщо polling вимкнено — все одно крутимо фон-цикли
        await asyncio.Event().wait()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
