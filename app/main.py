# main.py
import asyncio
import logging
import os
import signal
from contextlib import suppress
from typing import Optional

from telegram import Update
from telegram.error import TimedOut, RetryAfter, NetworkError
from telegram.ext import Application, CommandHandler, ContextTypes
from telegram.request import HTTPXRequest

# ---------- Config ----------
BOT_TOKEN = os.getenv("BOT_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN env var is required")

# Tuneable via env:
POOL_SIZE = int(os.getenv("TG_POOL_SIZE", "100"))            # HTTP pool capacity
POOL_TIMEOUT = float(os.getenv("TG_POOL_TIMEOUT", "10.0"))   # seconds
SEND_CONCURRENCY = int(os.getenv("TG_SEND_CONCURRENCY", "5"))  # concurrent send_message
MAX_SEND_RETRIES = int(os.getenv("TG_MAX_SEND_RETRIES", "5"))

# (Optional) Your database URL (already using sqlite+aiosqlite per your setup)
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///./bot.db")

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)-8s | %(name)s:%(lineno)d - %(message)s",
)
log = logging.getLogger(__name__)

# Global limiter to avoid PoolTimeout & flood control
SEND_SEM = asyncio.Semaphore(SEND_CONCURRENCY)


async def init_db():
    # Put your real DB init/migrations here.
    # For now we just log; your earlier runs showed “Database initialized.”
    log.info("Database initialized at %s", DATABASE_URL)


async def safe_send_message(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    text: str,
    *,
    parse_mode: Optional[str] = None,
    disable_web_page_preview: bool = True,
):
    """
    Send a Telegram message with:
    - global concurrency limiting
    - RetryAfter backoff (flood control)
    - exponential backoff on TimedOut/NetworkError
    """
    attempt = 0
    while True:
        try:
            async with SEND_SEM:
                return await context.bot.send_message(
                    chat_id=chat_id,
                    text=text,
                    parse_mode=parse_mode,
                    disable_web_page_preview=disable_web_page_preview,
                )
        except RetryAfter as e:
            # Telegram says to wait
            wait_s = getattr(e, "retry_after", 5) or 5
            log.warning("Flood control: retrying in %.1fs", wait_s)
            await asyncio.sleep(wait_s + 1.0)
        except (TimedOut, NetworkError) as e:
            if attempt >= MAX_SEND_RETRIES:
                log.error("Send failed after %d attempts: %r", attempt, e)
                raise
            backoff = min(2 ** attempt * 0.5, 10.0)
            attempt += 1
            log.warning("Send timeout/network error: retrying in %.1fs", backoff)
            await asyncio.sleep(backoff)


# --- Demo command so you can test sends safely ---
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await safe_send_message(context, update.effective_chat.id, "Bot is alive ✅")


# Example worker that would publish listings without spamming Telegram
async def listings_publisher(app: Application):
    """
    Replace this with your real polling/parsing loop.
    Make sure to call safe_send_message() instead of bot.send_message().
    """
    chat_id = int(os.getenv("TEST_CHAT_ID", "0")) or None
    if not chat_id:
        log.info("TEST_CHAT_ID not set; listings_publisher is idle.")
        return

    while True:
        # Example message burst (simulate your exchange scanners)
        msgs = [
            "KUCOIN SPOT: VSYS listed",
            "KUCOIN FUTURES: PEPE perpetual notice",
            "GATE SPOT: NEWCOIN announced",
        ]
        for m in msgs:
            await safe_send_message(app, chat_id, m)  # app works as ContextTypes.DEFAULT_TYPE in this helper
            await asyncio.sleep(0.05)  # tiny gap helps keep pool healthy
        await asyncio.sleep(10)  # wait before next cycle


async def on_startup(app: Application):
    await init_db()
    log.info("Telegram polling started.")
    # Kick off background tasks here (real workers that post to channels)
    app.job_queue.run_once(lambda *_: None, when=0)  # ensure job_queue initialized
    app.data["bg_task"] = asyncio.create_task(listings_publisher(app))


async def on_shutdown(app: Application):
    log.info("Shutdown initiated.")
    with suppress(Exception):
        task = app.data.pop("bg_task", None)
        if task:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task


def build_app() -> Application:
    # Single HTTPX client for the WHOLE app with bigger pool & sane timeouts
    request = HTTPXRequest(
        connection_pool_size=POOL_SIZE,
        pool_timeout=POOL_TIMEOUT,
        read_timeout=20.0,
        write_timeout=20.0,
        connect_timeout=10.0,
    )

    application = (
        Application.builder()
        .token(BOT_TOKEN)
        .request(request)
        .build()
    )

    # Handlers
    application.add_handler(CommandHandler("start", start_cmd))

    # Lifecycle
    application.post_init = on_startup
    application.post_shutdown = on_shutdown

    return application


def main():
    app = build_app()

    # Graceful shutdown on SIGTERM/SIGINT
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(app.stop()))

    # Run polling (blocks)
    app.run_polling(
        allowed_updates=None,  # default
        drop_pending_updates=True,
        stop_signals=None,  # handled above
    )


if __name__ == "__main__":
    main()
