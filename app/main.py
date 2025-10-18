# app/main.py
import asyncio
import os
import signal
from contextlib import suppress

try:
    import uvloop  # optional perf on linux
    uvloop.install()
except Exception:
    pass

from telegram.ext import Application, CommandHandler
from telegram.request import HTTPXRequest
from telegram import Update

from app.config import load_settings
from app.store import init_db
from app.bot_handlers import register_admin
from app.poller import run_all
from app.utils.logging import logger


# ---------- simple /start for sanity check ----------
async def cmd_start(update: Update, _):
    await update.message.reply_text("Bot is alive ✅")


async def on_startup(app: Application):
    """Initialize DB and kick off background pollers (no JobQueue)."""
    settings = load_settings()

    if not settings.bot_token:
        logger.error("BOT_TOKEN is empty. Set BOT_TOKEN in env.")
        raise SystemExit(1)
    if not settings.target_chat_id:
        logger.error("TARGET_CHAT_ID is empty. Set TARGET_CHAT_ID in env.")
        raise SystemExit(1)

    # DB (auto-creates SQLite file/tables)
    sessionmaker = await init_db(settings.database_url)
    logger.info("Database initialized at %s", settings.database_url)

    # save for shutdown
    app.bot_data["settings"] = settings
    app.bot_data["sessionmaker"] = sessionmaker

    # Start exchange pollers as a single background task
    # We send messages using the same bot instance Application manages.
    bot = app.bot
    bot._default_chat_id = settings.target_chat_id  # convenience for poller
    task = asyncio.create_task(run_all(settings, bot, sessionmaker))
    app.bot_data["pollers_task"] = task

    logger.info("Telegram polling started.")


async def on_shutdown(app: Application):
    """Graceful shutdown: cancel pollers task."""
    logger.info("Shutdown initiated.")
    task = app.bot_data.pop("pollers_task", None)
    if task:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task
    logger.info("Shutdown complete.")


def build_app() -> Application:
    # Tune HTTP pool to avoid PoolTimeout under bursts
    pool_size = int(os.getenv("TG_POOL_SIZE", "100"))
    pool_timeout = float(os.getenv("TG_POOL_TIMEOUT", "10.0"))

    request = HTTPXRequest(
        connection_pool_size=pool_size,
        pool_timeout=pool_timeout,
        read_timeout=20.0,
        write_timeout=20.0,
        connect_timeout=10.0,
    )

    settings = load_settings()
    app = (
        Application.builder()
        .token(settings.bot_token or "")
        .request(request)
        .build()
    )

    # Admin handlers from your project + a basic /start
    app.add_handler(CommandHandler("start", cmd_start))
    asyncio.get_event_loop()  # ensure loop exists before registering
    app.post_init = on_startup
    app.post_shutdown = on_shutdown

    # Your existing admin commands
    # (register_admin adds /ping, /status, etc.)
    # Needs to be awaited later; do it here via a convenience init hook:
    async def _register(_):
        await register_admin(app)
    app.pre_run = _register  # PTB will await this before entering polling

    return app


def main():
    app = build_app()

    # Let PTB handle signals; also add explicit handlers for Railway
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with suppress(NotImplementedError):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(app.stop()))

    # Run polling (blocking). No JobQueue needed.
    app.run_polling(
        drop_pending_updates=True,
        stop_signals=None,  # we handle signals above
    )


if __name__ == "__main__":
    main()
