# app/main.py
import asyncio
import os
import signal
from contextlib import suppress

# Optional perf on Linux
try:
    import uvloop  # type: ignore
    uvloop.install()
except Exception:
    pass

from telegram import Update
from telegram.ext import Application, CommandHandler
from telegram.request import HTTPXRequest

from app.config import load_settings
from app.store import init_db
from app.bot_handlers import register_admin
from app.poller import run_all
from app.reconciler import run_announcements
from app.utils.logging import logger


# ---------- simple /start for sanity check ----------
async def cmd_start(update: Update, _):
    await update.message.reply_text("Bot is alive ✅")


async def on_startup(app: Application):
    """
    - Validates envs
    - Initializes DB (creates tables)
    - Starts pollers + announcements reconciler as background tasks
    """
    # Load settings (env-based)
    settings = load_settings()

    if not settings.bot_token:
        logger.error("BOT_TOKEN is empty. Set BOT_TOKEN in env.")
        raise SystemExit(1)
    if not settings.target_chat_id:
        logger.error("TARGET_CHAT_ID is empty. Set TARGET_CHAT_ID in env.")
        raise SystemExit(1)

    # DB (auto-creates SQLite file/tables; parent dir ensured in store.py)
    sessionmaker = await init_db(settings.database_url)
    logger.info("Database initialized at %s", settings.database_url)

    # Save for shutdown
    app.bot_data["settings"] = settings
    app.bot_data["sessionmaker"] = sessionmaker

    # Handlers (/ping, /status, etc.)
    await register_admin(app)

    # Convenience for background tasks
    bot = app.bot
    bot._default_chat_id = settings.target_chat_id  # type: ignore[attr-defined]

    # Log enabled adapters
    enabled = [f"{ex.name}<{ex.module}>" for ex in settings.exchanges if ex.enabled]
    logger.info("Enabled exchanges: %s", ", ".join(enabled) or "(none)")

    # Launch exchange pollers (concurrent)
    pollers_task = asyncio.create_task(run_all(settings, bot, sessionmaker))
    app.bot_data["pollers_task"] = pollers_task

    # Launch announcements reconciler (Phase B)
    ann_interval = int(os.getenv("ANN_INTERVAL_SEC", "600"))
    ann_task = asyncio.create_task(run_announcements(bot, sessionmaker, ann_interval))
    app.bot_data["ann_task"] = ann_task

    logger.info("Telegram polling started.")


async def on_shutdown(app: Application):
    """Graceful shutdown: cancel background tasks and wait for them."""
    logger.info("Shutdown initiated.")
    for key in ("pollers_task", "ann_task"):
        task = app.bot_data.pop(key, None)
        if task:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
    logger.info("Shutdown complete.")


def build_app() -> Application:
    """
    Build the PTB Application with a bigger HTTPX pool to avoid PoolTimeout
    during message bursts, and attach lifecycle hooks.
    """
    # Throughput knobs (can set in Railway env)
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

    # Basic sanity command
    app.add_handler(CommandHandler("start", cmd_start))

    # Lifecycle hooks
    app.post_init = on_startup
    app.post_shutdown = on_shutdown

    return app


def main():
    app = build_app()

    # Let PTB run polling; add explicit signal hooks for Railway/Docker
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with suppress(NotImplementedError):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(app.stop()))

    app.run_polling(
        drop_pending_updates=True,
        stop_signals=None,  # handled above
    )


if __name__ == "__main__":
    main()
