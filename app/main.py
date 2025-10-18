# app/main.py
import asyncio
import signal
import sys
from contextlib import suppress

try:
    import uvloop  # type: ignore
    uvloop.install()
except Exception:
    pass

from telegram.ext import Application
from telegram import Bot

from app.config import load_settings
from app.store import init_db
from app.bot_handlers import register_admin
from app.poller import run_all
from app.utils.logging import logger


async def _startup():
    """Initialize settings, DB, Telegram app/bot."""
    settings = load_settings()

    # Basic sanity checks (fail fast on Railway if envs are missing)
    if not settings.bot_token:
        logger.error("BOT_TOKEN is empty. Set BOT_TOKEN in environment.")
        raise SystemExit(1)
    if not settings.target_chat_id:
        logger.error("TARGET_CHAT_ID is empty. Set TARGET_CHAT_ID in environment.")
        raise SystemExit(1)

    # DB (auto-creates SQLite file/tables)
    sessionmaker = await init_db(settings.database_url)
    logger.info("Database initialized.")

    # Telegram app
    app = Application.builder().token(settings.bot_token).build()
    await register_admin(app)

    # Dedicated Bot instance (so we can send from background tasks)
    bot = Bot(token=settings.bot_token)
    # Store default chat id for convenience in poller
    bot._default_chat_id = settings.target_chat_id  # type: ignore[attr-defined]

    # Bring Telegram app online (explicit lifecycle to avoid PTB shutdown errors)
    await app.initialize()
    await app.start()

    # Start long-polling (non-blocking)
    await app.updater.start_polling(drop_pending_updates=True)
    logger.info("Telegram polling started.")

    return settings, sessionmaker, app, bot


async def _shutdown(app: Application, tasks: list[asyncio.Task]):
    """Graceful shutdown for pollers and Telegram app."""
    logger.info("Shutdown initiated...")
    # Cancel background tasks
    for t in tasks:
        t.cancel()
    for t in tasks:
        with suppress(asyncio.CancelledError):
            await t

    # Stop Telegram app cleanly
    with suppress(Exception):
        await app.updater.stop()
    with suppress(Exception):
        await app.stop()
    with suppress(Exception):
        await app.shutdown()

    logger.info("Shutdown complete.")


async def main():
    settings, sessionmaker, app, bot = await _startup()

    # Run all exchange pollers (concurrently)
    # We keep one wrapper task so we can cancel on signals.
    pollers_task = asyncio.create_task(run_all(settings, bot, sessionmaker))
    tasks = [pollers_task]

    # Signal handling for Railway / Docker
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _set_stop():
        if not stop_event.is_set():
            logger.info("Termination signal received.")
            stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        with suppress(NotImplementedError):
            loop.add_signal_handler(sig, _set_stop)

    # Wait until a signal arrives or a task fails
    waiter = asyncio.create_task(stop_event.wait())
    done, pending = await asyncio.wait(
        {waiter, pollers_task}, return_when=asyncio.FIRST_COMPLETED
    )

    # If pollers_task crashed, surface the exception
    if pollers_task in done and pollers_task.exception():
        logger.exception("Pollers crashed:", exc_info=pollers_task.exception())

    # Proceed to shutdown
    await _shutdown(app, tasks)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(130)
