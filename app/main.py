import asyncio
from telegram.ext import Application
from telegram import Bot
from app.config import load_settings
from app.store import init_db
from app.bot_handlers import register_admin
from app.poller import run_all

async def main():
    settings = load_settings()
    sessionmaker = await init_db(settings.database_url)

    app = Application.builder().token(settings.bot_token).build()
    await register_admin(app)

    # For convenience, store default chat on Bot instance
    bot = Bot(token=settings.bot_token)
    bot._default_chat_id = settings.target_chat_id  # type: ignore

    async with app:
        # run pollers concurrently with bot's long-polling
        await asyncio.gather(
            app.initialize(),
            run_all(settings, bot, sessionmaker),
            app.start(),
            app.updater.start_polling(),
        )

if __name__ == "__main__":
    asyncio.run(main())