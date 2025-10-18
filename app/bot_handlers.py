from telegram.ext import Application, CommandHandler
from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import ContextTypes
from app.utils.time import now_utc

async def cmd_ping(update: Update, _: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("pong")

async def cmd_status(update: Update, _: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(f"OK {now_utc().isoformat()}")

async def register_admin(app: Application):
    app.add_handler(CommandHandler("ping", cmd_ping))
    app.add_handler(CommandHandler("status", cmd_status))