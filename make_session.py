from telethon.sync import TelegramClient
from telethon.sessions import StringSession
import os

API_ID = int(os.getenv("API_ID") or input("API_ID: "))
API_HASH = os.getenv("API_HASH") or input("API_HASH: ")

with TelegramClient(StringSession(), API_ID, API_HASH) as client:
    print("✅ SESSION_STRING:")
    print(client.session.save())
