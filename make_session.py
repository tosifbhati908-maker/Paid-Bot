import os
from telethon.sync import TelegramClient
from telethon.sessions import StringSession

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]

with TelegramClient("myboard", API_ID, API_HASH) as client:
    print("\nSESSION_STRING:\n")
    print(StringSession.save(client.session))
    print("\nCopy the complete line into your hosting ENV variable named SESSION_STRING.")
