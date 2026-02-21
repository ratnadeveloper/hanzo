import asyncio
import importlib
import os
import threading
import time
import requests as http_requests
import config
from pyrogram import idle
from pytgcalls.exceptions import NoActiveGroupCall
from hanzo import LOGGER, app, userbot
from hanzo.crushex.crushehitman import Hanzo
from hanzo.misc import sudo
from hanzo.slogix import ALL_MODULES
from hanzo.crushor.database import get_banned_users, get_gbanned
from config import BANNED_USERS


# ─── Self-Ping (keeps Render awake) ──────────────────────────────────
def keep_alive():
    """Pings own URL every 5 min so Render doesn't spin down."""
    time.sleep(10)
    url = os.getenv("RENDER_EXTERNAL_URL")
    if not url:
        return  # Not on Render, skip
    url = url.rstrip("/") + "/health"
    LOGGER("hanzo.keepalive").info(f"Keep-alive → {url}")
    while True:
        try:
            http_requests.get(url, timeout=10)
        except:
            pass
        time.sleep(30)  # every 30 seconds — Render never spins down


# ─── Bot Initialization ──────────────────────────────────────────────
async def init_bot():
    if not config.STRING1:
        LOGGER(__name__).error(
            "String Session not filled, please provide a valid session."
        )
        exit()

    await sudo()
    try:
        users = await get_gbanned()
        for user_id in users:
            BANNED_USERS.add(user_id)
        users = await get_banned_users()
        for user_id in users:
            BANNED_USERS.add(user_id)
    except Exception as e:
        LOGGER("hanzo").warning(f"Error loading banned users: {e}")

    await app.start()
    for all_module in ALL_MODULES:
        importlib.import_module("hanzo.slogix" + all_module)
    LOGGER("hanzo.slogix").info("All Features Loaded!")

    await userbot.start()
    await Hanzo.start()
    try:
        await Hanzo.stream_call(
            "https://te.legra.ph/file/29f784eb49d230ab62e9e.mp4"
        )
    except NoActiveGroupCall:
        LOGGER("hanzo").warning(
            "No active voice chat in log group/channel. "
            "Start one to enable music playback."
        )
    except Exception as e:
        LOGGER("hanzo").warning(f"Error starting stream: {e}")

    await Hanzo.decorators()
    LOGGER("hanzo").info("hanzo Bot is running! 🚀")
    await idle()
    await app.stop()
    await userbot.stop()
    LOGGER("hanzo").info("hanzo Bot stopped.")


if __name__ == "__main__":
    # Render needs an HTTP port — Flask handles it via Dockerfile CMD
    # Self-ping keeps Render awake (only runs on Render)
    keepalive_thread = threading.Thread(target=keep_alive, daemon=True)
    keepalive_thread.start()

    asyncio.get_event_loop().run_until_complete(init_bot())
