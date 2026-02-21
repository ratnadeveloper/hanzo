from pyrogram import Client, errors
from pyrogram.enums import ChatMemberStatus, ParseMode
import asyncio

import config

from ..logging import LOGGER


class Hanzo(Client):
    def __init__(self):
        LOGGER(__name__).info(f"Starting Bot...")
        super().__init__(
            name="hanzo",
            api_id=config.API_ID,
            api_hash=config.API_HASH,
            bot_token=config.BOT_TOKEN,
            in_memory=True,
            max_concurrent_transmissions=7,
        )

    async def start(self):
        LOGGER(__name__).info("Connecting to Telegram...")

        # Handle FloodWait (Telegram rate limit from too many restarts)
        while True:
            try:
                await super().start()
                break
            except errors.FloodWait as fw:
                LOGGER(__name__).warning(
                    f"FloodWait: Telegram says wait {fw.value}s "
                    f"({fw.value // 60}m {fw.value % 60}s). Waiting..."
                )
                await asyncio.sleep(fw.value + 1)
                LOGGER(__name__).info("FloodWait done, retrying connection...")

        LOGGER(__name__).info("Connected to Telegram!")

        self.id = self.me.id
        self.name = self.me.first_name + " " + (self.me.last_name or "")
        self.username = self.me.username
        self.mention = self.me.mention

        try:
            await self.get_chat(config.LOGGER_ID)
            await self.send_message(
                chat_id=config.LOGGER_ID,
                text=f"<u><b>» {self.mention} ʙᴏᴛ sᴛᴀʀᴛᴇᴅ :</b><u>\n\nɪᴅ : <code>{self.id}</code>\nɴᴀᴍᴇ : {self.name}\nᴜsᴇʀɴᴀᴍᴇ : @{self.username}",
            )
        except Exception:
            LOGGER(__name__).warning("Log group not accessible. Continuing...")

        try:
            a = await self.get_chat_member(config.LOGGER_ID, self.id)
            if a.status != ChatMemberStatus.ADMINISTRATOR:
                LOGGER(__name__).warning("Bot is not admin in log group.")
        except Exception:
            pass

        LOGGER(__name__).info(f"Music Bot Started as {self.name}")

    async def stop(self):
        await super().stop()
