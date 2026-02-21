from pyrogram import Client, errors
import asyncio

import config

from ..logging import LOGGER

assistants = []
assistantids = []


class Userbot(Client):
    def __init__(self):
        self.one = Client(
            name="HanzoAss1",
            api_id=config.API_ID,
            api_hash=config.API_HASH,
            session_string=str(config.STRING1),
            no_updates=True,
        )

    async def start(self):
        LOGGER(__name__).info(f"Starting Assistants...")
        if config.STRING1:
            # Handle FloodWait and AuthKeyDuplicated
            try:
                while True:
                    try:
                        await self.one.start()
                        break
                    except errors.FloodWait as fw:
                        LOGGER(__name__).warning(
                            f"FloodWait for assistant: wait {fw.value}s. Waiting..."
                        )
                        await asyncio.sleep(fw.value + 1)
            except errors.AuthKeyDuplicated:
                LOGGER(__name__).warning(
                    "AUTH_KEY_DUPLICATED: Another bot instance is using the same "
                    "session string. Assistant disabled — bot continues without it. "
                    "Voice chat features won't work until this is resolved."
                )
                return
            except Exception as e:
                LOGGER(__name__).warning(
                    f"Assistant failed to start: {e}. Continuing without assistant."
                )
                return

            try:
                await self.one.join_chat("https://t.me/welcometomyheart0")
                await self.one.join_chat("welcometomyheart0")
            except:
                pass
            assistants.append(1)
            try:
                await self.one.get_chat(config.LOGGER_ID)
                await self.one.send_message(config.LOGGER_ID, "Assistant Started")
            except:
                LOGGER(__name__).warning(
                    "Assistant cannot access log group. Continuing..."
                )
            self.one.id = self.one.me.id
            self.one.name = self.one.me.mention
            self.one.username = self.one.me.username
            assistantids.append(self.one.id)
            LOGGER(__name__).info(f"Assistant Started as {self.one.name}")


    async def stop(self):
        LOGGER(__name__).info(f"Stopping Assistants...")
        try:
            if config.STRING1:
                await self.one.stop()
        except:
            pass
