import re
import os
from os import getenv
from dotenv import load_dotenv
from pyrogram import filters
# ------------------------------------
load_dotenv()
# ─── Required (set in Render/Koyeb Environment Variables) ─────────────
API_ID = int(getenv("API_ID", "25276967"))
API_HASH = getenv("API_HASH", "daf793293a5a244e5c426a129656e0a1")
BOT_TOKEN = getenv("BOT_TOKEN", "")
OWNER_USERNAME = getenv("OWNER_USERNAME", "hanzoo")
BOT_USERNAME = getenv("BOT_USERNAME", "hanzo")
BOT_NAME = getenv("BOT_NAME", "🏯 Hanzo Music")
ASSUSERNAME = getenv("ASSUSERNAME", "🏯 Hanzo Music")
# ---------------------------------------------------------
BASE_URL = getenv("BASE_URL", "")
API_KEY = getenv("API_KEY", "")
# ---------------------------------------------------------
MONGO_DB_URI = getenv("MONGO_DB_URI", "")
DURATION_LIMIT_MIN = int(getenv("DURATION_LIMIT", "17000"))
LOGGER_ID = int(getenv("LOGGER_ID", "-1002493565037"))
OWNER_ID = int(getenv("OWNER_ID", "7446465090"))
CHANNEL_ID = int(getenv("CHANNEL_ID", "-1002329264016"))
HEROKU_APP_NAME = getenv("HEROKU_APP_NAME")
HEROKU_API_KEY = getenv("HEROKU_API_KEY")
UPSTREAM_REPO = getenv(
    "UPSTREAM_REPO",
    "https://t.me/She_who_remain",
)
UPSTREAM_BRANCH = getenv("UPSTREAM_BRANCH", "main")
GIT_TOKEN = getenv("GIT_TOKEN", None)
# ─── Public Links (safe to keep here) ────────────────────────────────
SUPPORT_CHANNEL = getenv("SUPPORT_CHANNEL", "https://t.me/+po0SXQOB7btiNGI1")
SUPPORT_CHAT = getenv("SUPPORT_CHAT", "https://t.me/+po0SXQOB7btiNGI1")
SOURCE = getenv("SOURCE", "https://t.me/She_who_remain")
CHAT = getenv("CHAT", "https://t.me/+po0SXQOB7btiNGI1")
# ─── Bot Settings ────────────────────────────────────────────────────
AUTO_LEAVING_ASSISTANT = getenv("AUTO_LEAVING_ASSISTANT", "False")
AUTO_LEAVE_ASSISTANT_TIME = int(getenv("ASSISTANT_LEAVE_TIME", "9000"))
SONG_DOWNLOAD_DURATION = int(getenv("SONG_DOWNLOAD_DURATION", "9999999"))
SONG_DOWNLOAD_DURATION_LIMIT = int(getenv("SONG_DOWNLOAD_DURATION_LIMIT", "9999999"))
# ─── Spotify (set in Render Environment Variables) ───────────────────
SPOTIFY_CLIENT_ID = getenv("SPOTIFY_CLIENT_ID", "1c21247d714244ddbb09925dac565aed")
SPOTIFY_CLIENT_SECRET = getenv("SPOTIFY_CLIENT_SECRET", "709e1a2969664491b58200860623ef19")
PLAYLIST_FETCH_LIMIT = int(getenv("PLAYLIST_FETCH_LIMIT", "25"))
SPOTIFY_SP_DC = getenv("SPOTIFY_SP_DC", "")
# ─── Telegram Limits ────────────────────────────────────────────────
TG_AUDIO_FILESIZE_LIMIT = int(getenv("TG_AUDIO_FILESIZE_LIMIT", "5242880000"))
TG_VIDEO_FILESIZE_LIMIT = int(getenv("TG_VIDEO_FILESIZE_LIMIT", "5242880000"))
# ─── Session ─────────────────────────────────────────────────────────
STRING1 = getenv("STRING_SESSION", "")
BANNED_USERS = filters.user()
adminlist = {}
lyrical = {}
votemode = {}
autoclean = []
confirmer = {}
# ─── Images ──────────────────────────────────────────────────────────
START_IMG_URL = getenv(
    "START_IMG_URL", "https://iili.io/q2Nxi9n.jpg"
)
PING_IMG_URL = getenv(
    "PING_IMG_URL", "https://telegra.ph/file/fd827f9a4fe8eaa3e8bf4.jpg"
)
PLAYLIST_IMG_URL = "https://telegra.ph/file/d723f4c80da157fca1678.jpg"
STATS_IMG_URL = "https://telegra.ph/file/d30d11c4365c025c25e3e.jpg"
TELEGRAM_AUDIO_URL = "https://telegra.ph/file/c832e84cd991c865c7e4f.jpg"
TELEGRAM_VIDEO_URL = "https://telegra.ph/file/e575ae40d6635250974e1.jpg"
STREAM_IMG_URL = "https://telegra.ph/file/03efec694e41e891b29dc.jpg"
IQ_Proxy = "https://i.ytimg.com/vi"
SOUNCLOUD_IMG_URL = "https://telegra.ph/file/d723f4c80da157fca1678.jpg"
YOUTUBE_IMG_URL = "https://telegra.ph/file/4dc854f961cd3ce46899b.jpg"
SPOTIFY_ARTIST_IMG_URL = "https://telegra.ph/file/d723f4c80da157fca1678.jpg"
SPOTIFY_ALBUM_IMG_URL = "https://telegra.ph/file/6c741a6bc1e1663ac96fc.jpg"
SPOTIFY_PLAYLIST_IMG_URL = "https://telegra.ph/file/6c741a6bc1e1663ac96fc.jpg"
# ─── Helpers ─────────────────────────────────────────────────────────
def time_to_seconds(time):
    stringt = str(time)
    return sum(int(x) * 60**i for i, x in enumerate(reversed(stringt.split(":"))))

DURATION_LIMIT = int(time_to_seconds(f"{DURATION_LIMIT_MIN}:00"))
# ─── Validation ──────────────────────────────────────────────────────
if SUPPORT_CHANNEL:
    if not re.match("(?:http|https)://", SUPPORT_CHANNEL):
        raise SystemExit(
            "[ERROR] - Your SUPPORT_CHANNEL url is wrong. Please ensure that it starts with https://"
        )

if SUPPORT_CHAT:
    if not re.match("(?:http|https)://", SUPPORT_CHAT):
        raise SystemExit(
            "[ERROR] - Your SUPPORT_CHAT url is wrong. Please ensure that it starts with https://"
        )

__all__ = [
    "IQ_Proxy",
]
