from hanzo.crushex.slogo import Hanzo
from hanzo.crushex.hanzofy import dirr
from hanzo.crushex.hanzoX import git
from hanzo.crushex.slogor import Userbot
from hanzo.misc import dbb, heroku
from pyrogram import Client
from SafoneAPI import SafoneAPI
from .logging import LOGGER

dirr()
git()
dbb()
heroku()

app = Hanzo()
api = SafoneAPI()
userbot = Userbot()

from .hanzoCore import *

Apple = AppleAPI()
Carbon = CarbonAPI()
SoundCloud = SoundAPI()
Spotify = SpotifyAPI()
Resso = RessoAPI()
Telegram = TeleAPI()
YouTube = YouTubeAPI()
