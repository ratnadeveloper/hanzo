from os import path
import os
import yt_dlp
from yt_dlp.utils import DownloadError

# Cookie support — bypass YouTube anti-bot on servers
_cookies_file = None
for _p in ["hanzo/assets/cookies.txt", "hanzo/hanzofy/cookies.txt"]:
    if os.path.exists(_p):
        _cookies_file = _p
        break

ytdl_opts = {
    "outtmpl": "downloads/%(id)s.%(ext)s",
    "format": "bestaudio[ext=m4a]",
    "geo_bypass": True,
    "nocheckcertificate": True,
}
if _cookies_file:
    ytdl_opts["cookiefile"] = _cookies_file

ytdl = yt_dlp.YoutubeDL(ytdl_opts)


def download(url: str, my_hook) -> str:       
    ydl_optssx = {
        'format' : 'bestaudio[ext=m4a]',
        "outtmpl": "downloads/%(id)s.%(ext)s",
        "geo_bypass": True,
        "nocheckcertificate": True,
        'quiet': True,
        'no_warnings': True,
    }
    if _cookies_file:
        ydl_optssx["cookiefile"] = _cookies_file
    info = ytdl.extract_info(url, False)
    try:
        x = yt_dlp.YoutubeDL(ydl_optssx)
        x.add_progress_hook(my_hook)
        dloader = x.download([url])
    except Exception as y_e:
        return print(y_e)
    else:
        dloader
    xyz = path.join("downloads", f"{info['id']}.{info['ext']}")
    return xyz

