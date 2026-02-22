"""
/spdownload — Spotify downloader (v6) for ALL users.

Commands:
  /spdownload <url>      — Download & upload Spotify tracks
  /setchannel <id>       — Set your preferred upload channel
  /removechannel         — Remove channel preference (uploads go to current chat)

Track fetching strategy (in order):
  1. sp_dc COOKIE → get_access_token → spotipy v1 API (full pagination)
  2. Embed anonymous token → spotipy v1 API (public playlists)
  3. Embed page scraping (fallback, capped at ~100 tracks)

Features:
  • Per-user channel preferences (MongoDB)
  • Concurrent download pipeline (ThreadPoolExecutor)
  • FloodWait protection (3s delay + exponential backoff)
  • MP3 metadata + cover art (mutagen)
  • Auto-cleanup after upload
  • Progress tracking with throttled status updates
"""
import os
import re
import json
import asyncio
import unicodedata
from difflib import SequenceMatcher
import time
import logging

import aiohttp
import spotipy
# NOTE: youtubesearchpython and yt_dlp are no longer used for /spdownload.
# JioSaavn API provides direct CDN download URLs (no IP blocking).
# These imports are kept only if other modules need them.
try:
    from youtubesearchpython.__future__ import VideosSearch
except ImportError:
    VideosSearch = None
try:
    import yt_dlp
except ImportError:
    yt_dlp = None
from mutagen.mp3 import MP3
from mutagen.id3 import ID3, TIT2, TPE1, APIC
from mutagen.mp4 import MP4, MP4Cover
from concurrent.futures import ThreadPoolExecutor

from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardButton, InlineKeyboardMarkup
from pyrogram.errors import FloodWait

import config
from hanzo import app
from hanzo.crushex.crushbit import mongodb

logger = logging.getLogger(__name__)

# ─── MongoDB: per-user channel preferences ────────────────────────────

crushbit_channels = mongodb.user_channels  # collection: user_channels


async def crushbit_get_channel(user_id: int):
    """Get user's preferred upload channel ID, or None."""
    doc = await crushbit_channels.find_one({"user_id": user_id})
    return doc.get("channel_id") if doc else None


async def crushbit_set_channel(user_id: int, channel_id: int):
    """Set user's preferred upload channel."""
    await crushbit_channels.update_one(
        {"user_id": user_id},
        {"$set": {"channel_id": channel_id}},
        upsert=True,
    )


async def crushbit_remove_channel(user_id: int):
    """Remove user's channel preference."""
    await crushbit_channels.delete_one({"user_id": user_id})

# ─── Concurrency ──────────────────────────────────────────────────────

thread_pool = ThreadPoolExecutor(max_workers=3)
crushex_active = {}  # user_id → {"cancel": False, "chat_id": int}

# ─── MongoDB: failed tracks + download history ───────────────────────

slogix_fails = mongodb.failed_tracks  # collection: failed_tracks


async def slognet_store_fails(user_id: int, tracks: list, url: str):
    """Store failed tracks for later retry."""
    if not tracks:
        return
    await slogix_fails.update_one(
        {"user_id": user_id},
        {"$set": {"tracks": tracks, "url": url, "timestamp": time.time()}},
        upsert=True,
    )


async def slognet_get_fails(user_id: int):
    """Get user's failed tracks."""
    doc = await slogix_fails.find_one({"user_id": user_id})
    return doc if doc else None


async def slognet_clear_fails(user_id: int):
    """Clear user's failed tracks."""
    await slogix_fails.delete_one({"user_id": user_id})


# ─── Progress bar helper ──────────────────────────────────────────

def slogor_bar(current: int, total: int, length: int = 12) -> str:
    """Generate a visual progress bar. Example: █████░░░░░░░ 42%"""
    if total == 0:
        return "░" * length + " 0%"
    pct = current / total
    filled = int(length * pct)
    bar = "█" * filled + "░" * (length - filled)
    return f"{bar} {int(pct * 100)}%"


def slogor_eta(elapsed: float, done: int, total: int) -> str:
    """Calculate and format ETA based on average time per track."""
    if done == 0:
        return "calculating..."
    avg_per_track = elapsed / done
    remaining = (total - done) * avg_per_track
    if remaining < 60:
        return f"~{int(remaining)}s"
    elif remaining < 3600:
        return f"~{int(remaining // 60)}m {int(remaining % 60)}s"
    else:
        return f"~{int(remaining // 3600)}h {int((remaining % 3600) // 60)}m"

EMBED_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
}

# Cached token from sp_dc cookie
_sp_token_cache = {"token": None, "expires": 0}


# ─── Get FULL access token via sp_dc cookie ──────────────────────────

async def crushbit_dc_token():
    """
    Get a full Spotify access token using the sp_dc cookie.
    This token can access ANY playlist (including private ones the user follows)
    and supports full pagination via the v1 API.
    """
    sp_dc = config.SPOTIFY_SP_DC
    if not sp_dc:
        return None

    # Return cached token if still valid
    now = time.time()
    if _sp_token_cache["token"] and _sp_token_cache["expires"] > now + 60:
        return _sp_token_cache["token"]

    try:
        cookies = {"sp_dc": sp_dc}
        url = "https://open.spotify.com/get_access_token?reason=transport&productType=web_player"
        async with aiohttp.ClientSession(cookies=cookies) as session:
            async with session.get(url, headers=EMBED_HEADERS) as resp:
                if resp.status != 200:
                    logger.warning(f"sp_dc token request failed: {resp.status}")
                    return None
                data = await resp.json()
                token = data.get("accessToken")
                expires_ms = data.get("accessTokenExpirationTimestampMs", 0)
                if token:
                    _sp_token_cache["token"] = token
                    _sp_token_cache["expires"] = expires_ms / 1000.0
                    is_anon = data.get("isAnonymous", True)
                    logger.info(f"Got sp_dc token (anonymous={is_anon})")
                    return token
    except Exception as e:
        logger.error(f"sp_dc token error: {e}")
    return None


# ─── Get anonymous token from embed page (fallback) ──────────────────

async def crushbit_token():
    """Get an anonymous access token from Spotify embed page."""
    test_ids = [
        "37i9dQZF1DXcBWIGoYBM5M",
        "37i9dQZF1DX0XUsuxWHRQd",
        "37i9dQZF1DWWMOmoXKqHTD",
    ]
    for pid in test_ids:
        try:
            async with aiohttp.ClientSession() as session:
                embed_url = f"https://open.spotify.com/embed/playlist/{pid}"
                async with session.get(embed_url, headers=EMBED_HEADERS) as resp:
                    if resp.status != 200:
                        continue
                    text = await resp.text()
                for script in re.findall(r"<script[^>]*>(.*?)</script>", text, re.DOTALL):
                    if '"props"' in script and len(script) > 500:
                        try:
                            data = json.loads(script.strip())
                            token = data["props"]["pageProps"]["state"]["settings"]["session"]["accessToken"]
                            if token:
                                return token
                        except (json.JSONDecodeError, KeyError):
                            continue
        except Exception:
            continue
    return None


# ─── PRIMARY: Fetch tracks via sp_dc token + spotipy ─────────────────

async def slogo_fetch_dc(sp_type, sp_id):
    """
    Use sp_dc cookie token with spotipy for FULL playlist pagination.
    Works for ANY playlist the authenticated user can see.
    """
    token = await crushbit_dc_token()
    if not token:
        return None

    try:
        sp = spotipy.Spotify(auth=token, requests_timeout=15, retries=5, backoff_factor=0.5)

        if sp_type == "track":
            track = sp.track(sp_id)
            if not track:
                return None
            cover = None
            if track.get("album", {}).get("images"):
                cover = track["album"]["images"][0]["url"]
            return [{
                "title": track["name"],
                "artist": ", ".join(a["name"] for a in track.get("artists", [])),
                "duration": track.get("duration_ms", 0),
                "thumbnail": cover,
            }]

        elif sp_type == "album":
            album = sp.album(sp_id)
            if not album:
                return None
            cover = album["images"][0]["url"] if album.get("images") else None
            tracks = []
            results = album["tracks"]
            while True:
                for item in results["items"]:
                    tracks.append({
                        "title": item["name"],
                        "artist": ", ".join(a["name"] for a in item.get("artists", [])),
                        "duration": item.get("duration_ms", 0),
                        "thumbnail": cover,
                    })
                if results.get("next"):
                    results = sp.next(results)
                else:
                    break
            return tracks

        elif sp_type == "playlist":
            playlist = sp.playlist(sp_id, fields="name,images,tracks.total")
            if not playlist:
                return None
            cover = playlist["images"][0]["url"] if playlist.get("images") else None
            total = playlist["tracks"]["total"]
            logger.info(f"Playlist total tracks (API): {total}")

            tracks = []
            offset = 0
            while offset < total:
                results = sp.playlist_tracks(
                    sp_id, limit=100, offset=offset,
                    fields="items(track(name,artists(name),duration_ms,album(images)))"
                )
                for item in results.get("items", []):
                    t = item.get("track")
                    if t:
                        track_cover = cover
                        if t.get("album", {}).get("images"):
                            track_cover = t["album"]["images"][0]["url"]
                        tracks.append({
                            "title": t["name"],
                            "artist": ", ".join(a["name"] for a in t.get("artists", [])),
                            "duration": t.get("duration_ms", 0),
                            "thumbnail": track_cover,
                        })
                offset += 100
                logger.info(f"Fetched {min(offset, total)}/{total} tracks")
            return tracks

    except Exception as e:
        logger.warning(f"sp_dc API fetch failed: {e}")
        return None


# ─── SECONDARY: Fetch via embed token + spotipy (public only) ────────

async def slogo_fetch_api(sp_type, sp_id):
    """
    Use anonymous embed token with spotipy for public playlist pagination.
    Works for PUBLIC playlists only but supports full pagination.
    """
    token = await crushbit_token()
    if not token:
        return None

    try:
        sp = spotipy.Spotify(auth=token, requests_timeout=15, retries=5, backoff_factor=0.5)

        if sp_type == "track":
            track = sp.track(sp_id)
            if not track:
                return None
            cover = None
            if track.get("album", {}).get("images"):
                cover = track["album"]["images"][0]["url"]
            return [{
                "title": track["name"],
                "artist": ", ".join(a["name"] for a in track.get("artists", [])),
                "duration": track.get("duration_ms", 0),
                "thumbnail": cover,
            }]

        elif sp_type == "album":
            album = sp.album(sp_id)
            if not album:
                return None
            cover = album["images"][0]["url"] if album.get("images") else None
            tracks = []
            results = album["tracks"]
            while True:
                for item in results["items"]:
                    tracks.append({
                        "title": item["name"],
                        "artist": ", ".join(a["name"] for a in item.get("artists", [])),
                        "duration": item.get("duration_ms", 0),
                        "thumbnail": cover,
                    })
                if results.get("next"):
                    results = sp.next(results)
                else:
                    break
            return tracks

        elif sp_type == "playlist":
            playlist = sp.playlist(sp_id, fields="name,images,tracks.total")
            if not playlist:
                return None
            cover = playlist["images"][0]["url"] if playlist.get("images") else None
            total = playlist["tracks"]["total"]
            logger.info(f"Playlist total tracks (embed API): {total}")

            tracks = []
            offset = 0
            while offset < total:
                results = sp.playlist_tracks(sp_id, limit=100, offset=offset)
                for item in results.get("items", []):
                    t = item.get("track")
                    if t:
                        track_cover = cover
                        if t.get("album", {}).get("images"):
                            track_cover = t["album"]["images"][0]["url"]
                        tracks.append({
                            "title": t["name"],
                            "artist": ", ".join(a["name"] for a in t.get("artists", [])),
                            "duration": t.get("duration_ms", 0),
                            "thumbnail": track_cover,
                        })
                offset += 100
            return tracks

    except Exception as e:
        logger.warning(f"Embed API fetch failed: {e}")
        return None


# ─── FALLBACK: Embed page scraping ───────────────────────────────────

async def slogo_fetch_embed(sp_type, sp_id):
    """
    Scrape track info from Spotify embed page.
    No API keys needed but capped at ~100 tracks for large playlists.
    """
    embed_url = f"https://open.spotify.com/embed/{sp_type}/{sp_id}"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(embed_url, headers=EMBED_HEADERS) as resp:
                if resp.status != 200:
                    return None
                text = await resp.text()

        for script in re.findall(r"<script[^>]*>(.*?)</script>", text, re.DOTALL):
            if '"props"' in script and len(script) > 500:
                try:
                    data = json.loads(script.strip())
                    state = data.get("props", {}).get("pageProps", {}).get("state")
                    if not state:
                        continue
                    entity = state["data"]["entity"]

                    cover_list = entity.get("coverArt", {}).get("sources", [])
                    default_cover = cover_list[0].get("url") if cover_list else None

                    tracks = []
                    if sp_type == "track":
                        tracks.append({
                            "title": entity.get("title", "Unknown"),
                            "artist": entity.get("subtitle", ""),
                            "duration": entity.get("duration", 0),
                            "thumbnail": default_cover,
                        })
                    else:
                        for t in entity.get("trackList", []):
                            tracks.append({
                                "title": t.get("title", "Unknown"),
                                "artist": t.get("subtitle", ""),
                                "duration": t.get("duration", 0),
                                "thumbnail": default_cover,
                            })
                    return tracks if tracks else None
                except (json.JSONDecodeError, KeyError) as e:
                    logger.error(f"Embed parse error: {e}")
                    continue
    except Exception as e:
        logger.error(f"Embed fetch error: {e}")
    return None


# ─── Main track fetcher: tries all methods ────────────────────────────

async def crushex_fetch(url: str):
    """
    Get ALL tracks from a Spotify URL.
    1. sp_dc cookie token + spotipy (full pagination, any playlist)
    2. Embed anonymous token + spotipy (full pagination, public only)
    3. Embed page scraping (capped at ~100 tracks)
    """
    m = re.match(
        r"https?://open\.spotify\.com/(track|album|playlist)/([a-zA-Z0-9]+)", url
    )
    if not m:
        return None, None
    sp_type, sp_id = m.group(1), m.group(2)

    # Method 1: sp_dc cookie (best — works for all playlists)
    tracks = await slogo_fetch_dc(sp_type, sp_id)
    if tracks:
        return tracks, "sp_dc"

    # Method 2: Embed token + API (public playlists only)
    tracks = await slogo_fetch_api(sp_type, sp_id)
    if tracks:
        return tracks, "api"

    # Method 3: Embed page scraping (last resort, capped at ~100)
    tracks = await slogo_fetch_embed(sp_type, sp_id)
    if tracks:
        return tracks, "embed"

    return None, None


# ─── JioSaavn (primary source — replaces YouTube + yt-dlp) ────────────
# Uses jiosaavnpy library for direct JioSaavn access (no external API needed).
# Falls back to YouTube/yt-dlp if JioSaavn fails.

try:
    from jiosaavnpy import JioSaavn
    _saavn_client = JioSaavn()
except ImportError:
    _saavn_client = None
    logger.warning("jiosaavnpy not installed — JioSaavn source unavailable")


def _clean_title(title: str) -> str:
    """Strip feat/remix/remaster tags and special chars for cleaner search."""
    # Remove (feat. ...), [feat. ...], (ft. ...), (with ...), (Remix), [TECHNO], etc.
    cleaned = re.sub(r'\s*[\(\[](?:feat\.?|ft\.?|with|prod\.?|remix|remaster(?:ed)?|deluxe|bonus|techno|house|edm|trance|acoustic|live|version|edit|original|mix|radio)[^\)\]]*[\)\]]', '', title, flags=re.IGNORECASE)
    # Remove trailing " - ..." like " - Radio Edit", " - Acoustic"
    cleaned = re.sub(r'\s*-\s*(?:radio edit|acoustic|live|remix|remaster(?:ed)?).*$', '', cleaned, flags=re.IGNORECASE)
    # Remove special characters that confuse search
    cleaned = re.sub(r'[\'\"‘’“”\u200b]', '', cleaned)
    return cleaned.strip()


def _strip_accents(text: str) -> str:
    """Convert accented chars to ASCII: é→e, ü→u, ñ→n, etc."""
    nfkd = unicodedata.normalize('NFKD', text)
    return ''.join(c for c in nfkd if not unicodedata.combining(c))


def _normalize(text: str) -> str:
    """Lowercase and strip non-alphanumeric for comparison."""
    return re.sub(r'[^a-z0-9 ]', '', text.lower()).strip()


def _similarity(a: str, b: str) -> float:
    """Return 0.0-1.0 similarity ratio between two strings."""
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a, b).ratio()


def _is_match(wanted_title: str, got_title: str,
              wanted_artist: str = "", got_artist: str = "") -> bool:
    """
    Generic song matching using string similarity + substring containment.
    Works for ANY song — no hardcoded patterns.

    Steps:
      1. Clean both titles (strip feat/remix/genre tags)
      2. Check if wanted title is contained in the YouTube title (handles short titles)
      3. Compute character-level similarity (SequenceMatcher) as fallback
      4. Check artist similarity (first artist name matching)
    """
    ct_wanted = _normalize(_clean_title(wanted_title))
    ct_got = _normalize(_clean_title(got_title))

    # Title matching: substring containment OR similarity ratio
    title_ok = False

    # Check 1: Is the wanted title contained in the YouTube title?
    # This handles cases like "Shayad" being in "Shayad - Chaahat Kasam Nahi Hai"
    # or "Bon Appetit" being in "Katy Perry - Bon Appetit (Audio) ft. Migos"
    if ct_wanted and ct_wanted in ct_got:
        title_ok = True

    # Check 2: SequenceMatcher similarity ratio
    if not title_ok:
        title_sim = _similarity(ct_wanted, ct_got)
        if title_sim >= 0.55:
            title_ok = True

    if not title_ok:
        return False

    # Artist matching: check full artist string AND first artist name
    if wanted_artist and got_artist:
        norm_wanted_artist = _normalize(wanted_artist)
        norm_got_artist = _normalize(got_artist)

        # Extract first artist from comma-separated list
        first_wanted = _normalize(wanted_artist.split(",")[0].strip())
        first_got = _normalize(got_artist.split(",")[0].strip())

        # Check multiple matching strategies:
        # 1. Full artist similarity
        artist_sim = _similarity(norm_wanted_artist, norm_got_artist)
        # 2. First artist similarity
        first_sim = _similarity(first_wanted, first_got)
        # 3. Is wanted artist name contained in got_artist or got_title?
        artist_in_channel = first_wanted in norm_got_artist
        artist_in_title = first_wanted in ct_got

        best_score = max(artist_sim, first_sim)

        # Strong title match (substring containment) = relax artist threshold.
        # This handles movie/film songs where the YouTube channel is a label
        # (e.g. "Aditya Music" instead of "Sid Sriram" for "22 Sumari").
        # If the full Spotify title appears inside the YouTube title, the title
        # signal is very strong, so we only need a minimal artist sanity check.
        strong_title = ct_wanted and ct_wanted in ct_got
        artist_threshold = 0.15 if strong_title else 0.40

        if best_score < artist_threshold and not artist_in_channel and not artist_in_title:
            logger.debug(
                f"Song match rejected — "
                f"artist_sim={artist_sim:.2f} first_sim={first_sim:.2f} "
                f"(threshold={artist_threshold:.2f} strong_title={strong_title}): "
                f"wanted '{wanted_artist} - {wanted_title}', "
                f"got '{got_artist} - {got_title}'"
            )
            return False

    return True


async def jiosaavn_search(title: str, artist: str, spotify_duration_ms: int = 0):
    """
    Search JioSaavn for a song using the jiosaavnpy library.
    Returns dict with name, artist, download_url, quality, image, duration.
    Returns None if not found or library unavailable.

    Validation:
      - String similarity on title (>=0.55) and artist (>=0.40)
      - Duration tolerance: within 30 seconds of Spotify duration
      - Accent-stripped queries for better JioSaavn search results
    """
    if not _saavn_client:
        return None

    loop = asyncio.get_running_loop()

    # Strip accents for search queries (e.g. Bon Appetit instead of Bon Appétit)
    ascii_title = _strip_accents(title)
    ascii_cleaned = _strip_accents(_clean_title(title))
    first_artist = artist.split(",")[0].strip() if "," in artist else artist
    ascii_artist = _strip_accents(artist)
    ascii_first = _strip_accents(first_artist)

    # Build unique query list (preserving order, skipping duplicates)
    seen = set()
    queries = []
    for q in [
        f"{ascii_title} {ascii_artist}",        # ascii title + all artists
        f"{ascii_cleaned} {ascii_first}",        # cleaned + first artist
        f"{ascii_cleaned} {ascii_artist}",       # cleaned + all artists
        f"{ascii_title} {ascii_first}",          # ascii title + first artist
        ascii_cleaned,                           # just cleaned title
        ascii_title,                             # just title (accent-free)
    ]:
        q_lower = q.strip().lower()
        if q_lower and q_lower not in seen:
            seen.add(q_lower)
            queries.append(q.strip())

    for query in queries:
        try:
            # jiosaavnpy is synchronous — run in executor to avoid blocking
            results = await loop.run_in_executor(
                None, _saavn_client.search_songs, query
            )
            if results and isinstance(results, list):
                for song in results:
                    song_title = song.get("title", "")
                    song_artist = song.get("primary_artists", "")
                    # ── Validate match using string similarity ──
                    if not _is_match(title, song_title, artist, song_artist):
                        continue
                    # ── Duration check: reject if >30s difference ──
                    if spotify_duration_ms > 0:
                        saavn_dur = song.get("duration")
                        if saavn_dur:
                            try:
                                diff = abs(int(saavn_dur) - (spotify_duration_ms / 1000))
                                if diff > 30:
                                    logger.debug(f"Duration mismatch ({diff:.0f}s): '{song_title}'")
                                    continue
                            except (ValueError, TypeError):
                                pass
                    stream_urls = song.get("stream_urls", {})
                    # Try highest quality first
                    download_url = (
                        stream_urls.get("very_high_quality")
                        or stream_urls.get("high_quality")
                        or stream_urls.get("medium_quality")
                        or stream_urls.get("low_quality")
                    )
                    if download_url:
                        quality = "320kbps" if stream_urls.get("very_high_quality") else "160kbps"
                        image_urls = song.get("image_urls", {})
                        image_url = (
                            image_urls.get("500x500")
                            or image_urls.get("150x150")
                            or image_urls.get("50x50")
                        ) if isinstance(image_urls, dict) else None
                        logger.info(f"JioSaavn hit via '{query}': {song_title} ({quality})")
                        return {
                            "name": song_title or title,
                            "artist": song.get("primary_artists", artist),
                            "duration": song.get("duration"),
                            "download_url": download_url,
                            "quality": quality,
                            "image": image_url,
                        }
        except Exception as e:
            logger.error(f"JioSaavn search error for '{query}': {e}")

    logger.warning(f"JioSaavn: no results for '{artist} - {title}' (tried {len(queries)} queries)")
    return None


async def saavn_api_search(title: str, artist: str, spotify_duration_ms: int = 0):
    """
    Fallback JioSaavn search using the saavn.dev REST API.
    Often returns better results than the jiosaavnpy library,
    especially for Western/international music.
    """
    SAAVN_API = "https://saavn.dev/api/search/songs"

    ascii_title = _strip_accents(_clean_title(title))
    first_artist = _strip_accents(artist.split(",")[0].strip()) if "," in artist else _strip_accents(artist)

    queries = []
    seen = set()
    for q in [f"{ascii_title} {first_artist}", ascii_title]:
        ql = q.strip().lower()
        if ql and ql not in seen:
            seen.add(ql)
            queries.append(q.strip())

    for query in queries:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    SAAVN_API,
                    params={"query": query, "limit": "10"},
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    if resp.status != 200:
                        continue
                    data = await resp.json()

            results = (data.get("data") or {}).get("results") or []
            for song in results:
                song_title = song.get("name", "")
                # Build artist string from primary artists list
                primary = song.get("artists", {}).get("primary", [])
                song_artist = ", ".join(a.get("name", "") for a in primary) if primary else ""

                if not _is_match(title, song_title, artist, song_artist):
                    continue

                # Duration check
                if spotify_duration_ms > 0:
                    saavn_dur = song.get("duration")
                    if saavn_dur:
                        try:
                            if abs(int(saavn_dur) - (spotify_duration_ms / 1000)) > 30:
                                continue
                        except (ValueError, TypeError):
                            pass

                # Get best quality download URL
                dl_list = song.get("downloadUrl") or []
                download_url = None
                quality = "160kbps"
                for item in reversed(dl_list):  # highest quality last
                    url = item.get("url") or item.get("link")
                    if url:
                        download_url = url
                        quality = item.get("quality", "320kbps")

                if download_url:
                    # Get image
                    img_list = song.get("image") or []
                    image_url = None
                    for item in reversed(img_list):
                        url = item.get("url") or item.get("link")
                        if url:
                            image_url = url
                            break

                    logger.info(f"saavn.dev hit via '{query}': {song_title} ({quality})")
                    return {
                        "name": song_title or title,
                        "artist": song_artist or artist,
                        "duration": song.get("duration"),
                        "download_url": download_url,
                        "quality": quality,
                        "image": image_url,
                    }
        except Exception as e:
            logger.error(f"saavn.dev API error for '{query}': {e}")

    return None


# ── Piped API (open-source YouTube frontend — bypasses IP blocks) ──
PIPED_INSTANCES = [
    "https://pipedapi.kavin.rocks",
    "https://api.piped.projectsegfau.lt",
    "https://pipedapi.moomoo.me",
    "https://piped-api.privacy.com.de",
]


async def _piped_request(path: str, params: dict = None):
    """Try each Piped instance until one responds."""
    for base in PIPED_INSTANCES:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{base}{path}",
                    params=params,
                    timeout=aiohttp.ClientTimeout(total=12),
                ) as resp:
                    if resp.status == 200:
                        return await resp.json()
        except Exception:
            continue
    return None


async def piped_search(title: str, artist: str, spotify_duration_ms: int = 0):
    """
    Search YouTube Music via Piped API and return the best audio stream URL.
    Piped is an open-source YouTube frontend — no yt-dlp, no bot detection.
    Returns dict with name, artist, download_url, duration, or None.
    """
    ascii_title = _strip_accents(_clean_title(title))
    first_artist = _strip_accents(
        artist.split(",")[0].strip() if "," in artist else artist
    )

    queries = [f"{first_artist} {ascii_title}", ascii_title]

    for query in queries:
        data = await _piped_request(
            "/search", {"q": query, "filter": "music_songs"}
        )
        if not data:
            data = await _piped_request(
                "/search", {"q": query, "filter": "videos"}
            )
        if not data or not isinstance(data.get("items"), list):
            continue

        for item in data["items"][:8]:
            vid_title = item.get("title", "")
            vid_uploader = item.get("uploaderName", "")

            if not _is_match(title, vid_title, artist, vid_uploader):
                continue

            # Duration check (Piped returns seconds)
            if spotify_duration_ms > 0:
                vid_dur = item.get("duration", 0)
                if vid_dur and abs(vid_dur - (spotify_duration_ms / 1000)) > 30:
                    continue

            # Extract video ID from URL like /watch?v=xxxxx
            vid_url = item.get("url", "")
            vid_id = ""
            if "v=" in vid_url:
                vid_id = vid_url.split("v=")[-1].split("&")[0]
            elif vid_url.startswith("/watch/"):
                vid_id = vid_url.split("/watch/")[-1]

            if not vid_id:
                continue

            # Get audio stream URL
            streams = await _piped_request(f"/streams/{vid_id}")
            if not streams or not isinstance(streams.get("audioStreams"), list):
                continue

            # Pick best quality audio
            audio_streams = streams["audioStreams"]
            best = None
            best_bitrate = 0
            for s in audio_streams:
                br = s.get("bitrate", 0)
                if br > best_bitrate and s.get("url"):
                    best = s
                    best_bitrate = br

            if best:
                quality = f"{best_bitrate // 1000}kbps" if best_bitrate else "128kbps"
                logger.info(
                    f"Piped hit: '{vid_title}' by {vid_uploader} ({quality})"
                )
                return {
                    "name": vid_title or title,
                    "artist": vid_uploader or artist,
                    "duration": item.get("duration"),
                    "download_url": best["url"],
                    "quality": quality,
                    "image": item.get("thumbnail"),
                    "source": "piped",
                }

    return None


async def piped_download(download_url: str, title: str, artist: str,
                         download_dir: str = "downloads"):
    """Download audio from a Piped/Invidious/InnerTube stream URL."""
    os.makedirs(download_dir, exist_ok=True)
    safe_name = re.sub(r'[<>:"/\\|?*]', "_", f"{artist} - {title}")[:150]
    # Piped streams are usually webm/opus or m4a
    file_path = os.path.join(download_dir, f"{safe_name}.m4a")

    # googlevideo.com (InnerTube) requires the Android UA that requested the stream
    headers = {}
    if "googlevideo.com" in download_url:
        headers["User-Agent"] = (
            "com.google.android.youtube/19.29.37 "
            "(Linux; U; Android 14; en_US) gzip"
        )

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                download_url,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=120),
            ) as resp:
                if resp.status != 200:
                    logger.error(f"Piped download HTTP {resp.status}")
                    return None
                with open(file_path, "wb") as f:
                    async for chunk in resp.content.iter_chunked(8192):
                        f.write(chunk)
        logger.info(f"Piped download OK: {file_path}")
        return file_path
    except Exception as e:
        logger.error(f"Piped download error: {e}")
        return None


# ── Invidious API (another open-source YouTube frontend) ──
INVIDIOUS_INSTANCES = [
    "https://vid.puffyan.us",
    "https://invidious.fdn.fr",
    "https://yewtu.be",
    "https://invidious.nerdvpn.de",
]


async def _invidious_request(path: str, params: dict = None):
    """Try each Invidious instance until one responds."""
    for base in INVIDIOUS_INSTANCES:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{base}{path}",
                    params=params,
                    timeout=aiohttp.ClientTimeout(total=12),
                ) as resp:
                    if resp.status == 200:
                        return await resp.json()
        except Exception:
            continue
    return None


async def invidious_search(title: str, artist: str, spotify_duration_ms: int = 0):
    """
    Search YouTube via Invidious API and return direct audio stream URL.
    Independent from Piped — different codebase, different instances.
    """
    ascii_title = _strip_accents(_clean_title(title))
    first_artist = _strip_accents(
        artist.split(",")[0].strip() if "," in artist else artist
    )

    queries = [f"{first_artist} {ascii_title}", ascii_title]

    for query in queries:
        data = await _invidious_request(
            "/api/v1/search",
            {"q": query, "type": "video", "sort_by": "relevance"},
        )
        if not data or not isinstance(data, list):
            continue

        for item in data[:8]:
            if item.get("type") != "video":
                continue

            vid_title = item.get("title", "")
            vid_author = item.get("author", "")

            if not _is_match(title, vid_title, artist, vid_author):
                continue

            # Duration check (Invidious returns seconds)
            if spotify_duration_ms > 0:
                vid_dur = item.get("lengthSeconds", 0)
                if vid_dur and abs(vid_dur - (spotify_duration_ms / 1000)) > 30:
                    continue

            vid_id = item.get("videoId", "")
            if not vid_id:
                continue

            # Get audio streams from video details
            video_data = await _invidious_request(f"/api/v1/videos/{vid_id}")
            if not video_data:
                continue

            # adaptiveFormats contains separate audio/video streams
            formats = video_data.get("adaptiveFormats", [])
            best = None
            best_bitrate = 0
            for fmt in formats:
                mime = fmt.get("type", "")
                if "audio" not in mime:
                    continue
                br = fmt.get("bitrate", 0)
                url = fmt.get("url", "")
                if br > best_bitrate and url:
                    best = fmt
                    best_bitrate = br

            if best:
                quality = f"{best_bitrate // 1000}kbps" if best_bitrate else "128kbps"
                logger.info(
                    f"Invidious hit: '{vid_title}' by {vid_author} ({quality})"
                )
                # Get thumbnail
                thumbs = video_data.get("videoThumbnails", [])
                thumb_url = thumbs[0].get("url", "") if thumbs else None

                return {
                    "name": vid_title or title,
                    "artist": vid_author or artist,
                    "duration": item.get("lengthSeconds"),
                    "download_url": best["url"],
                    "quality": quality,
                    "image": thumb_url,
                    "source": "invidious",
                }

    return None


# ── YouTube InnerTube API (direct access — how savefrom.net works) ──
# This calls YouTube's OWN internal API, pretending to be an Android app.
# YouTube returns direct googlevideo.com audio URLs — no proxy needed.

INNERTUBE_API_URL = "https://www.youtube.com/youtubei/v1/player"
INNERTUBE_API_KEY = "AIzaSyAO_FJ2SlqU8Q4STEHLGCilw_Y9_11qcW8"  # Public, embedded in YouTube


async def innertube_extract_audio(video_id: str):
    """
    Call YouTube's InnerTube Player API with multiple client contexts.
    Tries ANDROID, IOS, and TV_EMBEDDED to maximize chances on server IPs.
    Returns dict with download_url, quality, or None.
    """
    clients = [
        {
            "name": "ANDROID",
            "payload": {
                "context": {
                    "client": {
                        "clientName": "ANDROID",
                        "clientVersion": "19.29.37",
                        "androidSdkVersion": 34,
                        "hl": "en",
                        "gl": "US",
                        "userAgent": "com.google.android.youtube/19.29.37 (Linux; U; Android 14; en_US) gzip",
                    }
                },
            },
            "headers": {
                "User-Agent": "com.google.android.youtube/19.29.37 (Linux; U; Android 14; en_US) gzip",
                "X-YouTube-Client-Name": "3",
                "X-YouTube-Client-Version": "19.29.37",
            },
        },
        {
            "name": "IOS",
            "payload": {
                "context": {
                    "client": {
                        "clientName": "IOS",
                        "clientVersion": "19.29.1",
                        "deviceModel": "iPhone16,2",
                        "hl": "en",
                        "gl": "US",
                    }
                },
            },
            "headers": {
                "User-Agent": "com.google.ios.youtube/19.29.1 (iPhone16,2; U; CPU iOS 17_5_1 like Mac OS X)",
                "X-YouTube-Client-Name": "5",
                "X-YouTube-Client-Version": "19.29.1",
            },
        },
        {
            "name": "TV_EMBEDDED",
            "payload": {
                "context": {
                    "client": {
                        "clientName": "TVHTML5_SIMPLY_EMBEDDED_PLAYER",
                        "clientVersion": "2.0",
                        "hl": "en",
                        "gl": "US",
                    },
                    "thirdParty": {"embedUrl": "https://www.google.com"},
                },
            },
            "headers": {
                "User-Agent": "Mozilla/5.0 (SMART-TV; LINUX; Tizen 6.5)",
                "X-YouTube-Client-Name": "85",
                "X-YouTube-Client-Version": "2.0",
            },
        },
    ]

    for client in clients:
        try:
            payload = {
                **client["payload"],
                "videoId": video_id,
                "playbackContext": {
                    "contentPlaybackContext": {
                        "html5Preference": "HTML5_PREF_WANTS",
                    }
                },
                "contentCheckOk": True,
                "racyCheckOk": True,
            }
            headers = {
                "Content-Type": "application/json",
                **client["headers"],
            }

            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{INNERTUBE_API_URL}?key={INNERTUBE_API_KEY}",
                    json=payload,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as resp:
                    if resp.status != 200:
                        logger.debug(f"InnerTube {client['name']} returned {resp.status}")
                        continue
                    data = await resp.json()

            status = data.get("playabilityStatus", {}).get("status", "?")
            if status != "OK":
                reason = data.get("playabilityStatus", {}).get("reason", "")
                logger.debug(f"InnerTube {client['name']}: status={status} reason={reason[:60]}")
                continue

            streaming = data.get("streamingData", {})
            formats = streaming.get("adaptiveFormats", [])

            # Find best audio-only stream
            best = None
            best_bitrate = 0
            for fmt in formats:
                mime = fmt.get("mimeType", "")
                if "audio" not in mime:
                    continue
                br = fmt.get("bitrate", 0)
                url = fmt.get("url", "")
                if not url:
                    continue
                if br > best_bitrate:
                    best = fmt
                    best_bitrate = br

            if best and best.get("url"):
                quality = f"{best_bitrate // 1000}kbps" if best_bitrate else "128kbps"
                logger.info(f"InnerTube audio found via {client['name']}: {video_id} ({quality})")
                return {
                    "download_url": best["url"],
                    "quality": quality,
                    "mime": best.get("mimeType", "audio/mp4"),
                    "client": client["name"],
                }
            else:
                audio_count = sum(1 for f in formats if "audio" in f.get("mimeType", ""))
                logger.debug(f"InnerTube {client['name']}: {audio_count} audio fmts, 0 with direct URL")

        except Exception as e:
            logger.error(f"InnerTube {client['name']} error: {e}")

    logger.warning(f"InnerTube: no audio from any client for {video_id}")
    return None


async def _innertube_yt_search(query: str):
    """
    Search YouTube using InnerTube search API directly (no youtube-search-python).
    Returns list of {id, title, channel, duration_text}.
    """
    payload = {
        "query": query,
        "context": {
            "client": {
                "clientName": "WEB",
                "clientVersion": "2.20240101",
                "hl": "en",
                "gl": "US",
            }
        },
    }
    headers = {
        "Content-Type": "application/json",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
        ),
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{INNERTUBE_API_URL.replace('/player', '/search')}?key={INNERTUBE_API_KEY}",
                json=payload,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status != 200:
                    return []
                data = await resp.json()

        results = []
        contents = (
            data.get("contents", {})
            .get("twoColumnSearchResultsRenderer", {})
            .get("primaryContents", {})
            .get("sectionListRenderer", {})
            .get("contents", [])
        )
        for section in contents:
            items = section.get("itemSectionRenderer", {}).get("contents", [])
            for item in items:
                vr = item.get("videoRenderer")
                if not vr:
                    continue
                vid_id = vr.get("videoId", "")
                title = "".join(
                    r.get("text", "") for r in vr.get("title", {}).get("runs", [])
                )
                channel = "".join(
                    r.get("text", "") for r in vr.get("ownerText", {}).get("runs", [])
                )
                dur_text = vr.get("lengthText", {}).get("simpleText", "")
                if vid_id and title:
                    results.append({
                        "id": vid_id,
                        "title": title,
                        "channel": channel,
                        "duration": dur_text,
                    })
        return results
    except Exception as e:
        logger.error(f"InnerTube search API error: {e}")
        return []


async def innertube_search(title: str, artist: str, spotify_duration_ms: int = 0):
    """
    Search YouTube → extract audio via InnerTube API (like savefrom.net does).
    Uses InnerTube search API directly (no youtube-search-python needed).
    Falls back to youtube-search-python if direct search fails.
    """
    ascii_title = _strip_accents(_clean_title(title))
    first_artist = _strip_accents(
        artist.split(",")[0].strip() if "," in artist else artist
    )

    queries = [f"{first_artist} {ascii_title} audio", f"{first_artist} {ascii_title}"]

    for query in queries:
        try:
            # Method 1: Direct InnerTube search API (no library needed)
            yt_results = await _innertube_yt_search(query)

            # Method 2: Fallback to youtube-search-python if direct fails
            if not yt_results and VideosSearch:
                try:
                    search = VideosSearch(query, limit=5)
                    result = await search.next()
                    if result and result.get("result"):
                        for vid in result["result"]:
                            yt_results.append({
                                "id": vid.get("link", "").split("v=")[-1].split("&")[0] if "v=" in vid.get("link", "") else "",
                                "title": vid.get("title", ""),
                                "channel": vid.get("channel", {}).get("name", ""),
                                "duration": vid.get("duration", ""),
                            })
                except Exception:
                    pass  # youtube-search-python may be broken on some Python versions

            if not yt_results:
                continue

            for vid in yt_results[:8]:
                vid_title = vid["title"]
                vid_channel = vid["channel"]

                if not _is_match(title, vid_title, artist, vid_channel):
                    continue

                # Duration check
                if spotify_duration_ms > 0 and vid.get("duration"):
                    dur_text = vid["duration"]
                    if ":" in dur_text:
                        parts = dur_text.split(":")
                        try:
                            vid_secs = int(parts[-2]) * 60 + int(parts[-1])
                            if len(parts) > 2:
                                vid_secs += int(parts[-3]) * 3600
                            if abs(vid_secs - (spotify_duration_ms / 1000)) > 30:
                                continue
                        except (ValueError, IndexError):
                            pass

                vid_id = vid["id"]
                if not vid_id:
                    continue

                # Try direct InnerTube audio extraction first
                yt_url = f"https://www.youtube.com/watch?v={vid_id}"
                audio = await innertube_extract_audio(vid_id)
                if audio and audio.get("download_url"):
                    logger.info(f"InnerTube hit: '{vid_title}' ({audio['quality']})")
                    return {
                        "name": vid_title or title,
                        "artist": vid_channel or artist,
                        "duration": None,
                        "download_url": audio["download_url"],
                        "quality": audio["quality"],
                        "video_id": vid_id,
                        "yt_url": yt_url,
                        "image": None,
                        "source": "innertube",
                    }
                else:
                    # No direct audio URL, return YT URL for yt-dlp fallback
                    logger.info(f"InnerTube matched (no direct audio): '{vid_title}' → {yt_url}")
                    return {
                        "name": vid_title or title,
                        "artist": vid_channel or artist,
                        "duration": None,
                        "download_url": yt_url,
                        "quality": "YouTube",
                        "video_id": vid_id,
                        "yt_url": yt_url,
                        "image": None,
                        "source": "innertube",
                    }
        except Exception as e:
            logger.error(f"InnerTube search error for '{query}': {e}")

    return None


async def jiosaavn_download(download_url: str, title: str, artist: str, download_dir: str = "downloads"):
    """
    Download audio file directly from JioSaavn CDN.
    Returns file path on success, None on failure.
    """
    os.makedirs(download_dir, exist_ok=True)
    safe_name = re.sub(r'[<>:"/\\|?*]', "_", f"{artist} - {title}")[:150]
    file_path = os.path.join(download_dir, f"{safe_name}.m4a")

    # Download caching: skip if already downloaded
    if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
        logger.info(f"Cache hit: {safe_name}.m4a")
        return file_path

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                download_url,
                timeout=aiohttp.ClientTimeout(total=60),
            ) as resp:
                if resp.status == 200:
                    with open(file_path, "wb") as f:
                        async for chunk in resp.content.iter_chunked(8192):
                            f.write(chunk)
                    if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
                        return file_path
                    else:
                        logger.error(f"Downloaded file is empty: {safe_name}")
                        return None
                else:
                    logger.error(f"JioSaavn CDN returned status {resp.status}")
    except asyncio.TimeoutError:
        logger.warning(f"JioSaavn download timeout for: {safe_name}")
    except Exception as e:
        logger.error(f"JioSaavn download error: {e}")
    return None


# ─── YouTube/yt-dlp fallback (used only if JioSaavn fails) ───────────

import os as _os
_cookies_file = None
for _p in ["hanzo/assets/cookies.txt", "hanzo/hanzofy/cookies.txt"]:
    if _os.path.exists(_p):
        _cookies_file = _p
        break


async def _yt_search_single(query: str):
    """Single YouTube search attempt."""
    if not VideosSearch:
        return None
    try:
        search = VideosSearch(query, limit=1)
        result = await search.next()
        if result and result["result"]:
            return result["result"][0]["link"]
    except Exception:
        pass
    return None


async def hitman_search(query: str, title: str = "", artist: str = ""):
    """Search YouTube with multiple fallback queries."""
    queries = [query]
    if title and artist:
        queries.append(f"{title} {artist} official audio")
        queries.append(f"{title} {artist}")
    for q in queries:
        url = await _yt_search_single(q)
        if url:
            return url
    logger.warning(f"YouTube fallback: no results for: {queries[0]}")
    return None


def hitman_download(yt_url: str, title: str, artist: str, download_dir: str = "downloads"):
    """Download audio via yt-dlp. Tries YouTube first, then SoundCloud fallback."""
    if not yt_dlp:
        logger.error("yt-dlp not installed, cannot use YouTube fallback")
        return None
    os.makedirs(download_dir, exist_ok=True)
    safe_name = re.sub(r'[<>:"/\\|?*]', "_", f"{artist} - {title}")[:150]
    output_template = os.path.join(download_dir, f"{safe_name}.%(ext)s")

    base_opts = {
        "format": "bestaudio/best",
        "outtmpl": output_template,
        "quiet": True,
        "no_warnings": True,
        "geo_bypass": True,
        "nocheckcertificate": True,
        "prefer_ffmpeg": True,
        "postprocessors": [
            {
                "key": "FFmpegExtractAudio",
                "preferredcodec": "mp3",
                "preferredquality": "192",
            }
        ],
    }

    # Sources to try in order
    sources = [
        {
            "name": "YouTube",
            "url": yt_url,
            "extra_opts": {
                "extractor_args": {"youtube": {"player_client": ["web_creator"]}},
            },
            "verify": False,
        },
        {
            "name": "SoundCloud",
            "url": f"scsearch1:{title} {artist}",
            "extra_opts": {},
            "verify": True,  # Need to verify SoundCloud search results
        },
    ]

    def _title_matches(found_title, found_artist, want_title, want_artist):
        """Check if SoundCloud result matches the requested song + artist."""
        import re as _re

        def _normalize(s):
            return _re.sub(r'[^a-z0-9 ]', ' ', s.lower().strip())

        found_t = _normalize(found_title)
        found_a = _normalize(found_artist)
        want_t = _normalize(want_title)
        want_a = _normalize(want_artist)
        found_all = f"{found_t} {found_a}"

        # ── Version tag mismatch check ──
        # Reject if one has remix/vip/cover/bootleg/slowed but the other doesn't
        version_tags = ["remix", "vip", "cover", "bootleg", "slowed", "reverb",
                        "sped up", "bass boosted", "nightcore", "acoustic", "live"]
        for tag in version_tags:
            want_has = tag in want_t
            found_has = tag in found_t
            if want_has != found_has:
                return False  # Version mismatch

        # Extract significant keywords (3+ chars)
        title_words = [w for w in want_t.split() if len(w) >= 3]
        artist_words = [w for w in want_a.split() if len(w) >= 3]

        if not title_words:
            return True

        # Count title word matches in found title OR found artist
        title_hits = sum(1 for w in title_words if w in found_all)
        # Count artist word matches in found title OR found artist
        artist_hits = sum(1 for w in artist_words if w in found_all) if artist_words else 0

        # STRICT: need >=50% title words AND at least 1 artist word
        title_ok = title_hits / len(title_words) >= 0.5
        artist_ok = artist_hits >= 1 if artist_words else True

        return title_ok and artist_ok

    for src in sources:
        try:
            opts = {**base_opts, **src["extra_opts"]}
            logger.info(f"yt-dlp trying {src['name']}: {src['url']}")

            if src.get("verify"):
                # Extract info first to verify title + artist before downloading
                check_opts = {**opts, "quiet": True, "no_warnings": True}
                with yt_dlp.YoutubeDL(check_opts) as ydl:
                    info = ydl.extract_info(src["url"], download=False)
                    if info:
                        found_title = info.get("title", "")
                        found_artist = info.get("uploader", "") or info.get("artist", "")
                        if not _title_matches(found_title, found_artist, title, artist):
                            logger.warning(
                                f"SoundCloud mismatch: wanted '{title}' by '{artist}', "
                                f"got '{found_title}' by '{found_artist}' — skipping"
                            )
                            continue
                        logger.info(f"SoundCloud verified: '{found_title}' by '{found_artist}'")

            with yt_dlp.YoutubeDL(opts) as ydl:
                ydl.download([src["url"]])

            # Check if file was downloaded
            expected = os.path.join(download_dir, f"{safe_name}.mp3")
            if os.path.exists(expected):
                logger.info(f"yt-dlp {src['name']} success: {expected}")
                return expected
            for f in os.listdir(download_dir):
                if f.startswith(safe_name):
                    logger.info(f"yt-dlp {src['name']} success: {f}")
                    return os.path.join(download_dir, f)

        except Exception as e:
            logger.error(f"yt-dlp {src['name']} error: {e}")
            continue

    return None


# ─── MP3 metadata + cover art ─────────────────────────────────────────

async def slogix_metadata(file_path: str, title: str, artist: str, thumbnail_url: str = None):
    """Embed metadata and cover art into the audio file (MP3 or M4A)."""
    is_m4a = file_path.lower().endswith(".m4a") or file_path.lower().endswith(".mp4")

    def _sync_embed():
        try:
            if is_m4a:
                audio = MP4(file_path)
                audio["\xa9nam"] = [title]   # title
                audio["\xa9ART"] = [artist]  # artist
                audio.save()
            else:
                audio = MP3(file_path, ID3=ID3)
                try:
                    audio.add_tags()
                except Exception:
                    pass
                audio.tags["TIT2"] = TIT2(encoding=3, text=title)
                audio.tags["TPE1"] = TPE1(encoding=3, text=artist)
                audio.save()
        except Exception as e:
            logger.error(f"Metadata error: {e}")

    await asyncio.get_running_loop().run_in_executor(thread_pool, _sync_embed)

    if thumbnail_url:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(thumbnail_url) as resp:
                    if resp.status == 200:
                        img_data = await resp.read()
                        def _embed_cover():
                            try:
                                if is_m4a:
                                    audio = MP4(file_path)
                                    audio["covr"] = [MP4Cover(
                                        img_data,
                                        imageformat=MP4Cover.FORMAT_JPEG
                                    )]
                                    audio.save()
                                else:
                                    audio = MP3(file_path, ID3=ID3)
                                    audio.tags["APIC"] = APIC(
                                        encoding=3, mime="image/jpeg",
                                        type=3, desc="Cover", data=img_data
                                    )
                                    audio.save()
                            except Exception as e:
                                logger.error(f"Cover art error: {e}")
                        await asyncio.get_running_loop().run_in_executor(thread_pool, _embed_cover)
        except Exception as e:
            logger.error(f"Thumbnail download error: {e}")


# ─── Upload with FloodWait handling ───────────────────────────────────

def _get_userbot_client():
    """Get the actual Pyrogram Client from the Userbot wrapper (userbot.one)."""
    try:
        from hanzo import userbot
        if userbot and hasattr(userbot, 'one') and userbot.one and userbot.one.is_connected:
            return userbot.one
    except Exception:
        pass
    return None

async def crushex_upload(client, chat_id, file_path, title, artist, max_retries=3, message=None):
    """Upload audio file with FloodWait retry and multiple fallback strategies.
    
    Strategy order:
      1. message.reply_audio() — uses already-resolved peer from incoming msg
      2. client.send_audio(chat_id) — needs peer in session cache
      3. userbot.one.send_audio(chat_id) — fallback via userbot client
    """
    caption = (
        f"🎵 **{title}**\n"
        f"🎤 {artist}\n"
        f"📀 Source: Spotify"
    )

    for attempt in range(max_retries):
        try:
            # Strategy 1: reply_audio via original message (bypasses peer cache)
            if message:
                await message.reply_audio(
                    audio=file_path,
                    caption=caption,
                    title=title,
                    performer=artist,
                    quote=False,  # Send as a new message, not a reply
                )
                return True

            # Strategy 2: send_audio via bot client (needs peer in cache)
            await client.send_audio(
                chat_id=chat_id,
                audio=file_path,
                caption=caption,
                title=title,
                performer=artist,
            )
            return True
        except FloodWait as e:
            wait_time = e.value
            logger.warning(f"FloodWait: waiting {wait_time}s before retry")
            await asyncio.sleep(wait_time + 1)
        except Exception as e:
            error_msg = str(e).lower()
            logger.error(f"Upload error (attempt {attempt + 1}): {e}")
            # On peer error, try remaining strategies
            if "peer" in error_msg or "chat" in error_msg or "invalid" in error_msg:
                # Try client.send_audio if reply_audio failed
                if message:
                    try:
                        await client.send_audio(
                            chat_id=chat_id,
                            audio=file_path,
                            caption=caption,
                            title=title,
                            performer=artist,
                        )
                        return True
                    except Exception:
                        pass
                # Try userbot
                ub = _get_userbot_client()
                if ub:
                    try:
                        logger.info(f"Trying upload via userbot for chat {chat_id}")
                        await ub.send_audio(
                            chat_id=chat_id,
                            audio=file_path,
                            caption=caption,
                            title=title,
                            performer=artist,
                        )
                        return True
                    except FloodWait as fw:
                        await asyncio.sleep(fw.value + 1)
                    except Exception as ub_err:
                        logger.error(f"Userbot upload also failed: {ub_err}")
            if attempt < max_retries - 1:
                await asyncio.sleep(3)
    return False


# ─── Throttled status message editor ─────────────────────────────────

class SlogoHitman:
    """Throttle status message edits to avoid FloodWait on edits."""
    def __init__(self, message: Message, min_interval: float = 4.0):
        self.message = message
        self.min_interval = min_interval
        self.last_edit = 0.0

    async def update(self, text: str, force: bool = False, reply_markup=None):
        now = time.time()
        if force or (now - self.last_edit) >= self.min_interval:
            try:
                await self.message.edit_text(text, reply_markup=reply_markup)
                self.last_edit = now
            except FloodWait as e:
                await asyncio.sleep(e.value)
            except Exception:
                pass


# ─── /setchannel command ─────────────────────────────────────────────

@app.on_message(filters.command("setchannel"))
async def setchannel_cmd(client: Client, message: Message):
    """
    /setchannel <channel_id or @username>
    Set your preferred channel for song uploads.
    Bot must be admin in the channel with 'Post Messages' permission.
    """
    if len(message.command) < 2:
        return await message.reply_text(
            "**Usage:** `/setchannel <channel_id or @username>`\n\n"
            "**Example:**\n"
            "• `/setchannel -1001234567890`\n"
            "• `/setchannel @mychannel`\n\n"
            "ℹ️ Bot must be admin in the channel with **Post Messages** permission."
        )

    target = message.command[1]

    try:
        # Resolve channel — works with @username or numeric ID
        if target.lstrip("-").isdigit():
            channel_id = int(target)
        else:
            # Try to resolve @username
            chat = await client.get_chat(target)
            channel_id = chat.id

        # Verify bot can post in the channel
        try:
            test_msg = await client.send_message(channel_id, "✅ Channel linked! Songs will be uploaded here.")
            await test_msg.delete()
        except Exception as e:
            return await message.reply_text(
                f"❌ **Cannot post in that channel.**\n"
                f"Make sure the bot is **admin** with **Post Messages** permission.\n\n"
                f"Error: `{e}`"
            )

        # Save preference
        await crushbit_set_channel(message.from_user.id, channel_id)

        await message.reply_text(
            f"✅ **Channel set!**\n\n"
            f"Your `/spdownload` songs will now be uploaded to:\n"
            f"📢 `{channel_id}`\n\n"
            f"Use `/removechannel` to go back to uploading in the current chat."
        )

    except Exception as e:
        await message.reply_text(
            f"❌ **Invalid channel.**\n"
            f"Use a channel ID (e.g., `-1001234567890`) or username (e.g., `@mychannel`).\n\n"
            f"Error: `{e}`"
        )


# ─── /removechannel command ──────────────────────────────────────────

@app.on_message(filters.command("removechannel"))
async def removechannel_cmd(client: Client, message: Message):
    """/removechannel — Remove channel preference, uploads go to current chat."""
    user_id = message.from_user.id
    existing = await crushbit_get_channel(user_id)

    if not existing:
        return await message.reply_text(
            "ℹ️ You don't have a channel set. Songs are already uploaded to the current chat."
        )

    await crushbit_remove_channel(user_id)
    await message.reply_text(
        "✅ **Channel removed!**\n\n"
        "Songs will now be uploaded in the chat where you use `/spdownload`."
    )


# ─── /spdownload command ─────────────────────────────────────────────

@app.on_message(filters.command("spdownload"))
async def spdownload_cmd(client: Client, message: Message):
    """
    /spdownload <spotify_url>
    Downloads Spotify songs and uploads them in the current chat.
    Works in: DM, groups, channels (bot needs send permission).
    Supports: track, album, playlist (any size).
    """
    user_id = message.from_user.id

    # Determine upload target: user's channel preference or current chat
    user_channel = await crushbit_get_channel(user_id)
    chat_id = user_channel if user_channel else message.chat.id
    dest_label = "your channel" if user_channel else "this chat"

    if user_id in crushex_active:
        return await message.reply_text(
            "⏳ **You already have a download in progress!** Wait for it to finish."
        )

    if len(message.command) < 2:
        return await message.reply_text(
            "**Usage:** `/spdownload <spotify_url>`\n\n"
            "Supported:\n"
            "• Track: `https://open.spotify.com/track/...`\n"
            "• Album: `https://open.spotify.com/album/...`\n"
            "• Playlist: `https://open.spotify.com/playlist/...`"
        )

    url = message.command[1]
    m = re.match(r"https?://open\.spotify\.com/(track|album|playlist)/([a-zA-Z0-9]+)", url)
    if not m:
        return await message.reply_text("❌ **Invalid Spotify URL.**")

    sp_type = m.group(1)
    crushex_active[user_id] = {"cancel": False, "chat_id": chat_id}

    status_msg = await message.reply_text(f"🔍 **Fetching {sp_type} info from Spotify...**")
    status = SlogoHitman(status_msg)

    try:
        # Step 1: Fetch tracks
        tracks, method = await crushex_fetch(url)
        if not tracks:
            await status.update("❌ **Failed to fetch track info.** Check the URL and try again.", force=True)
            return

        total = len(tracks)
        method_labels = {"sp_dc": "🔐 Authenticated", "api": "🔗 API", "embed": "🌐 Embed"}
        method_label = method_labels.get(method, method)

        await status.update(
            f"📥 **Downloading {total} track(s)...**\n"
            f"Type: `{sp_type}` | Source: {method_label}\n"
            f"📢 Uploading to: {dest_label}\n"
            f"🛡️ FloodWait protection enabled",
            force=True,
        )

        loop = asyncio.get_running_loop()
        success = 0
        failed = 0
        failed_list = []  # Track failed songs for retry
        start_time = time.time()

        for i, track in enumerate(tracks, 1):
            file_path = None
            try:
                # ── Check for cancel ──
                dl_state = crushex_active.get(user_id, {})
                if dl_state.get("cancel"):
                    await status.update(
                        f"🛑 **Download cancelled!**\n\n"
                        f"✅ {success} uploaded · ❌ {failed} failed · ⏭️ {total - i + 1} skipped",
                        force=True,
                    )
                    break

                title = track["title"]
                artist = track["artist"]
                thumbnail = track.get("thumbnail")

                # ── Search: JioSaavn (primary) → saavn.dev → Piped → YouTube ──
                elapsed_so_far = time.time() - start_time
                eta = slogor_eta(elapsed_so_far, i - 1, total)
                bar = slogor_bar(i - 1, total)
                await status.update(
                    f"🔎 **[{i}/{total}]** Searching...\n"
                    f"🎵 `{artist} - {title}`\n\n"
                    f"{bar}\n"
                    f"✅ {success} uploaded · ❌ {failed} failed\n"
                    f"⏳ ETA: {eta}"
                )

                song_info = await jiosaavn_search(
                    title=title, artist=artist,
                    spotify_duration_ms=track.get("duration", 0),
                )
                source = "jiosaavn" if song_info else None

                # Fallback 1: saavn.dev REST API (better search for Western music)
                if not song_info:
                    song_info = await saavn_api_search(
                        title=title, artist=artist,
                        spotify_duration_ms=track.get("duration", 0),
                    )
                    if song_info:
                        source = "jiosaavn"  # same CDN, different search

                # Fallback 2: Piped (open-source YouTube frontend — no IP block)
                if not song_info:
                    song_info = await piped_search(
                        title=title, artist=artist,
                        spotify_duration_ms=track.get("duration", 0),
                    )
                    if song_info:
                        source = "piped"

                # Fallback 3: Invidious (another open-source YouTube frontend)
                if not song_info:
                    song_info = await invidious_search(
                        title=title, artist=artist,
                        spotify_duration_ms=track.get("duration", 0),
                    )
                    if song_info:
                        source = "invidious"

                # Fallback 4: InnerTube (direct YouTube API — like savefrom.net)
                if not song_info:
                    song_info = await innertube_search(
                        title=title, artist=artist,
                        spotify_duration_ms=track.get("duration", 0),
                    )
                    if song_info:
                        source = "innertube"

                # Fallback 5: YouTube/yt-dlp (with Android player client bypass)
                yt_url = None
                if not song_info:
                    logger.info(f"All APIs failed, trying yt-dlp for: {artist} - {title}")
                    query = f"{artist} - {title} audio"
                    yt_url = await hitman_search(query, title=title, artist=artist)
                    if yt_url:
                        source = "youtube"

                if not song_info and not yt_url:
                    failed += 1
                    failed_list.append({"title": title, "artist": artist})
                    logger.warning(f"All 6 sources failed for '{artist} - {title}'")
                    continue

                # ── Download from selected source ──
                elapsed_so_far = time.time() - start_time
                eta = slogor_eta(elapsed_so_far, max(i - 1, 1), total)
                bar = slogor_bar(i - 1, total)
                source_labels = {
                    "jiosaavn": "JioSaavn",
                    "piped": "YouTube (Piped)",
                    "invidious": "YouTube (Invidious)",
                    "innertube": "YouTube (Direct)",
                    "youtube": "YouTube (yt-dlp)",
                }
                source_label = source_labels.get(source, source)
                quality_label = f" ({song_info['quality']})" if song_info else ""
                await status.update(
                    f"📥 **[{i}/{total}]** Downloading from {source_label}{quality_label}...\n"
                    f"🎵 `{artist} - {title}`\n\n"
                    f"{bar}\n"
                    f"✅ {success} uploaded · ❌ {failed} failed\n"
                    f"⏳ ETA: {eta}"
                )

                if source == "jiosaavn":
                    file_path = await jiosaavn_download(
                        song_info["download_url"], title, artist
                    )
                elif source in ("piped", "invidious"):
                    file_path = await piped_download(
                        song_info["download_url"], title, artist
                    )
                elif source == "innertube":
                    # Try direct download first (works on some server IPs)
                    dl_url = song_info.get("download_url", "")
                    if "googlevideo.com" in dl_url:
                        file_path = await piped_download(dl_url, title, artist)
                    else:
                        file_path = None
                    # Fallback to yt-dlp if direct download failed
                    if not file_path or not os.path.exists(file_path):
                        fallback_url = song_info.get("yt_url") or dl_url
                        logger.info(f"Direct download failed, trying yt-dlp: {fallback_url}")
                        file_path = await loop.run_in_executor(
                            thread_pool, hitman_download,
                            fallback_url, title, artist
                        )
                else:
                    file_path = await loop.run_in_executor(
                        thread_pool, hitman_download,
                        yt_url, title, artist
                    )

                if not file_path or not os.path.exists(file_path):
                    failed += 1
                    failed_list.append({"title": title, "artist": artist})
                    continue

                # ── Embed metadata + cover art ──
                # Use JioSaavn image if Spotify thumbnail is missing, YouTube uses Spotify thumbnail
                cover_url = thumbnail or (song_info.get("image") if song_info else None)
                await slogix_metadata(file_path, title, artist, cover_url)

                # ── Upload to chat ──
                elapsed_so_far = time.time() - start_time
                eta = slogor_eta(elapsed_so_far, max(i - 1, 1), total)
                bar = slogor_bar(i - 1, total)
                await status.update(
                    f"📤 **[{i}/{total}]** Uploading...\n"
                    f"🎵 `{artist} - {title}`\n\n"
                    f"{bar}\n"
                    f"✅ {success} uploaded · ❌ {failed} failed\n"
                    f"⏳ ETA: {eta}"
                )

                uploaded = await crushex_upload(
                    client, chat_id, file_path, title, artist,
                    message=message,
                )

                if uploaded:
                    success += 1
                else:
                    failed += 1
                    failed_list.append({"title": title, "artist": artist})

                # ── Telegram FloodWait protection delay ──
                # (No YouTube anti-ban pauses needed — JioSaavn has no IP blocking)
                if i < total:
                    await asyncio.sleep(3)
                    if i % 10 == 0:
                        bar = slogor_bar(i, total)
                        await status.update(
                            f"⏸️ **Cooldown** (preventing Telegram ban)...\n"
                            f"{bar}\n"
                            f"✅ {success}/{total} uploaded · ❌ {failed} failed",
                            force=True,
                        )
                        await asyncio.sleep(5)

            except Exception as e:
                logger.error(f"Error processing track {i}: {e}")
                failed += 1
            finally:
                if file_path and os.path.exists(file_path):
                    try:
                        os.remove(file_path)
                    except Exception:
                        pass

        # ── Final summary ──
        elapsed = time.time() - start_time
        mins = int(elapsed // 60)
        secs = int(elapsed % 60)
        bar = slogor_bar(total, total)

        summary_text = (
            f"✅ **Done!**\n\n"
            f"{bar}\n\n"
            f"📊 **Results:**\n"
            f"• Uploaded: `{success}/{total}`\n"
            f"• Failed: `{failed}`\n"
            f"• Source: {method_label}\n"
            f"• Time: `{mins}m {secs}s`"
        )

        # Store failed tracks for /failed command
        if failed_list:
            await slognet_store_fails(user_id, failed_list, url)
            summary_text += f"\n\n🔄 Use `/failed` to see & retry {len(failed_list)} failed track(s)."

        # Show retry button if there were failures
        reply_markup = None
        if failed_list:
            reply_markup = InlineKeyboardMarkup([
                [InlineKeyboardButton(
                    f"🔄 Retry {len(failed_list)} Failed",
                    callback_data=f"spretry_{user_id}"
                )]
            ])

        await status.update(summary_text, force=True, reply_markup=reply_markup)

        # DM notification if download was in a group
        if message.chat.type in ["group", "supergroup"]:
            try:
                await client.send_message(
                    user_id,
                    f"📢 **Download complete!**\n\n"
                    f"✅ {success}/{total} uploaded · ❌ {failed} failed\n"
                    f"⏱ Time: {mins}m {secs}s\n\n"
                    f"Songs were sent to: {dest_label}"
                )
            except Exception:
                pass  # User hasn't started the bot in DM

    except Exception as e:
        logger.error(f"spdownload fatal error: {e}")
        await status.update(f"❌ **Fatal error:** `{e}`", force=True)
    finally:
        crushex_active.pop(user_id, None)


# ─── /sphelp — Help menu ─────────────────────────────────────────────

HELP_TEXT = """
🎵 **Spotify Hanzo — Commands**

**📥 Download:**
• `/spdownload <url>` — Download track/album/playlist
• Just paste a Spotify link — auto-detected!

**ℹ️ Info:**
• `/spinfo <url>` — Quick info (name, tracks, duration)

**⚙️ Settings:**
• `/setchannel <id/@name>` — Set upload channel
• `/removechannel` — Remove channel preference

**🛠️ Controls:**
• `/cancel` — Stop ongoing download
• `/failed` — View & retry failed tracks

**🎵 Supported URLs:**
• `https://open.spotify.com/track/...`
• `https://open.spotify.com/album/...`
• `https://open.spotify.com/playlist/...`

💡 **Tip:** Songs go to the chat where you type, or to your set channel!
"""


@app.on_message(filters.command("sphelp"))
async def sphelp_cmd(client: Client, message: Message):
    """/sphelp — Show all Spotify commands."""
    await message.reply_text(HELP_TEXT)


# ─── /cancel — Stop ongoing download ─────────────────────────────────

@app.on_message(filters.command("cancel"))
async def cancel_cmd(client: Client, message: Message):
    """/cancel — Cancel the current ongoing download."""
    user_id = message.from_user.id

    if user_id not in crushex_active:
        return await message.reply_text(
            "ℹ️ You don't have any active downloads to cancel."
        )

    # Set cancel flag — the download loop will pick it up
    crushex_active[user_id]["cancel"] = True
    await message.reply_text(
        "🛑 **Cancelling download...** The current track will finish, then it stops."
    )


# ─── /spinfo — Quick playlist/album/track info ──────────────────────

@app.on_message(filters.command("spinfo"))
async def spinfo_cmd(client: Client, message: Message):
    """/spinfo <url> — Show info about a Spotify link without downloading."""
    if len(message.command) < 2:
        return await message.reply_text(
            "**Usage:** `/spinfo <spotify_url>`\n\n"
            "Shows playlist/album/track details without downloading."
        )

    url = message.command[1]
    m = re.match(
        r"https?://open\.spotify\.com/(track|album|playlist)/([a-zA-Z0-9]+)", url
    )
    if not m:
        return await message.reply_text("❌ **Invalid Spotify URL.**")

    sp_type = m.group(1)
    status_msg = await message.reply_text(f"🔍 **Fetching {sp_type} info...**")

    try:
        tracks, method = await crushex_fetch(url)
        if not tracks:
            return await status_msg.edit_text(
                "❌ **Could not fetch info.** The link may be private or invalid."
            )

        total = len(tracks)
        total_duration_ms = sum(t.get("duration", 0) for t in tracks)
        total_mins = int(total_duration_ms / 60000)
        total_hours = total_mins // 60
        remaining_mins = total_mins % 60

        # Get first few track names for preview
        preview = ""
        for i, t in enumerate(tracks[:5], 1):
            preview += f"  {i}. {t['artist']} — {t['title']}\n"
        if total > 5:
            preview += f"  ... and {total - 5} more\n"

        # Duration display
        if total_hours > 0:
            duration_str = f"{total_hours}h {remaining_mins}m"
        else:
            duration_str = f"{total_mins}m"

        # Estimate download time (~12s per track)
        est_mins = (total * 12) // 60
        est_str = f"~{est_mins} min" if est_mins > 0 else "< 1 min"

        method_labels = {"sp_dc": "🔐 Authenticated", "api": "🔗 API", "embed": "🌐 Embed"}
        method_label = method_labels.get(method, method)

        info_text = (
            f"📋 **Spotify {sp_type.title()} Info**\n\n"
            f"🎵 **Tracks:** `{total}`\n"
            f"⏱️ **Duration:** `{duration_str}`\n"
            f"📡 **Source:** {method_label}\n"
            f"⏳ **Est. download:** `{est_str}`\n\n"
            f"**Preview:**\n{preview}\n"
            f"Use `/spdownload` to download!"
        )

        await status_msg.edit_text(info_text)

    except Exception as e:
        logger.error(f"spinfo error: {e}")
        await status_msg.edit_text(f"❌ **Error:** `{e}`")


# ─── Auto-detect Spotify links in messages ───────────────────────────

SPOTIFY_URL_PATTERN = re.compile(
    r"https?://open\.spotify\.com/(track|album|playlist)/([a-zA-Z0-9]+)"
)


@app.on_message(filters.regex(r"https?://open\.spotify\.com/(track|album|playlist)/") & ~filters.command(["spdownload", "spinfo"]))
async def auto_detect_spotify(client: Client, message: Message):
    """
    Auto-detect Spotify links in messages and ask to download.
    Shows an inline button — user taps to start download.
    """
    if not message.text:
        return

    # Extra safety: skip if this is a /spdownload or /spinfo command
    text_lower = message.text.strip().lower()
    if text_lower.startswith("/spdownload") or text_lower.startswith("/spinfo"):
        return

    match = SPOTIFY_URL_PATTERN.search(message.text)
    if not match:
        return

    url = match.group(0)
    sp_type = match.group(1)

    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                f"📥 Download {sp_type.title()}",
                callback_data=f"spd_{sp_type}_{match.group(2)}"
            ),
            InlineKeyboardButton(
                "ℹ️ Info",
                callback_data=f"spi_{sp_type}_{match.group(2)}"
            ),
        ]
    ])

    await message.reply_text(
        f"🎵 **Spotify {sp_type.title()} detected!**\n\n"
        f"Tap below to download or get info:",
        reply_markup=keyboard,
    )


# ─── Callback handler for auto-detect buttons ────────────────────────

from pyrogram.types import CallbackQuery


@app.on_callback_query(filters.regex(r"^sp[di]_"))
async def spotify_callback(client: Client, callback: CallbackQuery):
    """Handle inline button presses for auto-detect."""
    data = callback.data
    parts = data.split("_", 2)  # spd_track_id or spi_track_id
    if len(parts) != 3:
        return await callback.answer("❌ Invalid data", show_alert=True)

    action, sp_type, sp_id = parts
    url = f"https://open.spotify.com/{sp_type}/{sp_id}"
    user_id = callback.from_user.id

    if action == "spi":
        # Info request — inline response
        await callback.answer("🔍 Fetching info...", show_alert=False)
        try:
            tracks, method = await crushex_fetch(url)
            if tracks:
                total = len(tracks)
                total_duration_ms = sum(t.get("duration", 0) for t in tracks)
                total_mins = int(total_duration_ms / 60000)
                est_mins = (total * 12) // 60

                await callback.message.edit_text(
                    f"📋 **{sp_type.title()} Info**\n\n"
                    f"🎵 Tracks: `{total}`\n"
                    f"⏱️ Duration: `{total_mins}m`\n"
                    f"⏳ Est. download: `~{est_mins} min`\n\n"
                    f"Tap to download:",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton(
                            f"📥 Download {total} tracks",
                            callback_data=f"spd_{sp_type}_{sp_id}"
                        )
                    ]])
                )
            else:
                await callback.message.edit_text("❌ Could not fetch info.")
        except Exception as e:
            await callback.answer(f"Error: {e}", show_alert=True)
        return

    if action == "spd":
        # Download request
        if user_id in crushex_active:
            return await callback.answer(
                "⏳ You already have a download in progress!", show_alert=True
            )

        await callback.answer("📥 Starting download...", show_alert=False)

        # Determine upload target
        user_channel = await crushbit_get_channel(user_id)
        chat_id = user_channel if user_channel else callback.message.chat.id
        dest_label = "your channel" if user_channel else "this chat"

        crushex_active[user_id] = {"cancel": False, "chat_id": chat_id}

        status_msg = await callback.message.edit_text(
            f"🔍 **Fetching {sp_type} info from Spotify...**"
        )
        status = SlogoHitman(status_msg)

        try:
            tracks, method = await crushex_fetch(url)
            if not tracks:
                await status.update("❌ **Failed to fetch track info.**", force=True)
                return

            total = len(tracks)
            method_labels = {"sp_dc": "🔐 Authenticated", "api": "🔗 API", "embed": "🌐 Embed"}
            method_label = method_labels.get(method, method)

            await status.update(
                f"📥 **Downloading {total} track(s)...**\n"
                f"Type: `{sp_type}` | Source: {method_label}\n"
                f"📢 Uploading to: {dest_label}\n"
                f"🛡️ FloodWait protection enabled",
                force=True,
            )

            loop = asyncio.get_running_loop()
            success = 0
            failed = 0
            start_time = time.time()
            next_yt_url = None

            for i, track in enumerate(tracks, 1):
                file_path = None
                try:
                    # Cancel check
                    dl_state = crushex_active.get(user_id, {})
                    if dl_state.get("cancel"):
                        await status.update(
                            f"🛑 **Download cancelled!**\n\n"
                            f"✅ {success} uploaded · ❌ {failed} failed · ⏭️ {total - i + 1} skipped",
                            force=True,
                        )
                        break

                    title = track["title"]
                    artist = track["artist"]
                    thumbnail = track.get("thumbnail")

                    await status.update(
                        f"🔎 **[{i}/{total}]** Searching YouTube...\n"
                        f"🎵 `{artist} - {title}`\n\n"
                        f"✅ {success} uploaded · ❌ {failed} failed"
                    )

                    if next_yt_url:
                        yt_url = next_yt_url
                        next_yt_url = None
                    else:
                        query = f"{artist} - {title} audio"
                        yt_url = await hitman_search(query, title=title, artist=artist)

                    if not yt_url:
                        failed += 1
                        continue

                    # Pre-search next
                    next_search_task = None
                    if i < total:
                        nt = tracks[i]
                        next_search_task = asyncio.create_task(
                            hitman_search(f"{nt['artist']} - {nt['title']} audio", title=nt['title'], artist=nt['artist'])
                        )

                    file_path = await loop.run_in_executor(
                        thread_pool, hitman_download, yt_url, title, artist
                    )

                    if not file_path or not os.path.exists(file_path):
                        failed += 1
                        if next_search_task:
                            next_yt_url = await next_search_task
                        continue

                    await slogix_metadata(file_path, title, artist, thumbnail)

                    uploaded = await crushex_upload(
                        client, chat_id, file_path, title, artist,
                        message=message,
                    )
                    if uploaded:
                        success += 1
                    else:
                        failed += 1

                    if next_search_task:
                        next_yt_url = await next_search_task

                    if i < total:
                        await asyncio.sleep(3)
                        if i % 10 == 0:
                            await asyncio.sleep(5)

                except Exception as e:
                    logger.error(f"Error processing track {i}: {e}")
                    failed += 1
                finally:
                    if file_path and os.path.exists(file_path):
                        try:
                            os.remove(file_path)
                        except Exception:
                            pass

            elapsed = time.time() - start_time
            mins = int(elapsed // 60)
            secs = int(elapsed % 60)

            await status.update(
                f"✅ **Done!**\n\n"
                f"📊 **Results:**\n"
                f"• Uploaded: `{success}/{total}`\n"
                f"• Failed: `{failed}`\n"
                f"• Source: {method_label}\n"
                f"• Time: `{mins}m {secs}s`",
                force=True,
            )

        except Exception as e:
            logger.error(f"Auto-detect download error: {e}")
            await status.update(f"❌ **Fatal error:** `{e}`", force=True)
        finally:
            crushex_active.pop(user_id, None)


# ─── /failed — View and retry failed tracks ─────────────────────────

@app.on_message(filters.command("failed"))
async def failed_cmd(client: Client, message: Message):
    """/failed — Show failed tracks from last download with retry option."""
    user_id = message.from_user.id
    doc = await slognet_get_fails(user_id)

    if not doc or not doc.get("tracks"):
        return await message.reply_text(
            "✅ No failed tracks! All your songs were downloaded successfully."
        )

    tracks = doc["tracks"]
    track_list = ""
    for i, t in enumerate(tracks[:20], 1):  # Show max 20
        track_list += f"  {i}. {t['artist']} — {t['title']}\n"
    if len(tracks) > 20:
        track_list += f"  ... and {len(tracks) - 20} more\n"

    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                f"🔄 Retry All {len(tracks)} Tracks",
                callback_data=f"spretry_{user_id}"
            ),
            InlineKeyboardButton(
                "🗑️ Clear List",
                callback_data=f"spclear_{user_id}"
            ),
        ]
    ])

    await message.reply_text(
        f"❌ **Failed Tracks ({len(tracks)}):**\n\n"
        f"{track_list}\n"
        f"Tap below to retry or clear:",
        reply_markup=keyboard,
    )


# ─── Callback: retry failed / clear failed ─────────────────────────

@app.on_callback_query(filters.regex(r"^sp(retry|clear)_"))
async def retry_clear_callback(client: Client, callback: CallbackQuery):
    """Handle retry/clear failed tracks buttons."""
    data = callback.data
    action = "retry" if data.startswith("spretry_") else "clear"
    target_user_id = int(data.split("_", 1)[1])
    user_id = callback.from_user.id

    # Only the user who owns the failed tracks can retry/clear
    if user_id != target_user_id:
        return await callback.answer("❌ This is not your download.", show_alert=True)

    if action == "clear":
        await slognet_clear_fails(user_id)
        await callback.message.edit_text("🗑️ **Failed tracks cleared!**")
        return await callback.answer("✅ Cleared!")

    # Retry failed tracks
    doc = await slognet_get_fails(user_id)
    if not doc or not doc.get("tracks"):
        return await callback.answer("✅ No failed tracks to retry.", show_alert=True)

    if user_id in crushex_active:
        return await callback.answer("⏳ Download already in progress!", show_alert=True)

    tracks = doc["tracks"]
    await callback.answer(f"🔄 Retrying {len(tracks)} tracks...", show_alert=False)

    # Determine upload target
    user_channel = await crushbit_get_channel(user_id)
    chat_id = user_channel if user_channel else callback.message.chat.id
    dest_label = "your channel" if user_channel else "this chat"

    crushex_active[user_id] = {"cancel": False, "chat_id": chat_id}

    status_msg = await callback.message.edit_text(
        f"🔄 **Retrying {len(tracks)} failed track(s)...**"
    )
    status = SlogoHitman(status_msg)

    try:
        total = len(tracks)
        loop = asyncio.get_running_loop()
        success = 0
        failed = 0
        still_failed = []
        start_time = time.time()

        for i, track in enumerate(tracks, 1):
            file_path = None
            try:
                dl_state = crushex_active.get(user_id, {})
                if dl_state.get("cancel"):
                    await status.update(
                        f"🛑 **Retry cancelled!**\n\n"
                        f"✅ {success} uploaded · ❌ {failed} failed",
                        force=True,
                    )
                    break

                title = track["title"]
                artist = track["artist"]

                bar = slogor_bar(i - 1, total)
                eta = slogor_eta(time.time() - start_time, max(i - 1, 1), total)
                await status.update(
                    f"🔄 **Retry [{i}/{total}]** Searching...\n"
                    f"🎵 `{artist} - {title}`\n\n"
                    f"{bar}\n"
                    f"✅ {success} · ❌ {failed} · ⏳ ETA: {eta}"
                )

                query = f"{artist} - {title} audio"
                yt_url = await hitman_search(query, title=title, artist=artist)

                if not yt_url:
                    failed += 1
                    still_failed.append(track)
                    continue

                file_path = await loop.run_in_executor(
                    thread_pool, hitman_download, yt_url, title, artist
                )

                if not file_path or not os.path.exists(file_path):
                    failed += 1
                    still_failed.append(track)
                    continue

                await slogix_metadata(file_path, title, artist, None)

                uploaded = await crushex_upload(
                    client, chat_id, file_path, title, artist,
                    message=message,
                )
                if uploaded:
                    success += 1
                else:
                    failed += 1
                    still_failed.append(track)

                if i < total:
                    await asyncio.sleep(3)

            except Exception as e:
                logger.error(f"Retry error track {i}: {e}")
                failed += 1
                still_failed.append(track)
            finally:
                if file_path and os.path.exists(file_path):
                    try:
                        os.remove(file_path)
                    except Exception:
                        pass

        # Update or clear failed tracks
        if still_failed:
            await slognet_store_fails(user_id, still_failed, doc.get("url", ""))
        else:
            await slognet_clear_fails(user_id)

        elapsed = time.time() - start_time
        mins = int(elapsed // 60)
        secs = int(elapsed % 60)
        bar = slogor_bar(total, total)

        summary = (
            f"🔄 **Retry Done!**\n\n"
            f"{bar}\n\n"
            f"✅ Recovered: `{success}/{total}`\n"
            f"❌ Still failed: `{len(still_failed)}`\n"
            f"⏱ Time: `{mins}m {secs}s`"
        )

        reply_markup = None
        if still_failed:
            reply_markup = InlineKeyboardMarkup([
                [InlineKeyboardButton(
                    f"🔄 Retry {len(still_failed)} Again",
                    callback_data=f"spretry_{user_id}"
                )]
            ])

        await status.update(summary, force=True, reply_markup=reply_markup)

    except Exception as e:
        logger.error(f"Retry fatal error: {e}")
        await status.update(f"❌ **Retry error:** `{e}`", force=True)
    finally:
        crushex_active.pop(user_id, None)
