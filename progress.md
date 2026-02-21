# Hanzo Bot — Download Sources Progress

## What We're Fixing
The `/spdownload` command was only downloading 2 out of 6 songs correctly because:
- **JioSaavn** doesn't have many Western songs
- **YouTube/yt-dlp** is blocked on Render (bot detection on server IPs)

## What We've Done

### 1. Fixed Song Matching (✅ Done)
- Replaced word-overlap matching with `difflib.SequenceMatcher` (character-level  similarity)
- Title similarity threshold: ≥ 55%
- Artist similarity threshold: ≥ 40%
- Duration validation: within 30 seconds of Spotify duration
- Accent stripping for search queries (e.g., "Bon Appétit" → "Bon Appetit")
- Clean title function strips feat/remix/genre tags

### 2. Added 6 Download Sources (✅ Done)

| # | Source | Type | Status | Notes |
|---|---|---|---|---|
| 1 | **jiosaavnpy** | JioSaavn Python lib | ✅ Works for Indian music | Original source |
| 2 | **saavn.dev API** | JioSaavn REST API | ⚠️ Needs testing on Render | Better search for Western music |
| 3 | **Piped API** (4 instances) | YouTube frontend | ⚠️ Instances unreliable | Public instances returning 403/502 |
| 4 | **Invidious API** (4 instances) | YouTube frontend | ⚠️ Instances unreliable | Connection issues from local |
| 5 | **InnerTube API** | YouTube direct API | ✅ **CONFIRMED WORKING** | savefrom.net technique! |
| 6 | **yt-dlp** (Android bypass) | YouTube download | ⚠️ May work with Android client | Added player_client=android |

### 3. InnerTube API — The Key Breakthrough (✅ Confirmed)
This is the same technique savefrom.net and save.tube use:
```
POST https://www.youtube.com/youtubei/v1/player
Body: { clientName: "ANDROID", clientVersion: "19.29.37" }
→ Returns streamingData.adaptiveFormats with direct googlevideo.com audio URLs
```
**Test result**: Got 145kbps audio stream for "Katy Perry - Bon Appétit" ✅

### 4. Local Test Results (Feb 21, 2025)
Test song: **Katy Perry - Bon Appétit** (Western, hard to find on JioSaavn)

| Source | Result | Details |
|---|---|---|
| saavn.dev | ❌ | API responded but URL extraction issue |
| Piped | ❌ | Instances returned 403/502 |
| Invidious | ❌ | Connection issues from local |
| **InnerTube** | **✅** | **Direct googlevideo.com URL at 145kbps** |

## Download Flow in Code
```
spdownload.py → Search chain:
  1. jiosaavn_search()       → JioSaavn via Python library
  2. saavn_api_search()      → JioSaavn via saavn.dev REST API
  3. piped_search()          → YouTube via Piped (4 instances)
  4. invidious_search()      → YouTube via Invidious (4 instances)
  5. innertube_search()      → YouTube via InnerTube API (savefrom.net)
  6. hitman_search/download  → YouTube via yt-dlp (Android bypass)
```

## Files Modified
- `hanzo/slogix/hitman/spdownload.py` — All download sources and matching logic
- `hanzo/crushex/slogor.py` — AUTH_KEY_DUPLICATED error handling
- `hanzo/crushex/crushehitman.py` — PyTgCalls graceful error handling

## Git Commits
- `7b42c91` — feat: add saavn.dev REST API as second JioSaavn search source
- `5ad79b6` — feat: add Piped API as YouTube alternative
- `4bcd1b8` — feat: add Invidious API + improve yt-dlp with Android player client
- `1936717` — feat: add InnerTube direct YouTube API (savefrom.net technique)

## Next Steps
- [ ] Test on Render (Piped/Invidious may work better from Render IPs)
- [ ] Fix saavn.dev URL extraction if needed
- [ ] Monitor Render logs for source hit rates
- [ ] Consider removing unreliable Piped/Invidious instances
