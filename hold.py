"""
hold.py — External Keep-Alive for Hanzo Bot
============================================
Run this script on a SEPARATE machine (your PC, VPS, etc.)
to keep the bot alive on free hosting services.

Usage:
    python hold.py

Set your Koyeb/Render/Heroku URL below.
The script pings the bot every 20 seconds to prevent sleeping.

NOTE: The bot already has a BUILT-IN keep-alive system.
      This is only needed as an EXTRA safety net.
"""

import requests
import time
import os
from datetime import datetime

# ══════════════════════════════════════════════════════════
# SET YOUR BOT'S PUBLIC URL HERE
# ══════════════════════════════════════════════════════════
BOT_URL = os.getenv("BOT_URL", "https://YOUR-APP-NAME.koyeb.app")  # Change this!
# Examples:
#   Koyeb:  "https://your-app-name.koyeb.app"
#   Render: "https://your-app-name.onrender.com"
#   Heroku: "https://your-app-name.herokuapp.com"
# ══════════════════════════════════════════════════════════

PING_INTERVAL = 20  # seconds between pings
HEALTH_ENDPOINT = "/health"

def format_time():
    return datetime.now().strftime("%H:%M:%S")

def ping_bot():
    url = BOT_URL.rstrip("/") + HEALTH_ENDPOINT
    ping_count = 0
    fail_count = 0

    print(f"""
╔══════════════════════════════════════════════╗
║   🏯 HANZO BOT — External Keep-Alive        ║
╠══════════════════════════════════════════════╣
║   Target: {BOT_URL[:35]:<35}║
║   Interval: Every {PING_INTERVAL}s{' ' * 25}║
║   Press Ctrl+C to stop                       ║
╚══════════════════════════════════════════════╝
    """)

    while True:
        try:
            response = requests.get(url, timeout=15)
            ping_count += 1
            status = "✅ OK" if response.status_code == 200 else f"⚠️ {response.status_code}"
            print(f"[{format_time()}] Ping #{ping_count} → {status} ({response.elapsed.total_seconds():.1f}s)")
            fail_count = 0  # Reset on success
        except requests.exceptions.ConnectionError:
            fail_count += 1
            print(f"[{format_time()}] ❌ Connection failed (attempt #{fail_count})")
            if fail_count >= 5:
                print(f"[{format_time()}] 🔄 Bot might be restarting... waiting 30s")
                time.sleep(30)
                fail_count = 0
                continue
        except requests.exceptions.Timeout:
            fail_count += 1
            print(f"[{format_time()}] ⏱️ Timeout (attempt #{fail_count})")
        except requests.exceptions.RequestException as e:
            fail_count += 1
            print(f"[{format_time()}] ❌ Error: {e}")

        time.sleep(PING_INTERVAL)

if __name__ == "__main__":
    if "YOUR-APP-NAME" in BOT_URL:
        print("⚠️  Please set your bot's URL in hold.py first!")
        print("   Edit BOT_URL = 'https://your-app.koyeb.app'")
        print()
        url = input("Or enter URL now: ").strip()
        if url:
            BOT_URL = url
        else:
            exit(1)

    try:
        ping_bot()
    except KeyboardInterrupt:
        print(f"\n[{format_time()}] 👋 Keep-alive stopped.")
