import threading
from flask import Flask
import discord
from dotenv import load_dotenv
from discord.ext import commands
import yt_dlp
import asyncio
import concurrent.futures
from collections import deque
import tempfile
import os

import subprocess
import shutil

# --- Node.js Diagnostics ---
print("--- Node.js Diagnostics ---")
node_path = shutil.which('node')
if node_path:
    print(f"✅ Node.js found at: {node_path}")
    try:
        node_ver = subprocess.check_output([node_path, '--version'], text=True).strip()
        print(f"✅ Node.js version: {node_ver}")
    except Exception as e:
        print(f"❌ Node.js found but failed to execute: {e}")
else:
    print("❌ Node.js NOT FOUND in PATH! This will cause yt-dlp signature failures.")
print("---------------------------")

import gc
import ctypes

# --- Bot Setup ---
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

# --- Cookie Diagnostics ---
cookie_path = 'cookies.txt'
if os.path.exists(cookie_path):
    size = os.path.getsize(cookie_path)
    print(f"✅ Cookie file found! Size: {size} bytes.")
    with open(cookie_path, 'r') as f:
        first_line = f.readline().strip()
        if first_line == "# Netscape HTTP Cookie File":
            print("✅ Cookie file format looks correct (Netscape).")
        else:
            print(f"❌ Cookie file format looks WRONG! First line is: '{first_line}'")
            print("❌ It MUST start with '# Netscape HTTP Cookie File'")
else:
    print("❌ CRITICAL ERROR: cookies.txt NOT FOUND in the directory!")
    print(f"Current directory contents: {os.listdir('.')}")

# --- Real-Time Audio Priority ---
gc.disable()

try:
    libc = ctypes.CDLL('libc.so.6')
    libc.mlockall(3)
except Exception:
    print("⚠️ Could not lock memory. Run with `sudo` or set `cap_ipc_lock` for zero stutters.")

# --- Thread Pool ---
executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)

# --- Storage Directories ---
TEMP_DIR_RAM = tempfile.mkdtemp(prefix="musicbot_ram_", dir="/dev/shm")
TEMP_DIR_DISK = "./bot_downloads"
os.makedirs(TEMP_DIR_DISK, exist_ok=True)

# --- yt-dlp Options (FIXED) ---
#
# CHANGES MADE:
#   1. Removed 'extractor_args' — the multiple player_client values
#      (tv, android, ios, web) caused yt-dlp to report formats that
#      aren't actually downloadable, leading to the
#      "Requested format is not available" error.
#   2. Changed format from '251/250/249/bestaudio/best' to
#      'bestaudio/best' — this lets yt-dlp pick the best available
#      audio format automatically instead of demanding specific
#      itag numbers that may not exist for every video.
#   3. Removed 'http_headers' — cookies handle authentication;
#      a custom User-Agent can actually trigger anti-bot detection.
#
YDL_OPTIONS_RAM = {
    'format': 'bestaudio/best',
    'noplaylist': False, 'quiet': True, 'no_warnings': True,
    'default_search': 'ytsearch', 'source_address': '0.0.0.0',
    'outtmpl': f'{TEMP_DIR_RAM}/%(id)s.%(ext)s',
    'cookiefile': 'cookies.txt',
}

YDL_OPTIONS_DISK = {
    'format': 'bestaudio/best',
    'noplaylist': False, 'quiet': True, 'no_warnings': True,
    'default_search': 'ytsearch', 'source_address': '0.0.0.0',
    'outtmpl': f'{TEMP_DIR_DISK}/%(id)s.%(ext)s',
    'cookiefile': 'cookies.txt',
}

# --- FFmpeg Options ---
FFMPEG_OPTIONS = {
    'before_options': (
        '-analyzeduration 20000000 '
        '-probesize 20000000 '
        '-thread_queue_size 8192 '
        '-threads 4'
    ),
    'options': (
        '-vn '
        '-bufsize 2048k '
        '-af aresample=async=1:min_comp=0.01:max_soft_comp=10'
    ),
}

# --- Queue Storage ---
queues = {}

def get_queue(guild_id):
    if guild_id not in queues:
        queues[guild_id] = deque()
    return queues[guild_id]

def fetch_audio(query):
    # 1. Quick probe (no downloading yet)
    #    FIXED: removed extractor_args here too
    probe_opts = {
        'quiet': True,
        'cookiefile': 'cookies.txt',
        'default_search': 'ytsearch',
    }

    with yt_dlp.YoutubeDL(probe_opts) as ydl:
        if not query.startswith("http"):
            query = f"ytsearch:{query}"
        info = ydl.extract_info(query, download=False)
        if 'entries' in info:
            info = info['entries'][0]

    duration = info.get('duration') or 0
    title = info.get('title', 'Unknown Title')

    # 2. Decide where to download based on length
    if duration > 3600:
        ydl_opts = YDL_OPTIONS_DISK
        storage_type = 'disk'
    else:
        ydl_opts = YDL_OPTIONS_RAM
        storage_type = 'ram'

    # 3. Download the file
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(query, download=True)
        if 'entries' in info:
            info = info['entries'][0]

        filepath = info['requested_downloads'][0]['filepath']

        # Fallback just in case
        if not os.path.exists(filepath):
            base = os.path.splitext(filepath)[0]
            for ext in ('.webm', '.opus', '.m4a', '.ogg', '.mp3'):
                candidate = base + ext
                if os.path.exists(candidate):
                    filepath = candidate
                    break

    return {
        'filepath': filepath,
        'title': title,
        'duration': duration,
        'storage_type': storage_type
    }

# --- Cleanup ---
def cleanup_song(song):
    try:
        filepath = song.get('filepath')
        if filepath and os.path.exists(filepath):
            os.remove(filepath)
    except Exception:
        pass

# --- Play Next ---
async def play_next(ctx):
    gc.collect()

    queue = get_queue(ctx.guild.id)
    if not queue:
        if ctx.voice_client:
            await ctx.send("✅ Queue is empty. Leaving voice channel.")
            await ctx.voice_client.disconnect()
        return

    if not ctx.voice_client:
        return

    song = queue.popleft()

    if not os.path.exists(song.get('filepath', '')):
        await ctx.send(f"❌ File missing for **{song['title']}**, skipping.")
        cleanup_song(song)
        await play_next(ctx)
        return

    try:
        source = discord.FFmpegOpusAudio(
            song['filepath'],
            bitrate=128,
            **FFMPEG_OPTIONS
        )
    except Exception as e:
        await ctx.send(f"❌ Error playing **{song['title']}**: {e}")
        cleanup_song(song)
        await play_next(ctx)
        return

    def after_play(error):
        cleanup_song(song)
        if error:
            print(f"Playback error: {error}")
        asyncio.run_coroutine_threadsafe(play_next(ctx), bot.loop)

    ctx.voice_client.play(source, after=after_play)

    storage_emoji = "💾" if song['storage_type'] == 'disk' else "⚡"
    await ctx.send(f"🎵 Now playing: **{song['title']}** {storage_emoji}")

# --- Commands ---

@bot.command(name="play", aliases=["p"])
async def play(ctx, *, query: str):
    if not ctx.author.voice:
        return await ctx.send("❌ You must be in a voice channel!")

    if not ctx.voice_client:
        await ctx.author.voice.channel.connect()

    await ctx.send(f"🔍 Searching and downloading: `{query}`...")
    try:
        song = await asyncio.get_event_loop().run_in_executor(executor, fetch_audio, query)
    except Exception as e:
        return await ctx.send(f"❌ Error fetching audio: {e}")

    queue = get_queue(ctx.guild.id)

    if ctx.voice_client.is_playing() or ctx.voice_client.is_paused():
        queue.append(song)
        await ctx.send(f"➕ Added to queue: **{song['title']}**")
    else:
        queue.append(song)
        await play_next(ctx)

@bot.command(name="skip", aliases=["s"])
async def skip(ctx):
    if ctx.voice_client and ctx.voice_client.is_playing():
        ctx.voice_client.stop()
        await ctx.send("⏭️ Skipped!")
    else:
        await ctx.send("❌ Nothing is playing.")

@bot.command(name="pause")
async def pause(ctx):
    if ctx.voice_client and ctx.voice_client.is_playing():
        ctx.voice_client.pause()
        await ctx.send("⏸️ Paused.")
    else:
        await ctx.send("❌ Nothing is playing.")

@bot.command(name="resume")
async def resume(ctx):
    if ctx.voice_client and ctx.voice_client.is_paused():
        ctx.voice_client.resume()
        await ctx.send("▶️ Resumed.")
    else:
        await ctx.send("❌ Nothing is paused.")

@bot.command(name="stop")
async def stop(ctx):
    queue = get_queue(ctx.guild.id)
    for song in queue:
        cleanup_song(song)
    queue.clear()

    if ctx.voice_client:
        ctx.voice_client.stop()
        await ctx.voice_client.disconnect()
    await ctx.send("⏹️ Stopped and disconnected.")

@bot.command(name="queue", aliases=["q"])
async def show_queue(ctx):
    queue = get_queue(ctx.guild.id)
    if not queue:
        return await ctx.send("📭 The queue is empty.")

    lines = []
    for i, s in enumerate(queue):
        title = s.get('title', 'Unknown')
        downloaded = "✅" if os.path.exists(s.get('filepath', '')) else "⏳"
        lines.append(f"{i+1}. {downloaded} {title}")

    msg = "\n".join(lines)
    await ctx.send(f"📋 **Queue:**\n{msg}")

@bot.command(name="leave")
async def leave(ctx):
    queue = get_queue(ctx.guild.id)
    for song in queue:
        cleanup_song(song)
    queue.clear()

    if ctx.voice_client:
        await ctx.voice_client.disconnect()
        await ctx.send("👋 Disconnected.")

@bot.event
async def on_close():
    import shutil
    shutil.rmtree(TEMP_DIR_RAM, ignore_errors=True)

load_dotenv()

# --- Dummy Web Server for Hugging Face ---
app = Flask(__name__)

@app.route('/')
def home():
    return "Music Bot is running!", 200

def run_web():
    port = int(os.environ.get("PORT", 7860))
    app.run(host='0.0.0.0', port=port)

if __name__ == "__main__":
    threading.Thread(target=run_web, daemon=True).start()
    bot.run(os.getenv("DISCORD_TOKEN"))