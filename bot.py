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
import time
import gc
import ctypes

# --- Deno Diagnostics ---
print("--- Deno Diagnostics ---")
deno_path = shutil.which('deno')
if deno_path:
    print(f"✅ Deno found at: {deno_path}")
    try:
        deno_ver = subprocess.check_output([deno_path, '--version'], text=True).strip().split('\n')[0]
        print(f"✅ {deno_ver}")
    except Exception as e:
        print(f"❌ Deno found but failed to execute: {e}")
else:
    print("❌ Deno NOT FOUND in PATH!")
print("------------------------")

load_dotenv()

cookies_content = os.getenv("COOKIES_TXT")
if cookies_content:
    with open('cookies.txt', 'w') as f:
        f.write(cookies_content)
    print("✅ cookies.txt written from COOKIES_TXT secret.")
else:
    print("❌ COOKIES_TXT secret not found! YouTube may not work.")

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
else:
    print("❌ CRITICAL ERROR: cookies.txt NOT FOUND!")
    print(f"Current directory contents: {os.listdir('.')}")

# --- Real-Time Audio Priority ---
gc.disable()

try:
    libc = ctypes.CDLL('libc.so.6')
    libc.mlockall(3)
except Exception:
    print("⚠️ Could not lock memory.")

# --- Thread Pool ---
executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)

# --- FFmpeg Options for Streaming ---
FFMPEG_OPTIONS = {
    'before_options': (
        '-reconnect 1 '
        '-reconnect_streamed 1 '
        '-reconnect_delay_max 5 '
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
loop_modes = {}   # guild_id -> 'off' | 'single' | 'queue'
volumes = {}      # guild_id -> float (0.0 to 1.0)
now_playing = {}
pending_downloads = {}  # guild_id -> count of active fetches
skip_flag = set()       # guilds where skip was requested


# --- Helpers ---

def get_loop_mode(guild_id):
    return loop_modes.get(guild_id, 'off')


def get_volume(guild_id):
    return volumes.get(guild_id, 0.5)


def get_queue(guild_id):
    if guild_id not in queues:
        queues[guild_id] = deque()
    return queues[guild_id]


def cleanup_song(song):
    """Streaming songs have no file. Kept for compatibility."""
    if not song:
        return
    try:
        filepath = song.get('filepath')
        if filepath and os.path.exists(filepath):
            os.remove(filepath)
    except Exception:
        pass


# --- Audio Fetching (Stream, No Download) ---

def fetch_audio(query):
    """
    Extracts a direct stream URL from YouTube (or any yt-dlp source).
    Does NOT download the file — FFmpeg streams it directly.
    """
    ydl_opts = {
        'format': 'bestaudio/best',
        'quiet': True,
        'no_warnings': True,
        'cookiefile': 'cookies.txt',
        'default_search': 'ytsearch',
        'noplaylist': False,
    }

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        if not query.startswith("http"):
            query = f"ytsearch:{query}"
        info = ydl.extract_info(query, download=False)
        if 'entries' in info:
            info = info['entries'][0]

    duration = info.get('duration') or 0
    title = info.get('title', 'Unknown Title')

    # Primary: yt-dlp puts the chosen format's URL here
    stream_url = info.get('url')

    # Fallback: walk formats list
    if not stream_url:
        for f in reversed(info.get('formats', [])):
            if f.get('acodec') != 'none' and f.get('url'):
                stream_url = f['url']
                break

    if not stream_url:
        raise ValueError(f"Could not extract stream URL for: {title}")

    return {
        'stream_url': stream_url,
        'http_headers': info.get('http_headers', {}),
        'title': title,
        'duration': duration,
        'storage_type': 'stream',
        'filepath': None,
        'webpage_url': info.get('webpage_url', query),
        'fetched_at': time.time(),
    }


async def fetch_with_timeout(ctx, query, timeout=120):
    """Wraps fetch_audio with a timeout."""
    try:
        song = await asyncio.wait_for(
            asyncio.get_event_loop().run_in_executor(executor, fetch_audio, query),
            timeout=timeout
        )
        return song
    except asyncio.TimeoutError:
        await ctx.send(f"❌ Timed out fetching: `{query}`")
        return None


# --- URL Refresh (for songs sitting in queue > 4 hours) ---

async def maybe_refresh_url(song):
    """
    YouTube stream URLs expire in ~6 hours.
    Re-extract if the song has been in the queue for more than 4 hours.
    """
    age = time.time() - song.get('fetched_at', time.time())
    if age > 4 * 3600:
        print(f"🔄 Refreshing expired URL for: {song['title']}")
        try:
            fresh = await asyncio.get_event_loop().run_in_executor(
                executor,
                fetch_audio,
                song['webpage_url']
            )
            song['stream_url'] = fresh['stream_url']
            song['http_headers'] = fresh['http_headers']
            song['fetched_at'] = time.time()
        except Exception as e:
            print(f"❌ Failed to refresh URL for {song['title']}: {e}")
    return song


# --- Core Playback ---

async def play_next(ctx):
    gc.collect()
    guild_id = ctx.guild.id
    queue = get_queue(guild_id)

    if not queue:
        if pending_downloads.get(guild_id, 0) > 0:
            await ctx.send("⏳ Waiting for remaining songs to be fetched...")
            while pending_downloads.get(guild_id, 0) > 0 and not queue:
                await asyncio.sleep(1)
            if not queue:
                if ctx.voice_client:
                    await ctx.send("✅ Queue is empty. Leaving voice channel.")
                    await ctx.voice_client.disconnect()
                return
        else:
            if ctx.voice_client:
                await ctx.send("✅ Queue is empty. Leaving voice channel.")
                await ctx.voice_client.disconnect()
            return

    if not ctx.voice_client:
        return

    song = queue.popleft()

    # Refresh URL if it's been sitting in the queue too long
    song = await maybe_refresh_url(song)

    # Build FFmpeg before_options with HTTP headers injected
    http_headers = song.get('http_headers', {})
    header_str = ''.join(f"{k}: {v}\r\n" for k, v in http_headers.items())

    before_options = FFMPEG_OPTIONS['before_options']
    if header_str:
        before_options = f"-headers '{header_str}' " + before_options

    ffmpeg_opts = {
        'before_options': before_options,
        'options': FFMPEG_OPTIONS['options'],
    }

    try:
        source = discord.FFmpegPCMAudio(song['stream_url'], **ffmpeg_opts)
        source = discord.PCMVolumeTransformer(source, volume=get_volume(guild_id))
    except Exception as e:
        await ctx.send(f"❌ Error starting playback for **{song['title']}**: {e}")
        await play_next(ctx)
        return

    def after_play(error):
        current_loop_mode = get_loop_mode(ctx.guild.id)
        is_skip = ctx.guild.id in skip_flag
        skip_flag.discard(ctx.guild.id)

        if is_skip:
            # Skipped — discard the song entirely
            cleanup_song(song)
        elif current_loop_mode == 'single':
            queue.appendleft(song)
        elif current_loop_mode == 'queue':
            queue.append(song)
        else:
            cleanup_song(song)

        if error:
            print(f"Playback error: {error}")

        asyncio.run_coroutine_threadsafe(play_next(ctx), bot.loop)

    now_playing[guild_id] = song
    ctx.voice_client.play(source, after=after_play)

    loop_mode = get_loop_mode(guild_id)
    loop_emoji = {'off': '', 'single': ' 🔂', 'queue': ' 🔁'}
    await ctx.send(
        f"🎵 Now playing: **{song['title']}** 📡"
        f"{loop_emoji[loop_mode]}"
    )


# --- Commands ---

@bot.command(name="play", aliases=["p"])
async def play(ctx, *, query: str):
    if not ctx.author.voice:
        return await ctx.send("❌ You must be in a voice channel!")

    if not ctx.voice_client:
        await ctx.author.voice.channel.connect()

    await ctx.send(f"🔍 Fetching: `{query}`...")

    guild_id = ctx.guild.id
    pending_downloads[guild_id] = pending_downloads.get(guild_id, 0) + 1

    try:
        song = await fetch_with_timeout(ctx, query)
        if song is None:
            return
    except Exception as e:
        return await ctx.send(f"❌ Error fetching audio: {e}")
    finally:
        pending_downloads[guild_id] = max(0, pending_downloads.get(guild_id, 0) - 1)

    queue = get_queue(guild_id)

    if ctx.voice_client.is_playing() or ctx.voice_client.is_paused():
        queue.append(song)
        await ctx.send(f"➕ Added to queue: **{song['title']}**")
    else:
        queue.append(song)
        await play_next(ctx)


@bot.command(name="skip", aliases=["s"])
async def skip(ctx):
    if not ctx.voice_client or not ctx.voice_client.is_playing():
        return await ctx.send("❌ Nothing is playing.")
    skip_flag.add(ctx.guild.id)
    ctx.voice_client.stop()
    await ctx.send("⏭️ Skipped!")


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
    pending_downloads.pop(ctx.guild.id, None)
    queue = get_queue(ctx.guild.id)
    for song in queue:
        cleanup_song(song)
    queue.clear()
    if ctx.voice_client:
        ctx.voice_client.stop()
        await ctx.voice_client.disconnect()
    await ctx.send("⏹️ Stopped and disconnected.")


@bot.command(name="leave")
async def leave(ctx):
    pending_downloads.pop(ctx.guild.id, None)
    queue = get_queue(ctx.guild.id)
    for song in queue:
        cleanup_song(song)
    queue.clear()
    if ctx.voice_client:
        await ctx.voice_client.disconnect()
        await ctx.send("👋 Disconnected.")


@bot.command(name="queue", aliases=["q"])
async def show_queue(ctx):
    queue = get_queue(ctx.guild.id)
    if not queue:
        return await ctx.send("📭 The queue is empty.")

    lines = []
    for i, s in enumerate(queue):
        title = s.get('title', 'Unknown')
        lines.append(f"{i+1}. 📡 {title}")

    await ctx.send(f"📋 **Queue:**\n" + "\n".join(lines))


@bot.command(name="nowplaying", aliases=["np"])
async def now_playing_cmd(ctx):
    song = now_playing.get(ctx.guild.id)
    if not song or not ctx.voice_client or not ctx.voice_client.is_playing():
        return await ctx.send("❌ Nothing is playing right now.")

    duration = song.get('duration', 0)
    mins, secs = divmod(int(duration), 60)
    loop_mode = get_loop_mode(ctx.guild.id)
    vol = int(get_volume(ctx.guild.id) * 100)

    await ctx.send(
        f"🎵 **Now Playing:** {song['title']}\n"
        f"⏱️ Duration: {mins}:{secs:02d} | 📡 Stream\n"
        f"🔁 Loop: {loop_mode} | 🔊 Volume: {vol}%"
    )


@bot.command(name="loop", aliases=["l"])
async def loop_cmd(ctx, mode: str = None):
    """Usage: !loop off / single / queue"""
    modes = {'off', 'single', 'queue'}
    if mode is None:
        current = get_loop_mode(ctx.guild.id)
        return await ctx.send(f"🔁 Current loop mode: **{current}**")
    mode = mode.lower()
    if mode not in modes:
        return await ctx.send("❌ Options: `off`, `single`, `queue`")
    loop_modes[ctx.guild.id] = mode
    emojis = {'off': '➡️', 'single': '🔂', 'queue': '🔁'}
    await ctx.send(f"{emojis[mode]} Loop mode set to: **{mode}**")


@bot.command(name="volume", aliases=["vol", "v"])
async def volume_cmd(ctx, vol: int = None):
    """Set volume 1-100. No argument to see current."""
    if vol is None:
        current = int(get_volume(ctx.guild.id) * 100)
        return await ctx.send(f"🔊 Current volume: **{current}%**")
    if not 1 <= vol <= 100:
        return await ctx.send("❌ Volume must be between 1 and 100.")
    volumes[ctx.guild.id] = vol / 100.0
    if ctx.voice_client and ctx.voice_client.source:
        ctx.voice_client.source.volume = vol / 100.0
    await ctx.send(f"🔊 Volume set to **{vol}%**")


# --- Events ---

@bot.event
async def on_voice_state_update(member, before, after):
    """Clean up if bot is disconnected unexpectedly."""
    if member == bot.user:
        if before.channel and not after.channel:
            guild_id = before.channel.guild.id
            queue = get_queue(guild_id)
            for song in queue:
                cleanup_song(song)
            queue.clear()
            now_playing.pop(guild_id, None)
            loop_modes.pop(guild_id, None)
            pending_downloads.pop(guild_id, None)
            print(f"Bot disconnected from guild {guild_id}, cleaned up.")


@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.CommandNotFound):
        return
    elif isinstance(error, commands.MissingRequiredArgument):
        await ctx.send(f"❌ Missing argument: `{error.param.name}`")
    elif isinstance(error, commands.BadArgument):
        await ctx.send(f"❌ Bad argument: {error}")
    else:
        await ctx.send(f"❌ An error occurred: {error}")
        print(f"Unhandled error in {ctx.command}: {error}")
        import traceback
        traceback.print_exc()


@bot.event
async def on_close():
    pass  # Nothing to clean up — no temp files used


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