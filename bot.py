# bot.py
import threading
import asyncio
import concurrent.futures
import os
import shutil
import subprocess
import time
import random
from collections import deque

from flask import Flask, jsonify
import discord
from discord.ext import commands
from dotenv import load_dotenv
import yt_dlp

# ─────────────────────────────────────────────
#  Startup diagnostics
# ─────────────────────────────────────────────
print("--- Deno Diagnostics ---")
_deno = shutil.which("deno")
if _deno:
    try:
        _ver = subprocess.check_output([_deno, "--version"], text=True).split("\n")[0]
        print(f"✅ {_ver} ({_deno})")
    except Exception as _e:
        print(f"⚠️  Deno found but failed to run: {_e}")
else:
    print("⚠️  Deno NOT found in PATH — yt-dlp EJS may fail.")
print("------------------------")

load_dotenv()

# Write cookies from environment secret
_cookies_content = os.getenv("COOKIES_TXT", "")
if _cookies_content:
    with open("cookies.txt", "w") as _f:
        _f.write(_cookies_content)
    print("✅ cookies.txt written from COOKIES_TXT secret.")
else:
    print("⚠️  COOKIES_TXT not set — age-restricted content may fail.")

# Validate cookie file
if os.path.exists("cookies.txt"):
    _size = os.path.getsize("cookies.txt")
    with open("cookies.txt", "r") as _f:
        _first = _f.readline().strip()
    if _first == "# Netscape HTTP Cookie File":
        print(f"✅ cookies.txt looks correct ({_size} bytes).")
    else:
        print(f"⚠️  cookies.txt format may be wrong. First line: '{_first}'")
else:
    print("❌ cookies.txt NOT found after write attempt!")

# ─────────────────────────────────────────────
#  Constants
# ─────────────────────────────────────────────
DEFAULT_VOLUME       = 0.5
MAX_QUEUE_SIZE       = 200
MAX_ERRORS_IN_A_ROW  = 5
URL_REFRESH_SECS     = 4 * 3600   # re-fetch stream URL after 4 h
FETCH_TIMEOUT_SECS   = 120
EXECUTOR_WORKERS     = 4

LOOP_OFF    = "off"
LOOP_SINGLE = "single"
LOOP_QUEUE  = "queue"
LOOP_EMOJIS = {LOOP_OFF: "➡️", LOOP_SINGLE: "🔂", LOOP_QUEUE: "🔁"}

FFMPEG_BEFORE_BASE = (
    "-reconnect 1 "
    "-reconnect_streamed 1 "
    "-reconnect_delay_max 5 "
    "-analyzeduration 20000000 "
    "-probesize 20000000 "
    "-thread_queue_size 8192 "
    "-threads 2"
)
FFMPEG_OPTIONS = (
    "-vn "
    "-bufsize 2048k "
    "-af aresample=async=1:min_comp=0.01:max_soft_comp=10"
)

# ─────────────────────────────────────────────
#  Per-guild state  (replaces all global dicts)
# ─────────────────────────────────────────────
import threading as _threading

class GuildState:
    """Thread-safe container for all mutable state of one guild."""

    def __init__(self, guild_id: int):
        self.guild_id        = guild_id
        self._lock           = _threading.Lock()
        self._queue          = deque()
        self.loop_mode       = LOOP_OFF
        self.volume          = DEFAULT_VOLUME
        self.now_playing     = None
        self._pending        = 0
        self._skip_requested = False
        self.is_seeking = False

    # ── queue ops ──────────────────────────────
    def enqueue(self, song: dict) -> bool:
        with self._lock:
            if len(self._queue) >= MAX_QUEUE_SIZE:
                return False
            self._queue.append(song)
            return True

    def enqueue_left(self, song: dict) -> None:
        with self._lock:
            self._queue.appendleft(song)

    def dequeue(self):
        with self._lock:
            return self._queue.popleft() if self._queue else None

    def clear(self) -> list:
        with self._lock:
            songs = list(self._queue)
            self._queue.clear()
            return songs

    def snapshot(self) -> list:
        with self._lock:
            return list(self._queue)

    def remove_at(self, index: int):          # 0-based
        with self._lock:
            items = list(self._queue)
            if not 0 <= index < len(items):
                return None
            removed = items.pop(index)
            self._queue = deque(items)
            return removed

    def move(self, from_idx: int, to_idx: int) -> bool:   # 0-based
        with self._lock:
            items = list(self._queue)
            n = len(items)
            if not (0 <= from_idx < n and 0 <= to_idx < n):
                return False
            song = items.pop(from_idx)
            items.insert(to_idx, song)
            self._queue = deque(items)
            return True

    def shuffle(self) -> None:
        with self._lock:
            items = list(self._queue)
            random.shuffle(items)
            self._queue = deque(items)

    @property
    def empty(self) -> bool:
        with self._lock:
            return len(self._queue) == 0

    @property
    def length(self) -> int:
        with self._lock:
            return len(self._queue)

    # ── pending fetch counter ──────────────────
    def inc_pending(self):
        with self._lock:
            self._pending += 1

    def dec_pending(self):
        with self._lock:
            self._pending = max(0, self._pending - 1)

    @property
    def has_pending(self) -> bool:
        with self._lock:
            return self._pending > 0

    # ── skip flag ─────────────────────────────
    def request_skip(self):
        with self._lock:
            self._skip_requested = True

    def consume_skip(self) -> bool:
        with self._lock:
            val = self._skip_requested
            self._skip_requested = False
            return val


# Global registry
_states: dict[int, GuildState] = {}
_states_lock = _threading.Lock()

def get_state(guild_id: int) -> GuildState:
    with _states_lock:
        if guild_id not in _states:
            _states[guild_id] = GuildState(guild_id)
        return _states[guild_id]

def remove_state(guild_id: int):
    with _states_lock:
        _states.pop(guild_id, None)

# ─────────────────────────────────────────────
#  Thread pool
# ─────────────────────────────────────────────
executor = concurrent.futures.ThreadPoolExecutor(max_workers=EXECUTOR_WORKERS)

# ─────────────────────────────────────────────
#  yt-dlp helpers
# ─────────────────────────────────────────────
_YDL_OPTS = {
    "format":         "bestaudio/best",
    "quiet":          True,
    "no_warnings":    True,
    "cookiefile":     "cookies.txt",
    "default_search": "ytsearch",
    "noplaylist":     False,
    "format_sort":    ["acodec:opus", "acodec:aac"],
}

def _best_url(info: dict) -> str | None:
    """Pull the best audio stream URL from a yt-dlp info dict."""
    if info.get("url"):
        return info["url"]
    for fmt in reversed(info.get("formats", [])):
        if fmt.get("acodec") not in (None, "none") and fmt.get("url"):
            return fmt["url"]
    return None

def fetch_audio(query: str) -> dict:
    """
    Blocking — always run in executor.
    Raises ValueError if nothing usable is found.
    """
    if not query.startswith("http"):
        query = f"ytsearch:{query}"

    with yt_dlp.YoutubeDL(_YDL_OPTS) as ydl:
        info = ydl.extract_info(query, download=False)

    if "entries" in info:
        entries = [e for e in info["entries"] if e]
        if not entries:
            raise ValueError("No results found.")
        info = entries[0]

    url = _best_url(info)
    if not url:
        raise ValueError(f"No stream URL found for: {info.get('title')}")

    return {
        "title":        info.get("title", "Unknown Title"),
        "duration":     info.get("duration") or 0,
        "stream_url":   url,
        "http_headers": info.get("http_headers", {}),
        "webpage_url":  info.get("webpage_url", query),
        "fetched_at":   time.time(),
    }

async def fetch_audio_async(query: str) -> dict:
    loop = asyncio.get_event_loop()
    return await asyncio.wait_for(
        loop.run_in_executor(executor, fetch_audio, query),
        timeout=FETCH_TIMEOUT_SECS,
    )

async def maybe_refresh(song: dict) -> dict:
    """Re-fetch the stream URL if it's older than URL_REFRESH_SECS."""
    if time.time() - song.get("fetched_at", time.time()) <= URL_REFRESH_SECS:
        return song
    print(f"🔄 Refreshing URL: {song['title']}")
    try:
        fresh = await fetch_audio_async(song["webpage_url"])
        song["stream_url"]   = fresh["stream_url"]
        song["http_headers"] = fresh["http_headers"]
        song["fetched_at"]   = time.time()
    except Exception as e:
        print(f"⚠️  URL refresh failed for {song['title']}: {e}")
    return song

# ─────────────────────────────────────────────
#  FFmpeg helpers
# ─────────────────────────────────────────────
def _build_ffmpeg_opts(song: dict, seek_secs: int = 0) -> dict:
    """
    Build FFmpeg option dicts for a song.
    Headers are sanitised to prevent injection.
    seek_secs > 0 adds a -ss offset.
    """
    headers = song.get("http_headers", {})
    header_str = "".join(
        f"{k}: {str(v).replace(chr(13), '').replace(chr(10), '')}\r\n"
        for k, v in headers.items()
    )

    before = FFMPEG_BEFORE_BASE
    if header_str:
        safe = header_str.replace('"', '\\"')
        before = f'-headers "{safe}" ' + before
    if seek_secs > 0:
        before = f"-ss {seek_secs} " + before

    return {"before_options": before, "options": FFMPEG_OPTIONS}

# ─────────────────────────────────────────────
#  Bot setup
# ─────────────────────────────────────────────
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents, help_command=None)

# ─────────────────────────────────────────────
#  Core playback engine
# ─────────────────────────────────────────────
async def play_next(ctx: commands.Context, error_count: int = 0) -> None:
    """
    Advance to the next track.
    error_count guards against infinite recursion on repeated FFmpeg failures.
    """

    state = get_state(ctx.guild.id)

    if state.is_seeking:
        return

    if error_count >= MAX_ERRORS_IN_A_ROW:
        await ctx.send("❌ Too many consecutive errors. Stopping.")
        if ctx.voice_client:
            await ctx.voice_client.disconnect()
        return

    # ── wait if queue is empty but fetches are still running ──
    if state.empty:
        if state.has_pending:
            await ctx.send("⏳ Waiting for songs to finish fetching…")
            for _ in range(60):
                await asyncio.sleep(1)
                if not state.empty:
                    break

        if state.empty:
            await ctx.send("✅ Queue empty. Leaving voice channel.")
            if ctx.voice_client:
                await ctx.voice_client.disconnect()
            return

    if not ctx.voice_client:
        return

    song = state.dequeue()
    if song is None:
        return

    song = await maybe_refresh(song)
    ffmpeg_opts = _build_ffmpeg_opts(song)

    try:
        raw    = discord.FFmpegPCMAudio(song["stream_url"], **ffmpeg_opts)
        source = discord.PCMVolumeTransformer(raw, volume=state.volume)
    except Exception as e:
        await ctx.send(f"❌ FFmpeg error for **{song['title']}**: {e}")
        await play_next(ctx, error_count + 1)
        return

    def after_play(error):
        was_skipped = state.consume_skip()

        if not was_skipped:
            if state.loop_mode == LOOP_SINGLE:
                state.enqueue_left(song)
            elif state.loop_mode == LOOP_QUEUE:
                state.enqueue(song)

        if error:
            print(f"⚠️  Playback error in '{song['title']}': {error}")

        asyncio.run_coroutine_threadsafe(play_next(ctx), bot.loop)

    state.now_playing = song
    ctx.voice_client.play(source, after=after_play)

    emoji = LOOP_EMOJIS.get(state.loop_mode, "")
    await ctx.send(f"🎵 Now playing: **{song['title']}** 📡 {emoji}")


async def _ensure_voice(ctx: commands.Context) -> bool:
    """Join the author's voice channel. Returns False if not possible."""
    if not ctx.author.voice:
        await ctx.send("❌ You must be in a voice channel!")
        return False
    if not ctx.voice_client:
        await ctx.author.voice.channel.connect()
    return True

# ─────────────────────────────────────────────
#  Commands
# ─────────────────────────────────────────────
@bot.command(name="play", aliases=["p"])
async def play(ctx: commands.Context, *, query: str):
    """Play a song or add it to the queue."""
    if not await _ensure_voice(ctx):
        return

    state = get_state(ctx.guild.id)
    await ctx.send(f"🔍 Fetching: `{query}`…")
    state.inc_pending()

    try:
        song = await fetch_audio_async(query)
    except asyncio.TimeoutError:
        await ctx.send(f"❌ Timed out fetching: `{query}`")
        return
    except Exception as e:
        await ctx.send(f"❌ Error fetching audio: {e}")
        return
    finally:
        state.dec_pending()

    if not state.enqueue(song):
        await ctx.send(f"❌ Queue is full ({MAX_QUEUE_SIZE} songs max).")
        return

    if ctx.voice_client.is_playing() or ctx.voice_client.is_paused():
        await ctx.send(f"➕ Added to queue (#{state.length}): **{song['title']}**")
    else:
        await play_next(ctx)


@bot.command(name="skip", aliases=["s"])
async def skip(ctx: commands.Context):
    """Skip the current song."""
    if not ctx.voice_client or not ctx.voice_client.is_playing():
        return await ctx.send("❌ Nothing is playing.")
    get_state(ctx.guild.id).request_skip()
    ctx.voice_client.stop()
    await ctx.send("⏭️ Skipped!")


@bot.command(name="pause")
async def pause(ctx: commands.Context):
    """Pause playback."""
    if ctx.voice_client and ctx.voice_client.is_playing():
        ctx.voice_client.pause()
        await ctx.send("⏸️ Paused.")
    else:
        await ctx.send("❌ Nothing is playing.")


@bot.command(name="resume")
async def resume(ctx: commands.Context):
    """Resume paused playback."""
    if ctx.voice_client and ctx.voice_client.is_paused():
        ctx.voice_client.resume()
        await ctx.send("▶️ Resumed.")
    else:
        await ctx.send("❌ Nothing is paused.")


@bot.command(name="stop")
async def stop(ctx: commands.Context):
    """Stop playback, clear the queue, and disconnect."""
    state = get_state(ctx.guild.id)
    state.clear()
    if ctx.voice_client:
        ctx.voice_client.stop()
        await ctx.voice_client.disconnect()
    await ctx.send("⏹️ Stopped and disconnected.")


@bot.command(name="leave")
async def leave(ctx: commands.Context):
    """Disconnect from voice."""
    get_state(ctx.guild.id).clear()
    if ctx.voice_client:
        await ctx.voice_client.disconnect()
        await ctx.send("👋 Disconnected.")


@bot.command(name="queue", aliases=["q"])
async def show_queue(ctx: commands.Context):
    """Show the current queue (up to 20 entries)."""
    songs = get_state(ctx.guild.id).snapshot()
    if not songs:
        return await ctx.send("📭 The queue is empty.")

    lines = [
        f"`{i+1}.` 📡 {s.get('title', 'Unknown')}"
        for i, s in enumerate(songs[:20])
    ]
    if len(songs) > 20:
        lines.append(f"… and {len(songs) - 20} more.")

    await ctx.send("📋 **Queue:**\n" + "\n".join(lines))


@bot.command(name="nowplaying", aliases=["np"])
async def now_playing_cmd(ctx: commands.Context):
    """Show what's currently playing."""
    state = get_state(ctx.guild.id)
    song  = state.now_playing

    if not song or not ctx.voice_client or not ctx.voice_client.is_playing():
        return await ctx.send("❌ Nothing is playing right now.")

    mins, secs = divmod(int(song.get("duration", 0)), 60)
    vol = int(state.volume * 100)

    embed = discord.Embed(
        title="🎵 Now Playing",
        description=f"**{song['title']}**",
        color=discord.Color.blurple(),
    )
    embed.add_field(name="⏱️ Duration", value=f"{mins}:{secs:02d}")
    embed.add_field(name="🔁 Loop",     value=state.loop_mode)
    embed.add_field(name="🔊 Volume",   value=f"{vol}%")
    embed.add_field(name="📋 In queue", value=str(state.length))

    await ctx.send(embed=embed)


@bot.command(name="loop", aliases=["l"])
async def loop_cmd(ctx: commands.Context, mode: str = None):
    """Set loop mode: off | single | queue"""
    state = get_state(ctx.guild.id)
    if mode is None:
        return await ctx.send(f"🔁 Current loop mode: **{state.loop_mode}**")

    mode = mode.lower()
    if mode not in {LOOP_OFF, LOOP_SINGLE, LOOP_QUEUE}:
        return await ctx.send("❌ Options: `off`, `single`, `queue`")

    state.loop_mode = mode
    await ctx.send(f"{LOOP_EMOJIS[mode]} Loop mode set to: **{mode}**")


@bot.command(name="volume", aliases=["vol", "v"])
async def volume_cmd(ctx: commands.Context, vol: int = None):
    """Set or view volume (1–100)."""
    state = get_state(ctx.guild.id)
    if vol is None:
        return await ctx.send(f"🔊 Current volume: **{int(state.volume * 100)}%**")

    if not 1 <= vol <= 100:
        return await ctx.send("❌ Volume must be between 1 and 100.")

    state.volume = vol / 100.0
    if ctx.voice_client and ctx.voice_client.source:
        ctx.voice_client.source.volume = state.volume
    await ctx.send(f"🔊 Volume set to **{vol}%**")


@bot.command(name="shuffle")
async def shuffle_cmd(ctx: commands.Context):
    """Shuffle the queue."""
    state = get_state(ctx.guild.id)
    if state.empty:
        return await ctx.send("📭 The queue is empty.")
    state.shuffle()
    await ctx.send("🔀 Queue shuffled!")


@bot.command(name="remove", aliases=["rm"])
async def remove_cmd(ctx: commands.Context, index: int):
    """Remove a song by queue position (1-based)."""
    state   = get_state(ctx.guild.id)
    removed = state.remove_at(index - 1)
    if removed is None:
        return await ctx.send(f"❌ No song at position {index}.")
    await ctx.send(f"🗑️ Removed: **{removed['title']}**")


@bot.command(name="move", aliases=["mv"])
async def move_cmd(ctx: commands.Context, from_pos: int, to_pos: int):
    """Move a song in the queue: !move <from> <to> (1-based)."""
    state = get_state(ctx.guild.id)
    ok    = state.move(from_pos - 1, to_pos - 1)
    if not ok:
        return await ctx.send(f"❌ Invalid positions (queue length: {state.length}).")
    songs = state.snapshot()
    await ctx.send(f"↕️ Moved **{songs[to_pos - 1]['title']}** to position {to_pos}.")


@bot.command(name="seek")
async def seek_cmd(ctx: commands.Context, seconds: int):
    """Seek to a position (seconds) in the current song."""
    state = get_state(ctx.guild.id)
    song  = state.now_playing

    if not song or not ctx.voice_client:
        return await ctx.send("❌ Nothing is playing.")

    if not ctx.voice_client.is_playing() and not ctx.voice_client.is_paused():
        return await ctx.send("❌ Nothing is playing.")

    duration = song.get("duration", 0)
    if not 0 <= seconds <= duration:
        return await ctx.send(f"❌ Seek must be between 0 and {int(duration)}s.")

    # 1. Snapshot the song BEFORE stopping (stop() triggers after_play
    #    which may mutate now_playing / the queue)
    seek_song = dict(song)

    # 2. Set flag so after_play knows to do nothing
    state.is_seeking = True
    state.request_skip()          # suppress loop re-enqueue inside after_play
    ctx.voice_client.stop()       # after_play fires but consume_skip() == True
                                  # so no re-enqueue; is_seeking blocks play_next

    # Small yield so the after_play callback can complete
    await asyncio.sleep(0.3)
    state.is_seeking = False

    ffmpeg_opts = _build_ffmpeg_opts(seek_song, seek_secs=seconds)
    try:
        raw    = discord.FFmpegPCMAudio(seek_song["stream_url"], **ffmpeg_opts)
        source = discord.PCMVolumeTransformer(raw, volume=state.volume)
    except Exception as e:
        return await ctx.send(f"❌ FFmpeg error during seek: {e}")

    def after_seek(error):
        if error:
            print(f"⚠️  Seek playback error: {error}")
        # Normal after_play logic: respect loop mode
        if state.loop_mode == LOOP_SINGLE:
            state.enqueue_left(seek_song)
        elif state.loop_mode == LOOP_QUEUE:
            state.enqueue(seek_song)
        asyncio.run_coroutine_threadsafe(play_next(ctx), bot.loop)

    state.now_playing = seek_song
    ctx.voice_client.play(source, after=after_seek)
    await ctx.send(f"⏩ Seeked to **{seconds}s** in **{seek_song['title']}**.")


@bot.command(name="playnext", aliases=["pn"])
async def play_next_cmd(ctx: commands.Context, *, query: str):
    """Fetch a song and place it at the front of the queue."""
    if not await _ensure_voice(ctx):
        return

    state = get_state(ctx.guild.id)
    await ctx.send(f"🔍 Fetching (next up): `{query}`…")
    state.inc_pending()

    try:
        song = await fetch_audio_async(query)
    except asyncio.TimeoutError:
        await ctx.send(f"❌ Timed out fetching: `{query}`")
        return
    except Exception as e:
        await ctx.send(f"❌ Error fetching audio: {e}")
        return
    finally:
        state.dec_pending()

    state.enqueue_left(song)

    if ctx.voice_client.is_playing() or ctx.voice_client.is_paused():
        await ctx.send(f"⏫ Playing next: **{song['title']}**")
    else:
        await play_next(ctx)


@bot.command(name="clearqueue", aliases=["cq"])
async def clear_queue_cmd(ctx: commands.Context):
    """Clear all songs from the queue without stopping current playback."""
    state = get_state(ctx.guild.id)
    count = state.length
    state.clear()
    await ctx.send(f"🗑️ Cleared {count} song(s) from the queue.")


@bot.command(name="help", aliases=["h"])
async def help_cmd(ctx: commands.Context):
    """Show all available commands."""
    embed = discord.Embed(
        title="🎵 Music Bot Commands",
        color=discord.Color.blurple(),
    )
    commands_list = [
        ("!play / !p `<query>`",        "Play a song or add to queue"),
        ("!playnext / !pn `<query>`",   "Play a song next"),
        ("!skip / !s",                  "Skip current song"),
        ("!pause",                      "Pause playback"),
        ("!resume",                     "Resume playback"),
        ("!stop",                       "Stop and disconnect"),
        ("!leave",                      "Disconnect from voice"),
        ("!queue / !q",                 "Show the queue"),
        ("!nowplaying / !np",           "Show current song"),
        ("!loop / !l `[off|single|queue]`", "Set loop mode"),
        ("!volume / !v `[1-100]`",      "Set or view volume"),
        ("!shuffle",                    "Shuffle the queue"),
        ("!remove / !rm `<pos>`",       "Remove song at position"),
        ("!move / !mv `<from> <to>`",   "Move song in queue"),
        ("!seek `<seconds>`",           "Seek within current song"),
        ("!clearqueue / !cq",           "Clear the queue"),
    ]
    for name, value in commands_list:
        embed.add_field(name=name, value=value, inline=True)

    await ctx.send(embed=embed)

# ─────────────────────────────────────────────
#  Events
# ─────────────────────────────────────────────
@bot.event
async def on_ready():
    print(f"✅ Logged in as {bot.user} (ID: {bot.user.id})")
    await bot.change_presence(
        activity=discord.Activity(
            type=discord.ActivityType.listening,
            name="!play | !help",
        )
    )


@bot.event
async def on_voice_state_update(
    member: discord.Member,
    before: discord.VoiceState,
    after:  discord.VoiceState,
):
    guild = before.channel.guild if before.channel else None
    if guild is None:
        return

    # Bot was forcibly disconnected → clean up
    if member == bot.user and before.channel and not after.channel:
        remove_state(guild.id)
        print(f"🔌 Bot disconnected from {guild.name}; state cleaned up.")
        return

    # Auto-leave if everyone else leaves
    vc = guild.voice_client
    if vc and before.channel == vc.channel:
        non_bots = [m for m in vc.channel.members if not m.bot]
        if not non_bots:
            await vc.disconnect()
            remove_state(guild.id)
            print(f"🚪 Auto-left {guild.name} — channel empty.")


@bot.event
async def on_command_error(ctx: commands.Context, error):
    if isinstance(error, commands.CommandNotFound):
        return
    if isinstance(error, commands.MissingRequiredArgument):
        await ctx.send(f"❌ Missing argument: `{error.param.name}`")
    elif isinstance(error, commands.BadArgument):
        await ctx.send(f"❌ Bad argument: {error}")
    elif isinstance(error, commands.CommandInvokeError):
        await ctx.send(f"❌ Command error: {error.original}")
        raise error
    else:
        await ctx.send(f"❌ Unexpected error: {error}")
        raise error

# ─────────────────────────────────────────────
#  Keepalive web server (Hugging Face)
# ─────────────────────────────────────────────
_web_start_time = time.time()
app = Flask(__name__)

@app.route("/")
def home():
    return "🎵 Music bot is running!", 200

@app.route("/health")
def health():
    return jsonify({
        "status":         "ok",
        "uptime_seconds": int(time.time() - _web_start_time),
    }), 200

def _run_web():
    port = int(os.environ.get("PORT", 7860))
    app.run(host="0.0.0.0", port=port, use_reloader=False)

# ─────────────────────────────────────────────
#  Entry point
# ─────────────────────────────────────────────
if __name__ == "__main__":
    threading.Thread(target=_run_web, daemon=True, name="web-server").start()

    token = os.getenv("DISCORD_TOKEN")
    if not token:
        raise RuntimeError("DISCORD_TOKEN environment variable is not set!")

    bot.run(token)