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

import re
import json
import urllib.request
import urllib.parse

# ─────────────────────────────────────────────
#  Startup diagnostics
# ─────────────────────────────────────────────

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

# Write cookies from environment secret if on server
if not os.getenv("DEBUG", ""):
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
        self._play_lock = None
        self.tracker = PlaybackTracker()
        self.silent          = True

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
        
    # get lock
    def get_play_lock(self):
        if self._play_lock is None:
            self._play_lock = asyncio.Lock()
        return self._play_lock


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
#  Monkeypatch Context.send for global silent mode support
# ─────────────────────────────────────────────
_original_send = commands.Context.send

async def _silent_send(self, *args, **kwargs):
    if self.guild:
        state = get_state(self.guild.id)
        if state.silent:
            kwargs["silent"] = True
    return await _original_send(self, *args, **kwargs)

commands.Context.send = _silent_send

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
#  Playback position tracking
# ─────────────────────────────────────────────
class PlaybackTracker:
    """
    Tracks elapsed playback time for the current song.
    Thread-safe. Call start() when playback begins,
    pause()/resume() on pause events, reset() on stop.
    """

    def __init__(self):
        self._lock        = _threading.Lock()
        self._start_time  = None   # monotonic time when playback started
        self._offset      = 0.0    # seconds already elapsed before current start
        self._paused      = False
        self._pause_time  = None

    def start(self, offset: float = 0.0) -> None:
        with self._lock:
            self._offset     = offset
            self._start_time = time.monotonic()
            self._paused     = False
            self._pause_time = None

    def pause(self) -> None:
        with self._lock:
            if not self._paused and self._start_time is not None:
                self._paused     = True
                self._pause_time = time.monotonic()

    def resume(self) -> None:
        with self._lock:
            if self._paused and self._start_time is not None:
                # Shift start forward by the paused duration
                paused_for       = time.monotonic() - self._pause_time
                self._start_time += paused_for
                self._paused     = False
                self._pause_time = None

    def reset(self) -> None:
        with self._lock:
            self._start_time = None
            self._offset     = 0.0
            self._paused     = False
            self._pause_time = None

    @property
    def position(self) -> float:
        """Current playback position in seconds."""
        with self._lock:
            if self._start_time is None:
                return 0.0
            if self._paused:
                elapsed = self._pause_time - self._start_time
            else:
                elapsed = time.monotonic() - self._start_time
            return self._offset + elapsed


# ─────────────────────────────────────────────
#  Search result registry (per guild, per user)
# ─────────────────────────────────────────────
# Structure: { guild_id: { user_id: [song_dict, ...] } }
_search_results: dict[int, dict[int, list]] = {}
_search_lock = _threading.Lock()

def _store_search(guild_id: int, user_id: int, results: list) -> None:
    with _search_lock:
        if guild_id not in _search_results:
            _search_results[guild_id] = {}
        _search_results[guild_id][user_id] = results

def _get_search(guild_id: int, user_id: int) -> list | None:
    with _search_lock:
        return _search_results.get(guild_id, {}).get(user_id)

def _clear_search(guild_id: int, user_id: int) -> None:
    with _search_lock:
        _search_results.get(guild_id, {}).pop(user_id, None)


def fetch_search_results(query: str, max_results: int = 5) -> list[dict]:
    """
    Blocking — run in executor.
    Returns up to max_results song dicts (with stream URLs already resolved).
    """
    opts = {**_YDL_OPTS, "noplaylist": True}
    search_query = f"ytsearch{max_results}:{query}"

    with yt_dlp.YoutubeDL(opts) as ydl:
        info = ydl.extract_info(search_query, download=False)

    entries = [e for e in info.get("entries", []) if e]
    results = []
    for entry in entries[:max_results]:
        url = _best_url(entry)
        if not url:
            continue
        results.append({
            "title":        entry.get("title", "Unknown Title"),
            "duration":     entry.get("duration") or 0,
            "stream_url":   url,
            "http_headers": entry.get("http_headers", {}),
            "webpage_url":  entry.get("webpage_url", ""),
            "fetched_at":   time.time(),
            "channel":      entry.get("channel") or entry.get("uploader", "Unknown"),
        })
    return results


# ─────────────────────────────────────────────
#  Seek helpers
# ─────────────────────────────────────────────
def _progress_bar(position: float, duration: float, width: int = 20) -> str:
    """Unicode progress bar, e.g.  ██████░░░░░░░░  2:14 / 5:00"""
    if duration <= 0:
        return ""
    filled  = int(width * position / duration)
    filled  = max(0, min(filled, width))
    bar     = "█" * filled + "░" * (width - filled)
    pos_str = _fmt_time(int(position))
    dur_str = _fmt_time(int(duration))
    return f"`{bar}` {pos_str} / {dur_str}"


def _fmt_time(seconds: int) -> str:
    """Format seconds → M:SS or H:MM:SS."""
    seconds = max(0, seconds)
    h, rem  = divmod(seconds, 3600)
    m, s    = divmod(rem, 60)
    if h:
        return f"{h}:{m:02d}:{s:02d}"
    return f"{m}:{s:02d}"


def _parse_time(raw: str) -> int | None:
    """
    Parse a time string into seconds.
    Accepts:  90  |  1:30  |  1h30m  |  1h  |  30s
    Returns None on failure.
    """
    raw = raw.strip().lower()

    # Plain integer seconds
    if raw.isdigit():
        return int(raw)

    # MM:SS or HH:MM:SS
    if ":" in raw:
        parts = raw.split(":")
        try:
            parts = [int(p) for p in parts]
        except ValueError:
            return None
        if len(parts) == 2:
            return parts[0] * 60 + parts[1]
        if len(parts) == 3:
            return parts[0] * 3600 + parts[1] * 60 + parts[2]
        return None

    # e.g. 1h30m20s  /  2h  /  45m  /  30s
    import re
    pattern = re.fullmatch(
        r"(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?", raw
    )
    if pattern and pattern.group(0):
        h = int(pattern.group(1) or 0)
        m = int(pattern.group(2) or 0)
        s = int(pattern.group(3) or 0)
        total = h * 3600 + m * 60 + s
        return total if total > 0 else None

    return None


async def _do_seek(ctx: commands.Context, seconds: float) -> None:
    """
    Core seek routine shared by !seek, !forward, and !rewind.
    seconds is the absolute target position.
    """
    state = get_state(ctx.guild.id)
    song  = state.now_playing

    if not song:
        await ctx.send("❌ Nothing is playing.")
        return

    if not ctx.voice_client or (
        not ctx.voice_client.is_playing() and not ctx.voice_client.is_paused()
    ):
        await ctx.send("❌ Nothing is playing.")
        return

    duration = song.get("duration", 0)
    seconds  = max(0.0, min(float(seconds), float(duration) - 1))

    seek_song = dict(song)   # snapshot before stop()

    # Signal: suppress loop re-enqueue and block play_next re-entry
    state.is_seeking = True
    state.request_skip()
    ctx.voice_client.stop()

    # Let the after_play callback fire and exit cleanly
    await asyncio.sleep(0.15)
    state.is_seeking = False

    ffmpeg_opts = _build_ffmpeg_opts(seek_song, seek_secs=int(seconds))

    try:
        raw    = discord.FFmpegPCMAudio(seek_song["stream_url"], **ffmpeg_opts)
        source = discord.PCMVolumeTransformer(raw, volume=state.volume)
    except Exception as e:
        await ctx.send(f"❌ FFmpeg error during seek: {e}")
        return

    def after_seek(err):
        if err:
            print(f"⚠️  Seek playback error: {err}")
        if state.loop_mode == LOOP_SINGLE:
            state.enqueue_left(seek_song)
        elif state.loop_mode == LOOP_QUEUE:
            state.enqueue(seek_song)
        asyncio.run_coroutine_threadsafe(play_next(ctx), bot.loop)

    state.now_playing = seek_song
    state.tracker.start(offset=seconds)            # ← accurate position tracking
    ctx.voice_client.play(source, after=after_seek)

    bar = _progress_bar(seconds, duration)
    await ctx.send(
        f"⏩ **{seek_song['title']}**\n"
        f"{bar}"
    )

# --- Lyrics (LRCLIB) ---
_LRCLIB_SEARCH = "https://lrclib.net/api/search"
_LRCLIB_UA     = f"MyBot/1.0 (Discord music bot)"


def _clean_title(title: str) -> str:
    """
    Strip common YouTube noise like '(Official Video)', '[HD]', etc.
    to improve lyrics search accuracy.
    """
    noise = re.compile(
        r"[\(\[](?:official\s*(?:video|audio|music\s*video|lyric\s*video)?|"
        r"lyrics?|hd|4k|visualizer|live|explicit|audio|mv|m/v|remastered"
        r"|\d{4}\s*remaster)[\)\]]",
        re.IGNORECASE,
    )
    title = noise.sub("", title).strip()
    # Also strip trailing whitespace and dashes left over
    title = re.sub(r"\s*[-–—]\s*$", "", title).strip()
    return title


def _fetch_lyrics(title: str) -> str | None:
    """
    Blocking — run in executor.
    Queries LRCLIB's free search API and returns plain lyrics or None.
    """
    cleaned = _clean_title(title)
    params  = urllib.parse.urlencode({"q": cleaned})
    url     = f"{_LRCLIB_SEARCH}?{params}"

    req = urllib.request.Request(url, headers={"User-Agent": _LRCLIB_UA})

    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
    except Exception as e:
        print(f"⚠️  LRCLIB fetch error: {e}")
        return None

    if not data or not isinstance(data, list):
        return None

    # Prefer results that have plain lyrics, skip instrumentals
    for entry in data:
        if entry.get("instrumental"):
            continue
        lyrics = entry.get("plainLyrics")
        if lyrics:
            return lyrics

    return None


async def _fetch_lyrics_async(title: str) -> str | None:
    loop = asyncio.get_event_loop()
    return await asyncio.wait_for(
        loop.run_in_executor(executor, _fetch_lyrics, title),
        timeout=15.0,
    )


def _chunk_lyrics(lyrics: str, limit: int = 1900) -> list[str]:
    """
    Split lyrics into Discord-safe chunks (<= limit chars),
    breaking on newlines where possible.
    """
    chunks = []
    current = ""
    for line in lyrics.splitlines(keepends=True):
        if len(current) + len(line) > limit:
            if current:
                chunks.append(current)
            # If a single line is huge, hard-split it
            while len(line) > limit:
                chunks.append(line[:limit])
                line = line[limit:]
            current = line
        else:
            current += line
    if current:
        chunks.append(current)
    return chunks
# -------------------------

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
    state = get_state(ctx.guild.id)

    if state.is_seeking:
        return

    # PREVENT CONCURRENT play_next CALLS
    if state.get_play_lock().locked():
        return

    async with state.get_play_lock():
        if error_count >= MAX_ERRORS_IN_A_ROW:
            await ctx.send("❌ Too many consecutive errors. Stopping.")
            if ctx.voice_client:
                await ctx.voice_client.disconnect()
            return

        if state.empty:
            if state.has_pending:
                await ctx.send("⏳ Waiting for songs to finish fetching…")
                for _ in range(60):
                    await asyncio.sleep(1)
                    if not state.empty:
                        break

            if state.empty:
                state.now_playing = None
                await ctx.send("✅ Queue empty.")
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
        state.tracker.start(offset=0.0)
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

    if ctx.voice_client and (ctx.voice_client.is_playing() or ctx.voice_client.is_paused()):
        await ctx.send(f"➕ Added to queue (#{state.length}): **{song['title']}**")
    elif not state.get_play_lock().locked():
        await play_next(ctx)
    else:
        await ctx.send(f"➕ Added to queue (#{state.length}): **{song['title']}**")


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
        get_state(ctx.guild.id).tracker.pause()
        await ctx.send("⏸️ Paused.")
    else:
        await ctx.send("❌ Nothing is playing.")


@bot.command(name="resume")
async def resume(ctx: commands.Context):
    """Resume paused playback."""
    if ctx.voice_client and ctx.voice_client.is_paused():
        ctx.voice_client.resume()
        get_state(ctx.guild.id).tracker.resume()
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

    position = state.tracker.position
    bar      = _progress_bar(position, song.get("duration", 0))

    embed = discord.Embed(
        title="🎵 Now Playing",
        description=f"**{song['title']}**\n{bar}",
        color=discord.Color.blurple(),
    )
    # embed.add_field(name="⏱️ Duration", value=f"{mins}:{secs:02d}")
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


@bot.command(name="silent")
async def silent_cmd(ctx: commands.Context):
    """Toggle silent mode (no notifications)."""
    state = get_state(ctx.guild.id)
    state.silent = not state.silent
    status = "ON" if state.silent else "OFF"
    await ctx.send(f"🔇 Silent mode is now **{status}**.")


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



# ─────────────────────────────────────────────
#  Seek commands 
# ─────────────────────────────────────────────
@bot.command(name="seek")
async def seek_cmd(ctx: commands.Context, *, position: str):
    """
    Seek to a position in the current song.

    Accepts:  !seek 90        (seconds)
              !seek 1:30      (MM:SS)
              !seek 1h30m     (hours/minutes)
              !seek 1h30m20s
    """
    seconds = _parse_time(position)
    if seconds is None:
        return await ctx.send(
            "❌ Invalid time format.\n"
            "Examples: `90` · `1:30` · `1h30m` · `2h15m30s`"
        )

    song = get_state(ctx.guild.id).now_playing
    if song:
        duration = song.get("duration", 0)
        if seconds > duration:
            return await ctx.send(
                f"❌ `{_fmt_time(seconds)}` is past the end of the song "
                f"(duration: `{_fmt_time(int(duration))}`)."
            )

    await _do_seek(ctx, seconds)


@bot.command(name="forward", aliases=["ff"])
async def forward_cmd(ctx: commands.Context, *, amount: str = "30"):
    """
    Skip forward by an amount (default 30 s).

    !forward          → +30 s
    !forward 60       → +60 s
    !forward 1:30     → +1 m 30 s
    !forward 2m       → +2 minutes
    """
    delta = _parse_time(amount)
    if delta is None:
        return await ctx.send(
            "❌ Invalid time format. Examples: `30` · `1:30` · `2m`"
        )

    state    = get_state(ctx.guild.id)
    position = state.tracker.position + delta
    await _do_seek(ctx, position)


@bot.command(name="rewind", aliases=["rw"])
async def rewind_cmd(ctx: commands.Context, *, amount: str = "30"):
    """
    Rewind by an amount (default 30 s).

    !rewind           → −30 s
    !rewind 60        → −60 s
    !rewind 1:30      → −1 m 30 s
    !rewind 2m        → −2 minutes
    """
    delta    = _parse_time(amount)
    if delta is None:
        return await ctx.send(
            "❌ Invalid time format. Examples: `30` · `1:30` · `2m`"
        )

    state    = get_state(ctx.guild.id)
    position = max(0.0, state.tracker.position - delta)
    await _do_seek(ctx, position)


@bot.command(name="position", aliases=["pos"])
async def position_cmd(ctx: commands.Context):
    """Show current playback position with a progress bar."""
    state = get_state(ctx.guild.id)
    song  = state.now_playing

    if not song or not ctx.voice_client or (
        not ctx.voice_client.is_playing() and not ctx.voice_client.is_paused()
    ):
        return await ctx.send("❌ Nothing is playing right now.")

    position = state.tracker.position
    duration = song.get("duration", 0)
    bar      = _progress_bar(position, duration)
    status   = "⏸️ Paused" if ctx.voice_client.is_paused() else "▶️ Playing"

    embed = discord.Embed(
        title=f"🎵 {song['title']}",
        description=bar,
        color=discord.Color.blurple(),
    )
    embed.add_field(name="Status",   value=status)
    embed.add_field(name="Position", value=f"`{_fmt_time(int(position))}`")
    embed.add_field(name="Duration", value=f"`{_fmt_time(int(duration))}`")

    await ctx.send(embed=embed)


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
        ("!play / !p `<query>`",         "Play a song or add to queue"),
        ("!playnext / !pn `<query>`",    "Play a song next"),
        ("!skip / !s",                   "Skip current song"),
        ("!pause",                       "Pause playback"),
        ("!resume",                      "Resume playback"),
        ("!stop",                        "Stop and disconnect"),
        ("!leave",                       "Disconnect from voice"),
        ("!queue / !q",                  "Show the queue"),
        ("!nowplaying / !np",            "Show current song"),
        ("!loop / !l `[off|single|queue]`", "Set loop mode"),
        ("!volume / !v `[1-100]`",       "Set or view volume"),
        ("!silent",                      "Toggle silent mode (no notifications)"),
        ("!shuffle",                     "Shuffle the queue"),
        ("!remove / !rm `<pos>`",        "Remove song at position"),
        ("!move / !mv `<from> <to>`",    "Move song in queue"),
        ("!clearqueue / !cq",            "Clear the queue"),
        ("!search / !se `<query>`",      "Search YouTube and pick a result"),
        ("!pick / !pk `<number>`",       "Pick a search result to play"),
        ("!seek `<time>`",               "Seek to position (90, 1:30, 1h30m)"),
        ("!forward / !ff `[time]`",      "Skip forward (default 30s)"),
        ("!rewind / !rw `[time]`",       "Rewind (default 30s)"),
        ("!position / !pos",             "Show playback position"),
        ("!lyrics / !ly `[query]`",      "Show lyrics for current song or query"),
    ]
    for name, value in commands_list:
        embed.add_field(name=name, value=value, inline=True)

    await ctx.send(embed=embed)

@bot.command(name="search", aliases=["se"])
async def search_cmd(ctx: commands.Context, *, query: str):
    """Search YouTube and pick a result to play."""
    if not await _ensure_voice(ctx):
        return

    await ctx.send(f"🔍 Searching for: `{query}`…")

    try:
        results = await asyncio.wait_for(
            asyncio.get_event_loop().run_in_executor(
                executor, fetch_search_results, query
            ),
            timeout=FETCH_TIMEOUT_SECS,
        )
    except asyncio.TimeoutError:
        return await ctx.send("❌ Search timed out.")
    except Exception as e:
        return await ctx.send(f"❌ Search error: {e}")

    if not results:
        return await ctx.send("❌ No results found.")

    # Store results so !pick can retrieve them
    _store_search(ctx.guild.id, ctx.author.id, results)

    # Build the results embed
    embed = discord.Embed(
        title=f"🔎 Search results for: {query}",
        description="Type `!pick <number>` to queue a song, or `!pick cancel` to cancel.",
        color=discord.Color.blurple(),
    )
    for i, song in enumerate(results, start=1):
        mins, secs = divmod(int(song["duration"]), 60)
        duration_str = f"{mins}:{secs:02d}" if song["duration"] else "Unknown"
        embed.add_field(
            name=f"{i}. {song['title']}",
            value=f"📺 {song['channel']}  |  ⏱️ {duration_str}",
            inline=False,
        )

    await ctx.send(embed=embed)


@bot.command(name="pick", aliases=["pk"])
async def pick_cmd(ctx: commands.Context, choice: str):
    """Pick a search result by number, or 'cancel'."""
    if choice.lower() == "cancel":
        _clear_search(ctx.guild.id, ctx.author.id)
        return await ctx.send("❌ Search cancelled.")

    # Validate input is a number
    if not choice.isdigit():
        return await ctx.send("❌ Please enter a number or `cancel`.")

    results = _get_search(ctx.guild.id, ctx.author.id)
    if not results:
        return await ctx.send("❌ No active search. Use `!search <query>` first.")

    index = int(choice) - 1
    if not 0 <= index < len(results):
        return await ctx.send(f"❌ Pick a number between 1 and {len(results)}.")

    song = results[index]
    _clear_search(ctx.guild.id, ctx.author.id)

    state = get_state(ctx.guild.id)

    if not state.enqueue(song):
        return await ctx.send(f"❌ Queue is full ({MAX_QUEUE_SIZE} songs max).")

    if ctx.voice_client and (ctx.voice_client.is_playing() or ctx.voice_client.is_paused()):
        await ctx.send(f"➕ Added to queue (#{state.length}): **{song['title']}**")
    elif not state.get_play_lock().locked():
        await play_next(ctx)
    else:
        await ctx.send(f"➕ Added to queue (#{state.length}): **{song['title']}**")


# ─────────────────────────────────────────────
#  Lyrics
# ───────────────────────────────────────────── 

@bot.command(name="lyrics", aliases=["ly"])
async def lyrics_cmd(ctx: commands.Context, *, query: str = None):
    """
    Show lyrics for the current song or a specific query.

    !lyrics               → lyrics for whatever is playing
    !lyrics Bohemian Rhapsody
    !lyrics Queen - Bohemian Rhapsody
    """

    # Determine what to look up
    if query is None:
        state = get_state(ctx.guild.id)
        song  = state.now_playing
        if not song:
            return await ctx.send(
                "❌ Nothing is playing. Use `!lyrics <song name>` to search."
            )
        title = song["title"]
    else:
        title = query

    await ctx.send(f"🔍 Fetching lyrics for: **{title}**…")

    try:
        lyrics = await _fetch_lyrics_async(title)
    except asyncio.TimeoutError:
        return await ctx.send("❌ Lyrics fetch timed out.")
    except Exception as e:
        return await ctx.send(f"❌ Lyrics error: {e}")

    if not lyrics:
        return await ctx.send(f"❌ No lyrics found for **{title}**.")

    chunks = _chunk_lyrics(lyrics)

    # Send first chunk as embed, rest as plain text
    embed = discord.Embed(
        title=f"🎤 Lyrics — {title}",
        description=f"```{chunks[0]}```",
        color=discord.Color.green(),
    )
    embed.set_footer(text=f"Page 1/{len(chunks)} · Powered by LRCLIB")
    await ctx.send(embed=embed)

    # Remaining chunks (rare for short songs)
    for i, chunk in enumerate(chunks[1:], start=2):
        await ctx.send(f"```{chunk}```  *(page {i}/{len(chunks)})*")


# ─────────────────────────────────────────────
#  Events
# ─────────────────────────────────────────────
@bot.event
async def on_ready():
    print(f"✅ Logged in as {bot.user} (ID: {bot.user.id})")
    await bot.change_presence(
        activity=discord.Activity(
            type=discord.ActivityType.listening,
            name="!help",
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