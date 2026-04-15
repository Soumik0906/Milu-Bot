FROM python:3.11-slim

WORKDIR /app

# Install FFmpeg + Deno (the recommended JS runtime for yt-dlp EJS).
#
# WHY DENO INSTEAD OF NODE.JS:
#   - yt-dlp EJS requires Node.js >= 20.0.0, but Debian Bookworm only ships 18.x
#   - Deno is yt-dlp's *recommended* runtime and is auto-detected (no config needed)
#   - Deno runs EJS scripts in a sandbox with restricted permissions
#   - Installs as a single static binary — no version-management headaches
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ffmpeg \
        curl \
        unzip \
        ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Install Deno (latest stable)
RUN curl -fsSL https://deno.land/install.sh | DENO_INSTALL=/usr/local sh
RUN deno --version

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY bot.py .
COPY cookies.txt .

EXPOSE 7860

CMD ["python", "bot.py"]