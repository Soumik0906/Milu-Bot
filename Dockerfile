FROM python:3.11-slim

WORKDIR /app

# Install FFmpeg and Node.js from Debian's official repositories.
# Debian Bookworm includes Node.js 18, which is the minimum required by yt-dlp.
# This guarantees all system dependencies/shared libraries are perfectly matched.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ffmpeg \
        nodejs \
        ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Verify Node is working during build
RUN node --version

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY bot.py .
COPY cookies.txt .

EXPOSE 7860

CMD ["python", "bot.py"]