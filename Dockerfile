FROM python:3.11-slim

# Install FFmpeg and prerequisites for NodeSource
RUN apt-get update && apt-get install -y ffmpeg curl gnupg && rm -rf /var/lib/apt/lists/*

# Install modern Node.js 20 via NodeSource (Debian's default is broken/missing for yt-dlp)
RUN curl -fsSL https://deb.nodesource.com/setup_20.x | bash - && \
    apt-get install -y nodejs && \
    rm -rf /var/lib/apt/lists/*

# Verify Node is working
RUN node --version

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY bot.py .
COPY cookies.txt .

EXPOSE 7860

CMD ["python", "bot.py"]