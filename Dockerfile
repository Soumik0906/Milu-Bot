FROM python:3.11-slim

# Install FFmpeg AND Node.js (Node is required by yt-dlp to solve YouTube signatures)
RUN apt-get update && apt-get install -y ffmpeg nodejs && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY bot.py .
COPY cookies.txt .

EXPOSE 7860

CMD ["python", "bot.py"]