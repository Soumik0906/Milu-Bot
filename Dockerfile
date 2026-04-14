# Use a lightweight Python base image
FROM python:3.10-slim

# Install FFmpeg (Required for audio processing)
RUN apt-get update && apt-get install -y ffmpeg && rm -rf /var/lib/apt/lists/*

# Set the working directory
WORKDIR /app

# Copy requirements and install them
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the bot code and cookie file
COPY cookies.txt .
COPY bot.py .

# Hugging Face Spaces expects traffic on port 7860
EXPOSE 7860

# Run the bot
CMD ["python", "bot.py"]