FROM python:3.12-slim

# HuggingFace Spaces requires the app to listen on port 7860
# and runs as a non-root user (uid 1000).

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user (HF Spaces requirement)
RUN useradd -m -u 1000 appuser

WORKDIR /app

# Install Python deps as root first (faster layer cache)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app source
COPY . .

# Create the tmp download directory and set permissions
RUN mkdir -p /tmp/tg_imports && chown -R appuser:appuser /app /tmp/tg_imports

USER 1000

# Support dynamic PORT (defaults to 8080 for Replit/Docker, 7860 for HF Spaces)
EXPOSE 8080 7860

ENV PORT=8080

CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port ${PORT:-8080}"]
