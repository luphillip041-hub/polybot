FROM python:3.12-slim

WORKDIR /app

# Install system deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy and install deps first (layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the project
COPY polymarket_bot/ polymarket_bot/
COPY scripts/ scripts/
COPY archive_config.json .
COPY .env.example .env

# Create runs directory
RUN mkdir -p runs

EXPOSE 8080 8710

CMD ["python", "-u", "-m", "polymarket_bot.cli"]
