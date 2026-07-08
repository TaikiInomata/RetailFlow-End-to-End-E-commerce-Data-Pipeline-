# ============================================================
# Simulation Image — retailflow-simulation
# Base: python:3.11-slim (nhẹ — không cần Java/Spark)
# Role: Chạy clickstream_bot.py + mock_backend.py song song
# ============================================================
FROM python:3.11-slim

WORKDIR /app

# Cài build dependencies tối thiểu cho psycopg2-binary
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Cài Python dependencies
COPY requirements-python.txt .
RUN pip install --no-cache-dir -r requirements-python.txt

# Copy simulation scripts và utils
COPY scripts/simulation/ ./scripts/simulation/
COPY scripts/utils/ ./scripts/utils/

# Copy startup script
COPY docker/simulation/start.sh ./start.sh
RUN chmod +x ./start.sh

# Entrypoint: chạy cả 2 bots song song
CMD ["./start.sh"]
