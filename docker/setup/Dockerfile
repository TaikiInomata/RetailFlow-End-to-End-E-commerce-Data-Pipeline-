FROM python:3.10-slim

WORKDIR /app

# Install curl for API requests
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Copy requirements first to leverage Docker layer caching
COPY requirements.txt .

# Install python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy specific directories needed for setup
COPY common/ ./common/
COPY scripts/ ./scripts/
COPY config/debezium/ ./config/debezium/
COPY config/s3/ ./config/s3/
COPY docker/setup/entrypoint.sh ./docker/setup/entrypoint.sh

# Set entrypoint
RUN chmod +x ./docker/setup/entrypoint.sh
CMD ["./docker/setup/entrypoint.sh"]

