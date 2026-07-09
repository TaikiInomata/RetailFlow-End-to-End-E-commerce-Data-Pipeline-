FROM python:3.10-slim

WORKDIR /app

# Install curl for API requests
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Copy project files
COPY . /app/

# Install python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Set entrypoint
RUN chmod +x /app/docker/setup/entrypoint.sh
CMD ["/app/docker/setup/entrypoint.sh"]

