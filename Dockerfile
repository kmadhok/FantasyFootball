FROM python:3.12-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/ ./src/
COPY .env .env

# Create data directory for SQLite
RUN mkdir -p data

# Expose port (optional, for health checks or API)
EXPOSE 8000

# Run the application
CMD ["python", "-m", "src.main"]