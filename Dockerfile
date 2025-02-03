# Use the official Python 3.9 slim image
FROM python:3.9-slim

# Install system-level dependencies needed for building packages like matplotlib (if needed)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    g++ \
    pkg-config \
    libfreetype6-dev \
    libpng-dev \
 && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --upgrade pip setuptools wheel
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files into container
COPY . .

# Mount volume in Docker Compose or via docker run to see live changes
CMD ["python", "pipeline.py"]
