# Custom Airflow image with ffmpeg and Deno support
# Based on Apache Airflow 2.10.2
# Build with: docker build -t my-airflow:latest .

FROM apache/airflow:2.10.2

# Switch to root user to install system packages
USER root

# Install ffmpeg, curl, unzip, nodejs, and git (for DAG-based git sync)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    ffmpeg \
    curl \
    unzip \
    nodejs \
    npm \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Verify installations
RUN ffmpeg -version && node --version

# Switch back to airflow user
USER airflow

# Note: Python packages are installed via _PIP_ADDITIONAL_REQUIREMENTS in docker-compose.yml
# This keeps the image flexible and allows package updates without rebuilding the image
