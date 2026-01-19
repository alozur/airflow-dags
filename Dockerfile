# Custom Airflow image with ffmpeg and Deno support
# Based on Apache Airflow 2.10.2
# Build with: docker build -t airflow-custom:2.10.2-ffmpeg-deno .

FROM apache/airflow:2.10.2

# Switch to root user to install system packages
USER root

# Install ffmpeg, curl, unzip, and nodejs (as alternative JS runtime for yt-dlp)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    ffmpeg \
    curl \
    unzip \
    nodejs \
    npm \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Verify installations
RUN ffmpeg -version && node --version

# Switch back to airflow user
USER airflow

# Note: Python packages are installed via _PIP_ADDITIONAL_REQUIREMENTS in docker-compose.yml
# This keeps the image flexible and allows package updates without rebuilding the image
