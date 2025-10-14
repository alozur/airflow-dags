# Custom Airflow image with ffmpeg support
# Based on Apache Airflow 2.10.2

FROM apache/airflow:2.10.2

# Switch to root user to install system packages
USER root

# Install ffmpeg and dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    ffmpeg \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Verify ffmpeg installation
RUN ffmpeg -version

# Switch back to airflow user
USER airflow

# Note: Python packages are installed via _PIP_ADDITIONAL_REQUIREMENTS in docker-compose.yml
# This keeps the image flexible and allows package updates without rebuilding the image
