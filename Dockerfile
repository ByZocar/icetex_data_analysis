# ─────────────────────────────────────────────────────────────────────────────
# Dockerfile — ICETEX ETL Airflow Image
# Extends the official Apache Airflow 2.8.1 image and installs the project's
# Python dependencies while respecting Airflow's constraint file to avoid
# dependency conflicts with the scheduler/webserver core packages.
#
# Build:  docker compose build
# Push:   docker tag icetex-airflow <registry>/icetex-airflow:latest
# ─────────────────────────────────────────────────────────────────────────────
FROM apache/airflow:2.8.1-python3.10

# Airflow's official image already runs as the 'airflow' user (UID 50000).
# Switch to root only for OS-level packages, then drop back immediately.
USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
        # Required to compile psycopg2 and some C-extension packages
        build-essential \
        libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Drop back to the unprivileged airflow user for all pip operations.
# This matches Airflow's security model and avoids pip root warnings.
USER airflow

# Copy only requirements — cache this layer independently of source code changes.
COPY --chown=airflow:root requirements.txt /opt/airflow/requirements.txt

# Install project dependencies.
# The --constraint flag pins transitive deps to the versions Airflow 2.8.1
# was tested with, preventing silent version conflicts in the scheduler.
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.1/constraints-3.10.txt" \
    -r /opt/airflow/requirements.txt
