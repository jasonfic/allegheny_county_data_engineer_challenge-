FROM python:3.11-slim-bullseye

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy pipeline project files
COPY config/ /app/config/
COPY dbt_project/ /app/dbt_project/
COPY flows/ /app/flows/

# Create persistent data volume directory
RUN mkdir -p /app/data

# Pre-install dbt dependencies (if packages.yml exists)
WORKDIR /app/dbt_project
RUN dbt deps --profiles-dir /app/config || true

WORKDIR /app

ENV PYTHONUNBUFFERED=1
ENV DBT_PROFILES_DIR=/app/config

# Run the Prefect flow entrypoint by default
CMD ["python", "flows/pipeline_flow.py"]