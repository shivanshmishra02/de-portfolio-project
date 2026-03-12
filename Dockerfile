# SkillPulse India — Cloud Run Pipeline Container
# ================================================
# Runs run_pipeline.py as a one-shot Cloud Run Job.
# All secrets/config injected via env vars — no .env file inside container.

FROM python:3.11-slim

# git required by dbt deps
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python deps before copying src (layer cache optimisation)
COPY requirements-pipeline.txt .
RUN pip install --no-cache-dir -r requirements-pipeline.txt

# Copy pipeline source
COPY src/ ./src/
COPY dbt_project/ ./dbt_project/
COPY run_pipeline.py .

# Cloud Run Jobs expect a non-interactive process that exits when done
ENTRYPOINT ["python", "run_pipeline.py"]
