FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DEBIAN_FRONTEND=noninteractive \
    PYTHONPATH=/app

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install uv (Astral)
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.local/bin:${PATH}"

COPY pyproject.toml README.md /app/
COPY services /app/services
COPY population_data /app/population_data

# Optionally use pinned requirements for reproducible builds if present
RUN if [ -f /app/services/requirements.txt ]; then \
        uv pip install --system -r /app/services/requirements.txt ; \
    else \
        uv pip install --system -e . ; \
    fi

CMD ["celery", "-A", "services.worker.app.main:celery_app", "worker", "--loglevel=info", "--concurrency=2", "-Q", "default"]
