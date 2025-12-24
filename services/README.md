# Services

Core application components for Wave Metrics.

| Component | Description |
|-----------|-------------|
| [`api/`](api/README.md) | FastAPI server exposing HTTP endpoints for metrics reads and sync job submissions |
| [`worker/`](worker/README.md) | Celery application executing GitHub ingestion and metrics computation jobs |
| [`shared/`](shared/README.md) | Core logic for ingestion, metrics computation, percentile lookup, and rate limiting |

For system architecture and deployment, see the [root README](../README.md).
