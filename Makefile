ENV_FILE ?= $(if $(wildcard .env),.env,.env.example)
COMPOSE := docker compose --env-file $(ENV_FILE)

UV ?= uv

BASELINE_ID ?= 2025-09-30_v1
CSV ?= ./population_data/output/population_cdfs.csv

.PHONY: help dev-install compile-service-reqs compile-dev-reqs load-baseline verify-baseline up down logs fmt test test-e2e test-e2e-verbose migrate scheduler-daily scheduler-backfill

help:
	@echo "Targets:"
	@echo "  dev-install            Install dev dependencies with uv"
	@echo "  compile-service-reqs   Compile pinned runtime requirements (services/requirements.txt)"
	@echo "  compile-dev-reqs       Compile pinned dev requirements (requirements-dev.lock)"
	@echo "  up                     Start stack and run migrations (+ optional baseline load)"
	@echo "  down                   Stop stack and remove volumes"
	@echo "  logs                   Tail docker-compose logs"
	@echo "  test                   Run test suite (uv run)"
	@echo "  test-e2e               Run E2E tests (issues actual GitHub API requests)"
	@echo "  migrate                Run DB migrations in the api container"
	@echo "  scheduler-daily         Enqueue daily refresh jobs (docker)"
	@echo "  scheduler-backfill      Enqueue backfill jobs (docker)"

# Local Python dev (non-Docker)
dev-install:
	$(UV) pip install -e '.[dev]'

compile-service-reqs:
	$(UV) pip compile pyproject.toml -o services/requirements.txt

compile-dev-reqs:
	$(UV) pip compile pyproject.toml -o requirements-dev.lock --extra dev

load-baseline:
	DOCKER_COMPOSE='$(COMPOSE)' BASELINE_ID="$(BASELINE_ID)" CSV="$(CSV)" ./population_data/load_population_cdfs.sh

verify-baseline:
	$(COMPOSE) exec -T postgres psql -U postgres -d wave-metrics -c "SELECT baseline_id, COUNT(*) AS rows FROM population_cdfs GROUP BY baseline_id ORDER BY baseline_id;"
	$(COMPOSE) exec -T postgres psql -U postgres -d wave-metrics -c "SELECT baseline_id, baseline_start_date, baseline_end_date FROM baseline_metadata ORDER BY computed_at DESC LIMIT 5;"

LOAD_BASELINE ?= 1

up:
	$(COMPOSE) up -d --build
	$(MAKE) migrate
	@if [ "$(LOAD_BASELINE)" = "1" ]; then $(MAKE) load-baseline; fi

down:
	$(COMPOSE) down -v

logs:
	$(COMPOSE) logs -f --tail=200

fmt:
	@echo "(No formatter configured)"

E2E_BASE_URL ?= http://localhost:8000
E2E_VALID_OUTPUTS_PATH ?= tests/data_fixtures/valid_outputs_by_token.json
E2E_PYTEST_FLAGS ?=

# Prefer explicit env vars, but fall back to reading API_AUTH_TOKEN from ENV_FILE
E2E_API_AUTH_TOKEN ?= $(WAVE_METRICS_API_AUTH_TOKEN)
ifeq ($(strip $(E2E_API_AUTH_TOKEN)),)
E2E_API_AUTH_TOKEN := $(API_AUTH_TOKEN)
endif
ifeq ($(strip $(E2E_API_AUTH_TOKEN)),)
E2E_API_AUTH_TOKEN := $(shell grep -E '^API_AUTH_TOKEN=' $(ENV_FILE) 2>/dev/null | tail -n 1 | cut -d= -f2- | sed -E 's/^[[:space:]]+//; s/[[:space:]]+$$//; s/^"(.*)"$$/\1/; s/^'\''(.*)'\''$$/\1/')
endif


test:
	$(UV) run --extra dev pytest -q -v

test-e2e:
	@WAVE_METRICS_E2E=1 \
	WAVE_METRICS_API_AUTH_TOKEN="$(E2E_API_AUTH_TOKEN)" \
	WAVE_METRICS_BASE_URL="$(E2E_BASE_URL)" \
	WAVE_METRICS_VALID_OUTPUTS_PATH="$(E2E_VALID_OUTPUTS_PATH)" \
	$(UV) run --extra dev pytest -q -v -m e2e $(E2E_PYTEST_FLAGS)

test-e2e-verbose:
	@WAVE_METRICS_E2E=1 \
	WAVE_METRICS_E2E_LOG=1 \
	WAVE_METRICS_API_AUTH_TOKEN="$(E2E_API_AUTH_TOKEN)" \
	WAVE_METRICS_BASE_URL="$(E2E_BASE_URL)" \
	WAVE_METRICS_VALID_OUTPUTS_PATH="$(E2E_VALID_OUTPUTS_PATH)" \
	$(UV) run --extra dev pytest -q -v -m e2e -o log_cli=true --log-cli-level=INFO $(E2E_PYTEST_FLAGS)

migrate:
	$(COMPOSE) run --rm api python -c "from services.shared.database import apply_pending_migrations; apply_pending_migrations()"

SCHEDULER_DAILY_ARGS ?=
SCHEDULER_BACKFILL_ARGS ?=

scheduler-daily:
	$(COMPOSE) run --rm api python -m services.scheduler.app.main daily $(SCHEDULER_DAILY_ARGS)

scheduler-backfill:
	$(COMPOSE) run --rm api python -m services.scheduler.app.main backfill $(SCHEDULER_BACKFILL_ARGS)
