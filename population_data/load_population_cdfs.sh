#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  BASELINE_ID='2025-09-30_v1' CSV='../population_data/output/population_cdfs.csv' ./scripts/load_population_cdfs.sh

Notes:
- Streams the CSV from your host into Postgres running in docker compose
- Uses a TEMP staging table per run (no persistent population_cdfs_load_staging required)
USAGE
}

BASELINE_ID=${BASELINE_ID:-${1:-}}
CSV=${CSV:-${2:-}}

if [[ -z "${BASELINE_ID}" || -z "${CSV}" ]]; then
  usage
  exit 2
fi

if [[ ! -f "${CSV}" ]]; then
  echo "CSV not found: ${CSV}" >&2
  exit 2
fi

DOCKER_COMPOSE=${DOCKER_COMPOSE:-docker\ compose}
DB_SERVICE=${DB_SERVICE:-postgres}
DB_NAME=${DB_NAME:-wave-metrics}
DB_USER=${DB_USER:-postgres}

{
  cat <<'SQL'
BEGIN;

CREATE TEMP TABLE population_cdfs_load_staging (
    metric_name TEXT NOT NULL,
    percentile NUMERIC(4, 1) NOT NULL,
    threshold_value NUMERIC NOT NULL,
    baseline_start_date DATE NOT NULL,
    baseline_end_date DATE NOT NULL
);

COPY population_cdfs_load_staging (
    metric_name,
    percentile,
    threshold_value,
    baseline_start_date,
    baseline_end_date
)
FROM STDIN WITH (FORMAT csv, HEADER true);
SQL

  python3 - "${CSV}" <<'PY'
import sys
from pathlib import Path

csv_path = Path(sys.argv[1])
data = csv_path.read_bytes()
sys.stdout.buffer.write(data)
if not data.endswith(b"\n"):
    sys.stdout.write("\n")
PY

  cat <<'SQL'
\.

DELETE FROM population_cdfs WHERE baseline_id = :'baseline_id';

INSERT INTO population_cdfs (
    metric_name,
    percentile,
    threshold_value,
    baseline_id,
    baseline_start_date,
    baseline_end_date,
    computed_at
)
SELECT
    metric_name,
    percentile,
    threshold_value,
    :'baseline_id',
    baseline_start_date,
    baseline_end_date,
    NOW()
FROM population_cdfs_load_staging;

INSERT INTO baseline_metadata (baseline_id, baseline_start_date, baseline_end_date, computed_at, notes)
SELECT
    :'baseline_id',
    MIN(baseline_start_date),
    MAX(baseline_end_date),
    NOW(),
    'Loaded from full_cdf_table.csv'
FROM population_cdfs_load_staging
ON CONFLICT (baseline_id) DO UPDATE
SET baseline_start_date = EXCLUDED.baseline_start_date,
    baseline_end_date = EXCLUDED.baseline_end_date,
    computed_at = EXCLUDED.computed_at,
    notes = EXCLUDED.notes;

COMMIT;

SELECT metric_name, COUNT(*)
FROM population_cdfs
WHERE baseline_id = :'baseline_id'
GROUP BY 1
ORDER BY 1;
SQL
} | ${DOCKER_COMPOSE} exec -T "${DB_SERVICE}" psql -U "${DB_USER}" -d "${DB_NAME}" -v ON_ERROR_STOP=1 -v baseline_id="${BASELINE_ID}"
