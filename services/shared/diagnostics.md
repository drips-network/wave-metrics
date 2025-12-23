# Diagnostics

This directory contains diagnostic helpers for debugging ingestion and eligibility parity issues

`services/shared/diagnostics.py` is intentionally not used in the normal API/worker runtime. It provides a one-off parity check between:

- A GitHub Search approximation of the eligibility rule (commented/reviewed PRs), and
- The Postgres eligibility view (`v_eligible_prs`)

## When to use

We can use this tool if we suspect eligibility drift between GitHub GraphQL ingestion and the DB view, for example:

- We see a mismatch between expected and computed `total_opened_prs`
- We want to validate that `v_eligible_prs` matches the eligibility spec for a specific user/window

## How to run (local)

1. Ensure the stack is running: `make up`
2. Set environment variables for DB and GitHub:

```bash
export DATABASE_URL='postgresql+psycopg2://postgres:postgres@localhost:5432/wave-metrics'
export GH_TOKEN='ghp_...'
export USER_ID='...'
export GH_LOGIN='octocat'
```

3. Run the diagnostic:

```bash
uv run python -c "\
from datetime import datetime, timedelta, timezone\
\
from services.shared.database import db_session\
from services.shared.diagnostics import diagnose_pr_eligibility_parity\
\
import os\
\
end = datetime.now(timezone.utc)\
start = end - timedelta(days=30)\
\
with db_session() as session:\
    result = diagnose_pr_eligibility_parity(\
        session=session,\
        github_token=os.environ['GH_TOKEN'],\
        login=os.environ['GH_LOGIN'],\
        user_id=os.environ['USER_ID'],\
        window_start=start,\
        window_end=end,\
    )\
    print(result)\
"
```

The function logs at `INFO` level for matches and `WARNING` for mismatches
