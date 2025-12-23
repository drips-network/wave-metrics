from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Any, Dict, Iterator, List, Optional, Tuple
import hashlib
import logging
import random
import time

import httpx

from services.shared.config import GH_BACKOFF_BASE_SECONDS, GH_BACKOFF_CAP_SECONDS, GH_MAX_RETRIES
from services.shared.throttle import send_github_graphql, DEFAULT_GITHUB_TIMEOUT

logger = logging.getLogger("github_client")


class GitHubAPIError(RuntimeError):
    """
    Raised when GitHub GraphQL API returns application-level errors in the JSON payload

    Attributes:
        errors (list): Raw error objects from GitHub
        operation (str): Best-effort label for the GraphQL operation type
    """

    def __init__(self, message, errors=None, operation=None):
        super().__init__(message)
        self.errors = errors or []
        self.operation = operation

    @property
    def is_transient(self) -> bool:
        """
        Best-effort classification of whether the error is likely transient

        Returns:
            bool: True when error code suggests a transient issue
        """
        for err in self.errors:
            ext = err.get("extensions") or {}
            code = (ext.get("code") or "").upper()
            if code in {"INTERNAL", "RATE_LIMITED", "ABUSE_DETECTED"}:
                return True
        return False

@contextmanager
def _http_client() -> Iterator[httpx.Client]:
    """
    Provide a configured httpx client

    Returns:
        httpx.Client context manager
    """
    with httpx.Client(timeout=DEFAULT_GITHUB_TIMEOUT) as client:
        yield client


def _compute_graphql_error_backoff_seconds(attempt):
    base = max(0, int(GH_BACKOFF_BASE_SECONDS))
    cap = max(0, int(GH_BACKOFF_CAP_SECONDS))

    if base <= 0:
        return 0.0

    raw = float(base) * (2.0 ** float(max(0, int(attempt))))
    jitter = random.uniform(0.0, 1.0)
    wait = raw + jitter

    if cap > 0:
        return min(float(cap), wait)
    return wait


def graphql_query(github_token, query, variables) -> Dict[str, Any]:
    """
    Execute a GitHub GraphQL query using Redis-coordinated throttling

    Args:
        github_token (str): OAuth token for the user
        query (str): GraphQL query string
        variables (dict): Variables for the query

    Returns:
        dict response json
    """
    operation = "unknown"
    if isinstance(query, str):
        stripped = query.lstrip()
        if stripped.startswith("mutation"):
            operation = "mutation"
        elif stripped.startswith("query"):
            operation = "query"

    payload = {"query": query, "variables": variables}
    max_retries = max(0, int(GH_MAX_RETRIES))

    for attempt in range(max_retries + 1):
        r = send_github_graphql(github_token, payload, timeout=DEFAULT_GITHUB_TIMEOUT)
        data = r.json()

        errors = data.get("errors") or []
        if not errors:
            return data["data"]

        first = errors[0] if isinstance(errors, list) and errors else {}
        message = first.get("message") or "GitHub GraphQL error"
        code = ((first.get("extensions") or {}).get("code") or "").upper() or "UNKNOWN"
        token_hash = hashlib.sha256((github_token or "").encode("utf-8")).hexdigest()[:6]

        exc = GitHubAPIError(message=message, errors=errors, operation=operation)

        if exc.is_transient and attempt < max_retries:
            wait_seconds = _compute_graphql_error_backoff_seconds(attempt)
            logger.warning(
                "GitHub GraphQL transient error: op=%s code=%s token=%s retry_in=%.2fs",
                operation,
                code,
                token_hash,
                wait_seconds,
            )
            time.sleep(wait_seconds)
            continue

        logger.error(
            "GitHub GraphQL error: op=%s code=%s token=%s attempt=%s/%s msg=%s",
            operation,
            code,
            token_hash,
            attempt + 1,
            max_retries + 1,
            message,
        )
        raise exc

    raise RuntimeError("unreachable")


def fetch_user_login(github_token) -> Tuple[Optional[str], Dict[str, Any]]:
    """
    Resolve the viewer's login

    Args:
        github_token (str): OAuth token

    Returns:
        str login
    """
    data = graphql_query(
        github_token,
        "query{ viewer { login databaseId createdAt avatarUrl } }",
        {},
    )
    viewer = data.get("viewer") or {}
    return viewer.get("login"), viewer


def _search_issue_count(github_token, author_login, start_date_iso, end_date_iso) -> int:
    """
    Return the total issueCount for PRs updated within a date window for an author

    Args:
        github_token (str): OAuth token
        author_login (str): GitHub login
        start_date_iso (str): YYYY-MM-DD
        end_date_iso (str): YYYY-MM-DD

    Returns:
        int issueCount (capped by GitHub at ~1000)
    """
    search_query = f"is:pr author:{author_login} updated:{start_date_iso}..{end_date_iso}"
    query = (
        "query($q:String!){\n"
        "  search(type: ISSUE, first: 1, query: $q) {\n"
        "    issueCount\n"
        "  }\n"
        "}"
    )
    data = graphql_query(github_token, query, {"q": search_query})
    return int(((data.get("search") or {}).get("issueCount") or 0))


def iter_user_prs_between(github_token, author_login, start_date_iso, end_date_iso) -> Iterator[Dict[str, Any]]:
    """
    Iterate PRs for an author in an inclusive date window based on updated timestamp

    Args:
        github_token (str): OAuth token
        author_login (str): GitHub login
        start_date_iso (str): YYYY-MM-DD
        end_date_iso (str): YYYY-MM-DD

    Yields:
        PR nodes
    """
    cursor = None
    search_query = f"is:pr author:{author_login} updated:{start_date_iso}..{end_date_iso} sort:updated-asc"

    query = (
        "query($cursor:String, $q:String!){\n"
        "  search(type: ISSUE, first: 50, query: $q, after: $cursor) {\n"
        "    pageInfo { hasNextPage endCursor }\n"
        "    edges { node { ... on PullRequest {\n"
        "      id databaseId number url state isDraft createdAt mergedAt closedAt updatedAt\n"
        "      additions deletions changedFiles merged\n"
        "      comments(first: 1) { totalCount }\n"
        "      reviews(first: 1) { totalCount }\n"
        "      repository { id databaseId name isPrivate owner { login } }\n"
        "    } } }\n"
        "  }\n"
        "}"
    )

    while True:
        data = graphql_query(github_token, query, {"cursor": cursor, "q": search_query})
        search = data.get("search") or {}
        for e in (search.get("edges") or []):
            node = (e or {}).get("node") or {}
            if node:
                yield node
        page = search.get("pageInfo") or {}
        if not page.get("hasNextPage"):
            break
        cursor = page.get("endCursor")


def iter_user_prs_windowed(
    github_token,
    author_login,
    since_dt,
    until_dt,
    max_window_days=180,
) -> Iterator[Dict[str, Any]]:
    """
    Iterate PRs across time windows, splitting windows recursively if result count approaches caps

    Args:
        github_token (str): OAuth token
        author_login (str): GitHub login
        since_dt (datetime): Start datetime (UTC)
        until_dt (datetime): End datetime (UTC)
        max_window_days (int): Initial window size in days

    Yields:
        PR nodes
    """
    def _date_str(d: datetime) -> str:
        return d.strftime("%Y-%m-%d")

    def _yield_range(start_dt: datetime, end_dt: datetime) -> Iterator[Dict[str, Any]]:
        # Ensure date-only inclusive windows
        start_iso = _date_str(start_dt)
        end_iso = _date_str(end_dt)
        count = _search_issue_count(github_token, author_login, start_iso, end_iso)
        if count >= 1000 and (end_dt - start_dt).days > 1:
            mid = start_dt + (end_dt - start_dt) / 2
            mid_date = datetime(mid.year, mid.month, mid.day, tzinfo=start_dt.tzinfo)
            left_end = mid_date
            right_start = mid_date + timedelta(days=1)
            yield from _yield_range(start_dt, left_end)
            yield from _yield_range(right_start, end_dt)
        else:
            # Pull this window fully
            yield from iter_user_prs_between(github_token, author_login, start_iso, end_iso)

    # Walk windows in chunks to keep a reasonable bound on search_count calls
    cursor_start = since_dt
    while cursor_start <= until_dt:
        window_end = min(cursor_start + timedelta(days=max_window_days - 1), until_dt)
        yield from _yield_range(cursor_start, window_end)
        cursor_start = window_end + timedelta(days=1)


def fetch_repo_languages(github_token, owner, name) -> List[Dict[str, Any]]:
    """
    Fetch repository language inventory

    Args:
        github_token (str): OAuth token
        owner (str): Owner login
        name (str): Repo name

    Returns:
        list of {language, bytes}
    """
    query = (
        "query($owner:String!,$name:String!){\n"
        "  repository(owner:$owner,name:$name){ languages(first:100, orderBy:{field:SIZE,direction:DESC}) {\n"
        "    totalSize\n"
        "    edges { size node { name } }\n"
        "  }}\n"
        "}"
    )
    data = graphql_query(github_token, query, {"owner": owner, "name": name})
    repo = data.get("repository") or {}
    langs: List[Dict[str, Any]] = []
    rl = repo.get("languages") or {}
    edges = rl.get("edges") or []
    for e in edges:
        node = e.get("node") or {}
        langs.append({"language": node.get("name"), "bytes": e.get("size")})
    return langs


def _subtract_years(dt, years):
    """
    Subtract calendar years, normalizing Feb 29 to Feb 28 when needed

    Args:
        dt (datetime): Aware datetime (UTC)
        years (int): Years to subtract

    Returns:
        datetime: Aware datetime (UTC)
    """
    try:
        return dt.replace(year=dt.year - years)
    except ValueError:
        return dt.replace(year=dt.year - years, day=28)


def fetch_contributions_collection_counts(
    github_token,
    login,
    from_dt,
    to_dt
) -> Dict[str, Any]:
    """
    Fetch contributionsCollection counts for a single window

    Args:
        github_token (str): OAuth token
        login (str): GitHub login
        from_dt (datetime): Inclusive lower bound (UTC)
        to_dt (datetime): Exclusive upper bound (UTC)

    Returns:
        dict with keys: oss_reviews, oss_issues_opened
    """
    query = (
        "query($login:String!, $from:DateTime!, $to:DateTime!){\n"
        "  user(login:$login){\n"
        "    contributionsCollection(from:$from, to:$to){\n"
        "      totalPullRequestReviewContributions\n"
        "      totalIssueContributions\n"
        "    }\n"
        "  }\n"
        "}"
    )
    data = graphql_query(
        github_token,
        query,
        {
            "login": login,
            "from": from_dt.isoformat().replace("+00:00", "Z"),
            "to": to_dt.isoformat().replace("+00:00", "Z"),
        },
    )
    user = data.get("user") or {}
    cc = user.get("contributionsCollection") or {}
    return {
        "oss_reviews": int(cc.get("totalPullRequestReviewContributions") or 0),
        "oss_issues_opened": int(cc.get("totalIssueContributions") or 0),
    }


def fetch_oss_activity_counts_3y(
    github_token,
    login,
    window_end
) -> Dict[str, Any]:
    """
    Fetch review + issue counts over ~3y using 3 contiguous 1-year calls

    Per spec: max 1 year per call, contiguous windows, from inclusive, to exclusive

    Args:
        github_token (str): OAuth token
        login (str): GitHub login
        window_end (datetime): End instant (UTC)

    Returns:
        dict with keys: oss_reviews, oss_issues_opened
    """
    totals = {"oss_reviews": 0, "oss_issues_opened": 0}

    windows = []
    end_cursor = window_end
    for _ in range(3):
        start_cursor = _subtract_years(end_cursor, 1)
        windows.append((start_cursor, end_cursor))
        end_cursor = start_cursor

    # Assert contiguity: [w0_start,w0_end), [w1_start,w1_end), [w2_start,w2_end)
    if windows[1][1] != windows[0][0] or windows[2][1] != windows[1][0]:
        raise ValueError("contributionsCollection windows are not contiguous")

    logger.debug(
        "contributionsCollection windows: %s",
        [(w[0].isoformat(), w[1].isoformat()) for w in windows],
    )

    for start_cursor, end_cursor in windows:
        counts = fetch_contributions_collection_counts(
            github_token, login, start_cursor, end_cursor
        )
        totals["oss_reviews"] += counts["oss_reviews"]
        totals["oss_issues_opened"] += counts["oss_issues_opened"]

    return totals
