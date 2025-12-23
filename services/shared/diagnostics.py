import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import text

from services.shared.github_client import graphql_query


logger = logging.getLogger("diagnostics")


def _format_date_for_search(dt):
    """
    Convert a UTC datetime to a GitHub search date qualifier string

    GitHub search qualifiers are date-oriented; we use YYYY-MM-DD.

    Args:
        dt (datetime): Datetime (assumed UTC)

    Returns:
        str YYYY-MM-DD
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d")


def _search_pr_urls(github_token, search_query):
    """
    Fetch PR URLs matching a search query

    Args:
        github_token (str): OAuth token
        search_query (str): GitHub search query

    Returns:
        set of PR URLs
    """
    urls = set()
    cursor = None

    query = (
        "query($cursor:String, $q:String!){\n"
        "  search(type: ISSUE, first: 100, query: $q, after: $cursor) {\n"
        "    pageInfo { hasNextPage endCursor }\n"
        "    edges { node { ... on PullRequest { url } } }\n"
        "  }\n"
        "}"
    )

    while True:
        data = graphql_query(
            github_token,
            query,
            {"cursor": cursor, "q": search_query},
        )

        search = data.get("search") or {}
        for edge in (search.get("edges") or []):
            node = (edge or {}).get("node") or {}
            url = node.get("url")
            if url:
                urls.add(str(url))

        page = search.get("pageInfo") or {}
        if not page.get("hasNextPage"):
            break
        cursor = page.get("endCursor")

    return urls


def diagnose_pr_eligibility_parity(session, github_token, login, user_id, window_start, window_end):
    """
    Compare eligible PR counts between GitHub search and DB eligibility view

    GraphQL (spec parity intent):
        - created:{start}..{end} comments:>=1
        - created:{start}..{end} -review:none
        - union + dedupe by URL

    DB method:
        - count PRs in v_eligible_prs with created_at in [start,end) and is_eligible = TRUE

    Args:
        session: DB session
        github_token (str): OAuth token
        login (str): GitHub login
        user_id (str): Internal user UUID
        window_start (datetime): Inclusive UTC lower bound
        window_end (datetime): Exclusive UTC upper bound

    Returns:
        dict with counts and delta
    """
    if window_start.tzinfo is None:
        window_start = window_start.replace(tzinfo=timezone.utc)
    if window_end.tzinfo is None:
        window_end = window_end.replace(tzinfo=timezone.utc)

    start_date = _format_date_for_search(window_start)
    # GitHub created: range is inclusive by date;
    #   approximate exclusive end by subtracting a day only when end is midnight
    if window_end.astimezone(timezone.utc).time() == datetime.min.time():
        search_end = window_end - timedelta(days=1)
    else:
        search_end = window_end
    end_date = _format_date_for_search(search_end)

    q_comments = f"is:pr author:{login} created:{start_date}..{end_date} comments:>=1"
    q_reviews = f"is:pr author:{login} created:{start_date}..{end_date} -review:none"

    graphql_urls = _search_pr_urls(github_token, q_comments) | _search_pr_urls(github_token, q_reviews)

    db_count_row = session.execute(
        text(
            "SELECT COUNT(*)"
            " FROM v_eligible_prs"
            " WHERE user_id = :user_id"
            "   AND is_eligible = TRUE"
            "   AND created_at >= :window_start"
            "   AND created_at < :window_end"
        ),
        {"user_id": user_id, "window_start": window_start, "window_end": window_end},
    ).fetchone()
    db_count = int(db_count_row[0] or 0) if db_count_row else 0

    graphql_count = len(graphql_urls)
    delta = graphql_count - db_count

    result = {
        "user_id": user_id,
        "login": login,
        "window_start": window_start,
        "window_end": window_end,
        "graphql_count": graphql_count,
        "db_count": db_count,
        "delta": delta,
    }

    if delta != 0:
        logger.warning("Eligibility parity mismatch: %s", result)
    else:
        logger.info("Eligibility parity match: %s", result)

    return result
