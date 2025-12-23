import logging

from sqlalchemy import text


logger = logging.getLogger("login_aliases")


def _normalize_github_login(value):
    if value is None:
        return ""
    return str(value).strip().lower()


def confirm_login_alias(session, github_login, user_id, github_user_id) -> None:
    """
    Confirm the canonical mapping for a GitHub login

    This upserts github_login_aliases so future login-only /sync requests can resolve
    without GitHub calls

    Args:
        session: SQLAlchemy session
        github_login (str): GitHub login
        user_id (str): Canonical internal user id
        github_user_id (int): Stable GitHub databaseId

    Returns:
        None
    """
    normalized_login = _normalize_github_login(github_login)
    if not normalized_login:
        logger.debug(
            "confirm_login_alias: skipping empty login",
            extra={"user_id": user_id, "github_user_id": github_user_id},
        )
        return

    stable_github_user_id = int(github_user_id or 0)
    if stable_github_user_id <= 0:
        logger.warning(
            "confirm_login_alias: skipping invalid github_user_id",
            extra={"github_login": normalized_login, "user_id": user_id, "github_user_id": github_user_id},
        )
        return

    session.execute(
        text(
            "INSERT INTO github_login_aliases (github_login, user_id, confirmed_at, expires_at, github_user_id, updated_at) "
            "VALUES (:login, :user_id, NOW(), NULL, :gh_uid, NOW()) "
            "ON CONFLICT (github_login) DO UPDATE SET "
            "user_id=EXCLUDED.user_id, "
            "updated_at=NOW(), "
            "confirmed_at=NOW(), "
            "expires_at=NULL, "
            "github_user_id=EXCLUDED.github_user_id"
        ),
        {"login": normalized_login, "user_id": user_id, "gh_uid": stable_github_user_id},
    )
