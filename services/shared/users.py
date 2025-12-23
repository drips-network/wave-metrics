from sqlalchemy import text

from services.shared.database import db_session


def list_tracked_users() -> list[str]:
    """
    Return a list of user ids that should be refreshed by schedulers

    Returns:
        list of user id strings
    """
    with db_session() as session:
        rows = session.execute(text("SELECT id FROM users")).fetchall()
        return [r[0] for r in rows]
