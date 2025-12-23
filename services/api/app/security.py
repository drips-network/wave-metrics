"""
API security: simple bearer token authentication
"""

import secrets

from fastapi import Header, HTTPException, status

from services.shared.config import API_AUTH_TOKEN


async def verify_api_auth_token(authorization: str = Header(default=None)) -> None:
    """
    Verify static bearer token when API_AUTH_TOKEN is configured

    Args:
        authorization (str): Authorization header

    Returns:
        None. Raises HTTPException on failure
    """
    if not API_AUTH_TOKEN:
        return

    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized")

    provided_token = authorization[7:]
    if not secrets.compare_digest(provided_token, API_AUTH_TOKEN):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized")
