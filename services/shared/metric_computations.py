from datetime import datetime
from typing import Any, Dict, List, Optional


def compute_merge_success_rate(merged_count: int, total_count: int) -> float:
    """
    Compute merge success rate

    Args:
        merged_count (int): Number of merged PRs
        total_count (int): Number of PRs considered (non-draft)

    Returns:
        float percentage 0..100
    """
    if not total_count:
        return 0.0
    return 100.0 * float(merged_count) / float(total_count)


def hours_between(t0: Optional[datetime], t1: Optional[datetime]) -> Optional[float]:
    """
    Compute hours between two timestamps

    Args:
        t0 (datetime): Earlier time
        t1 (datetime): Later time

    Returns:
        float hours or None
    """
    if not t0 or not t1:
        return None
    return (t1 - t0).total_seconds() / 3600.0


def allocate_language_points(
    prs: List[Dict[str, Any]],
    repo_langs: Dict[Any, List[Dict[str, Any]]],
) -> Dict[str, float]:
    """
    Allocate language points given PR weights and repository language inventories

    Args:
        prs (list): List of dicts with keys: repository_id, additions, deletions, changed_files
        repo_langs (dict): Mapping repo_id -> list of {language, bytes}

    Returns:
        dict mapping language -> points
    """
    totals: Dict[str, float] = {}
    for pr in prs:
        repo_id = pr.get("repository_id")
        addn = pr.get("additions") or 0
        deln = pr.get("deletions") or 0
        files = pr.get("changed_files") or 0
        w = addn + deln
        if w <= 0:
            w = max(1, files)
        langs = repo_langs.get(repo_id) or []
        total_bytes = sum(x.get("bytes") for x in langs) or 0
        if total_bytes <= 0:
            continue
        for item in langs:
            lang = item.get("language")
            pct = float(item.get("bytes")) / float(total_bytes)
            totals[lang] = totals.get(lang, 0.0) + w * pct
    return totals
