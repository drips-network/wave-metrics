"""
Percentile lookup and computation helpers for contributor metrics

Implements spec-conformant percentile lookup, inversion, clamping, and binning
"""

from sqlalchemy import text

from services.shared.config import POPULATION_BASELINE_ID
from services.shared.metric_definitions import INVERT_METRICS


def resolve_baseline(session):
    """
    Resolve active baseline_id and dates

    Resolution order:
        1. POPULATION_BASELINE_ID env var if set
        2. Latest baseline_metadata row (end desc, computed desc)
        3. Fallback: latest population_cdfs row (end desc, computed desc)

    Args:
        session: DB session

    Returns:
        dict with baseline_id, baseline_start_date, baseline_end_date

    Raises:
        ValueError: If no baseline found
    """
    if POPULATION_BASELINE_ID:
        row = session.execute(
            text(
                "SELECT baseline_id, baseline_start_date, baseline_end_date "
                "FROM baseline_metadata WHERE baseline_id = :bid"
            ),
            {"bid": POPULATION_BASELINE_ID},
        ).fetchone()
        if row:
            return {
                "baseline_id": row[0],
                "baseline_start_date": row[1],
                "baseline_end_date": row[2],
            }

        row = session.execute(
            text(
                "SELECT DISTINCT baseline_id, baseline_start_date, baseline_end_date "
                "FROM population_cdfs WHERE baseline_id = :bid LIMIT 1"
            ),
            {"bid": POPULATION_BASELINE_ID},
        ).fetchone()
        if row:
            return {
                "baseline_id": row[0],
                "baseline_start_date": row[1],
                "baseline_end_date": row[2],
            }
        raise ValueError(f"Baseline '{POPULATION_BASELINE_ID}' not found")

    row = session.execute(
        text(
            "SELECT baseline_id, baseline_start_date, baseline_end_date "
            "FROM baseline_metadata "
            "ORDER BY baseline_end_date DESC, computed_at DESC "
            "LIMIT 1"
        )
    ).fetchone()
    if row:
        return {
            "baseline_id": row[0],
            "baseline_start_date": row[1],
            "baseline_end_date": row[2],
        }

    row = session.execute(
        text(
            "SELECT DISTINCT baseline_id, baseline_start_date, baseline_end_date "
            "FROM population_cdfs "
            "ORDER BY baseline_end_date DESC, computed_at DESC "
            "LIMIT 1"
        )
    ).fetchone()
    if row:
        return {
            "baseline_id": row[0],
            "baseline_start_date": row[1],
            "baseline_end_date": row[2],
        }

    raise ValueError("No baseline found in baseline_metadata or population_cdfs")


def lookup_raw_percentile(session, baseline_id, metric_name, value):
    """
    Lookup raw percentile from population_cdfs per spec

    Implements: raw_percentile = MAX(percentile) WHERE threshold_value <= value

    Edge case handling:
        value is None -> None
        no rows (V < min threshold) -> 0.0
        V >= max threshold -> naturally returns 100.0

    Args:
        session: DB session
        baseline_id (str): Baseline identifier
        metric_name (str): Metric name
        value: Metric value (int, float, or None)

    Returns:
        float raw percentile 0..100, or None if value is None
    """
    if value is None:
        return None

    row = session.execute(
        text(
            "SELECT MAX(percentile) AS raw_percentile "
            "FROM population_cdfs "
            "WHERE metric_name = :metric_name "
            "  AND baseline_id = :baseline_id "
            "  AND threshold_value <= :value"
        ),
        {"metric_name": metric_name, "baseline_id": baseline_id, "value": float(value)},
    ).fetchone()

    if row is None or row[0] is None:
        return 0.0
    return float(row[0])


def to_display_percentile(metric_name, raw_percentile):
    """
    Compute display percentile with inversion and clamping per spec

    Order of operations:
        1. Invert for metrics where lower is better
        2. Clamp to [0.0, 99.9]

    Args:
        metric_name (str): Metric name
        raw_percentile (float): Raw percentile from lookup, or None

    Returns:
        float display percentile clamped to [0.0, 99.9], or None
    """
    if raw_percentile is None:
        return None

    display = 100.0 - raw_percentile if metric_name in INVERT_METRICS else raw_percentile
    return min(99.9, max(0.0, display))


def percentile_bin(display_percentile):
    """
    Map display percentile to bin label per spec

    Bins:
        0–24 -> Very Low
        25–49 -> Low
        50–74 -> Medium
        75–89 -> High
        90–98 -> Very High
        99–100 -> Exceptional (with clamp to 99.9, this is >= 99.0)

    Args:
        display_percentile (float): Display percentile, or None

    Returns:
        str bin label, or None
    """
    if display_percentile is None:
        return None

    if display_percentile < 25:
        return "Very Low"
    if display_percentile < 50:
        return "Low"
    if display_percentile < 75:
        return "Medium"
    if display_percentile < 90:
        return "High"
    if display_percentile < 99:
        return "Very High"
    return "Exceptional"


def compute_oss_composite_raw(p_reviews, p_prs, p_issues):
    """
    Compute OSS composite raw score from component percentiles

    Formula: composite_raw = 0.40 * p_reviews + 0.35 * p_prs + 0.25 * p_issues

    Args:
        p_reviews (float): Reviews component percentile
        p_prs (float): PRs component percentile
        p_issues (float): Issues component percentile

    Returns:
        float composite raw score, or None if any component is None
    """
    if p_reviews is None or p_prs is None or p_issues is None:
        return None
    return (0.40 * p_reviews) + (0.35 * p_prs) + (0.25 * p_issues)
