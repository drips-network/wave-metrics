"""
Metric definitions: keys, descriptions, gating thresholds, invert flags

Single source of truth for the 9 spec metrics
"""

# Metrics where lower is better (raw percentile inverted before display)
INVERT_METRICS = frozenset({"pr_drop_rate", "avg_merge_latency_hours"})

METRIC_DEFINITIONS = {
    "total_opened_prs": {
        "description": "Count of PRs opened within the 3-year window that received at least one comment or review",
    },
    "total_merged_prs": {
        "description": "Count of commented/reviewed PRs that were merged",
    },
    "pr_merge_rate": {
        "description": "Proportion of commented/reviewed PRs that were merged",
    },
    "pr_drop_rate": {
        "description": "Proportion of commented/reviewed PRs closed without merge (lower is better)",
    },
    "avg_merge_latency_hours": {
        "description": "Mean hours from PR open to merge (lower is better)",
    },
    "oss_prs_opened": {
        "description": "PRs opened (OSS Activity component)",
    },
    "oss_reviews": {
        "description": "Pull request reviews submitted",
    },
    "oss_issues_opened": {
        "description": "Issues opened",
    },
    "oss_composite": {
        "description": "Weighted OSS Activity Score (40% reviews + 35% PRs + 25% issues)",
    },
}


def get_metric_description(metric_name):
    """
    Get description for a metric

    Args:
        metric_name (str): Metric key

    Returns:
        str description
    """
    return METRIC_DEFINITIONS.get(metric_name, {}).get("description", "")
