from datetime import datetime, timedelta

from services.shared.metric_computations import (
    allocate_language_points,
    compute_merge_success_rate,
    hours_between,
)


def test_hours_between_fractional_hours():
    t0 = datetime(2025, 1, 1, 0, 0, 0)
    t1 = t0 + timedelta(hours=10, minutes=30)
    assert round(hours_between(t0, t1), 3) == 10.5


def test_compute_merge_success_rate_basic():
    assert round(compute_merge_success_rate(2, 3), 2) == 66.67
    assert compute_merge_success_rate(0, 0) == 0.0


def test_allocate_language_points_weighted():
    prs = [
        {"repository_id": 1, "additions": 100, "deletions": 50, "changed_files": 3},
        {"repository_id": 2, "additions": 10, "deletions": 10, "changed_files": 1},
    ]
    repo_langs = {
        1: [
            {"language": "Python", "bytes": 700},
            {"language": "Rust", "bytes": 300},
        ],
        2: [
            {"language": "TypeScript", "bytes": 800},
            {"language": "Python", "bytes": 200},
        ],
    }
    points = allocate_language_points(prs, repo_langs)
    assert points["Python"] > points["Rust"]
    assert points["Python"] > points["TypeScript"]

