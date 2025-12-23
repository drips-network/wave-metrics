from services.shared.percentiles import (
    compute_oss_composite_raw,
    lookup_raw_percentile,
    percentile_bin,
    to_display_percentile,
)


class _FakeSession:
    def __init__(self, row):
        self._row = row

    def execute(self, _statement, _params=None):
        class _Result:
            def __init__(self, row):
                self._row = row

            def fetchone(self):
                return self._row

        return _Result(self._row)


def test_lookup_raw_percentile_returns_none_when_value_none():
    session = _FakeSession((100.0,))
    assert lookup_raw_percentile(session, "b", "m", None) is None


def test_lookup_raw_percentile_below_min_returns_zero():
    session = _FakeSession((None,))
    assert lookup_raw_percentile(session, "b", "m", 1) == 0.0


def test_lookup_raw_percentile_returns_max_percentile_match():
    session = _FakeSession((75.0,))
    assert lookup_raw_percentile(session, "b", "m", 123) == 75.0


def test_to_display_percentile_none_passthrough():
    assert to_display_percentile("total_opened_prs", None) is None


def test_to_display_percentile_inverts_before_clamp():
    # Invert metric: raw 0 => display 100 => clamp to 99.9 (proves clamp after inversion)
    assert to_display_percentile("pr_drop_rate", 0.0) == 99.9
    # Invert metric: raw 100 => display 0 => clamp stays 0
    assert to_display_percentile("pr_drop_rate", 100.0) == 0.0


def test_percentile_bin_boundaries():
    assert percentile_bin(0.0) == "Very Low"
    assert percentile_bin(24.9) == "Very Low"
    assert percentile_bin(25.0) == "Low"
    assert percentile_bin(49.9) == "Low"
    assert percentile_bin(50.0) == "Medium"
    assert percentile_bin(74.9) == "Medium"
    assert percentile_bin(75.0) == "High"
    assert percentile_bin(89.9) == "High"
    assert percentile_bin(90.0) == "Very High"
    assert percentile_bin(98.9) == "Very High"
    assert percentile_bin(99.0) == "Exceptional"
    assert percentile_bin(99.9) == "Exceptional"


def test_compute_oss_composite_raw():
    assert compute_oss_composite_raw(None, 1.0, 2.0) is None
    assert round(compute_oss_composite_raw(10.0, 20.0, 30.0), 4) == 18.5
