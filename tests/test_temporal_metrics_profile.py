import pandas as pd

from tests.temporal_metrics_profile import (
    compute_burstiness_coefficient_deviation,
    compute_diurnal_pattern_similarity_score,
    compute_hourly_activity_distribution_divergence,
    compute_inter_arrival_time_distribution_divergence,
    compute_non_negative_duration_ratio,
    compute_periodicity_preservation_score,
    compute_start_end_timestamp_consistency_ratio,
    compute_timestamp_parse_success_ratio,
)


def test_temporal_consistency_metrics():
    df = pd.DataFrame({
        "timestamp": ["2024-01-01T00:00:00Z", "bad", "2024-01-01T00:00:02Z"],
        "start": ["2024-01-01T00:00:00Z", "2024-01-01T00:00:03Z", "bad"],
        "end": ["2024-01-01T00:00:01Z", "2024-01-01T00:00:02Z", "2024-01-01T00:00:04Z"],
        "duration": [1, -2, None],
    })

    assert compute_timestamp_parse_success_ratio(df, {"input_requirements": {"timestamp_field": "timestamp"}})["summary"]["timestamp_parse_success_ratio"] == 0.666667
    assert compute_start_end_timestamp_consistency_ratio(df, {"input_requirements": {"start_timestamp_field": "start", "end_timestamp_field": "end"}})["summary"]["start_end_timestamp_consistency_ratio"] == 0.5
    assert compute_non_negative_duration_ratio(df, {"input_requirements": {"duration_field": "duration"}})["summary"]["non_negative_duration_ratio"] == 0.5


def test_inter_arrival_and_burstiness_still_measure_internal_half_drift():
    timestamps = pd.date_range("2024-01-01T00:00:00Z", periods=8, freq="h")
    df = pd.DataFrame({"timestamp": timestamps})
    metric = {
        "input_requirements": {"timestamp_field": "timestamp"},
        "calculation": {"parameters": {"minimum_sample_size": 2}},
    }

    assert compute_inter_arrival_time_distribution_divergence(df, metric)["summary"]["inter_arrival_time_distribution_divergence"] == 0.0
    assert compute_burstiness_coefficient_deviation(df, metric)["summary"]["burstiness_coefficient_deviation"] == 0.0


def test_regular_multiday_activity_has_identical_diurnal_profiles_and_periodicity():
    timestamps = pd.date_range("2024-01-01T00:00:00Z", periods=72, freq="h")
    df = pd.DataFrame({"timestamp": timestamps})
    metric = {
        "input_requirements": {"timestamp_field": "timestamp"},
        "calculation": {
            "parameters": {
                "minimum_day_count": 2,
                "lags": [24],
                "minimum_lag_pairs": 2,
            }
        },
    }

    hourly = compute_hourly_activity_distribution_divergence(df, metric)["summary"]
    diurnal = compute_diurnal_pattern_similarity_score(df, metric)["summary"]
    periodicity = compute_periodicity_preservation_score(df, metric)

    assert hourly["runnable"] is True
    assert hourly["observed_day_count"] == 3
    assert hourly["hourly_activity_distribution_divergence"] == 0.0
    assert diurnal["runnable"] is True
    assert diurnal["diurnal_pattern_similarity_score"] == 1.0
    assert periodicity["summary"]["runnable"] is True
    assert periodicity["summary"]["periodicity_preservation_score"] == 1.0
    assert periodicity["lags"][0]["lag_hours"] == 24
    assert periodicity["lags"][0]["repeat_similarity"] == 1.0


def test_shifted_daily_activity_is_detected_as_diurnally_different():
    day_one = pd.date_range("2024-01-01T00:00:00Z", periods=12, freq="h")
    day_two = pd.date_range("2024-01-02T12:00:00Z", periods=12, freq="h")
    df = pd.DataFrame({"timestamp": list(day_one) + list(day_two)})
    metric = {
        "input_requirements": {"timestamp_field": "timestamp"},
        "calculation": {
            "parameters": {
                "minimum_day_count": 2,
                "lags": [24],
                "minimum_lag_pairs": 2,
            }
        },
    }

    hourly = compute_hourly_activity_distribution_divergence(df, metric)["summary"]
    diurnal = compute_diurnal_pattern_similarity_score(df, metric)["summary"]
    periodicity = compute_periodicity_preservation_score(df, metric)

    assert hourly["hourly_activity_distribution_divergence"] == 1.0
    assert diurnal["diurnal_pattern_similarity_score"] == 0.0
    assert periodicity["summary"]["periodicity_preservation_score"] == 0.0


def test_single_day_capture_is_not_mislabelled_as_diurnally_divergent():
    timestamps = pd.date_range("2024-01-01T00:00:00Z", periods=8, freq="h")
    df = pd.DataFrame({"timestamp": timestamps})
    metric = {
        "input_requirements": {"timestamp_field": "timestamp"},
        "calculation": {
            "parameters": {
                "minimum_day_count": 2,
                "lags": [24],
                "minimum_lag_pairs": 2,
            }
        },
    }

    hourly = compute_hourly_activity_distribution_divergence(df, metric)["summary"]
    diurnal = compute_diurnal_pattern_similarity_score(df, metric)["summary"]
    periodicity = compute_periodicity_preservation_score(df, metric)

    assert hourly["runnable"] is False
    assert hourly["hourly_activity_distribution_divergence"] is None
    assert diurnal["runnable"] is False
    assert diurnal["diurnal_pattern_similarity_score"] is None
    assert periodicity["summary"]["runnable"] is False
    assert periodicity["summary"]["periodicity_preservation_score"] is None
    assert periodicity["lags"][0]["runnable"] is False


def test_periodicity_rejects_non_positive_lags():
    df = pd.DataFrame({"timestamp": pd.date_range("2024-01-01", periods=48, freq="h", tz="UTC")})
    metric = {
        "input_requirements": {"timestamp_field": "timestamp"},
        "calculation": {"parameters": {"lags": [0]}},
    }

    try:
        compute_periodicity_preservation_score(df, metric)
    except ValueError as exc:
        assert "positive integers" in str(exc)
    else:
        raise AssertionError("Expected invalid lag configuration to raise ValueError")
