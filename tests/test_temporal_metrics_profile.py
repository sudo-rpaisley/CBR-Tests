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


def test_temporal_behaviour_metrics_compare_first_and_second_halves():
    timestamps = pd.date_range("2024-01-01T00:00:00Z", periods=8, freq="h")
    df = pd.DataFrame({"timestamp": timestamps})
    metric = {"input_requirements": {"timestamp_field": "timestamp"}, "calculation": {"parameters": {"minimum_sample_size": 2, "lags": [1]}}}

    assert compute_inter_arrival_time_distribution_divergence(df, metric)["summary"]["inter_arrival_time_distribution_divergence"] == 0.0
    assert compute_burstiness_coefficient_deviation(df, metric)["summary"]["burstiness_coefficient_deviation"] == 0.0
    assert compute_hourly_activity_distribution_divergence(df, metric)["summary"]["hourly_activity_distribution_divergence"] == 1.0
    assert compute_diurnal_pattern_similarity_score(df, metric)["summary"]["diurnal_pattern_similarity_score"] == 0.0
    assert compute_periodicity_preservation_score(df, metric)["summary"]["periodicity_preservation_score"] == 0.95
