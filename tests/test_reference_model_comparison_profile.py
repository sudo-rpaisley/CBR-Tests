import pandas as pd

from tests.reference_model_comparison_profile import (
    compute_feature_wise_ks_statistic_from_reference,
    compute_hourly_activity_divergence_from_reference,
    compute_pearson_matrix_deviation_from_reference,
    compute_per_slice_class_divergence_from_reference,
    compute_port_use_divergence_from_reference,
    compute_protocol_mix_divergence_from_reference,
    compute_slice_proportion_deviation_from_reference,
)


def test_reference_distribution_dependency_and_temporal_metrics(tmp_path):
    reference_path = tmp_path / "reference.csv"
    current = pd.DataFrame({
        "f1": [1, 2, 3, 4],
        "f2": [1, 2, 3, 4],
        "timestamp": pd.date_range("2024-01-01T00:00:00Z", periods=4, freq="h"),
    })
    reference = pd.DataFrame({
        "f1": [1, 2, 10, 11],
        "f2": [1, 2, 10, 11],
        "timestamp": pd.date_range("2024-01-01T00:00:00Z", periods=4, freq="h"),
    })
    reference.to_csv(reference_path, index=False)
    metric = {"input_requirements": {"reference_dataset_path": str(reference_path), "candidate_fields": ["f1", "f2"], "timestamp_field": "timestamp"}}

    assert compute_feature_wise_ks_statistic_from_reference(current, metric)["summary"]["runnable_field_count"] == 2
    assert compute_pearson_matrix_deviation_from_reference(current, metric)["summary"]["pair_count"] == 1
    assert compute_hourly_activity_divergence_from_reference(current, metric)["summary"]["hourly_activity_divergence_from_reference"] == 0.0


def test_reference_slice_and_protocol_metrics(tmp_path):
    reference_path = tmp_path / "reference.csv"
    current = pd.DataFrame({
        "slice": ["s1", "s1", "s2"],
        "label": ["benign", "attack", "benign"],
        "Protocol": [6, 6, 17],
        "Source Port": [80, 80, 53],
        "Destination Port": [1000, 1001, 53],
    })
    reference = pd.DataFrame({
        "slice": ["s1", "s2", "s2"],
        "label": ["benign", "attack", "attack"],
        "Protocol": [6, 17, 17],
        "Source Port": [80, 53, 53],
        "Destination Port": [1000, 53, 53],
    })
    reference.to_csv(reference_path, index=False)
    metric = {"input_requirements": {"reference_dataset_path": str(reference_path), "slice_field": "slice", "label_field": "label", "protocol_field": "Protocol", "port_fields": ["Source Port", "Destination Port"]}}

    assert compute_slice_proportion_deviation_from_reference(current, metric)["summary"]["slice_proportion_deviation_from_reference"] == 0.333333
    assert compute_per_slice_class_divergence_from_reference(current, metric)["summary"]["per_slice_class_divergence_from_reference"] > 0.0
    assert compute_protocol_mix_divergence_from_reference(current, metric)["summary"]["protocol_mix_divergence_from_reference"] == 0.333333
    assert compute_port_use_divergence_from_reference(current, metric)["summary"]["port_use_divergence_from_reference"] > 0.0
