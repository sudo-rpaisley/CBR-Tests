import pandas as pd

from tests.slice_representation_profile import (
    compute_cross_slice_duplicate_overlap_ratio,
    compute_cross_slice_identifier_leakage_ratio,
    compute_per_slice_class_coverage_ratio,
    compute_per_slice_feature_coverage_ratio,
    compute_per_slice_sample_coverage_ratio,
    compute_slice_distribution_imbalance_score,
)


def test_slice_coverage_and_balance_metrics():
    df = pd.DataFrame({
        "slice": ["s1", "s1", "s2"],
        "f1": [1, 2, 3],
        "f2": [None, None, 4],
        "label": ["benign", "attack", "benign"],
    })

    assert compute_per_slice_sample_coverage_ratio(df, {"input_requirements": {"slice_field": "slice", "expected_slice_ids": ["s1", "s2", "s3"]}})["summary"]["per_slice_sample_coverage_ratio"] == 0.666667
    assert compute_per_slice_feature_coverage_ratio(df, {"input_requirements": {"slice_field": "slice", "candidate_fields": ["f1", "f2"]}})["summary"]["per_slice_feature_coverage_ratio"] == 0.75
    assert compute_per_slice_class_coverage_ratio(df, {"input_requirements": {"slice_field": "slice", "label_field": "label", "expected_classes": ["benign", "attack"]}})["summary"]["per_slice_class_coverage_ratio"] == 0.75
    assert compute_slice_distribution_imbalance_score(df, {"input_requirements": {"slice_field": "slice"}})["summary"]["slice_distribution_imbalance_score"] == 0.333333


def test_cross_slice_isolation_metrics():
    df = pd.DataFrame({
        "slice": ["s1", "s2", "s1", "s2"],
        "flow_id": ["a", "a", "b", "c"],
        "src_ip": ["10.0.0.1", "10.0.0.1", "10.0.0.2", "10.0.0.3"],
        "dst_ip": ["8.8.8.8", "8.8.8.8", "1.1.1.1", "9.9.9.9"],
    })

    duplicate_result = compute_cross_slice_duplicate_overlap_ratio(df, {"input_requirements": {"slice_field": "slice", "subset_fields": ["src_ip", "dst_ip"]}})
    leakage_result = compute_cross_slice_identifier_leakage_ratio(df, {"input_requirements": {"slice_field": "slice", "identifier_fields": ["flow_id", "src_ip"]}})

    assert duplicate_result["summary"]["overlap_row_count"] == 2
    assert duplicate_result["summary"]["cross_slice_duplicate_overlap_ratio"] == 0.5
    assert leakage_result["summary"]["leaked_identifier_count"] == 2
    assert leakage_result["summary"]["cross_slice_identifier_leakage_ratio"] == 0.285714
