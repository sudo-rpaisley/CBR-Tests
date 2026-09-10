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

    assert compute_per_slice_sample_coverage_ratio(
        df,
        {"input_requirements": {"slice_field": "slice", "expected_slice_ids": ["s1", "s2", "s3"]}},
    )["summary"]["per_slice_sample_coverage_ratio"] == 0.666667

    assert compute_per_slice_feature_coverage_ratio(
        df,
        {"input_requirements": {"slice_field": "slice", "candidate_fields": ["f1", "f2"]}},
    )["summary"]["per_slice_feature_coverage_ratio"] == 0.75

    assert compute_per_slice_class_coverage_ratio(
        df,
        {
            "input_requirements": {
                "slice_field": "slice",
                "label_field": "label",
                "expected_classes": ["benign", "attack"],
            }
        },
    )["summary"]["per_slice_class_coverage_ratio"] == 0.75

    assert compute_slice_distribution_imbalance_score(
        df,
        {"input_requirements": {"slice_field": "slice"}},
    )["summary"]["slice_distribution_imbalance_score"] == 0.333333


def test_sample_coverage_requires_expected_slices_and_supports_minimum_counts():
    df = pd.DataFrame({"slice": [" s1 ", "s1", "s2"]})

    descriptive = compute_per_slice_sample_coverage_ratio(
        df,
        {"input_requirements": {"slice_field": "slice"}},
    )
    assert descriptive["summary"]["per_slice_sample_coverage_ratio"] is None
    assert descriptive["summary"]["reason"] == "expected_slice_ids_required_for_coverage"

    configured = compute_per_slice_sample_coverage_ratio(
        df,
        {
            "input_requirements": {
                "slice_field": "slice",
                "expected_slice_ids": ["s1", "s2", "s3"],
            },
            "calculation": {"parameters": {"minimum_sample_count": 2}},
        },
    )
    assert configured["slices"][0]["sample_count"] == 2
    assert configured["slices"][0]["covered"] is True
    assert configured["slices"][1]["covered"] is False
    assert configured["summary"]["per_slice_sample_coverage_ratio"] == 0.333333


def test_feature_coverage_is_row_level_not_any_value_presence():
    df = pd.DataFrame({
        "slice": ["s1", "s1", "s2", "s2"],
        "f1": [1, None, 3, 4],
        "f2": [10, 11, None, None],
    })
    result = compute_per_slice_feature_coverage_ratio(
        df,
        {"input_requirements": {"slice_field": "slice", "candidate_fields": ["f1", "f2"]}},
    )

    # s1: (0.5 + 1.0) / 2 = 0.75; s2: (1.0 + 0.0) / 2 = 0.5.
    assert result["slices"][0]["per_slice_feature_coverage_ratio"] == 0.75
    assert result["slices"][1]["per_slice_feature_coverage_ratio"] == 0.5
    assert result["summary"]["per_slice_feature_coverage_ratio"] == 0.625


def test_class_coverage_requires_independently_declared_expected_classes():
    df = pd.DataFrame({
        "slice": ["s1", "s2"],
        "label": ["benign", "attack"],
    })
    result = compute_per_slice_class_coverage_ratio(
        df,
        {"input_requirements": {"slice_field": "slice", "label_field": "label"}},
    )

    assert result["summary"]["per_slice_class_coverage_ratio"] is None
    assert result["summary"]["reason"] == "expected_classes_required_for_coverage"
    assert result["slices"][0]["observed_classes"] == ["benign"]


def test_slice_imbalance_excludes_missing_slice_ids_from_distribution():
    df = pd.DataFrame({"slice": ["s1", "s2", None]})
    result = compute_slice_distribution_imbalance_score(
        df,
        {"input_requirements": {"slice_field": "slice"}},
    )

    assert result["summary"]["eligible_slice_row_count"] == 2
    assert result["summary"]["missing_slice_row_count"] == 1
    assert result["summary"]["slice_distribution_imbalance_score"] == 0.0


def test_cross_slice_isolation_metrics_use_signature_and_identifier_denominators():
    df = pd.DataFrame({
        "slice": ["s1", "s2", "s1", "s2"],
        "flow_id": ["a", "a", "b", "c"],
        "src_ip": ["10.0.0.1", "10.0.0.1", "10.0.0.2", "10.0.0.3"],
        "dst_ip": ["8.8.8.8", "8.8.8.8", "1.1.1.1", "9.9.9.9"],
    })

    duplicate_result = compute_cross_slice_duplicate_overlap_ratio(
        df,
        {"input_requirements": {"slice_field": "slice", "subset_fields": ["src_ip", "dst_ip"]}},
    )
    leakage_result = compute_cross_slice_identifier_leakage_ratio(
        df,
        {
            "input_requirements": {
                "slice_field": "slice",
                "identifier_fields": ["flow_id", "src_ip"],
            },
            "calculation": {"parameters": {"expect_slice_exclusive": True}},
        },
    )

    assert duplicate_result["summary"]["checked_signature_count"] == 3
    assert duplicate_result["summary"]["overlap_signature_count"] == 1
    assert duplicate_result["summary"]["cross_slice_duplicate_overlap_ratio"] == 0.333333
    assert duplicate_result["summary"]["overlap_row_ratio"] == 0.5

    assert leakage_result["summary"]["unique_identifier_count"] == 6
    assert leakage_result["summary"]["overlap_identifier_count"] == 2
    assert leakage_result["summary"]["cross_slice_identifier_leakage_ratio"] == 0.333333


def test_identifier_overlap_is_not_called_leakage_without_exclusivity_policy():
    df = pd.DataFrame({
        "slice": ["s1", "s2", None],
        "src_ip": ["10.0.0.1", "10.0.0.1", "10.0.0.2"],
    })
    result = compute_cross_slice_identifier_leakage_ratio(
        df,
        {"input_requirements": {"slice_field": "slice", "identifier_fields": ["src_ip"]}},
    )

    assert result["summary"]["unique_identifier_count"] == 1
    assert result["summary"]["cross_slice_identifier_overlap_ratio"] == 1.0
    assert result["summary"]["cross_slice_identifier_leakage_ratio"] is None
    assert result["summary"]["reason"] == "slice_exclusivity_policy_required_for_leakage"
