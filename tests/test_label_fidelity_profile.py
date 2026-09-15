import pandas as pd

from tests.label_fidelity_profile import (
    compute_attack_window_alignment_score,
    compute_class_imbalance_score,
    compute_label_coverage_ratio,
    compute_per_slice_label_coverage_ratio,
    compute_per_slice_label_entropy_score,
    compute_pre_post_attack_label_bleed_ratio,
    compute_train_test_duplicate_overlap_ratio,
    compute_train_test_identifier_contamination_ratio,
)


def test_label_completeness_and_distribution_metrics():
    df = pd.DataFrame({
        "slice": ["s1", "s1", "s2", "s2"],
        "label": ["benign", "attack", "benign", None],
    })
    metric = {
        "input_requirements": {
            "label_field": "label",
            "slice_field": "slice",
            "expected_classes": ["benign", "attack"],
        }
    }

    assert compute_label_coverage_ratio(df, metric)["summary"]["label_coverage_ratio"] == 0.75
    assert compute_per_slice_label_coverage_ratio(df, metric)["summary"]["per_slice_label_coverage_ratio"] == 0.75
    assert compute_per_slice_label_entropy_score(df, metric)["summary"]["per_slice_label_entropy_score"] == 0.5
    assert compute_class_imbalance_score(df, metric)["summary"]["class_imbalance_score"] == 0.333333


def test_per_slice_label_metrics_exclude_missing_slice_ids():
    df = pd.DataFrame({
        "slice": ["s1", "s1", None],
        "label": ["benign", None, "attack"],
    })
    metric = {
        "input_requirements": {
            "label_field": "label",
            "slice_field": "slice",
            "expected_classes": ["benign", "attack"],
        }
    }

    coverage = compute_per_slice_label_coverage_ratio(df, metric)
    entropy = compute_per_slice_label_entropy_score(df, metric)

    assert coverage["summary"]["slice_count"] == 1
    assert coverage["summary"]["missing_slice_row_count"] == 1
    assert coverage["summary"]["per_slice_label_coverage_ratio"] == 0.5
    assert entropy["summary"]["slice_count"] == 1
    assert entropy["slices"][0]["slice_id"] == "s1"


def test_entropy_is_undefined_for_labels_outside_declared_class_universe():
    df = pd.DataFrame({
        "slice": ["s1", "s1", "s1"],
        "label": ["benign", "attack", "unknown"],
    })
    metric = {
        "input_requirements": {
            "label_field": "label",
            "slice_field": "slice",
            "expected_classes": ["benign", "attack"],
        }
    }

    result = compute_per_slice_label_entropy_score(df, metric)
    assert result["slices"][0]["unexpected_labels"] == ["unknown"]
    assert result["slices"][0]["per_slice_label_entropy_score"] is None
    assert result["summary"]["per_slice_label_entropy_score"] is None


def test_class_imbalance_requires_independent_expected_class_universe():
    df = pd.DataFrame({"label": ["benign", "benign", "attack"]})

    descriptive = compute_class_imbalance_score(
        df, {"input_requirements": {"label_field": "label"}}
    )
    assert descriptive["summary"]["class_imbalance_score"] is None
    assert descriptive["summary"]["reason"] == "expected_classes_required_for_canonical_imbalance"

    configured = compute_class_imbalance_score(
        df,
        {
            "input_requirements": {
                "label_field": "label",
                "expected_classes": ["benign", "attack"],
            }
        },
    )
    assert configured["summary"]["class_imbalance_score"] == 0.333333


def test_temporal_label_correctness_metrics():
    df = pd.DataFrame({
        "timestamp": [
            "2024-01-01T00:00:00Z",
            "2024-01-01T00:00:30Z",
            "2024-01-01T00:01:30Z",
            "2024-01-01T00:03:00Z",
        ],
        "label": ["benign", "attack", "attack", "benign"],
    })
    metric = {
        "input_requirements": {
            "timestamp_field": "timestamp",
            "label_field": "label",
            "attack_label_values": ["attack"],
            "attack_windows": [
                {
                    "start": "2024-01-01T00:00:20Z",
                    "end": "2024-01-01T00:01:00Z",
                }
            ],
        },
        "calculation": {"parameters": {"bleed_window_seconds": 60}},
    }

    alignment = compute_attack_window_alignment_score(df, metric)["summary"]
    assert alignment["attack_window_alignment_score"] == 0.75
    assert alignment["attack_window_precision"] == 0.5
    assert alignment["attack_window_recall"] == 1.0
    assert compute_pre_post_attack_label_bleed_ratio(df, metric)["summary"]["pre_post_attack_label_bleed_ratio"] == 0.5


def test_attack_metrics_refuse_unconfigured_ground_truth():
    df = pd.DataFrame({
        "timestamp": ["2024-01-01T00:00:00Z"],
        "label": ["benign"],
    })
    metric = {"input_requirements": {"timestamp_field": "timestamp", "label_field": "label"}}

    alignment = compute_attack_window_alignment_score(df, metric)["summary"]
    bleed = compute_pre_post_attack_label_bleed_ratio(df, metric)["summary"]
    assert alignment["configuration_complete"] is False
    assert alignment["attack_window_alignment_score"] is None
    assert bleed["configuration_complete"] is False
    assert bleed["pre_post_attack_label_bleed_ratio"] is None


def test_bleed_excludes_missing_labels_from_eligible_buffer_denominator():
    df = pd.DataFrame({
        "timestamp": [
            "2024-01-01T00:00:00Z",
            "2024-01-01T00:00:10Z",
            "2024-01-01T00:00:20Z",
            "2024-01-01T00:01:10Z",
            "2024-01-01T00:01:20Z",
        ],
        "label": [None, "benign", "attack", "attack", None],
    })
    metric = {
        "input_requirements": {
            "timestamp_field": "timestamp",
            "label_field": "label",
            "attack_label_values": ["attack"],
            "attack_windows": [
                {
                    "start": "2024-01-01T00:00:20Z",
                    "end": "2024-01-01T00:01:00Z",
                }
            ],
        },
        "calculation": {"parameters": {"bleed_window_seconds": 20}},
    }

    result = compute_pre_post_attack_label_bleed_ratio(df, metric)["summary"]
    assert result["boundary_row_count"] == 2
    assert result["missing_boundary_label_count"] == 2
    assert result["bleed_label_count"] == 1
    assert result["pre_post_attack_label_bleed_ratio"] == 0.5


def test_split_integrity_metrics_use_test_population_denominators():
    df = pd.DataFrame({
        "split": ["train", "test", "train", "test"],
        "flow_id": ["a", "a", "b", "c"],
        "src_ip": ["10.0.0.1", "10.0.0.1", "10.0.0.2", "10.0.0.3"],
        "dst_ip": ["8.8.8.8", "8.8.8.8", "1.1.1.1", "9.9.9.9"],
    })
    metric = {
        "input_requirements": {
            "split_field": "split",
            "subset_fields": ["src_ip", "dst_ip"],
            "identifier_fields": ["flow_id", "src_ip"],
            "entity_disjoint_expected": True,
        }
    }

    duplicate = compute_train_test_duplicate_overlap_ratio(df, metric)["summary"]
    contamination = compute_train_test_identifier_contamination_ratio(df, metric)["summary"]

    assert duplicate["train_key_count"] == 2
    assert duplicate["test_key_count"] == 2
    assert duplicate["overlap_key_count"] == 1
    assert duplicate["train_test_duplicate_overlap_ratio"] == 0.5
    assert duplicate["train_test_duplicate_jaccard"] == 0.333333

    assert contamination["test_identifier_count"] == 4
    assert contamination["overlap_identifier_count"] == 2
    assert contamination["train_test_identifier_contamination_ratio"] == 0.5


def test_identifier_overlap_is_not_called_contamination_without_entity_disjoint_policy():
    df = pd.DataFrame({
        "split": ["train", "test"],
        "device_id": ["device-1", "device-1"],
    })
    metric = {
        "input_requirements": {
            "split_field": "split",
            "identifier_fields": ["device_id"],
        }
    }

    result = compute_train_test_identifier_contamination_ratio(df, metric)["summary"]
    assert result["train_test_identifier_overlap_ratio"] == 1.0
    assert result["train_test_identifier_contamination_ratio"] is None
    assert result["reason"] == "entity_disjoint_expectation_required_for_contamination_claim"
