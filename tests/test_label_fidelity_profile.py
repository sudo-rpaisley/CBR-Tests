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
    metric = {"input_requirements": {"label_field": "label", "slice_field": "slice", "expected_classes": ["benign", "attack"]}}

    assert compute_label_coverage_ratio(df, metric)["summary"]["label_coverage_ratio"] == 0.75
    assert compute_per_slice_label_coverage_ratio(df, metric)["summary"]["per_slice_label_coverage_ratio"] == 0.75
    assert compute_per_slice_label_entropy_score(df, metric)["summary"]["per_slice_label_entropy_score"] == 0.5
    assert compute_class_imbalance_score(df, metric)["summary"]["class_imbalance_score"] == 0.333333


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
            "attack_windows": [{"start": "2024-01-01T00:00:20Z", "end": "2024-01-01T00:01:00Z"}],
        },
        "calculation": {"parameters": {"bleed_window_seconds": 60}},
    }

    assert compute_attack_window_alignment_score(df, metric)["summary"]["attack_window_alignment_score"] == 0.75
    assert compute_pre_post_attack_label_bleed_ratio(df, metric)["summary"]["pre_post_attack_label_bleed_ratio"] == 0.5


def test_split_integrity_metrics():
    df = pd.DataFrame({
        "split": ["train", "test", "train", "test"],
        "flow_id": ["a", "a", "b", "c"],
        "src_ip": ["10.0.0.1", "10.0.0.1", "10.0.0.2", "10.0.0.3"],
        "dst_ip": ["8.8.8.8", "8.8.8.8", "1.1.1.1", "9.9.9.9"],
    })
    metric = {"input_requirements": {"split_field": "split", "subset_fields": ["src_ip", "dst_ip"], "identifier_fields": ["flow_id", "src_ip"]}}

    assert compute_train_test_duplicate_overlap_ratio(df, metric)["summary"]["train_test_duplicate_overlap_ratio"] == 0.333333
    assert compute_train_test_identifier_contamination_ratio(df, metric)["summary"]["train_test_identifier_contamination_ratio"] == 0.333333
