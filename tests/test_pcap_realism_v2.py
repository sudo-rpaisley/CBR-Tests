"""Regression tests for opt-in PCAP realism metrics and temporal GAN controls."""
import math

import numpy as np
import pandas as pd
import pytest

from cbr_tests.metrics.data_quality import compute_missing_value_ratio, compute_duplicate_row_ratio
from cbr_tests.metrics.reference_comparison import _multivariate_rbf_mmd
from cbr_tests.metrics import pcap_realism_v2 as v2


def metric(**parameters):
    return {"calculation": {"parameters": parameters}}


def reference_metric(**parameters):
    return {"input_requirements": {"reference_dataset_path": "/not/read/by/mock.pcap"},
            "calculation": {"parameters": parameters}}


def packets(times, lengths=None, ports=None):
    n = len(times)
    if lengths is None:
        lengths = [100] * n
    if ports is None:
        ports = [443] * n
    return pd.DataFrame({
        "Timestamp": times,
        "Packet Length": lengths,
        "Source IP": ["192.0.2.10"]*n,
        "Destination IP": ["198.51.100.10"]*n,
        "Source Port": [52345]*n,
        "Destination Port": ports,
        "Protocol": [17]*n,
    })


def test_missing_requested_field_counts_as_missing():
    df = pd.DataFrame({"a": [1, None]})
    result = compute_missing_value_ratio(df, {"input_requirements": {"candidate_fields": ["a", "absent"]}})
    assert result["summary"]["missing_column_count"] == 1
    assert result["summary"]["missing_cells"] == 3
    assert result["summary"]["missing_value_ratio"] == 0.75
    assert result["summary"]["missing_columns"] == ["absent"]


def test_duplicate_signature_missing_column_is_unrunnable():
    df = pd.DataFrame({"a": [1, 1]})
    result = compute_duplicate_row_ratio(df, {"input_requirements": {"subset_fields": ["a", "absent"]}})
    assert result["summary"]["runnable"] is False
    assert result["summary"]["duplicate_row_ratio"] is None
    assert result["summary"]["missing_signature_fields"] == ["absent"]


def test_duplicate_signature_still_reports_repeated_signatures():
    df = pd.DataFrame({"a": [1, 1, 2]})
    result = compute_duplicate_row_ratio(df, {"input_requirements": {"subset_fields": ["a"]}})
    assert result["summary"]["runnable"] is True
    assert result["summary"]["duplicate_row_ratio"] == pytest.approx(1/3, abs=1e-6)


def test_capture_order_perturbation_detected_when_sorted_timestamps_would_hide_it(monkeypatch):
    original = packets([1., 2., 3., 4., 5.])
    reversed_middle = packets([1., 3., 2., 4., 5.])
    monkeypatch.setattr(v2, "_load_reference_df", lambda _: original)
    result = v2.compute_capture_order_inter_arrival_divergence_from_reference(
        reversed_middle, reference_metric())
    assert result["summary"]["candidate_backwards_gap_ratio"] > 0
    assert result["summary"]["reference_backwards_gap_ratio"] == 0
    assert result["summary"]["capture_order_inter_arrival_divergence_from_reference"] > 0


def test_global_time_shift_is_null_for_capture_order_gaps(monkeypatch):
    original = packets([1., 1.1, 1.4, 1.5, 1.9])
    shifted = packets([1001., 1001.1, 1001.4, 1001.5, 1001.9])
    monkeypatch.setattr(v2, "_load_reference_df", lambda _: original)
    result = v2.compute_capture_order_inter_arrival_divergence_from_reference(
        shifted, reference_metric())["summary"]
    assert result["capture_order_inter_arrival_divergence_from_reference"] == pytest.approx(0)


def test_multiscale_elapsed_time_shift_control(monkeypatch):
    original = packets([0., 0.1, 0.2, 0.9, 1., 1.01])
    shifted = packets([500., 500.1, 500.2, 500.9, 501., 501.01])
    monkeypatch.setattr(v2, "_load_reference_df", lambda _: original)
    result = v2.compute_multiscale_activity_divergence_from_reference(
        shifted, reference_metric(window_seconds=[0.5, 1.0]))
    assert result["summary"]["runnable"]
    assert result["summary"]["multiscale_activity_divergence_from_reference"] == pytest.approx(0)


def test_inter_arrival_per_flow_changes_without_affecting_global_packet_count(monkeypatch):
    original = packets([1., 1.1, 1.2, 1.3, 1.4, 1.5])
    altered = packets([1., 1.01, 1.02, 1.3, 1.4, 1.5])
    monkeypatch.setattr(v2, "_load_reference_df", lambda _: original)
    result = v2.compute_per_flow_inter_arrival_divergence_from_reference(
        altered, reference_metric(max_packets=50))["summary"]
    assert result["runnable"]
    assert result["per_flow_inter_arrival_divergence_from_reference"] > 0


def test_burst_structure_reacts_to_bursts(monkeypatch):
    original = packets([0, .001, .002, 1, 1.001, 1.002, 2, 2.001])
    altered = packets([0, .001, .002, .003, .004, .005, 2, 2.001])
    monkeypatch.setattr(v2, "_load_reference_df", lambda _: original)
    result = v2.compute_burst_structure_divergence_from_reference(
        altered, reference_metric(burst_gap_seconds=0.01))["summary"]
    assert result["runnable"]
    assert result["burst_structure_divergence_from_reference"] > 0


def test_temporal_autocorrelation_requires_evidence(monkeypatch):
    original = packets([0., 1., 2., 3.])
    monkeypatch.setattr(v2, "_load_reference_df", lambda _: original)
    result = v2.compute_temporal_autocorrelation_deviation_from_reference(
        original, reference_metric(lags=[1, 10]))["summary"]
    assert result["runnable"] is False
    assert result["reason_code"] == "insufficient_observations"


def test_port_class_ignores_ephemeral_port_renumbering(monkeypatch):
    original = packets([0, 1, 2], ports=[443, 52000, 52001])
    altered = packets([0, 1, 2], ports=[443, 61000, 63000])
    monkeypatch.setattr(v2, "_load_reference_df", lambda _: original)
    result = v2.compute_port_class_divergence_from_reference(
        altered, reference_metric())["summary"]
    assert result["port_class_divergence_from_reference"] == pytest.approx(0)


def test_reference_calibrated_mmd_symmetry_and_zero():
    a = np.array([[0., 1.], [1., 2.], [2., 1.], [3., 4.]])
    b = np.array([[2., 1.], [3., 4.], [5., 3.], [6., 7.]])
    value, _ = _multivariate_rbf_mmd(a, a, calibration_mode="reference_fixed")
    assert value == pytest.approx(0, abs=1e-10)
    value2, _ = _multivariate_rbf_mmd(b, a, calibration_mode="reference_fixed")
    assert value2 >= 0
    with pytest.raises(ValueError):
        _multivariate_rbf_mmd(a, b, calibration_mode="unknown")


def test_temporal_gan_selection_seed_and_boundaries():
    from gan_generator.temporal_gan import _selection
    assert _selection(100, 0, 42, "trailing").sum() == 0
    assert _selection(100, 100, 42, "trailing").sum() == 100
    assert _selection(100, 20, 42, "trailing")[-20:].all()
    assert np.array_equal(_selection(100, 25, 42, "distributed"),
                          _selection(100, 25, 42, "distributed"))
    assert _selection(100, 25, 43, "blocks").sum() == 25
    with pytest.raises(ValueError):
        _selection(100, 10, 42, "unknown")
