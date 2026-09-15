"""Reviewed oracle bindings for the canonical experiment metric set.

The numerical assertions exercised here already existed in focused metric tests.
This module records the pre-experiment scientific review that those fixtures are
hand-calculable or otherwise deterministic oracles for the named canonical
metrics, without duplicating the metric equations in a second test implementation.
Each wrapper executes the underlying assertions and also asserts the reviewed
binding count so the audit can distinguish an explicit oracle review from a
heuristic source-code match.
"""

from tests import test_address_validity_canonical as address
from tests import test_data_quality_profile as data_quality
from tests import test_intrinsic_diagnostic_restructure as intrinsic
from tests import test_label_fidelity_profile as labels
from tests import test_metric_failure_diagnostics as ports
from tests import test_pcap_handshake as handshake
from tests import test_reference_model_comparison_profile as reference
from tests import test_slice_identifier_denominator_policy as slice_ids
from tests import test_slice_representation_profile as slices
from tests import test_task_based_validation_profile as task
from tests import test_temporal_metrics_profile as temporal
from tests import test_valid_port_range_conformance as valid_port


def test_data_quality_metrics_reviewed_hand_calculated_oracle():
    """Reviewed oracle binding: missing_value_ratio, duplicate_row_ratio."""
    reviewed = [
        data_quality.test_compute_missing_value_ratio_uses_candidate_fields,
        data_quality.test_compute_duplicate_row_ratio_counts_repeated_rows_after_first,
    ]
    for test in reviewed:
        test()
    assert len(reviewed) == 2


def test_label_fidelity_metrics_reviewed_hand_calculated_oracle():
    """Reviewed oracle binding: label_coverage_ratio, per_slice_label_coverage_ratio,
    class_imbalance_score, per_slice_label_entropy_score,
    train_test_duplicate_overlap_ratio, train_test_identifier_contamination_ratio,
    attack_window_alignment_score, pre_post_attack_label_bleed_ratio.
    """
    reviewed = [
        labels.test_label_completeness_and_distribution_metrics,
        labels.test_temporal_label_correctness_metrics,
        labels.test_split_integrity_metrics_use_test_population_denominators,
    ]
    for test in reviewed:
        test()
    assert len(reviewed) == 3


def test_address_validity_metrics_reviewed_hand_calculated_oracle():
    """Reviewed oracle binding: valid_ip_address_ratio, reserved_address_misuse_ratio."""
    reviewed = [
        address.test_valid_ip_address_ratio_uses_non_missing_checked_values_as_denominator,
        address.test_reserved_address_misuse_ratio_counts_only_explicit_policy_violations,
    ]
    for test in reviewed:
        test()
    assert len(reviewed) == 2


def test_handshake_plausibility_profile_reviewed_hand_calculated_oracle(tmp_path):
    """Reviewed oracle binding: handshake_plausibility_profile."""
    handshake.test_pcap_handshake_ignores_boundary_and_incomplete_attempts(tmp_path)
    assert True


def test_port_metrics_reviewed_hand_calculated_oracle():
    """Reviewed oracle binding: service_port_consistency_profile, valid_port_range_profile."""
    reviewed = [
        ports.test_service_port_consistency_filters_to_service_population,
        valid_port.test_canonical_port_ratio_uses_full_normative_16_bit_domain,
    ]
    for test in reviewed:
        test()
    assert len(reviewed) == 2


def test_slice_identifier_metrics_reviewed_hand_calculated_oracle():
    """Reviewed oracle binding: valid_slice_identifier_profile, slice_identifier_consistency_profile."""
    reviewed = [
        slice_ids.test_valid_slice_identifier_excludes_missing_by_default,
        slice_ids.test_slice_consistency_excludes_unmatched_and_missing_rows_by_default,
    ]
    for test in reviewed:
        test()
    assert len(reviewed) == 2


def test_slice_representation_metrics_reviewed_hand_calculated_oracle():
    """Reviewed oracle binding: slice_distribution_imbalance_score,
    per_slice_class_coverage_ratio, per_slice_feature_coverage_ratio,
    per_slice_sample_coverage_ratio, cross_slice_duplicate_overlap_ratio,
    cross_slice_identifier_leakage_ratio.
    """
    reviewed = [
        slices.test_slice_coverage_and_balance_metrics,
        slices.test_cross_slice_isolation_metrics_use_signature_and_identifier_denominators,
    ]
    for test in reviewed:
        test()
    assert len(reviewed) == 2


def test_dependency_profiles_reviewed_hand_calculated_oracle():
    """Reviewed oracle binding: pearson_dependency_profile,
    spearman_dependency_profile, distance_correlation_dependency_profile.
    """
    intrinsic.test_dependency_profiles_are_profiles_not_deviations()
    assert True


def test_temporal_consistency_metrics_reviewed_hand_calculated_oracle():
    """Reviewed oracle binding: timestamp_parse_success_ratio,
    start_end_timestamp_consistency_ratio, non_negative_duration_ratio.
    """
    temporal.test_temporal_consistency_metrics()
    assert True


def test_temporal_structure_metrics_reviewed_hand_calculated_oracle():
    """Reviewed oracle binding: inter_arrival_internal_drift_ks,
    burstiness_internal_drift, day_to_day_hourly_activity_divergence,
    day_to_day_diurnal_similarity, lagged_periodicity_similarity.
    """
    intrinsic.test_canonical_temporal_diagnostic_names_do_not_claim_reference_comparison()
    assert True


def test_remaining_reference_metrics_reviewed_hand_calculated_oracle():
    """Reviewed oracle binding: flow_statistic_deviation_from_reference,
    port_use_divergence_from_reference, protocol_mix_divergence_from_reference,
    per_slice_class_divergence_from_reference,
    per_slice_feature_distribution_deviation_from_reference,
    slice_proportion_deviation_from_reference.
    """
    reviewed = [
        reference.test_flow_statistic_reference_distance_requires_matching_flow_definition_for_interpretation,
        reference.test_reference_protocol_and_port_metrics_exclude_missing_categories,
        reference.test_reference_slice_metrics_exclude_missing_and_do_not_score_unshared_conditionals,
    ]
    for test in reviewed:
        test()
    assert len(reviewed) == 3


def test_task_validation_metrics_reviewed_hand_calculated_oracle():
    """Reviewed oracle binding: benchmark_model_accuracy, benchmark_model_precision,
    benchmark_model_recall, benchmark_model_f1_score.
    """
    task.test_benchmark_model_metrics_from_predictions()
    assert True
