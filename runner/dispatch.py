from __future__ import annotations

from pathlib import Path

import pandas as pd

from cbr_tests.metrics.column_quality import compute_column_quality_profile
from cbr_tests.metrics.data_quality import (
    compute_duplicate_row_ratio,
    compute_missing_value_ratio,
)
from cbr_tests.metrics.pearson import compute_pearson_profile, validate_candidate_fields
from cbr_tests.metrics.spearman import (
    compute_spearman_profile,
    validate_spearman_candidate_fields,
)
from cbr_tests.metrics.statistical import (
    compute_distance_correlation_profile,
    compute_energy_distance,
    compute_ks_feature_divergence,
    compute_maximum_mean_discrepancy,
    compute_wasserstein_feature_distance,
)
from cbr_tests.metrics.task_validation import (
    compute_benchmark_model_accuracy,
    compute_benchmark_model_f1_score,
    compute_benchmark_model_precision,
    compute_benchmark_model_recall,
)
from cbr_tests.metrics.temporal import (
    compute_burstiness_coefficient_deviation,
    compute_diurnal_pattern_similarity_score,
    compute_hourly_activity_distribution_divergence,
    compute_inter_arrival_time_distribution_divergence,
    compute_non_negative_duration_ratio,
    compute_periodicity_preservation_score,
    compute_start_end_timestamp_consistency_ratio,
    compute_timestamp_parse_success_ratio,
)
from cbr_tests.metrics.timestamp_coherence import run_timestamp_coherence_metric
from runner.field_translation import translate_metric_fields
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
from tests.metrics.dataset_heuristics.protocol_and_network_realism.address_validity.reserved_ip_address_profile import (
    run_reserved_ip_address_metric,
)
from tests.metrics.dataset_heuristics.protocol_and_network_realism.address_validity.valid_ip_address_profile import (
    run_protocol_validity_metric,
)
from tests.metrics.dataset_heuristics.protocol_and_network_realism.flow_semantics.flow_duration_consistency_profile import (
    run_flow_duration_consistency_metric,
)
from tests.metrics.dataset_heuristics.protocol_and_network_realism.flow_semantics.derived_rate_consistency_profile import (
    run_derived_rate_consistency_metric,
)
from tests.metrics.dataset_heuristics.protocol_and_network_realism.flow_semantics.handshake_plausibility_profile import (
    run_handshake_plausibility_metric,
)
from tests.metrics.dataset_heuristics.protocol_and_network_realism.flow_semantics.packet_byte_consistency_profile import (
    run_packet_byte_consistency_metric,
)
from tests.metrics.dataset_heuristics.protocol_and_network_realism.flow_semantics.tcp_flag_consistency_profile import (
    run_tcp_flag_consistency_metric,
)
from tests.metrics.dataset_heuristics.protocol_and_network_realism.port_validity.service_port_consistency_profile import (
    run_service_port_consistency_metric,
)
from tests.metrics.dataset_heuristics.protocol_and_network_realism.port_validity.valid_port_range_profile import (
    run_valid_port_range_metric,
)
from tests.metrics.dataset_heuristics.protocol_and_network_realism.slice_metadata_integrity.slice_identifier_consistency_profile import (
    run_slice_identifier_consistency_metric,
)
from tests.metrics.dataset_heuristics.protocol_and_network_realism.slice_metadata_integrity.valid_slice_identifier_profile import (
    run_valid_slice_identifier_metric,
)
from tests.reference_model_comparison_profile import (
    compute_burstiness_deviation_from_reference,
    compute_distance_correlation_matrix_deviation_from_reference,
    compute_feature_set_mmd_score_from_reference,
    compute_feature_wise_energy_distance_from_reference,
    compute_feature_wise_ks_statistic_from_reference,
    compute_feature_wise_wasserstein_distance_from_reference,
    compute_flow_statistic_deviation_from_reference,
    compute_hourly_activity_divergence_from_reference,
    compute_inter_arrival_distribution_divergence_from_reference,
    compute_pearson_matrix_deviation_from_reference,
    compute_per_slice_class_divergence_from_reference,
    compute_per_slice_feature_distribution_deviation_from_reference,
    compute_port_use_divergence_from_reference,
    compute_protocol_mix_divergence_from_reference,
    compute_slice_proportion_deviation_from_reference,
    compute_spearman_matrix_deviation_from_reference,
)
from tests.slice_representation_profile import (
    compute_cross_slice_duplicate_overlap_ratio,
    compute_cross_slice_identifier_leakage_ratio,
    compute_per_slice_class_coverage_ratio,
    compute_per_slice_feature_coverage_ratio,
    compute_per_slice_sample_coverage_ratio,
    compute_slice_distribution_imbalance_score,
)

METRIC_REGISTRY = {}


def register_metric(metric_id: str):
    def _decorator(function):
        METRIC_REGISTRY[metric_id] = function
        return function

    return _decorator


def run_pearson_metric(
    dataset_path: Path,
    metric: dict,
    load_tabular_dataset,
    shared_df: pd.DataFrame | None = None,
):
    df = shared_df.copy() if shared_df is not None else load_tabular_dataset(dataset_path)
    candidate_fields = metric["input_requirements"]["candidate_fields"]
    minimum_runnable_fields = metric["input_requirements"]["minimum_runnable_fields"]
    column_validation, runnable_fields, df = validate_candidate_fields(df, candidate_fields)
    if len(runnable_fields) < minimum_runnable_fields:
        return False, {
            "column_validation": column_validation,
            "error": "Not enough usable numeric columns to compute Pearson correlation.",
        }
    pearson_profile = compute_pearson_profile(df, runnable_fields)
    return True, {
        "column_validation": column_validation,
        "test_results": {"pearson_correlation_profile": pearson_profile},
    }


def run_spearman_metric(
    dataset_path: Path,
    metric: dict,
    load_tabular_dataset,
    shared_df: pd.DataFrame | None = None,
):
    df = shared_df.copy() if shared_df is not None else load_tabular_dataset(dataset_path)
    candidate_fields = metric["input_requirements"]["candidate_fields"]
    minimum_runnable_fields = metric["input_requirements"].get(
        "minimum_runnable_fields", 2
    )
    column_validation, runnable_fields, df = validate_spearman_candidate_fields(
        df, candidate_fields
    )
    if len(runnable_fields) < minimum_runnable_fields:
        return False, {
            "column_validation": column_validation,
            "error": "Not enough usable numeric columns to compute Spearman correlation.",
        }
    spearman_profile = compute_spearman_profile(df, runnable_fields)
    return True, {
        "column_validation": column_validation,
        "test_results": {
            "spearman_correlation_matrix_deviation": spearman_profile
        },
    }


def run_missing_value_metric(
    dataset_path: Path,
    metric: dict,
    load_tabular_dataset,
    shared_df: pd.DataFrame | None = None,
):
    df = shared_df.copy() if shared_df is not None else load_tabular_dataset(dataset_path)
    return True, {
        "test_results": {"missing_value_ratio": compute_missing_value_ratio(df, metric)}
    }


def run_duplicate_row_metric(
    dataset_path: Path,
    metric: dict,
    load_tabular_dataset,
    shared_df: pd.DataFrame | None = None,
):
    df = shared_df.copy() if shared_df is not None else load_tabular_dataset(dataset_path)
    return True, {
        "test_results": {"duplicate_row_ratio": compute_duplicate_row_ratio(df, metric)}
    }


def run_tabular_metric(
    dataset_path: Path,
    metric: dict,
    load_tabular_dataset,
    shared_df: pd.DataFrame | None,
    metric_id: str,
    compute_fn,
):
    df = shared_df.copy() if shared_df is not None else load_tabular_dataset(dataset_path)
    return True, {"test_results": {metric_id: compute_fn(df, metric)}}


def run_distance_correlation_metric(
    dataset_path: Path,
    metric: dict,
    load_tabular_dataset,
    shared_df: pd.DataFrame | None = None,
):
    df = shared_df.copy() if shared_df is not None else load_tabular_dataset(dataset_path)
    candidate_fields = metric["input_requirements"]["candidate_fields"]
    minimum_runnable_fields = metric["input_requirements"].get(
        "minimum_runnable_fields", 2
    )
    parameters = metric.get("calculation", {}).get("parameters", {})
    max_sample_size = parameters.get("max_sample_size")
    sampling = None
    if max_sample_size is not None:
        max_sample_size = int(max_sample_size)
        if max_sample_size < 2:
            return False, {
                "error": "max_sample_size must be at least 2 for distance correlation.",
                "reason_code": "invalid_metric_configuration",
            }
        if len(df) > max_sample_size:
            original_row_count = len(df)
            step = (original_row_count - 1) / (max_sample_size - 1)
            positions = [round(index * step) for index in range(max_sample_size)]
            df = df.iloc[positions].copy()
            sampling = {
                "method": "deterministic_evenly_spaced_rows",
                "original_row_count": original_row_count,
                "sampled_row_count": len(df),
                "max_sample_size": max_sample_size,
            }
    result = compute_distance_correlation_profile(df, candidate_fields)
    runnable_fields = result["profile"]["fields"]
    if len(runnable_fields) < minimum_runnable_fields:
        return False, {
            "column_validation": result["column_validation"],
            "error": "Not enough usable numeric columns to compute distance correlation.",
        }
    payload = {
        "column_validation": result["column_validation"],
        "test_results": {
            "distance_correlation_matrix_deviation": result["profile"]
        },
    }
    if sampling is not None:
        payload["sampling"] = sampling
    return True, payload


def run_column_quality_metric(
    dataset_path: Path,
    metric: dict,
    load_tabular_dataset,
    shared_df: pd.DataFrame | None = None,
):
    df = shared_df.copy() if shared_df is not None else load_tabular_dataset(dataset_path)
    candidate_fields = metric["input_requirements"]["candidate_fields"]
    quality_profile = compute_column_quality_profile(df, candidate_fields)
    return True, {"test_results": {"column_quality_profile": quality_profile}}


@register_metric("timestamp_coherence_profile")
def _timestamp_metric(dataset_path: Path, metric: dict):
    return run_timestamp_coherence_metric(dataset_path, metric)


@register_metric("protocol_validity_profile")
def _protocol_metric(dataset_path: Path, metric: dict):
    return run_protocol_validity_metric(dataset_path, metric)


@register_metric("reserved_ip_address_profile")
def _reserved_ip_metric(dataset_path: Path, metric: dict):
    return run_reserved_ip_address_metric(dataset_path, metric)


@register_metric("valid_port_range_profile")
def _valid_port_metric(dataset_path: Path, metric: dict):
    return run_valid_port_range_metric(dataset_path, metric)


@register_metric("service_port_consistency_profile")
def _service_port_metric(dataset_path: Path, metric: dict):
    return run_service_port_consistency_metric(dataset_path, metric)


@register_metric("tcp_flag_consistency_profile")
def _tcp_flag_metric(dataset_path: Path, metric: dict):
    return run_tcp_flag_consistency_metric(dataset_path, metric)


@register_metric("handshake_plausibility_profile")
def _handshake_metric(dataset_path: Path, metric: dict):
    return run_handshake_plausibility_metric(dataset_path, metric)


@register_metric("flow_duration_consistency_profile")
def _flow_duration_metric(dataset_path: Path, metric: dict):
    return run_flow_duration_consistency_metric(dataset_path, metric)


@register_metric("packet_byte_consistency_profile")
def _packet_byte_metric(dataset_path: Path, metric: dict):
    return run_packet_byte_consistency_metric(dataset_path, metric)


@register_metric("derived_rate_consistency_profile")
def _derived_rate_metric(dataset_path: Path, metric: dict):
    return run_derived_rate_consistency_metric(dataset_path, metric)


@register_metric("valid_slice_identifier_profile")
def _slice_valid_metric(dataset_path: Path, metric: dict):
    return run_valid_slice_identifier_metric(dataset_path, metric)


@register_metric("slice_identifier_consistency_profile")
def _slice_consistency_metric(dataset_path: Path, metric: dict):
    return run_slice_identifier_consistency_metric(dataset_path, metric)


TABULAR_COMPUTE_METRICS = {
    "kolmogorov_smirnov_feature_divergence": compute_ks_feature_divergence,
    "wasserstein_feature_distance": compute_wasserstein_feature_distance,
    "energy_distance": compute_energy_distance,
    "maximum_mean_discrepancy": compute_maximum_mean_discrepancy,
    "timestamp_parse_success_ratio": compute_timestamp_parse_success_ratio,
    "start_end_timestamp_consistency_ratio": compute_start_end_timestamp_consistency_ratio,
    "non_negative_duration_ratio": compute_non_negative_duration_ratio,
    "inter_arrival_time_distribution_divergence": compute_inter_arrival_time_distribution_divergence,
    "burstiness_coefficient_deviation": compute_burstiness_coefficient_deviation,
    "hourly_activity_distribution_divergence": compute_hourly_activity_distribution_divergence,
    "diurnal_pattern_similarity_score": compute_diurnal_pattern_similarity_score,
    "periodicity_preservation_score": compute_periodicity_preservation_score,
    "per_slice_sample_coverage_ratio": compute_per_slice_sample_coverage_ratio,
    "per_slice_feature_coverage_ratio": compute_per_slice_feature_coverage_ratio,
    "per_slice_class_coverage_ratio": compute_per_slice_class_coverage_ratio,
    "slice_distribution_imbalance_score": compute_slice_distribution_imbalance_score,
    "cross_slice_duplicate_overlap_ratio": compute_cross_slice_duplicate_overlap_ratio,
    "cross_slice_identifier_leakage_ratio": compute_cross_slice_identifier_leakage_ratio,
    "label_coverage_ratio": compute_label_coverage_ratio,
    "per_slice_label_coverage_ratio": compute_per_slice_label_coverage_ratio,
    "per_slice_label_entropy_score": compute_per_slice_label_entropy_score,
    "class_imbalance_score": compute_class_imbalance_score,
    "attack_window_alignment_score": compute_attack_window_alignment_score,
    "pre_post_attack_label_bleed_ratio": compute_pre_post_attack_label_bleed_ratio,
    "train_test_duplicate_overlap_ratio": compute_train_test_duplicate_overlap_ratio,
    "train_test_identifier_contamination_ratio": compute_train_test_identifier_contamination_ratio,
    "feature_wise_wasserstein_distance_from_reference": compute_feature_wise_wasserstein_distance_from_reference,
    "feature_wise_ks_statistic_from_reference": compute_feature_wise_ks_statistic_from_reference,
    "feature_wise_energy_distance_from_reference": compute_feature_wise_energy_distance_from_reference,
    "feature_set_mmd_score_from_reference": compute_feature_set_mmd_score_from_reference,
    "pearson_matrix_deviation_from_reference": compute_pearson_matrix_deviation_from_reference,
    "spearman_matrix_deviation_from_reference": compute_spearman_matrix_deviation_from_reference,
    "distance_correlation_matrix_deviation_from_reference": compute_distance_correlation_matrix_deviation_from_reference,
    "inter_arrival_distribution_divergence_from_reference": compute_inter_arrival_distribution_divergence_from_reference,
    "burstiness_deviation_from_reference": compute_burstiness_deviation_from_reference,
    "hourly_activity_divergence_from_reference": compute_hourly_activity_divergence_from_reference,
    "slice_proportion_deviation_from_reference": compute_slice_proportion_deviation_from_reference,
    "per_slice_class_divergence_from_reference": compute_per_slice_class_divergence_from_reference,
    "per_slice_feature_distribution_deviation_from_reference": compute_per_slice_feature_distribution_deviation_from_reference,
    "protocol_mix_divergence_from_reference": compute_protocol_mix_divergence_from_reference,
    "port_use_divergence_from_reference": compute_port_use_divergence_from_reference,
    "flow_statistic_deviation_from_reference": compute_flow_statistic_deviation_from_reference,
    "benchmark_model_accuracy": compute_benchmark_model_accuracy,
    "benchmark_model_precision": compute_benchmark_model_precision,
    "benchmark_model_recall": compute_benchmark_model_recall,
    "benchmark_model_f1_score": compute_benchmark_model_f1_score,
}


def _wrap_registered_handler(
    handler,
    shared_df: pd.DataFrame | None,
    field_translation: dict[str, str] | None = None,
):
    def _wrapped(dataset_path: Path, metric: dict):
        metric = translate_metric_fields(
            metric,
            field_translation or {},
            shared_df.columns if shared_df is not None else None,
        )
        if shared_df is None:
            return handler(dataset_path, metric)
        metric_with_shared = dict(metric)
        metric_with_shared["_shared_df"] = shared_df
        return handler(dataset_path, metric_with_shared)

    return _wrapped


def _make_tabular_compute_handler(
    metric_id: str,
    compute_fn,
    shared_df: pd.DataFrame | None,
    load_tabular_dataset,
    field_translation: dict[str, str] | None = None,
):
    return lambda dataset_path, metric: run_tabular_metric(
        dataset_path,
        translate_metric_fields(
            metric,
            field_translation or {},
            shared_df.columns if shared_df is not None else None,
        ),
        load_tabular_dataset,
        shared_df,
        metric_id,
        compute_fn,
    )


def build_metric_handlers(
    shared_df: pd.DataFrame | None,
    load_tabular_dataset,
    field_translation: dict[str, str] | None = None,
):
    handlers = {
        metric_id: _wrap_registered_handler(handler, shared_df, field_translation)
        for metric_id, handler in METRIC_REGISTRY.items()
    }

    def _translate(metric: dict):
        return translate_metric_fields(
            metric,
            field_translation or {},
            shared_df.columns if shared_df is not None else None,
        )

    handlers.update(
        {
            "pearson_correlation_profile": lambda dataset_path, metric: run_pearson_metric(
                dataset_path, _translate(metric), load_tabular_dataset, shared_df
            ),
            "spearman_correlation_matrix_deviation": lambda dataset_path, metric: run_spearman_metric(
                dataset_path, _translate(metric), load_tabular_dataset, shared_df
            ),
            "column_quality_profile": lambda dataset_path, metric: run_column_quality_metric(
                dataset_path, _translate(metric), load_tabular_dataset, shared_df
            ),
            "missing_value_ratio": lambda dataset_path, metric: run_missing_value_metric(
                dataset_path, _translate(metric), load_tabular_dataset, shared_df
            ),
            "duplicate_row_ratio": lambda dataset_path, metric: run_duplicate_row_metric(
                dataset_path, _translate(metric), load_tabular_dataset, shared_df
            ),
            "distance_correlation_matrix_deviation": lambda dataset_path, metric: run_distance_correlation_metric(
                dataset_path, _translate(metric), load_tabular_dataset, shared_df
            ),
        }
    )
    handlers.update(
        {
            metric_id: _make_tabular_compute_handler(
                metric_id,
                compute_fn,
                shared_df,
                load_tabular_dataset,
                field_translation,
            )
            for metric_id, compute_fn in TABULAR_COMPUTE_METRICS.items()
        }
    )
    return handlers
