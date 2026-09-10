from __future__ import annotations

from copy import deepcopy

import pandas as pd

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
from cbr_tests.metrics.temporal import (
    compute_burstiness_coefficient_deviation,
    compute_diurnal_pattern_similarity_score,
    compute_hourly_activity_distribution_divergence,
    compute_inter_arrival_time_distribution_divergence,
    compute_periodicity_preservation_score,
)


def _rename_summary_key(result: dict, old_key: str, new_key: str) -> dict:
    output = deepcopy(result)
    summary = output.get("summary", {})
    if old_key in summary:
        summary[new_key] = summary.pop(old_key)
    return output


def compute_inter_arrival_internal_drift_ks(df: pd.DataFrame, metric: dict) -> dict:
    result = compute_inter_arrival_time_distribution_divergence(df, metric)
    return _rename_summary_key(
        result,
        "inter_arrival_time_distribution_divergence",
        "inter_arrival_internal_drift_ks",
    )


def compute_burstiness_internal_drift(df: pd.DataFrame, metric: dict) -> dict:
    result = compute_burstiness_coefficient_deviation(df, metric)
    return _rename_summary_key(
        result,
        "burstiness_coefficient_deviation",
        "burstiness_internal_drift",
    )


def compute_day_to_day_hourly_activity_divergence(df: pd.DataFrame, metric: dict) -> dict:
    result = compute_hourly_activity_distribution_divergence(df, metric)
    return _rename_summary_key(
        result,
        "hourly_activity_distribution_divergence",
        "day_to_day_hourly_activity_divergence",
    )


def compute_day_to_day_diurnal_similarity(df: pd.DataFrame, metric: dict) -> dict:
    result = compute_diurnal_pattern_similarity_score(df, metric)
    return _rename_summary_key(
        result,
        "diurnal_pattern_similarity_score",
        "day_to_day_diurnal_similarity",
    )


def compute_lagged_periodicity_similarity(df: pd.DataFrame, metric: dict) -> dict:
    result = compute_periodicity_preservation_score(df, metric)
    return _rename_summary_key(
        result,
        "periodicity_preservation_score",
        "lagged_periodicity_similarity",
    )


def _mark_internal_distribution_drift(result: dict, *, estimator: str) -> dict:
    output = deepcopy(result)
    output.setdefault("summary", {}).update(
        {
            "comparison_scope": "within_dataset_ordered_half_drift",
            "partition_method": "ordered_halves_of_usable_numeric_sequence",
            "interpretation_direction": "contextual",
            "estimator": estimator,
        }
    )
    return output


def compute_feature_ks_internal_drift(df: pd.DataFrame, metric: dict) -> dict:
    return _mark_internal_distribution_drift(
        compute_ks_feature_divergence(df, metric),
        estimator="empirical_two_sample_ks",
    )


def compute_feature_wasserstein_internal_drift(df: pd.DataFrame, metric: dict) -> dict:
    return _mark_internal_distribution_drift(
        compute_wasserstein_feature_distance(df, metric),
        estimator="empirical_first_wasserstein_1d",
    )


def compute_feature_energy_internal_drift(df: pd.DataFrame, metric: dict) -> dict:
    return _mark_internal_distribution_drift(
        compute_energy_distance(df, metric),
        estimator="empirical_energy_quantity_1d",
    )


def compute_feature_mmd2_internal_drift(df: pd.DataFrame, metric: dict) -> dict:
    return _mark_internal_distribution_drift(
        compute_maximum_mean_discrepancy(df, metric),
        estimator="biased_empirical_squared_rbf_mmd_per_feature",
    )


def _dependency_requirements(metric: dict) -> tuple[list[str], int]:
    requirements = metric.get("input_requirements", {})
    return (
        list(requirements.get("candidate_fields", [])),
        int(requirements.get("minimum_runnable_fields", 2)),
    )


def compute_pearson_dependency_profile(df: pd.DataFrame, metric: dict) -> dict:
    fields, minimum = _dependency_requirements(metric)
    validation, runnable_fields, numeric_df = validate_candidate_fields(df.copy(), fields)
    runnable = len(runnable_fields) >= minimum
    profile = compute_pearson_profile(numeric_df, runnable_fields) if runnable else None
    return {
        "column_validation": validation,
        "profile": profile,
        "summary": {
            "runnable": runnable,
            "runnable_field_count": len(runnable_fields),
            "minimum_runnable_fields": minimum,
            "comparison_scope": "within_dataset_dependency_profile",
            "interpretation_direction": "contextual",
        },
    }


def compute_spearman_dependency_profile(df: pd.DataFrame, metric: dict) -> dict:
    fields, minimum = _dependency_requirements(metric)
    validation, runnable_fields, numeric_df = validate_spearman_candidate_fields(
        df.copy(), fields
    )
    runnable = len(runnable_fields) >= minimum
    profile = compute_spearman_profile(numeric_df, runnable_fields) if runnable else None
    return {
        "column_validation": validation,
        "profile": profile,
        "summary": {
            "runnable": runnable,
            "runnable_field_count": len(runnable_fields),
            "minimum_runnable_fields": minimum,
            "comparison_scope": "within_dataset_dependency_profile",
            "interpretation_direction": "contextual",
        },
    }


def compute_distance_correlation_dependency_profile(df: pd.DataFrame, metric: dict) -> dict:
    fields, minimum = _dependency_requirements(metric)
    result = compute_distance_correlation_profile(df.copy(), fields)
    runnable_fields = result.get("profile", {}).get("fields", [])
    runnable = len(runnable_fields) >= minimum
    return {
        **result,
        "summary": {
            "runnable": runnable,
            "runnable_field_count": len(runnable_fields),
            "minimum_runnable_fields": minimum,
            "comparison_scope": "within_dataset_dependency_profile",
            "interpretation_direction": "contextual",
        },
    }
