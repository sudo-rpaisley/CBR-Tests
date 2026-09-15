from __future__ import annotations

from itertools import combinations
from math import exp, sqrt
from statistics import median

import numpy as np
import pandas as pd

from cbr_tests.metrics.pearson import validate_candidate_fields


# Pairwise estimators below are O(n^2). This ceiling is deliberately enforced
# inside the metric implementation so callers cannot accidentally bypass the
# memory guard by omitting or oversizing max_sample_size.
PAIRWISE_SAMPLE_HARD_LIMIT = 1000
DEFAULT_DISTANCE_CORRELATION_MAX_SAMPLE_SIZE = 1000


def _clean_numeric_values(df: pd.DataFrame, field: str) -> list[float]:
    series = pd.to_numeric(df[field], errors="coerce").dropna()
    return [float(value) for value in series.tolist()]


def _split_values(values: list[float]) -> tuple[list[float], list[float]]:
    midpoint = len(values) // 2
    return values[:midpoint], values[midpoint:]


def _validated_pairwise_limit(
    value,
    *,
    default: int = PAIRWISE_SAMPLE_HARD_LIMIT,
) -> tuple[int, int]:
    requested = default if value is None else int(value)
    if requested < 2:
        raise ValueError("max_sample_size must be at least 2 for pairwise statistical metrics")
    return requested, min(requested, PAIRWISE_SAMPLE_HARD_LIMIT)


def _even_positions(length: int, maximum: int) -> list[int]:
    if length <= maximum:
        return list(range(length))
    if maximum <= 1:
        return [0]
    step = (length - 1) / (maximum - 1)
    return [round(index * step) for index in range(maximum)]


def _bounded_pairwise_dataframe_sample(
    df: pd.DataFrame,
    max_sample_size: int | None = None,
) -> tuple[pd.DataFrame, dict]:
    requested, effective = _validated_pairwise_limit(
        max_sample_size,
        default=DEFAULT_DISTANCE_CORRELATION_MAX_SAMPLE_SIZE,
    )
    original_row_count = int(len(df))
    if original_row_count > effective:
        sampled = df.iloc[_even_positions(original_row_count, effective)].copy()
    else:
        sampled = df.copy()
    return sampled, {
        "method": "deterministic_evenly_spaced_rows",
        "original_row_count": original_row_count,
        "sampled_row_count": int(len(sampled)),
        "requested_max_sample_size": requested,
        "effective_max_sample_size": effective,
        "hard_max_sample_size": PAIRWISE_SAMPLE_HARD_LIMIT,
        "sampling_applied": original_row_count > effective,
        "safety_cap_applied": requested > PAIRWISE_SAMPLE_HARD_LIMIT,
    }


def _ensure_pairwise_sample_safe(*samples) -> None:
    largest = max((len(sample) for sample in samples), default=0)
    if largest > PAIRWISE_SAMPLE_HARD_LIMIT:
        raise ValueError(
            "Pairwise statistical sample exceeds the safety limit of "
            f"{PAIRWISE_SAMPLE_HARD_LIMIT} values; reduce max_sample_size."
        )


def _mean_pairwise_abs_distance(left: list[float], right: list[float]) -> float:
    if not left or not right:
        return 0.0
    _ensure_pairwise_sample_safe(left, right)
    total = sum(abs(a - b) for a in left for b in right)
    return total / (len(left) * len(right))


def _ks_statistic(left: list[float], right: list[float]) -> float:
    left_sorted = sorted(left)
    right_sorted = sorted(right)
    values = sorted(set(left_sorted + right_sorted))
    left_index = 0
    right_index = 0
    max_difference = 0.0
    for value in values:
        while left_index < len(left_sorted) and left_sorted[left_index] <= value:
            left_index += 1
        while right_index < len(right_sorted) and right_sorted[right_index] <= value:
            right_index += 1
        left_cdf = left_index / len(left_sorted)
        right_cdf = right_index / len(right_sorted)
        max_difference = max(max_difference, abs(left_cdf - right_cdf))
    return max_difference


def _wasserstein_distance(left: list[float], right: list[float]) -> float:
    left_sorted = sorted(left)
    right_sorted = sorted(right)
    values = sorted(set(left_sorted + right_sorted))
    if len(values) < 2:
        return 0.0

    left_index = 0
    right_index = 0
    distance = 0.0
    previous_value = values[0]
    for value in values[1:]:
        while left_index < len(left_sorted) and left_sorted[left_index] <= previous_value:
            left_index += 1
        while right_index < len(right_sorted) and right_sorted[right_index] <= previous_value:
            right_index += 1
        left_cdf = left_index / len(left_sorted)
        right_cdf = right_index / len(right_sorted)
        distance += abs(left_cdf - right_cdf) * (value - previous_value)
        previous_value = value
    return distance


def _energy_distance(left: list[float], right: list[float]) -> float:
    _ensure_pairwise_sample_safe(left, right)
    cross = _mean_pairwise_abs_distance(left, right)
    left_internal = _mean_pairwise_abs_distance(left, left)
    right_internal = _mean_pairwise_abs_distance(right, right)
    return max(0.0, 2 * cross - left_internal - right_internal)


def _rbf_mmd(
    left: list[float],
    right: list[float],
    gamma: float | None = None,
) -> float:
    _ensure_pairwise_sample_safe(left, right)
    combined = left + right
    if gamma is None:
        distances = [abs(a - b) for a, b in combinations(combined, 2) if a != b]
        sigma = median(distances) if distances else 1.0
        gamma = 1.0 / (2.0 * sigma * sigma) if sigma else 1.0

    def kernel_mean(a_values: list[float], b_values: list[float]) -> float:
        total = sum(
            exp(-gamma * (a - b) ** 2) for a in a_values for b in b_values
        )
        return total / (len(a_values) * len(b_values))

    value = (
        kernel_mean(left, left)
        + kernel_mean(right, right)
        - 2 * kernel_mean(left, right)
    )
    return max(0.0, value)


def _build_distributional_metric(
    df: pd.DataFrame,
    metric: dict,
    calculator,
    output_key: str,
) -> dict:
    candidate_fields = metric["input_requirements"]["candidate_fields"]
    parameters = metric.get("calculation", {}).get("parameters", {})
    minimum_sample_size = int(parameters.get("minimum_sample_size", 2))
    max_sample_size = int(parameters.get("max_sample_size", 1000))
    if max_sample_size < 2:
        raise ValueError("max_sample_size must be at least 2")

    field_results = []
    runnable_count = 0
    for field in candidate_fields:
        result = {
            "field": field,
            "exists": field in df.columns,
            "sample_a_count": 0,
            "sample_b_count": 0,
            "runnable": False,
            output_key: None,
            "reason": None,
        }
        if field not in df.columns:
            result["reason"] = "missing_column"
            field_results.append(result)
            continue

        values = _clean_numeric_values(df, field)[: max_sample_size * 2]
        left, right = _split_values(values)
        result["sample_a_count"] = len(left)
        result["sample_b_count"] = len(right)
        if len(left) < minimum_sample_size or len(right) < minimum_sample_size:
            result["reason"] = "insufficient_numeric_values"
            field_results.append(result)
            continue

        result["runnable"] = True
        result["reason"] = "usable"
        result[output_key] = round(float(calculator(left, right)), 6)
        runnable_count += 1
        field_results.append(result)

    values = [field[output_key] for field in field_results if field[output_key] is not None]
    return {
        "fields": field_results,
        "summary": {
            "field_count": len(field_results),
            "runnable_field_count": runnable_count,
            "max_sample_size_per_half": max_sample_size,
            f"mean_{output_key}": (
                round(sum(values) / len(values), 6) if values else None
            ),
            f"max_{output_key}": round(max(values), 6) if values else None,
        },
    }


def compute_ks_feature_divergence(df: pd.DataFrame, metric: dict) -> dict:
    return _build_distributional_metric(df, metric, _ks_statistic, "ks_statistic")


def compute_wasserstein_feature_distance(df: pd.DataFrame, metric: dict) -> dict:
    return _build_distributional_metric(
        df, metric, _wasserstein_distance, "wasserstein_distance"
    )


def compute_energy_distance(df: pd.DataFrame, metric: dict) -> dict:
    return _build_distributional_metric(df, metric, _energy_distance, "energy_distance")


def compute_maximum_mean_discrepancy(df: pd.DataFrame, metric: dict) -> dict:
    return _build_distributional_metric(
        df, metric, _rbf_mmd, "maximum_mean_discrepancy"
    )


def _distance_matrix(values: list[float]) -> list[list[float]]:
    """Compatibility helper retained for historical callers/tests."""
    _ensure_pairwise_sample_safe(values)
    return [[abs(a - b) for b in values] for a in values]


def _double_center(matrix: list[list[float]]) -> list[list[float]]:
    size = len(matrix)
    row_means = [sum(row) / size for row in matrix]
    column_means = [
        sum(matrix[row][column] for row in range(size)) / size
        for column in range(size)
    ]
    grand_mean = sum(row_means) / size
    return [
        [
            matrix[row][column]
            - row_means[row]
            - column_means[column]
            + grand_mean
            for column in range(size)
        ]
        for row in range(size)
    ]


def _mean_product(left: list[list[float]], right: list[list[float]]) -> float:
    size = len(left)
    total = sum(
        left[row][column] * right[row][column]
        for row in range(size)
        for column in range(size)
    )
    return total / (size * size)


def _distance_correlation(left: list[float], right: list[float]) -> float:
    if len(left) < 2 or len(right) < 2 or len(left) != len(right):
        return 0.0
    _ensure_pairwise_sample_safe(left, right)

    left_values = np.asarray(left, dtype=np.float64)
    right_values = np.asarray(right, dtype=np.float64)
    left_distances = np.abs(left_values[:, None] - left_values[None, :])
    right_distances = np.abs(right_values[:, None] - right_values[None, :])

    left_centered = (
        left_distances
        - left_distances.mean(axis=1, keepdims=True)
        - left_distances.mean(axis=0, keepdims=True)
        + left_distances.mean()
    )
    right_centered = (
        right_distances
        - right_distances.mean(axis=1, keepdims=True)
        - right_distances.mean(axis=0, keepdims=True)
        + right_distances.mean()
    )

    covariance_squared = float(np.mean(left_centered * right_centered))
    left_variance = float(np.mean(left_centered * left_centered))
    right_variance = float(np.mean(right_centered * right_centered))
    denominator = sqrt(left_variance * right_variance)
    if denominator <= 0:
        return 0.0
    return sqrt(max(0.0, covariance_squared / denominator))


def compute_distance_correlation_profile(
    df: pd.DataFrame,
    candidate_fields: list[str],
    max_sample_size: int | None = None,
) -> dict:
    df, sampling = _bounded_pairwise_dataframe_sample(df, max_sample_size)
    column_validation, runnable_fields, df = validate_candidate_fields(
        df, candidate_fields
    )
    for result in column_validation:
        result["usable_for_distance_correlation"] = result.pop("usable_for_pearson")

    matrix = {field: {field: 1.0} for field in runnable_fields}
    pairs = []
    for left_field, right_field in combinations(runnable_fields, 2):
        pair_df = df[[left_field, right_field]].dropna()
        left = [float(value) for value in pair_df[left_field].tolist()]
        right = [float(value) for value in pair_df[right_field].tolist()]
        value = round(_distance_correlation(left, right), 6)
        matrix[left_field][right_field] = value
        matrix[right_field][left_field] = value
        pairs.append(
            {
                "fields": [left_field, right_field],
                "value": value,
                "overlap_non_null_count": int(pair_df.shape[0]),
            }
        )

    mean_absolute_correlation = (
        round(sum(abs(pair["value"]) for pair in pairs) / len(pairs), 6)
        if pairs
        else None
    )
    return {
        "column_validation": column_validation,
        "sampling": sampling,
        "profile": {
            "fields": runnable_fields,
            "matrix": matrix,
            "summary": {
                "pair_count": len(pairs),
                "mean_absolute_correlation": mean_absolute_correlation,
                "pairs": pairs,
                "sampling": sampling,
            },
        },
    }
