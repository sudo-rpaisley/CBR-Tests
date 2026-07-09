from itertools import combinations
from math import exp, sqrt
from statistics import median

import pandas as pd

from tests.pearson_profile import validate_candidate_fields


def _clean_numeric_values(df: pd.DataFrame, field: str) -> list[float]:
    series = pd.to_numeric(df[field], errors="coerce").dropna()
    return [float(value) for value in series.tolist()]


def _split_values(values: list[float]) -> tuple[list[float], list[float]]:
    midpoint = len(values) // 2
    return values[:midpoint], values[midpoint:]


def _mean_pairwise_abs_distance(left: list[float], right: list[float]) -> float:
    if not left or not right:
        return 0.0
    total = 0.0
    for a in left:
        for b in right:
            total += abs(a - b)
    return total / (len(left) * len(right))


def _ks_statistic(left: list[float], right: list[float]) -> float:
    left_sorted = sorted(left)
    right_sorted = sorted(right)
    values = sorted(set(left_sorted + right_sorted))
    left_index = 0
    right_index = 0
    max_diff = 0.0
    for value in values:
        while left_index < len(left_sorted) and left_sorted[left_index] <= value:
            left_index += 1
        while right_index < len(right_sorted) and right_sorted[right_index] <= value:
            right_index += 1
        left_cdf = left_index / len(left_sorted)
        right_cdf = right_index / len(right_sorted)
        max_diff = max(max_diff, abs(left_cdf - right_cdf))
    return max_diff


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
    cross = _mean_pairwise_abs_distance(left, right)
    left_internal = _mean_pairwise_abs_distance(left, left)
    right_internal = _mean_pairwise_abs_distance(right, right)
    return max(0.0, 2 * cross - left_internal - right_internal)


def _rbf_mmd(left: list[float], right: list[float], gamma: float | None = None) -> float:
    combined = left + right
    if gamma is None:
        distances = [abs(a - b) for a, b in combinations(combined, 2) if a != b]
        sigma = median(distances) if distances else 1.0
        gamma = 1.0 / (2.0 * sigma * sigma) if sigma else 1.0

    def kernel_mean(a_values: list[float], b_values: list[float]) -> float:
        total = 0.0
        for a in a_values:
            for b in b_values:
                total += exp(-gamma * (a - b) ** 2)
        return total / (len(a_values) * len(b_values))

    value = kernel_mean(left, left) + kernel_mean(right, right) - 2 * kernel_mean(left, right)
    return max(0.0, value)


def _build_distributional_metric(df: pd.DataFrame, metric: dict, calculator, output_key: str) -> dict:
    candidate_fields = metric["input_requirements"]["candidate_fields"]
    minimum_sample_size = metric.get("calculation", {}).get("parameters", {}).get("minimum_sample_size", 2)
    max_sample_size = metric.get("calculation", {}).get("parameters", {}).get("max_sample_size", 1000)

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
            f"mean_{output_key}": round(sum(values) / len(values), 6) if values else None,
            f"max_{output_key}": round(max(values), 6) if values else None,
        },
    }


def compute_ks_feature_divergence(df: pd.DataFrame, metric: dict) -> dict:
    return _build_distributional_metric(df, metric, _ks_statistic, "ks_statistic")


def compute_wasserstein_feature_distance(df: pd.DataFrame, metric: dict) -> dict:
    return _build_distributional_metric(df, metric, _wasserstein_distance, "wasserstein_distance")


def compute_energy_distance(df: pd.DataFrame, metric: dict) -> dict:
    return _build_distributional_metric(df, metric, _energy_distance, "energy_distance")


def compute_maximum_mean_discrepancy(df: pd.DataFrame, metric: dict) -> dict:
    return _build_distributional_metric(df, metric, _rbf_mmd, "maximum_mean_discrepancy")


def _distance_matrix(values: list[float]) -> list[list[float]]:
    return [[abs(a - b) for b in values] for a in values]


def _double_center(matrix: list[list[float]]) -> list[list[float]]:
    n = len(matrix)
    row_means = [sum(row) / n for row in matrix]
    col_means = [sum(matrix[row][col] for row in range(n)) / n for col in range(n)]
    grand_mean = sum(row_means) / n
    return [[matrix[row][col] - row_means[row] - col_means[col] + grand_mean for col in range(n)] for row in range(n)]


def _mean_product(left: list[list[float]], right: list[list[float]]) -> float:
    n = len(left)
    total = 0.0
    for row in range(n):
        for col in range(n):
            total += left[row][col] * right[row][col]
    return total / (n * n)


def _distance_correlation(left: list[float], right: list[float]) -> float:
    if len(left) < 2 or len(right) < 2 or len(left) != len(right):
        return 0.0
    left_centered = _double_center(_distance_matrix(left))
    right_centered = _double_center(_distance_matrix(right))
    dcov2 = _mean_product(left_centered, right_centered)
    dvar_left = _mean_product(left_centered, left_centered)
    dvar_right = _mean_product(right_centered, right_centered)
    denominator = sqrt(dvar_left * dvar_right)
    if denominator <= 0:
        return 0.0
    return sqrt(max(0.0, dcov2 / denominator))


def compute_distance_correlation_profile(df: pd.DataFrame, candidate_fields: list[str]) -> dict:
    column_validation, runnable_fields, df = validate_candidate_fields(df, candidate_fields)
    for result in column_validation:
        result["usable_for_distance_correlation"] = result.pop("usable_for_pearson")

    matrix = {field: {} for field in runnable_fields}
    pairs = []
    for field in runnable_fields:
        matrix[field][field] = 1.0

    for a, b in combinations(runnable_fields, 2):
        pair_df = df[[a, b]].dropna()
        left = [float(value) for value in pair_df[a].tolist()]
        right = [float(value) for value in pair_df[b].tolist()]
        value = round(_distance_correlation(left, right), 6)
        matrix[a][b] = value
        matrix[b][a] = value
        pairs.append({
            "fields": [a, b],
            "value": value,
            "overlap_non_null_count": int(pair_df.shape[0]),
        })

    mean_abs_correlation = round(sum(abs(pair["value"]) for pair in pairs) / len(pairs), 6) if pairs else None
    return {
        "column_validation": column_validation,
        "profile": {
            "fields": runnable_fields,
            "matrix": matrix,
            "summary": {
                "pair_count": len(pairs),
                "mean_absolute_correlation": mean_abs_correlation,
                "pairs": pairs,
            },
        },
    }
