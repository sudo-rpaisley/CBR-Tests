from math import sqrt

import pandas as pd


def _timestamp_field(metric: dict, default: str = "timestamp") -> str:
    return metric.get("input_requirements", {}).get("timestamp_field", default)


def _parse_timestamp_series(df: pd.DataFrame, field: str) -> pd.Series:
    if field not in df.columns:
        return pd.Series([pd.NaT] * len(df), index=df.index)
    return pd.to_datetime(df[field], errors="coerce", utc=True)


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
        max_diff = max(max_diff, abs(left_index / len(left_sorted) - right_index / len(right_sorted)))
    return max_diff


def _split_list(values: list) -> tuple[list, list]:
    midpoint = len(values) // 2
    return values[:midpoint], values[midpoint:]


def _inter_arrival_seconds(timestamps: pd.Series) -> list[float]:
    parsed = timestamps.dropna().sort_values()
    if len(parsed) < 2:
        return []
    deltas = parsed.diff().dropna().dt.total_seconds()
    return [float(value) for value in deltas.tolist()]


def compute_timestamp_parse_success_ratio(df: pd.DataFrame, metric: dict) -> dict:
    field = _timestamp_field(metric)
    timestamps = _parse_timestamp_series(df, field)
    row_count = int(len(df))
    parsed_count = int(timestamps.notna().sum())
    return {
        "summary": {
            "timestamp_field": field,
            "row_count": row_count,
            "parsed_count": parsed_count,
            "failed_parse_count": row_count - parsed_count,
            "timestamp_parse_success_ratio": round(parsed_count / row_count, 6) if row_count else 0.0,
        }
    }


def compute_start_end_timestamp_consistency_ratio(df: pd.DataFrame, metric: dict) -> dict:
    requirements = metric.get("input_requirements", {})
    start_field = requirements.get("start_timestamp_field", "start_timestamp")
    end_field = requirements.get("end_timestamp_field", "end_timestamp")
    start = _parse_timestamp_series(df, start_field)
    end = _parse_timestamp_series(df, end_field)
    parseable = start.notna() & end.notna()
    consistent = parseable & (start <= end)
    parseable_count = int(parseable.sum())
    consistent_count = int(consistent.sum())
    return {
        "summary": {
            "start_timestamp_field": start_field,
            "end_timestamp_field": end_field,
            "row_count": int(len(df)),
            "parseable_pair_count": parseable_count,
            "consistent_pair_count": consistent_count,
            "inconsistent_pair_count": parseable_count - consistent_count,
            "start_end_timestamp_consistency_ratio": round(consistent_count / parseable_count, 6) if parseable_count else 0.0,
        }
    }


def compute_non_negative_duration_ratio(df: pd.DataFrame, metric: dict) -> dict:
    requirements = metric.get("input_requirements", {})
    duration_field = requirements.get("duration_field")
    if duration_field:
        durations = pd.to_numeric(df[duration_field], errors="coerce") if duration_field in df.columns else pd.Series([], dtype="float64")
    else:
        start_field = requirements.get("start_timestamp_field", "start_timestamp")
        end_field = requirements.get("end_timestamp_field", "end_timestamp")
        start = _parse_timestamp_series(df, start_field)
        end = _parse_timestamp_series(df, end_field)
        durations = (end - start).dt.total_seconds()

    valid = durations.dropna()
    valid_count = int(valid.shape[0])
    non_negative_count = int((valid >= 0).sum())
    return {
        "summary": {
            "duration_field": duration_field,
            "row_count": int(len(df)),
            "valid_duration_count": valid_count,
            "negative_duration_count": valid_count - non_negative_count,
            "non_negative_duration_ratio": round(non_negative_count / valid_count, 6) if valid_count else 0.0,
        }
    }


def compute_inter_arrival_time_distribution_divergence(df: pd.DataFrame, metric: dict) -> dict:
    timestamps = _parse_timestamp_series(df, _timestamp_field(metric))
    gaps = _inter_arrival_seconds(timestamps)
    left, right = _split_list(gaps)
    minimum_sample_size = metric.get("calculation", {}).get("parameters", {}).get("minimum_sample_size", 2)
    runnable = len(left) >= minimum_sample_size and len(right) >= minimum_sample_size
    divergence = round(_ks_statistic(left, right), 6) if runnable else None
    return {
        "summary": {
            "gap_count": len(gaps),
            "sample_a_count": len(left),
            "sample_b_count": len(right),
            "runnable": runnable,
            "inter_arrival_time_distribution_divergence": divergence,
        }
    }


def _burstiness(values: list[float]) -> float | None:
    if not values:
        return None
    mean = sum(values) / len(values)
    variance = sum((value - mean) ** 2 for value in values) / len(values)
    stddev = sqrt(variance)
    denominator = stddev + mean
    return (stddev - mean) / denominator if denominator else 0.0


def compute_burstiness_coefficient_deviation(df: pd.DataFrame, metric: dict) -> dict:
    timestamps = _parse_timestamp_series(df, _timestamp_field(metric))
    gaps = _inter_arrival_seconds(timestamps)
    left, right = _split_list(gaps)
    left_burstiness = _burstiness(left)
    right_burstiness = _burstiness(right)
    deviation = abs(left_burstiness - right_burstiness) if left_burstiness is not None and right_burstiness is not None else None
    return {
        "summary": {
            "gap_count": len(gaps),
            "sample_a_burstiness": round(left_burstiness, 6) if left_burstiness is not None else None,
            "sample_b_burstiness": round(right_burstiness, 6) if right_burstiness is not None else None,
            "burstiness_coefficient_deviation": round(deviation, 6) if deviation is not None else None,
        }
    }


def _hourly_counts(timestamps: list[pd.Timestamp]) -> list[int]:
    counts = [0] * 24
    for timestamp in timestamps:
        counts[int(timestamp.hour)] += 1
    return counts


def _probabilities(counts: list[int]) -> list[float]:
    total = sum(counts)
    return [count / total for count in counts] if total else [0.0] * len(counts)


def compute_hourly_activity_distribution_divergence(df: pd.DataFrame, metric: dict) -> dict:
    timestamps = _parse_timestamp_series(df, _timestamp_field(metric)).dropna().sort_values().tolist()
    left, right = _split_list(timestamps)
    left_probs = _probabilities(_hourly_counts(left))
    right_probs = _probabilities(_hourly_counts(right))
    divergence = 0.5 * sum(abs(a - b) for a, b in zip(left_probs, right_probs))
    return {
        "summary": {
            "sample_a_count": len(left),
            "sample_b_count": len(right),
            "hourly_activity_distribution_divergence": round(divergence, 6),
        }
    }


def compute_diurnal_pattern_similarity_score(df: pd.DataFrame, metric: dict) -> dict:
    timestamps = _parse_timestamp_series(df, _timestamp_field(metric)).dropna().sort_values().tolist()
    left, right = _split_list(timestamps)
    left_counts = _hourly_counts(left)
    right_counts = _hourly_counts(right)
    numerator = sum(a * b for a, b in zip(left_counts, right_counts))
    left_norm = sqrt(sum(a * a for a in left_counts))
    right_norm = sqrt(sum(b * b for b in right_counts))
    score = numerator / (left_norm * right_norm) if left_norm and right_norm else 0.0
    return {
        "summary": {
            "sample_a_count": len(left),
            "sample_b_count": len(right),
            "diurnal_pattern_similarity_score": round(score, 6),
        }
    }


def _autocorrelation(values: list[int], lag: int) -> float | None:
    if len(values) <= lag:
        return None
    mean = sum(values) / len(values)
    numerator = sum((values[i] - mean) * (values[i - lag] - mean) for i in range(lag, len(values)))
    denominator = sum((value - mean) ** 2 for value in values)
    return numerator / denominator if denominator else 0.0


def compute_periodicity_preservation_score(df: pd.DataFrame, metric: dict) -> dict:
    timestamps = _parse_timestamp_series(df, _timestamp_field(metric)).dropna().sort_values().tolist()
    left, right = _split_list(timestamps)
    lags = metric.get("calculation", {}).get("parameters", {}).get("lags", [1, 24])
    left_counts = _hourly_counts(left)
    right_counts = _hourly_counts(right)
    lag_results = []
    deviations = []
    for lag in lags:
        left_autocorr = _autocorrelation(left_counts, int(lag))
        right_autocorr = _autocorrelation(right_counts, int(lag))
        if left_autocorr is None or right_autocorr is None:
            continue
        deviation = abs(left_autocorr - right_autocorr)
        deviations.append(deviation)
        lag_results.append({
            "lag": int(lag),
            "sample_a_autocorrelation": round(left_autocorr, 6),
            "sample_b_autocorrelation": round(right_autocorr, 6),
            "deviation": round(deviation, 6),
        })
    score = 1.0 - min(1.0, sum(deviations) / len(deviations)) if deviations else None
    return {
        "lags": lag_results,
        "summary": {
            "sample_a_count": len(left),
            "sample_b_count": len(right),
            "periodicity_preservation_score": round(score, 6) if score is not None else None,
        },
    }
