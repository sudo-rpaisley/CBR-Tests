from __future__ import annotations

from math import sqrt

import pandas as pd


def _timestamp_field(metric: dict, default: str = "timestamp") -> str:
    return metric.get("input_requirements", {}).get("timestamp_field", default)


def _timestamp_unit(metric: dict) -> str | None:
    unit = metric.get("calculation", {}).get("parameters", {}).get("timestamp_unit")
    if unit is None:
        return None
    unit = str(unit).strip().lower()
    if unit not in {"s", "ms", "us", "ns"}:
        raise ValueError("timestamp_unit must be one of: s, ms, us, ns")
    return unit


def _parse_timestamp_series(
    df: pd.DataFrame,
    field: str,
    unit: str | None = None,
) -> pd.Series:
    if field not in df.columns:
        return pd.Series([pd.NaT] * len(df), index=df.index)
    if unit is not None:
        return pd.to_datetime(df[field], errors="coerce", utc=True, unit=unit)
    return pd.to_datetime(df[field], errors="coerce", utc=True)


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
        max_difference = max(
            max_difference,
            abs(left_index / len(left_sorted) - right_index / len(right_sorted)),
        )
    return max_difference


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
    timestamps = _parse_timestamp_series(df, field, _timestamp_unit(metric))
    row_count = int(len(df))
    parsed_count = int(timestamps.notna().sum())
    return {
        "summary": {
            "timestamp_field": field,
            "row_count": row_count,
            "parsed_count": parsed_count,
            "failed_parse_count": row_count - parsed_count,
            "timestamp_parse_success_ratio": (
                round(parsed_count / row_count, 6) if row_count else 0.0
            ),
        }
    }


def compute_start_end_timestamp_consistency_ratio(
    df: pd.DataFrame,
    metric: dict,
) -> dict:
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
            "start_end_timestamp_consistency_ratio": (
                round(consistent_count / parseable_count, 6)
                if parseable_count
                else 0.0
            ),
        }
    }


def compute_non_negative_duration_ratio(df: pd.DataFrame, metric: dict) -> dict:
    requirements = metric.get("input_requirements", {})
    duration_field = requirements.get("duration_field")
    if duration_field:
        durations = (
            pd.to_numeric(df[duration_field], errors="coerce")
            if duration_field in df.columns
            else pd.Series([], dtype="float64")
        )
    else:
        start = _parse_timestamp_series(
            df, requirements.get("start_timestamp_field", "start_timestamp")
        )
        end = _parse_timestamp_series(
            df, requirements.get("end_timestamp_field", "end_timestamp")
        )
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
            "non_negative_duration_ratio": (
                round(non_negative_count / valid_count, 6) if valid_count else 0.0
            ),
        }
    }


def compute_inter_arrival_time_distribution_divergence(
    df: pd.DataFrame,
    metric: dict,
) -> dict:
    timestamps = _parse_timestamp_series(df, _timestamp_field(metric), _timestamp_unit(metric))
    gaps = _inter_arrival_seconds(timestamps)
    left, right = _split_list(gaps)
    minimum_sample_size = metric.get("calculation", {}).get("parameters", {}).get(
        "minimum_sample_size", 2
    )
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
    standard_deviation = sqrt(variance)
    denominator = standard_deviation + mean
    return (standard_deviation - mean) / denominator if denominator else 0.0


def compute_burstiness_coefficient_deviation(df: pd.DataFrame, metric: dict) -> dict:
    timestamps = _parse_timestamp_series(df, _timestamp_field(metric), _timestamp_unit(metric))
    gaps = _inter_arrival_seconds(timestamps)
    left, right = _split_list(gaps)
    left_burstiness = _burstiness(left)
    right_burstiness = _burstiness(right)
    deviation = (
        abs(left_burstiness - right_burstiness)
        if left_burstiness is not None and right_burstiness is not None
        else None
    )
    return {
        "summary": {
            "gap_count": len(gaps),
            "sample_a_burstiness": (
                round(left_burstiness, 6) if left_burstiness is not None else None
            ),
            "sample_b_burstiness": (
                round(right_burstiness, 6) if right_burstiness is not None else None
            ),
            "burstiness_coefficient_deviation": (
                round(deviation, 6) if deviation is not None else None
            ),
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


def _daily_hour_vectors(
    timestamps: list[pd.Timestamp],
) -> list[tuple[str, list[int]]]:
    """Return one 24-hour UTC activity vector per observed calendar day."""

    by_day: dict[str, list[int]] = {}
    for timestamp in timestamps:
        key = timestamp.date().isoformat()
        counts = by_day.setdefault(key, [0] * 24)
        counts[int(timestamp.hour)] += 1
    return [(day, by_day[day]) for day in sorted(by_day)]


def _mean_pairwise_total_variation(vectors: list[list[int]]) -> tuple[float | None, int]:
    divergences: list[float] = []
    for left_index in range(len(vectors)):
        left = _probabilities(vectors[left_index])
        for right_index in range(left_index + 1, len(vectors)):
            right = _probabilities(vectors[right_index])
            divergences.append(
                0.5 * sum(
                    abs(left_value - right_value)
                    for left_value, right_value in zip(left, right)
                )
            )
    if not divergences:
        return None, 0
    return sum(divergences) / len(divergences), len(divergences)


def _cosine_similarity(left: list[int], right: list[int]) -> float | None:
    numerator = sum(a * b for a, b in zip(left, right))
    left_norm = sqrt(sum(value * value for value in left))
    right_norm = sqrt(sum(value * value for value in right))
    if not left_norm or not right_norm:
        return None
    return numerator / (left_norm * right_norm)


def _mean_pairwise_cosine_similarity(vectors: list[list[int]]) -> tuple[float | None, int]:
    similarities: list[float] = []
    for left_index in range(len(vectors)):
        for right_index in range(left_index + 1, len(vectors)):
            similarity = _cosine_similarity(vectors[left_index], vectors[right_index])
            if similarity is not None:
                similarities.append(similarity)
    if not similarities:
        return None, 0
    return sum(similarities) / len(similarities), len(similarities)


def compute_hourly_activity_distribution_divergence(
    df: pd.DataFrame,
    metric: dict,
) -> dict:
    """Measure day-to-day divergence in UTC hour-of-day activity distributions.

    Comparing chronological packet halves confounds the result with capture time:
    an eight-hour regular capture, for example, puts different hours in each half
    and appears maximally divergent.  This implementation instead compares one
    24-hour profile per observed calendar day and therefore requires at least two
    observed days before producing a value.
    """

    timestamps = (
        _parse_timestamp_series(df, _timestamp_field(metric), _timestamp_unit(metric))
        .dropna()
        .sort_values()
        .tolist()
    )
    parameters = metric.get("calculation", {}).get("parameters", {})
    minimum_day_count = max(2, int(parameters.get("minimum_day_count", 2)))
    day_vectors = _daily_hour_vectors(timestamps)
    day_count = len(day_vectors)
    divergence, pair_count = _mean_pairwise_total_variation(
        [counts for _day, counts in day_vectors]
    )
    runnable = day_count >= minimum_day_count and divergence is not None
    return {
        "summary": {
            "timestamp_count": len(timestamps),
            "observed_day_count": day_count,
            "minimum_day_count": minimum_day_count,
            "day_pair_count": pair_count,
            "first_observed_date": day_vectors[0][0] if day_vectors else None,
            "last_observed_date": day_vectors[-1][0] if day_vectors else None,
            "runnable": runnable,
            "hourly_activity_distribution_divergence": (
                round(divergence, 6) if runnable and divergence is not None else None
            ),
        }
    }


def compute_diurnal_pattern_similarity_score(df: pd.DataFrame, metric: dict) -> dict:
    """Measure day-to-day similarity of UTC hour-of-day activity shapes."""

    timestamps = (
        _parse_timestamp_series(df, _timestamp_field(metric), _timestamp_unit(metric))
        .dropna()
        .sort_values()
        .tolist()
    )
    parameters = metric.get("calculation", {}).get("parameters", {})
    minimum_day_count = max(2, int(parameters.get("minimum_day_count", 2)))
    day_vectors = _daily_hour_vectors(timestamps)
    day_count = len(day_vectors)
    score, pair_count = _mean_pairwise_cosine_similarity(
        [counts for _day, counts in day_vectors]
    )
    runnable = day_count >= minimum_day_count and score is not None
    return {
        "summary": {
            "timestamp_count": len(timestamps),
            "observed_day_count": day_count,
            "minimum_day_count": minimum_day_count,
            "day_pair_count": pair_count,
            "first_observed_date": day_vectors[0][0] if day_vectors else None,
            "last_observed_date": day_vectors[-1][0] if day_vectors else None,
            "runnable": runnable,
            "diurnal_pattern_similarity_score": (
                round(score, 6) if runnable and score is not None else None
            ),
        }
    }


def _continuous_hourly_counts(
    timestamps: list[pd.Timestamp],
) -> tuple[list[int], pd.Timestamp | None, pd.Timestamp | None]:
    if not timestamps:
        return [], None, None
    first_hour = timestamps[0].floor("h")
    last_hour = timestamps[-1].floor("h")
    hourly_index = pd.date_range(first_hour, last_hour, freq="h")
    counts_by_hour = {hour: 0 for hour in hourly_index}
    for timestamp in timestamps:
        counts_by_hour[timestamp.floor("h")] += 1
    return [counts_by_hour[hour] for hour in hourly_index], first_hour, last_hour


def _lag_repeat_similarity(
    values: list[int],
    lag: int,
    minimum_pairs: int,
) -> tuple[float | None, int]:
    if lag <= 0:
        raise ValueError("periodicity lags must be positive integers")
    pair_count = len(values) - lag
    if pair_count < minimum_pairs:
        return None, max(0, pair_count)

    earlier = values[:-lag]
    later = values[lag:]
    activity = sum(max(left, right) for left, right in zip(earlier, later))
    if activity == 0:
        return None, pair_count
    difference = sum(abs(left - right) for left, right in zip(earlier, later))
    return 1.0 - (difference / activity), pair_count


def compute_periodicity_preservation_score(df: pd.DataFrame, metric: dict) -> dict:
    """Measure how closely hourly activity repeats at configured temporal lags.

    The previous implementation autocorrelated two 24-element hour-of-day
    histograms, so a lag of 24 could never be evaluated.  Here the lag is applied
    to the actual continuous hourly activity series: lag 24 therefore compares
    each observed hour with the corresponding hour one day later.
    """

    timestamps = (
        _parse_timestamp_series(df, _timestamp_field(metric), _timestamp_unit(metric))
        .dropna()
        .sort_values()
        .tolist()
    )
    parameters = metric.get("calculation", {}).get("parameters", {})
    raw_lags = parameters.get("lags", [24])
    lags: list[int] = []
    for raw_lag in raw_lags:
        lag = int(raw_lag)
        if lag <= 0:
            raise ValueError("periodicity lags must be positive integers")
        if lag not in lags:
            lags.append(lag)
    if not lags:
        raise ValueError("periodicity lags must contain at least one positive integer")

    minimum_lag_pairs = max(1, int(parameters.get("minimum_lag_pairs", 2)))
    hourly_counts, first_hour, last_hour = _continuous_hourly_counts(timestamps)
    lag_results = []
    similarities: list[float] = []
    all_lags_runnable = True
    for lag in lags:
        similarity, pair_count = _lag_repeat_similarity(
            hourly_counts,
            lag,
            minimum_lag_pairs,
        )
        lag_runnable = similarity is not None
        all_lags_runnable = all_lags_runnable and lag_runnable
        if similarity is not None:
            similarities.append(similarity)
        lag_results.append(
            {
                "lag_hours": lag,
                "paired_hour_count": pair_count,
                "minimum_lag_pairs": minimum_lag_pairs,
                "runnable": lag_runnable,
                "repeat_similarity": (
                    round(similarity, 6) if similarity is not None else None
                ),
            }
        )

    runnable = bool(hourly_counts) and all_lags_runnable and len(similarities) == len(lags)
    score = sum(similarities) / len(similarities) if runnable else None
    return {
        "lags": lag_results,
        "summary": {
            "timestamp_count": len(timestamps),
            "hourly_bin_count": len(hourly_counts),
            "first_hour": first_hour.isoformat() if first_hour is not None else None,
            "last_hour": last_hour.isoformat() if last_hour is not None else None,
            "configured_lags_hours": lags,
            "minimum_lag_pairs": minimum_lag_pairs,
            "runnable": runnable,
            "periodicity_preservation_score": (
                round(score, 6) if score is not None else None
            ),
        },
    }
