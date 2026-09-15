from __future__ import annotations

from math import sqrt
from pathlib import Path
from threading import Lock

import numpy as np
import pandas as pd

from cbr_tests.metrics.statistical import (
    _energy_distance,
    _ks_statistic,
    _wasserstein_distance,
    compute_distance_correlation_profile,
)
from cbr_tests.metrics.temporal import (
    _burstiness,
    _hourly_counts,
    _inter_arrival_seconds,
    _parse_timestamp_series,
    _probabilities,
    _timestamp_unit,
)
from runner.pcap_adapter import build_pcap_packet_dataframe, is_packet_capture


def _reference_path(metric: dict) -> str | None:
    requirements = metric.get("input_requirements", {})
    params = metric.get("calculation", {}).get("parameters", {})
    return requirements.get("reference_dataset_path") or params.get("reference_dataset_path")


_REFERENCE_DF_CACHE: dict[str, pd.DataFrame] = {}
_REFERENCE_DF_CACHE_LOCK = Lock()


def _apply_reference_field_map(dataframe: pd.DataFrame, metric: dict) -> pd.DataFrame:
    mapping = metric.get("reference_field_map", {})
    if not isinstance(mapping, dict) or not mapping:
        return dataframe
    rename_map = {
        str(source): str(target)
        for source, target in mapping.items()
        if str(source).strip() and str(target).strip() and source != target
    }
    collisions = [
        target
        for source, target in rename_map.items()
        if target in dataframe.columns and target not in rename_map
    ]
    if collisions:
        raise ValueError(
            "Reference field mapping would overwrite existing columns: "
            + ", ".join(sorted(set(collisions)))
        )
    return dataframe.rename(columns=rename_map)


def _load_reference_df(metric: dict) -> pd.DataFrame:
    shared = metric.get("_reference_df")
    if isinstance(shared, pd.DataFrame):
        return _apply_reference_field_map(shared.copy(), metric)

    path_value = _reference_path(metric)
    if not path_value:
        raise ValueError("reference_dataset_path is required for reference-comparison metrics")
    path = Path(path_value).expanduser().resolve()
    if not path.exists() or not path.is_file():
        raise FileNotFoundError(f"Reference dataset does not exist or is not a file: {path}")

    cache_key = str(path)
    with _REFERENCE_DF_CACHE_LOCK:
        cached = _REFERENCE_DF_CACHE.get(cache_key)
        if cached is not None:
            return _apply_reference_field_map(cached.copy(), metric)
        suffix = path.suffix.lower()
        if is_packet_capture(path):
            dataframe = build_pcap_packet_dataframe(path)
        elif suffix in {".csv", ".tsv"}:
            dataframe = pd.read_csv(
                path,
                sep="\t" if suffix == ".tsv" else ",",
                skipinitialspace=True,
                low_memory=False,
            )
        elif suffix in {".xlsx", ".xls"}:
            dataframe = pd.read_excel(path)
        else:
            raise ValueError(f"Unsupported reference dataset format: {suffix or '<none>'}")
        if dataframe.empty:
            raise ValueError(f"Reference dataset contains no usable rows: {path}")
        _REFERENCE_DF_CACHE[cache_key] = dataframe
        return _apply_reference_field_map(dataframe.copy(), metric)


def _candidate_fields(metric: dict) -> list[str]:
    return metric.get("input_requirements", {}).get("candidate_fields", [])


def _even_positions(length: int, maximum: int) -> list[int]:
    if length <= maximum:
        return list(range(length))
    if maximum <= 1:
        return [0]
    step = (length - 1) / (maximum - 1)
    return [round(index * step) for index in range(maximum)]


def _numeric_values(df: pd.DataFrame, field: str, max_sample_size: int) -> list[float]:
    if field not in df.columns:
        return []
    series = pd.to_numeric(df[field], errors="coerce").dropna().reset_index(drop=True)
    if series.empty:
        return []
    positions = _even_positions(len(series), max(1, int(max_sample_size)))
    return [float(series.iloc[position]) for position in positions]


def _sample_dataframe(df: pd.DataFrame, max_sample_size: int) -> pd.DataFrame:
    if len(df) <= max_sample_size:
        return df
    return df.iloc[_even_positions(len(df), max_sample_size)].copy()


def _numeric_matrix(
    df: pd.DataFrame, fields: list[str], max_sample_size: int
) -> tuple[np.ndarray, list[str]]:
    usable = [field for field in fields if field in df.columns]
    if not usable:
        return np.empty((0, 0)), []
    numeric = df[usable].apply(pd.to_numeric, errors="coerce").dropna()
    numeric = _sample_dataframe(numeric, max_sample_size)
    return numeric.to_numpy(dtype=float), usable


def _multivariate_rbf_mmd(
    current: np.ndarray, reference: np.ndarray
) -> tuple[float | None, float | None]:
    if current.size == 0 or reference.size == 0 or current.shape[1] != reference.shape[1]:
        return None, None
    pooled = np.vstack([current, reference])
    scale = pooled.std(axis=0)
    scale = np.where(scale > 0, scale, 1.0)
    centre = pooled.mean(axis=0)
    current_z = (current - centre) / scale
    reference_z = (reference - centre) / scale
    pooled_z = np.vstack([current_z, reference_z])

    def squared_distances(left, right):
        values = (
            np.sum(left * left, axis=1)[:, None]
            + np.sum(right * right, axis=1)[None, :]
            - 2.0 * left.dot(right.T)
        )
        return np.maximum(values, 0.0)

    pooled_distances = squared_distances(pooled_z, pooled_z)
    upper = pooled_distances[np.triu_indices(len(pooled_z), 1)]
    positive = upper[upper > 0]
    median_squared_distance = float(np.median(positive)) if positive.size else 1.0
    gamma = 1.0 / (2.0 * median_squared_distance) if median_squared_distance > 0 else 1.0
    k_xx = np.exp(-gamma * squared_distances(current_z, current_z))
    k_yy = np.exp(-gamma * squared_distances(reference_z, reference_z))
    k_xy = np.exp(-gamma * squared_distances(current_z, reference_z))
    mmd_squared = float(k_xx.mean() + k_yy.mean() - 2.0 * k_xy.mean())
    return max(0.0, mmd_squared), sqrt(median_squared_distance)


def _feature_metric(df: pd.DataFrame, metric: dict, output_key: str, calculator) -> dict:
    reference_df = _load_reference_df(metric)
    max_sample_size = int(
        metric.get("calculation", {}).get("parameters", {}).get("max_sample_size", 1000)
    )
    field_results = []
    values = []
    for field in _candidate_fields(metric):
        current_values = _numeric_values(df, field, max_sample_size)
        reference_values = _numeric_values(reference_df, field, max_sample_size)
        result = {
            "field": field,
            "current_count": len(current_values),
            "reference_count": len(reference_values),
            output_key: None,
            "runnable": False,
        }
        if current_values and reference_values:
            result[output_key] = round(float(calculator(current_values, reference_values)), 6)
            result["runnable"] = True
            values.append(result[output_key])
        field_results.append(result)
    return {
        "fields": field_results,
        "summary": {
            "reference_dataset_path": _reference_path(metric),
            "field_count": len(field_results),
            "runnable_field_count": len(values),
            "sampling_policy": "evenly_spaced_after_numeric_coercion",
            "max_sample_size_per_dataset": max_sample_size,
            f"mean_{output_key}": round(sum(values) / len(values), 6) if values else None,
            f"max_{output_key}": round(max(values), 6) if values else None,
        },
    }


def compute_feature_wise_wasserstein_distance_from_reference(df: pd.DataFrame, metric: dict) -> dict:
    return _feature_metric(
        df, metric, "feature_wise_wasserstein_distance_from_reference", _wasserstein_distance
    )


def compute_feature_wise_ks_statistic_from_reference(df: pd.DataFrame, metric: dict) -> dict:
    return _feature_metric(
        df, metric, "feature_wise_ks_statistic_from_reference", _ks_statistic
    )


def compute_feature_wise_energy_distance_from_reference(df: pd.DataFrame, metric: dict) -> dict:
    return _feature_metric(
        df, metric, "feature_wise_energy_distance_from_reference", _energy_distance
    )


def compute_feature_set_mmd_score_from_reference(df: pd.DataFrame, metric: dict) -> dict:
    reference_df = _load_reference_df(metric)
    fields = [
        field
        for field in _candidate_fields(metric)
        if field in df.columns and field in reference_df.columns
    ]
    max_sample_size = int(
        metric.get("calculation", {}).get("parameters", {}).get("max_sample_size", 500)
    )
    if max_sample_size < 2:
        raise ValueError("max_sample_size must be at least 2 for MMD")
    current_matrix, current_fields = _numeric_matrix(df, fields, max_sample_size)
    reference_matrix, reference_fields = _numeric_matrix(reference_df, fields, max_sample_size)
    common_fields = [
        field for field in fields if field in current_fields and field in reference_fields
    ]
    score, bandwidth = _multivariate_rbf_mmd(current_matrix, reference_matrix)
    return {
        "summary": {
            "reference_dataset_path": _reference_path(metric),
            "fields": common_fields,
            "current_row_count": int(current_matrix.shape[0]),
            "reference_row_count": int(reference_matrix.shape[0]),
            "standardization": "pooled_mean_and_standard_deviation",
            "rbf_bandwidth": round(bandwidth, 6) if bandwidth is not None else None,
            "estimator": "biased_empirical_mmd_squared",
            "feature_set_mmd_score_from_reference": round(score, 6) if score is not None else None,
            "runnable": score is not None,
        }
    }


def _matrix_deviation(current_matrix: dict, reference_matrix: dict) -> dict:
    pairs = []
    for left, row in current_matrix.items():
        for right, current_value in row.items():
            if left >= right or left not in reference_matrix or right not in reference_matrix[left]:
                continue
            reference_value = reference_matrix[left][right]
            deviation = abs(float(current_value) - float(reference_value))
            pairs.append(
                {
                    "fields": [left, right],
                    "current_value": current_value,
                    "reference_value": reference_value,
                    "deviation": round(deviation, 6),
                }
            )
    return {
        "pair_count": len(pairs),
        "mean_deviation": (
            round(sum(pair["deviation"] for pair in pairs) / len(pairs), 6)
            if pairs
            else None
        ),
        "pairs": pairs,
    }


def _correlation_profile(df: pd.DataFrame, fields: list[str], method: str) -> dict:
    usable = []
    work_df = df.copy()
    for field in fields:
        if field in work_df.columns:
            numeric = pd.to_numeric(work_df[field], errors="coerce")
            if numeric.dropna().nunique() >= 2:
                work_df[field] = numeric
                usable.append(field)
    return work_df[usable].corr(method=method).round(6).to_dict() if usable else {}


def compute_pearson_matrix_deviation_from_reference(df: pd.DataFrame, metric: dict) -> dict:
    reference_df = _load_reference_df(metric)
    fields = _candidate_fields(metric)
    deviation = _matrix_deviation(
        _correlation_profile(df, fields, "pearson"),
        _correlation_profile(reference_df, fields, "pearson"),
    )
    return {
        "summary": {
            "reference_dataset_path": _reference_path(metric),
            "pearson_matrix_deviation_from_reference": deviation["mean_deviation"],
            "pair_count": deviation["pair_count"],
        },
        "pairs": deviation["pairs"],
    }


def compute_spearman_matrix_deviation_from_reference(df: pd.DataFrame, metric: dict) -> dict:
    reference_df = _load_reference_df(metric)
    fields = _candidate_fields(metric)
    deviation = _matrix_deviation(
        _correlation_profile(df, fields, "spearman"),
        _correlation_profile(reference_df, fields, "spearman"),
    )
    return {
        "summary": {
            "reference_dataset_path": _reference_path(metric),
            "spearman_matrix_deviation_from_reference": deviation["mean_deviation"],
            "pair_count": deviation["pair_count"],
        },
        "pairs": deviation["pairs"],
    }


def compute_distance_correlation_matrix_deviation_from_reference(df: pd.DataFrame, metric: dict) -> dict:
    reference_df = _load_reference_df(metric)
    fields = _candidate_fields(metric)
    max_sample_size = int(
        metric.get("calculation", {}).get("parameters", {}).get("max_sample_size", 1000)
    )
    if max_sample_size < 2:
        raise ValueError("max_sample_size must be at least 2 for distance correlation")
    current_df = _sample_dataframe(df, max_sample_size)
    reference_sample = _sample_dataframe(reference_df, max_sample_size)
    current = compute_distance_correlation_profile(current_df, fields)["profile"]["matrix"]
    reference = compute_distance_correlation_profile(reference_sample, fields)["profile"]["matrix"]
    deviation = _matrix_deviation(current, reference)
    return {
        "summary": {
            "reference_dataset_path": _reference_path(metric),
            "distance_correlation_matrix_deviation_from_reference": deviation["mean_deviation"],
            "pair_count": deviation["pair_count"],
            "current_sample_size": len(current_df),
            "reference_sample_size": len(reference_sample),
        },
        "pairs": deviation["pairs"],
    }


def _timestamp_field(metric: dict) -> str:
    return metric.get("input_requirements", {}).get("timestamp_field", "timestamp")


def _parsed_reference_timestamps(df: pd.DataFrame, metric: dict) -> pd.Series:
    return _parse_timestamp_series(df, _timestamp_field(metric), _timestamp_unit(metric))


def _timestamp_span(series: pd.Series) -> dict[str, str | None]:
    usable = series.dropna().sort_values()
    if usable.empty:
        return {"start": None, "end": None}
    return {"start": usable.iloc[0].isoformat(), "end": usable.iloc[-1].isoformat()}


def compute_inter_arrival_distribution_divergence_from_reference(df: pd.DataFrame, metric: dict) -> dict:
    reference_df = _load_reference_df(metric)
    current_ts = _parsed_reference_timestamps(df, metric)
    reference_ts = _parsed_reference_timestamps(reference_df, metric)
    current_gaps = _inter_arrival_seconds(current_ts)
    reference_gaps = _inter_arrival_seconds(reference_ts)
    divergence = (
        round(_ks_statistic(current_gaps, reference_gaps), 6)
        if current_gaps and reference_gaps
        else None
    )
    return {
        "summary": {
            "reference_dataset_path": _reference_path(metric),
            "timestamp_unit": _timestamp_unit(metric),
            "comparison_scope": "global_sorted_timestamp_sequence",
            "divergence": "two_sample_ks_statistic",
            "current_gap_count": len(current_gaps),
            "reference_gap_count": len(reference_gaps),
            "current_timestamp_span": _timestamp_span(current_ts),
            "reference_timestamp_span": _timestamp_span(reference_ts),
            "inter_arrival_distribution_divergence_from_reference": divergence,
        }
    }


def compute_burstiness_deviation_from_reference(df: pd.DataFrame, metric: dict) -> dict:
    reference_df = _load_reference_df(metric)
    current_ts = _parsed_reference_timestamps(df, metric)
    reference_ts = _parsed_reference_timestamps(reference_df, metric)
    current = _burstiness(_inter_arrival_seconds(current_ts))
    reference = _burstiness(_inter_arrival_seconds(reference_ts))
    deviation = abs(current - reference) if current is not None and reference is not None else None
    return {
        "summary": {
            "reference_dataset_path": _reference_path(metric),
            "timestamp_unit": _timestamp_unit(metric),
            "comparison_scope": "global_sorted_timestamp_sequence",
            "current_burstiness": round(current, 6) if current is not None else None,
            "reference_burstiness": round(reference, 6) if reference is not None else None,
            "current_timestamp_span": _timestamp_span(current_ts),
            "reference_timestamp_span": _timestamp_span(reference_ts),
            "burstiness_deviation_from_reference": (
                round(deviation, 6) if deviation is not None else None
            ),
        }
    }


def _activity_timezone(metric: dict) -> str:
    value = metric.get("calculation", {}).get("parameters", {}).get("activity_timezone", "UTC")
    return str(value).strip() or "UTC"


def _activity_timestamps(df: pd.DataFrame, metric: dict) -> pd.Series:
    parsed = _parsed_reference_timestamps(df, metric)
    timezone = _activity_timezone(metric)
    try:
        return parsed.dt.tz_convert(timezone)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid activity_timezone: {timezone}") from exc


def compute_hourly_activity_divergence_from_reference(df: pd.DataFrame, metric: dict) -> dict:
    reference_df = _load_reference_df(metric)
    current_series = _activity_timestamps(df, metric)
    reference_series = _activity_timestamps(reference_df, metric)
    current_ts = current_series.dropna().tolist()
    reference_ts = reference_series.dropna().tolist()
    divergence = None
    if current_ts and reference_ts:
        current_probs = _probabilities(_hourly_counts(current_ts))
        reference_probs = _probabilities(_hourly_counts(reference_ts))
        divergence = 0.5 * sum(
            abs(a - b) for a, b in zip(current_probs, reference_probs)
        )
    return {
        "summary": {
            "reference_dataset_path": _reference_path(metric),
            "timestamp_unit": _timestamp_unit(metric),
            "activity_timezone": _activity_timezone(metric),
            "divergence": "total_variation_distance_over_24_hour_bins",
            "current_timestamp_count": len(current_ts),
            "reference_timestamp_count": len(reference_ts),
            "current_timestamp_span": _timestamp_span(current_series),
            "reference_timestamp_span": _timestamp_span(reference_series),
            "hourly_activity_divergence_from_reference": (
                round(divergence, 6) if divergence is not None else None
            ),
        }
    }


def _slice_field(metric: dict) -> str:
    return metric.get("input_requirements", {}).get("slice_field", "slice")


def _label_field(metric: dict) -> str:
    return metric.get("input_requirements", {}).get("label_field", "label")


def _normalise_category(value) -> str | None:
    if pd.isna(value):
        return None
    if isinstance(value, (int, np.integer)):
        return str(int(value))
    if isinstance(value, (float, np.floating)):
        if not np.isfinite(value):
            return None
        if float(value).is_integer():
            return str(int(value))
    text = str(value).strip()
    return text if text else None


def _category_values(df: pd.DataFrame, field: str) -> list[str]:
    if field not in df.columns:
        return []
    return [
        value
        for value in (_normalise_category(raw) for raw in df[field])
        if value is not None
    ]


def _categorical_distribution(df: pd.DataFrame, field: str) -> dict[str, float]:
    values = _category_values(df, field)
    if not values:
        return {}
    counts = pd.Series(values, dtype="object").value_counts().to_dict()
    total = sum(counts.values())
    return {str(key): value / total for key, value in counts.items()} if total else {}


def _tv_distance(left: dict[str, float], right: dict[str, float]) -> float | None:
    if not left or not right:
        return None
    keys = set(left) | set(right)
    return 0.5 * sum(
        abs(left.get(key, 0.0) - right.get(key, 0.0)) for key in keys
    )


def _valid_categories(df: pd.DataFrame, field: str) -> set[str]:
    return set(_category_values(df, field))


def compute_slice_proportion_deviation_from_reference(df: pd.DataFrame, metric: dict) -> dict:
    reference_df = _load_reference_df(metric)
    field = _slice_field(metric)
    current_dist = _categorical_distribution(df, field)
    reference_dist = _categorical_distribution(reference_df, field)
    deviation = _tv_distance(current_dist, reference_dist)
    return {
        "summary": {
            "reference_dataset_path": _reference_path(metric),
            "slice_field": field,
            "current_valid_slice_count": len(_category_values(df, field)),
            "reference_valid_slice_count": len(_category_values(reference_df, field)),
            "divergence": "total_variation_distance",
            "slice_proportion_deviation_from_reference": (
                round(deviation, 6) if deviation is not None else None
            ),
            "runnable": deviation is not None,
        }
    }


def compute_per_slice_class_divergence_from_reference(df: pd.DataFrame, metric: dict) -> dict:
    reference_df = _load_reference_df(metric)
    slice_field = _slice_field(metric)
    label_field = _label_field(metric)
    current_slices = _valid_categories(df, slice_field)
    reference_slices = _valid_categories(reference_df, slice_field)
    shared_slices = sorted(current_slices & reference_slices)
    candidate_only = sorted(current_slices - reference_slices)
    reference_only = sorted(reference_slices - current_slices)
    results = []
    values = []
    current_slice_values = df[slice_field].map(_normalise_category) if slice_field in df.columns else pd.Series([], dtype="object")
    reference_slice_values = reference_df[slice_field].map(_normalise_category) if slice_field in reference_df.columns else pd.Series([], dtype="object")
    for slice_id in shared_slices:
        current_dist = _categorical_distribution(df[current_slice_values == slice_id], label_field)
        reference_dist = _categorical_distribution(reference_df[reference_slice_values == slice_id], label_field)
        divergence = _tv_distance(current_dist, reference_dist)
        result = {
            "slice_id": slice_id,
            "runnable": divergence is not None,
            "per_slice_class_divergence_from_reference": (
                round(divergence, 6) if divergence is not None else None
            ),
        }
        if divergence is not None:
            values.append(divergence)
        results.append(result)
    return {
        "slices": results,
        "summary": {
            "reference_dataset_path": _reference_path(metric),
            "shared_slice_count": len(shared_slices),
            "candidate_only_slices": candidate_only,
            "reference_only_slices": reference_only,
            "divergence": "total_variation_distance_per_shared_slice",
            "per_slice_class_divergence_from_reference": (
                round(sum(values) / len(values), 6) if values else None
            ),
        },
    }


def compute_per_slice_feature_distribution_deviation_from_reference(df: pd.DataFrame, metric: dict) -> dict:
    reference_df = _load_reference_df(metric)
    slice_field = _slice_field(metric)
    fields = _candidate_fields(metric)
    max_sample_size = int(
        metric.get("calculation", {}).get("parameters", {}).get("max_sample_size", 1000)
    )
    current_slices = _valid_categories(df, slice_field)
    reference_slices = _valid_categories(reference_df, slice_field)
    shared_slices = sorted(current_slices & reference_slices)
    candidate_only = sorted(current_slices - reference_slices)
    reference_only = sorted(reference_slices - current_slices)
    current_slice_values = df[slice_field].map(_normalise_category) if slice_field in df.columns else pd.Series([], dtype="object")
    reference_slice_values = reference_df[slice_field].map(_normalise_category) if slice_field in reference_df.columns else pd.Series([], dtype="object")
    results = []
    values = []
    for slice_id in shared_slices:
        current_slice = df[current_slice_values == slice_id]
        reference_slice = reference_df[reference_slice_values == slice_id]
        for field in fields:
            current_values = _numeric_values(current_slice, field, max_sample_size)
            reference_values = _numeric_values(reference_slice, field, max_sample_size)
            runnable = bool(current_values and reference_values)
            deviation = (
                round(_ks_statistic(current_values, reference_values), 6)
                if runnable
                else None
            )
            if deviation is not None:
                values.append(deviation)
            results.append(
                {
                    "slice_id": slice_id,
                    "field": field,
                    "current_count": len(current_values),
                    "reference_count": len(reference_values),
                    "runnable": runnable,
                    "per_slice_feature_distribution_deviation_from_reference": deviation,
                }
            )
    return {
        "fields": results,
        "summary": {
            "reference_dataset_path": _reference_path(metric),
            "shared_slice_count": len(shared_slices),
            "candidate_only_slices": candidate_only,
            "reference_only_slices": reference_only,
            "comparison_count": len([result for result in results if result["runnable"]]),
            "max_sample_size_per_slice_field": max_sample_size,
            "divergence": "two_sample_ks_statistic_per_shared_slice_feature",
            "per_slice_feature_distribution_deviation_from_reference": (
                round(sum(values) / len(values), 6) if values else None
            ),
        },
    }


def compute_protocol_mix_divergence_from_reference(df: pd.DataFrame, metric: dict) -> dict:
    reference_df = _load_reference_df(metric)
    protocol_field = metric.get("input_requirements", {}).get("protocol_field", "Protocol")
    current_dist = _categorical_distribution(df, protocol_field)
    reference_dist = _categorical_distribution(reference_df, protocol_field)
    divergence = _tv_distance(current_dist, reference_dist)
    return {
        "summary": {
            "reference_dataset_path": _reference_path(metric),
            "protocol_field": protocol_field,
            "current_valid_protocol_count": len(_category_values(df, protocol_field)),
            "reference_valid_protocol_count": len(_category_values(reference_df, protocol_field)),
            "divergence": "total_variation_distance",
            "protocol_mix_divergence_from_reference": (
                round(divergence, 6) if divergence is not None else None
            ),
            "runnable": divergence is not None,
        }
    }


def compute_port_use_divergence_from_reference(df: pd.DataFrame, metric: dict) -> dict:
    reference_df = _load_reference_df(metric)
    port_fields = metric.get("input_requirements", {}).get(
        "port_fields", ["Source Port", "Destination Port"]
    )
    field_results = []
    values = []
    for field in port_fields:
        current_dist = _categorical_distribution(df, field)
        reference_dist = _categorical_distribution(reference_df, field)
        divergence = _tv_distance(current_dist, reference_dist)
        result = {
            "field": field,
            "current_count": len(_category_values(df, field)),
            "reference_count": len(_category_values(reference_df, field)),
            "runnable": divergence is not None,
            "total_variation_distance": (
                round(divergence, 6) if divergence is not None else None
            ),
        }
        if divergence is not None:
            values.append(divergence)
        field_results.append(result)
    return {
        "fields": field_results,
        "summary": {
            "reference_dataset_path": _reference_path(metric),
            "port_fields": port_fields,
            "runnable_field_count": len(values),
            "aggregation": "mean_total_variation_distance_across_runnable_port_fields",
            "port_use_divergence_from_reference": (
                round(sum(values) / len(values), 6) if values else None
            ),
        },
    }


def _flow_definition_ids(metric: dict) -> tuple[str | None, str | None]:
    requirements = metric.get("input_requirements", {})
    params = metric.get("calculation", {}).get("parameters", {})
    current = requirements.get("flow_definition_id") or params.get("flow_definition_id")
    reference = requirements.get("reference_flow_definition_id") or params.get(
        "reference_flow_definition_id"
    )
    return (
        str(current).strip() if current is not None and str(current).strip() else None,
        str(reference).strip() if reference is not None and str(reference).strip() else None,
    )


def compute_flow_statistic_deviation_from_reference(df: pd.DataFrame, metric: dict) -> dict:
    current_definition, reference_definition = _flow_definition_ids(metric)
    result = _feature_metric(
        df, metric, "flow_statistic_deviation_from_reference", _wasserstein_distance
    )
    if current_definition is None or reference_definition is None:
        comparability = "undeclared"
    elif current_definition == reference_definition:
        comparability = "matched"
    else:
        comparability = "mismatched"
    result["summary"].update(
        {
            "distance": "feature_wise_wasserstein_1",
            "flow_definition_id": current_definition,
            "reference_flow_definition_id": reference_definition,
            "flow_segmentation_comparability": comparability,
            "interpretation_ready": comparability == "matched",
        }
    )
    return result
