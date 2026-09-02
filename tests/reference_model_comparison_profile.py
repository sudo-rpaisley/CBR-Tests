from math import sqrt
from pathlib import Path
from threading import Lock

import numpy as np
import pandas as pd

from cbr_tests.metrics.temporal import _timestamp_unit
from runner.pcap_adapter import build_pcap_packet_dataframe, is_packet_capture

from tests.statistical_fidelity_profile import (
    _energy_distance,
    _ks_statistic,
    _rbf_mmd,
    _wasserstein_distance,
    compute_distance_correlation_profile,
)
from tests.temporal_metrics_profile import _burstiness, _hourly_counts, _inter_arrival_seconds, _parse_timestamp_series, _probabilities


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
            "Reference field mapping would overwrite existing columns: " + ", ".join(sorted(set(collisions)))
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
            dataframe = pd.read_csv(path, sep="\t" if suffix == ".tsv" else ",", skipinitialspace=True, low_memory=False)
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


def _numeric_matrix(df: pd.DataFrame, fields: list[str], max_sample_size: int) -> tuple[np.ndarray, list[str]]:
    usable = [field for field in fields if field in df.columns]
    if not usable:
        return np.empty((0, 0)), []
    numeric = df[usable].apply(pd.to_numeric, errors="coerce").dropna()
    numeric = _sample_dataframe(numeric, max_sample_size)
    return numeric.to_numpy(dtype=float), usable


def _multivariate_rbf_mmd(current: np.ndarray, reference: np.ndarray) -> tuple[float | None, float | None]:
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
    max_sample_size = int(metric.get("calculation", {}).get("parameters", {}).get("max_sample_size", 1000))
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
    return {"fields": field_results, "summary": {"reference_dataset_path": _reference_path(metric), "field_count": len(field_results), "runnable_field_count": len(values), f"mean_{output_key}": round(sum(values) / len(values), 6) if values else None, f"max_{output_key}": round(max(values), 6) if values else None}}


def compute_feature_wise_wasserstein_distance_from_reference(df: pd.DataFrame, metric: dict) -> dict:
    return _feature_metric(df, metric, "feature_wise_wasserstein_distance_from_reference", _wasserstein_distance)


def compute_feature_wise_ks_statistic_from_reference(df: pd.DataFrame, metric: dict) -> dict:
    return _feature_metric(df, metric, "feature_wise_ks_statistic_from_reference", _ks_statistic)


def compute_feature_wise_energy_distance_from_reference(df: pd.DataFrame, metric: dict) -> dict:
    return _feature_metric(df, metric, "feature_wise_energy_distance_from_reference", _energy_distance)


def compute_feature_set_mmd_score_from_reference(df: pd.DataFrame, metric: dict) -> dict:
    reference_df = _load_reference_df(metric)
    fields = [field for field in _candidate_fields(metric) if field in df.columns and field in reference_df.columns]
    max_sample_size = int(metric.get("calculation", {}).get("parameters", {}).get("max_sample_size", 500))
    if max_sample_size < 2:
        raise ValueError("max_sample_size must be at least 2 for MMD")
    current_matrix, current_fields = _numeric_matrix(df, fields, max_sample_size)
    reference_matrix, reference_fields = _numeric_matrix(reference_df, fields, max_sample_size)
    common_fields = [field for field in fields if field in current_fields and field in reference_fields]
    score, bandwidth = _multivariate_rbf_mmd(current_matrix, reference_matrix)
    return {"summary": {
        "reference_dataset_path": _reference_path(metric),
        "fields": common_fields,
        "current_row_count": int(current_matrix.shape[0]),
        "reference_row_count": int(reference_matrix.shape[0]),
        "standardization": "pooled_mean_and_standard_deviation",
        "rbf_bandwidth": round(bandwidth, 6) if bandwidth is not None else None,
        "feature_set_mmd_score_from_reference": round(score, 6) if score is not None else None,
        "runnable": score is not None,
    }}


def _matrix_deviation(current_matrix: dict, reference_matrix: dict) -> dict:
    pairs = []
    for left, row in current_matrix.items():
        for right, current_value in row.items():
            if left >= right or left not in reference_matrix or right not in reference_matrix[left]:
                continue
            reference_value = reference_matrix[left][right]
            deviation = abs(float(current_value) - float(reference_value))
            pairs.append({"fields": [left, right], "current_value": current_value, "reference_value": reference_value, "deviation": round(deviation, 6)})
    return {"pair_count": len(pairs), "mean_deviation": round(sum(pair["deviation"] for pair in pairs) / len(pairs), 6) if pairs else None, "pairs": pairs}


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
    deviation = _matrix_deviation(_correlation_profile(df, fields, "pearson"), _correlation_profile(reference_df, fields, "pearson"))
    return {"summary": {"reference_dataset_path": _reference_path(metric), "pearson_matrix_deviation_from_reference": deviation["mean_deviation"], "pair_count": deviation["pair_count"]}, "pairs": deviation["pairs"]}


def compute_spearman_matrix_deviation_from_reference(df: pd.DataFrame, metric: dict) -> dict:
    reference_df = _load_reference_df(metric)
    fields = _candidate_fields(metric)
    deviation = _matrix_deviation(_correlation_profile(df, fields, "spearman"), _correlation_profile(reference_df, fields, "spearman"))
    return {"summary": {"reference_dataset_path": _reference_path(metric), "spearman_matrix_deviation_from_reference": deviation["mean_deviation"], "pair_count": deviation["pair_count"]}, "pairs": deviation["pairs"]}


def compute_distance_correlation_matrix_deviation_from_reference(df: pd.DataFrame, metric: dict) -> dict:
    reference_df = _load_reference_df(metric)
    fields = _candidate_fields(metric)
    max_sample_size = int(metric.get("calculation", {}).get("parameters", {}).get("max_sample_size", 1000))
    if max_sample_size < 2:
        raise ValueError("max_sample_size must be at least 2 for distance correlation")
    current_df = _sample_dataframe(df, max_sample_size)
    reference_sample = _sample_dataframe(reference_df, max_sample_size)
    current = compute_distance_correlation_profile(current_df, fields)["profile"]["matrix"]
    reference = compute_distance_correlation_profile(reference_sample, fields)["profile"]["matrix"]
    deviation = _matrix_deviation(current, reference)
    return {"summary": {"reference_dataset_path": _reference_path(metric), "distance_correlation_matrix_deviation_from_reference": deviation["mean_deviation"], "pair_count": deviation["pair_count"], "current_sample_size": len(current_df), "reference_sample_size": len(reference_sample)}, "pairs": deviation["pairs"]}


def _timestamp_field(metric: dict) -> str:
    return metric.get("input_requirements", {}).get("timestamp_field", "timestamp")


def compute_inter_arrival_distribution_divergence_from_reference(df: pd.DataFrame, metric: dict) -> dict:
    reference_df = _load_reference_df(metric)
    current_gaps = _inter_arrival_seconds(_parse_timestamp_series(df, _timestamp_field(metric), _timestamp_unit(metric)))
    reference_gaps = _inter_arrival_seconds(_parse_timestamp_series(reference_df, _timestamp_field(metric), _timestamp_unit(metric)))
    divergence = round(_ks_statistic(current_gaps, reference_gaps), 6) if current_gaps and reference_gaps else None
    return {"summary": {"reference_dataset_path": _reference_path(metric), "timestamp_unit": _timestamp_unit(metric), "current_gap_count": len(current_gaps), "reference_gap_count": len(reference_gaps), "inter_arrival_distribution_divergence_from_reference": divergence}}


def compute_burstiness_deviation_from_reference(df: pd.DataFrame, metric: dict) -> dict:
    reference_df = _load_reference_df(metric)
    current = _burstiness(_inter_arrival_seconds(_parse_timestamp_series(df, _timestamp_field(metric), _timestamp_unit(metric))))
    reference = _burstiness(_inter_arrival_seconds(_parse_timestamp_series(reference_df, _timestamp_field(metric), _timestamp_unit(metric))))
    deviation = abs(current - reference) if current is not None and reference is not None else None
    return {"summary": {"reference_dataset_path": _reference_path(metric), "timestamp_unit": _timestamp_unit(metric), "current_burstiness": round(current, 6) if current is not None else None, "reference_burstiness": round(reference, 6) if reference is not None else None, "burstiness_deviation_from_reference": round(deviation, 6) if deviation is not None else None}}


def compute_hourly_activity_divergence_from_reference(df: pd.DataFrame, metric: dict) -> dict:
    reference_df = _load_reference_df(metric)
    current_ts = _parse_timestamp_series(df, _timestamp_field(metric)).dropna().tolist()
    reference_ts = _parse_timestamp_series(reference_df, _timestamp_field(metric)).dropna().tolist()
    divergence = None
    if current_ts and reference_ts:
        current_probs = _probabilities(_hourly_counts(current_ts))
        reference_probs = _probabilities(_hourly_counts(reference_ts))
        divergence = 0.5 * sum(abs(a - b) for a, b in zip(current_probs, reference_probs))
    return {"summary": {"reference_dataset_path": _reference_path(metric), "timestamp_unit": _timestamp_unit(metric), "current_timestamp_count": len(current_ts), "reference_timestamp_count": len(reference_ts), "hourly_activity_divergence_from_reference": round(divergence, 6) if divergence is not None else None}}


def _slice_field(metric: dict) -> str:
    return metric.get("input_requirements", {}).get("slice_field", "slice")


def _label_field(metric: dict) -> str:
    return metric.get("input_requirements", {}).get("label_field", "label")


def _categorical_distribution(df: pd.DataFrame, field: str) -> dict[str, float]:
    if field not in df.columns or len(df) == 0:
        return {}
    counts = df[field].astype(str).value_counts(dropna=True).to_dict()
    total = sum(counts.values())
    return {key: value / total for key, value in counts.items()} if total else {}


def _tv_distance(left: dict[str, float], right: dict[str, float]) -> float:
    keys = set(left) | set(right)
    return 0.5 * sum(abs(left.get(key, 0.0) - right.get(key, 0.0)) for key in keys)


def compute_slice_proportion_deviation_from_reference(df: pd.DataFrame, metric: dict) -> dict:
    reference_df = _load_reference_df(metric)
    deviation = _tv_distance(_categorical_distribution(df, _slice_field(metric)), _categorical_distribution(reference_df, _slice_field(metric)))
    return {"summary": {"reference_dataset_path": _reference_path(metric), "slice_field": _slice_field(metric), "slice_proportion_deviation_from_reference": round(deviation, 6)}}


def compute_per_slice_class_divergence_from_reference(df: pd.DataFrame, metric: dict) -> dict:
    reference_df = _load_reference_df(metric)
    slice_field = _slice_field(metric)
    label_field = _label_field(metric)
    slices = sorted(set(df[slice_field].astype(str)) | set(reference_df[slice_field].astype(str))) if slice_field in df.columns and slice_field in reference_df.columns else []
    results = []
    values = []
    for slice_id in slices:
        current_dist = _categorical_distribution(df[df[slice_field].astype(str) == slice_id], label_field)
        reference_dist = _categorical_distribution(reference_df[reference_df[slice_field].astype(str) == slice_id], label_field)
        divergence = round(_tv_distance(current_dist, reference_dist), 6)
        values.append(divergence)
        results.append({"slice_id": slice_id, "per_slice_class_divergence_from_reference": divergence})
    return {"slices": results, "summary": {"reference_dataset_path": _reference_path(metric), "slice_count": len(results), "per_slice_class_divergence_from_reference": round(sum(values) / len(values), 6) if values else None}}


def compute_per_slice_feature_distribution_deviation_from_reference(df: pd.DataFrame, metric: dict) -> dict:
    reference_df = _load_reference_df(metric)
    slice_field = _slice_field(metric)
    fields = _candidate_fields(metric)
    slices = sorted(set(df[slice_field].astype(str)) | set(reference_df[slice_field].astype(str))) if slice_field in df.columns and slice_field in reference_df.columns else []
    results = []
    values = []
    for slice_id in slices:
        current_slice = df[df[slice_field].astype(str) == slice_id]
        reference_slice = reference_df[reference_df[slice_field].astype(str) == slice_id]
        for field in fields:
            current_values = _numeric_values(current_slice, field, 1000)
            reference_values = _numeric_values(reference_slice, field, 1000)
            if not current_values or not reference_values:
                continue
            deviation = round(_ks_statistic(current_values, reference_values), 6)
            values.append(deviation)
            results.append({"slice_id": slice_id, "field": field, "per_slice_feature_distribution_deviation_from_reference": deviation})
    return {"fields": results, "summary": {"reference_dataset_path": _reference_path(metric), "comparison_count": len(results), "per_slice_feature_distribution_deviation_from_reference": round(sum(values) / len(values), 6) if values else None}}


def compute_protocol_mix_divergence_from_reference(df: pd.DataFrame, metric: dict) -> dict:
    reference_df = _load_reference_df(metric)
    protocol_field = metric.get("input_requirements", {}).get("protocol_field", "Protocol")
    divergence = _tv_distance(_categorical_distribution(df, protocol_field), _categorical_distribution(reference_df, protocol_field))
    return {"summary": {"reference_dataset_path": _reference_path(metric), "protocol_field": protocol_field, "protocol_mix_divergence_from_reference": round(divergence, 6)}}


def compute_port_use_divergence_from_reference(df: pd.DataFrame, metric: dict) -> dict:
    reference_df = _load_reference_df(metric)
    port_fields = metric.get("input_requirements", {}).get("port_fields", ["Source Port", "Destination Port"])
    values = []
    for field in port_fields:
        values.append(_tv_distance(_categorical_distribution(df, field), _categorical_distribution(reference_df, field)))
    return {"summary": {"reference_dataset_path": _reference_path(metric), "port_fields": port_fields, "port_use_divergence_from_reference": round(sum(values) / len(values), 6) if values else None}}


def compute_flow_statistic_deviation_from_reference(df: pd.DataFrame, metric: dict) -> dict:
    return _feature_metric(df, metric, "flow_statistic_deviation_from_reference", _wasserstein_distance)
