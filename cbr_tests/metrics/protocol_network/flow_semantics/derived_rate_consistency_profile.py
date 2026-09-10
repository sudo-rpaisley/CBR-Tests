from __future__ import annotations

from pathlib import Path

import pandas as pd

from runner.tabular import load_tabular_dataset


_DURATION_TO_SECONDS = {
    "seconds": 1.0,
    "milliseconds": 1e-3,
    "microseconds": 1e-6,
    "nanoseconds": 1e-9,
}


def _within_tolerance(
    reported: pd.Series,
    expected: pd.Series,
    relative_tolerance: float,
    absolute_tolerance: float,
) -> pd.Series:
    allowed = (expected.abs() * relative_tolerance).clip(lower=absolute_tolerance)
    return (reported - expected).abs() <= allowed


def run_derived_rate_consistency_metric(dataset_path: Path, metric: dict) -> tuple[bool, dict]:
    """Check whether reported packet/byte rates agree with counts, bytes, and duration.

    The duration unit must be declared in ``calculation.parameters.duration_unit``.
    At least one of ``flow_packets_per_second`` and ``flow_bytes_per_second`` must
    be mapped. This prevents the metric from silently assuming a dataset-specific
    duration unit or rate convention.
    """

    field_map = metric.get("input_requirements", {}).get("field_map", {})
    required = [
        "flow_duration",
        "total_fwd_packets",
        "total_bwd_packets",
        "total_len_fwd_packets",
        "total_len_bwd_packets",
    ]
    missing_required = [key for key in required if key not in field_map]
    if missing_required:
        return False, {
            "error": "Missing required fields for derived_rate_consistency_profile.",
            "missing_fields": missing_required,
        }

    rate_keys = [
        key
        for key in ("flow_packets_per_second", "flow_bytes_per_second")
        if key in field_map
    ]
    if not rate_keys:
        return False, {
            "error": (
                "derived_rate_consistency_profile requires at least one reported "
                "packet-rate or byte-rate field."
            ),
            "missing_fields": [
                "flow_packets_per_second or flow_bytes_per_second"
            ],
        }

    parameters = metric.get("calculation", {}).get("parameters", {})
    duration_unit = str(parameters.get("duration_unit", "")).strip().lower()
    if duration_unit not in _DURATION_TO_SECONDS:
        return False, {
            "error": (
                "calculation.parameters.duration_unit must be one of: "
                + ", ".join(sorted(_DURATION_TO_SECONDS))
            )
        }

    relative_tolerance = float(parameters.get("relative_tolerance", 0.02))
    absolute_tolerance = float(parameters.get("absolute_tolerance", 1e-6))
    max_examples = int(parameters.get("max_examples", 10))
    pass_threshold = float(parameters.get("pass_threshold", 0.99))
    warn_threshold = float(parameters.get("warn_threshold", 0.95))

    if relative_tolerance < 0 or absolute_tolerance < 0:
        return False, {"error": "Rate tolerances must be non-negative."}
    if not 0 <= warn_threshold <= pass_threshold <= 1:
        return False, {
            "error": "Require 0 <= warn_threshold <= pass_threshold <= 1."
        }

    dataframe = metric.get("_shared_df")
    if dataframe is None:
        try:
            dataframe = load_tabular_dataset(dataset_path)
        except Exception as exc:
            return False, {"error": f"Failed to load dataset: {exc}"}

    mapped_keys = required + rate_keys
    missing_columns = [field_map[key] for key in mapped_keys if field_map[key] not in dataframe.columns]
    if missing_columns:
        return False, {
            "error": "Missing required columns for derived_rate_consistency_profile.",
            "missing_fields": missing_columns,
        }

    data = pd.DataFrame(
        {
            key: pd.to_numeric(dataframe[field_map[key]], errors="coerce")
            for key in mapped_keys
        }
    )
    checked_mask = data.notna().all(axis=1)
    invalid_numeric_row_count = int((~checked_mask).sum())

    total_packets = data["total_fwd_packets"] + data["total_bwd_packets"]
    total_bytes = data["total_len_fwd_packets"] + data["total_len_bwd_packets"]
    duration_seconds = data["flow_duration"] * _DURATION_TO_SECONDS[duration_unit]

    negative_value_mask = checked_mask & (
        (data[required] < 0).any(axis=1)
        | (data[rate_keys] < 0).any(axis=1)
    )
    zero_duration_nonzero_volume_mask = checked_mask & (
        (duration_seconds == 0) & ((total_packets > 0) | (total_bytes > 0))
    )

    packet_rate_mismatch_mask = pd.Series(False, index=dataframe.index)
    byte_rate_mismatch_mask = pd.Series(False, index=dataframe.index)

    positive_duration_mask = checked_mask & (duration_seconds > 0)
    zero_empty_mask = checked_mask & (
        (duration_seconds == 0) & (total_packets == 0) & (total_bytes == 0)
    )

    if "flow_packets_per_second" in rate_keys:
        expected_packet_rate = total_packets / duration_seconds.where(duration_seconds > 0)
        packet_rate_matches = _within_tolerance(
            data["flow_packets_per_second"],
            expected_packet_rate,
            relative_tolerance,
            absolute_tolerance,
        )
        packet_rate_mismatch_mask = positive_duration_mask & ~packet_rate_matches
        packet_rate_mismatch_mask |= zero_empty_mask & (
            data["flow_packets_per_second"].abs() > absolute_tolerance
        )

    if "flow_bytes_per_second" in rate_keys:
        expected_byte_rate = total_bytes / duration_seconds.where(duration_seconds > 0)
        byte_rate_matches = _within_tolerance(
            data["flow_bytes_per_second"],
            expected_byte_rate,
            relative_tolerance,
            absolute_tolerance,
        )
        byte_rate_mismatch_mask = positive_duration_mask & ~byte_rate_matches
        byte_rate_mismatch_mask |= zero_empty_mask & (
            data["flow_bytes_per_second"].abs() > absolute_tolerance
        )

    inconsistent_mask = (
        negative_value_mask
        | zero_duration_nonzero_volume_mask
        | packet_rate_mismatch_mask
        | byte_rate_mismatch_mask
    )

    checked_row_count = int(checked_mask.sum())
    inconsistent_row_count = int(inconsistent_mask.sum())
    consistent_row_count = checked_row_count - inconsistent_row_count
    ratio = round(consistent_row_count / checked_row_count, 6) if checked_row_count else 0.0
    status = "pass" if ratio >= pass_threshold else "warn" if ratio >= warn_threshold else "fail"

    examples = []
    if max_examples > 0:
        for index in dataframe.index[inconsistent_mask][:max_examples]:
            reasons = []
            if negative_value_mask.loc[index]:
                reasons.append("negative_value")
            if zero_duration_nonzero_volume_mask.loc[index]:
                reasons.append("zero_duration_nonzero_volume")
            if packet_rate_mismatch_mask.loc[index]:
                reasons.append("packet_rate_mismatch")
            if byte_rate_mismatch_mask.loc[index]:
                reasons.append("byte_rate_mismatch")
            examples.append({"row_index": int(index), "reasons": reasons})

    return True, {
        "test_results": {
            "derived_rate_consistency_profile": {
                "row_count": len(dataframe),
                "checked_row_count": checked_row_count,
                "consistent_row_count": consistent_row_count,
                "inconsistent_row_count": inconsistent_row_count,
                "invalid_numeric_row_count": invalid_numeric_row_count,
                "negative_value_count": int(negative_value_mask.sum()),
                "zero_duration_nonzero_volume_count": int(
                    zero_duration_nonzero_volume_mask.sum()
                ),
                "packet_rate_mismatch_count": int(packet_rate_mismatch_mask.sum()),
                "byte_rate_mismatch_count": int(byte_rate_mismatch_mask.sum()),
                "derived_rate_consistency_ratio": ratio,
                "duration_unit": duration_unit,
                "relative_tolerance": relative_tolerance,
                "absolute_tolerance": absolute_tolerance,
                "examples": examples,
                "status": status,
            }
        }
    }
