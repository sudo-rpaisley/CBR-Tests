"""Second-generation PCAP reference diagnostics (opt-in, new metric IDs).

All metrics retain native values and explicit applicability. No new metric is
treated as a universal realism verdict. The default legacy plan is unchanged.
For temporal metrics, original packet order is retained; no timestamp sorting.
"""
from __future__ import annotations

from collections import Counter
from math import floor
from typing import Any

import numpy as np
import pandas as pd

from cbr_tests.metrics.reference_comparison import _load_reference_df


def _parameters(metric: dict) -> dict:
    return metric.get("calculation", {}).get("parameters", {})


def _numeric(df: pd.DataFrame, field: str) -> np.ndarray:
    if field not in df:
        return np.empty(0, dtype=np.float64)
    return pd.to_numeric(df[field], errors="coerce").to_numpy(dtype=np.float64)


def _ordered_gaps(df: pd.DataFrame) -> np.ndarray:
    ts = _numeric(df, "Timestamp")
    if len(ts) < 2:
        return np.empty(0, dtype=np.float64)
    # Preserve capture order, including backwards/zero gaps. Do not sort!
    # Avoid spurious KS differences from IEEE-754 subtraction after a global time shift.
    return np.round(np.diff(ts), 9)


def _sample(a: np.ndarray, n: int = 4000) -> np.ndarray:
    a = np.asarray(a, dtype=np.float64)
    a = a[np.isfinite(a)]
    if len(a) <= n:
        return a
    return a[np.linspace(0, len(a) - 1, n, dtype=np.int64)]


def _ks(a: np.ndarray, b: np.ndarray) -> float | None:
    a, b = np.sort(_sample(a)), np.sort(_sample(b))
    if not len(a) or not len(b):
        return None
    values = np.unique(np.concatenate((a, b)))
    return float(np.max(np.abs(
        np.searchsorted(a, values, side="right") / len(a)
        - np.searchsorted(b, values, side="right") / len(b)
    )))


def _result(name: str, value: float | None, **kwargs: Any) -> dict:
    if value is not None and not np.isfinite(value):
        value = None
    return {"summary": {
        name: round(float(value), 8) if value is not None else None,
        "runnable": value is not None,
        "reason_code": "usable" if value is not None else "insufficient_observations",
        **kwargs
    }}


def compute_capture_order_inter_arrival_divergence_from_reference(df: pd.DataFrame, metric: dict) -> dict:
    reference = _load_reference_df(metric)
    a, b = _ordered_gaps(df), _ordered_gaps(reference)
    usable_a, usable_b = a[np.isfinite(a)], b[np.isfinite(b)]
    positive_a, positive_b = usable_a[usable_a >= 0], usable_b[usable_b >= 0]
    ks = _ks(positive_a, positive_b)
    invalid_a = float(np.mean(usable_a < 0)) if len(usable_a) else None
    invalid_b = float(np.mean(usable_b < 0)) if len(usable_b) else None
    score = max(ks, abs(invalid_a - invalid_b)) if ks is not None and invalid_a is not None and invalid_b is not None else None
    return _result("capture_order_inter_arrival_divergence_from_reference", score,
        nonnegative_gap_ks=ks, candidate_backwards_gap_ratio=invalid_a,
        reference_backwards_gap_ratio=invalid_b, candidate_zero_gap_count=int(np.count_nonzero(usable_a == 0)),
        reference_zero_gap_count=int(np.count_nonzero(usable_b == 0)),
        comparison_scope="capture_order_not_sorted")


def _flow_columns(df: pd.DataFrame) -> list[str] | None:
    cols = ["Source IP", "Destination IP", "Source Port", "Destination Port", "Protocol", "Timestamp"]
    return cols if all(x in df for x in cols) else None


def _flow_events(df: pd.DataFrame, max_packets: int = 2_000_000) -> pd.DataFrame | None:
    cols = _flow_columns(df)
    if cols is None:
        return None
    # A bounded deterministic view has explicit sample provenance. Avoid
    # silently interpreting a packet sample as complete flow population.
    work = df.loc[:, cols].head(max_packets).copy()
    s = work["Source IP"].astype(str) + ":" + work["Source Port"].astype(str)
    d = work["Destination IP"].astype(str) + ":" + work["Destination Port"].astype(str)
    reverse = s > d
    work["_flow_id"] = np.where(reverse, d + "|" + s, s + "|" + d) + "|" + work["Protocol"].astype(str)
    work["_direction"] = reverse.astype(np.int8)
    work["Timestamp"] = pd.to_numeric(work["Timestamp"], errors="coerce")
    work = work.dropna(subset=["Timestamp"])
    return work


def _per_flow_gaps(df: pd.DataFrame, max_packets: int) -> tuple[np.ndarray, dict]:
    work = _flow_events(df, max_packets)
    if work is None:
        return np.empty(0), {"reason": "required_flow_columns_missing"}
    gaps = work.groupby("_flow_id", sort=False, observed=True)["Timestamp"].diff().dropna().to_numpy()
    return gaps, {"sampled_packet_count": len(work), "truncated": len(df) > max_packets,
                  "observed_flow_count": int(work["_flow_id"].nunique())}


def compute_per_flow_inter_arrival_divergence_from_reference(df: pd.DataFrame, metric: dict) -> dict:
    cap = int(_parameters(metric).get("max_packets", 2_000_000))
    a, da = _per_flow_gaps(df, cap)
    b, db = _per_flow_gaps(_load_reference_df(metric), cap)
    value = _ks(a[a >= 0], b[b >= 0])
    return _result("per_flow_inter_arrival_divergence_from_reference", value,
        candidate=da, reference=db, comparison_scope="capture_order_within_bidirectional_five_tuple",
        interpretation="flow_timing_on_bounded_prefix; not independent sessions when truncated")


def _activity_signature(df: pd.DataFrame, step: float) -> tuple[dict[int, float], int]:
    ts = _numeric(df, "Timestamp")
    ts = ts[np.isfinite(ts)]
    if not len(ts):
        return {}, 0
    bins = np.floor((ts - ts[0]) / step).astype(np.int64)
    bins = bins[bins >= 0]
    if not len(bins):
        return {}, 0
    _, counts = np.unique(bins, return_counts=True)
    possible = int(np.max(bins)) + 1
    frequencies = Counter(map(int, counts))
    frequencies[0] += possible - len(counts)
    return {int(k): v / possible for k, v in frequencies.items()}, possible


def _tv(a: dict, b: dict) -> float | None:
    if not a or not b:
        return None
    return 0.5 * sum(abs(a.get(x, 0) - b.get(x, 0)) for x in set(a) | set(b))


def compute_multiscale_activity_divergence_from_reference(df: pd.DataFrame, metric: dict) -> dict:
    reference = _load_reference_df(metric)
    scales = _parameters(metric).get("window_seconds", [0.001, 0.01, 0.1, 1.0, 10.0])
    details = []
    values = []
    for scale in scales:
        scale = float(scale)
        if not 0 < scale <= 3600:
            raise ValueError("window_seconds must be within (0, 3600]")
        aa, an = _activity_signature(df, scale)
        bb, bn = _activity_signature(reference, scale)
        value = _tv(aa, bb)
        details.append({"window_seconds": scale, "candidate_bin_count": an,
                        "reference_bin_count": bn, "packet_count_histogram_tv": value})
        if value is not None:
            values.append(value)
    value = float(np.mean(values)) if len(values) == len(scales) else None
    result = _result("multiscale_activity_divergence_from_reference", value,
                     scale_count=len(scales), comparison_scope="elapsed_time_relative_to_capture_start")
    result["scales"] = details
    return result


def _burst_profile(df: pd.DataFrame, threshold: float) -> tuple[np.ndarray, np.ndarray]:
    gaps = _ordered_gaps(df)
    gaps = gaps[np.isfinite(gaps)]
    if not len(gaps):
        return np.empty(0), np.empty(0)
    bursts, idle = [], []
    burst_count = 1
    for gap in gaps:
        if 0 <= gap <= threshold:
            burst_count += 1
        else:
            bursts.append(burst_count)
            burst_count = 1
            if gap > threshold:
                idle.append(gap)
    bursts.append(burst_count)
    return np.asarray(bursts, dtype=float), np.asarray(idle, dtype=float)


def compute_burst_structure_divergence_from_reference(df: pd.DataFrame, metric: dict) -> dict:
    threshold = float(_parameters(metric).get("burst_gap_seconds", 0.01))
    if threshold <= 0:
        raise ValueError("burst_gap_seconds must be positive")
    candidate_bursts, candidate_idle = _burst_profile(df, threshold)
    reference_bursts, reference_idle = _burst_profile(_load_reference_df(metric), threshold)
    burst_ks = _ks(candidate_bursts, reference_bursts)
    idle_ks = _ks(candidate_idle, reference_idle)
    value = max(burst_ks, idle_ks) if burst_ks is not None and idle_ks is not None else None
    return _result("burst_structure_divergence_from_reference", value,
                   burst_length_ks=burst_ks, idle_interval_ks=idle_ks,
                   burst_gap_seconds=threshold, candidate_burst_count=len(candidate_bursts),
                   reference_burst_count=len(reference_bursts))


def _lag_correlation(df: pd.DataFrame, lag: int, cap: int = 50000) -> float | None:
    gaps = _ordered_gaps(df)
    gaps = gaps[np.isfinite(gaps) & (gaps >= 0)]
    gaps = np.log1p(_sample(gaps, cap))
    if len(gaps) <= lag + 2 or np.std(gaps[:-lag]) < 1e-12 or np.std(gaps[lag:]) < 1e-12:
        return None
    return float(np.corrcoef(gaps[:-lag], gaps[lag:])[0, 1])


def compute_temporal_autocorrelation_deviation_from_reference(df: pd.DataFrame, metric: dict) -> dict:
    reference = _load_reference_df(metric)
    lags = _parameters(metric).get("lags", [1, 2, 4, 8, 16, 32])
    details, vals = [], []
    for lag in lags:
        lag = int(lag)
        if lag < 1:
            raise ValueError("autocorrelation lags must be positive")
        a, b = _lag_correlation(df, lag), _lag_correlation(reference, lag)
        difference = abs(a-b) if a is not None and b is not None else None
        details.append({"lag_packets": lag, "candidate_correlation": a, "reference_correlation": b,
                        "absolute_deviation": difference})
        if difference is not None:
            vals.append(difference)
    result = _result("temporal_autocorrelation_deviation_from_reference",
                     float(np.mean(vals)) if len(vals) == len(lags) else None,
                     comparison_scope="lagged_log1p_capture_order_inter_arrival")
    result["lags"] = details
    return result


def _flow_population(df: pd.DataFrame, cap: int) -> tuple[np.ndarray, np.ndarray, dict]:
    work = _flow_events(df, cap)
    if work is None or work.empty:
        return np.empty(0), np.empty(0), {"reason": "no_usable_flow_data"}
    groups = work.groupby("_flow_id", sort=False, observed=True)["Timestamp"].agg(["size", "min", "max"])
    return groups["size"].to_numpy(dtype=float), (groups["max"]-groups["min"]).to_numpy(dtype=float), {
        "sampled_packet_count": len(work), "truncated": len(df) > cap, "observed_flow_count": len(groups)
    }


def compute_flow_population_deviation_from_reference(df: pd.DataFrame, metric: dict) -> dict:
    cap = int(_parameters(metric).get("max_packets", 2_000_000))
    counts_a, duration_a, info_a = _flow_population(df, cap)
    counts_b, duration_b, info_b = _flow_population(_load_reference_df(metric), cap)
    count_ks = _ks(counts_a, counts_b)
    duration_ks = _ks(duration_a, duration_b)
    value = max(count_ks, duration_ks) if count_ks is not None and duration_ks is not None else None
    return _result("flow_population_deviation_from_reference", value,
                   flow_packet_count_ks=count_ks, flow_duration_ks=duration_ks,
                   candidate=info_a, reference=info_b, comparison_scope="five_tuple_bounded_prefix")


def _direction_balance(df: pd.DataFrame, cap: int) -> tuple[np.ndarray, dict]:
    work = _flow_events(df, cap)
    if work is None or work.empty:
        return np.empty(0), {"reason": "no_usable_flow_data"}
    groups = work.groupby("_flow_id", sort=False, observed=True)["_direction"].agg(["size", "sum"])
    ratios = np.maximum(groups["sum"], groups["size"]-groups["sum"]) / groups["size"]
    return ratios.to_numpy(dtype=float), {
        "sampled_packet_count": len(work), "truncated": len(df) > cap,
        "observed_flow_count": len(groups)}


def compute_bidirectional_flow_balance_deviation_from_reference(df: pd.DataFrame, metric: dict) -> dict:
    cap = int(_parameters(metric).get("max_packets", 2_000_000))
    a, da = _direction_balance(df, cap)
    b, db = _direction_balance(_load_reference_df(metric), cap)
    return _result("bidirectional_flow_balance_deviation_from_reference", _ks(a, b),
                   candidate=da, reference=db,
                   interpretation="direction ratio only; not a request/response latency estimate")


def _port_classes(df: pd.DataFrame) -> dict:
    if "Source Port" not in df or "Destination Port" not in df:
        return {}
    ports = np.concatenate((_numeric(df, "Source Port"), _numeric(df, "Destination Port")))
    ports = ports[np.isfinite(ports)]
    if not len(ports):
        return {}
    labels = np.where(ports < 1024, 0, np.where(ports < 49152, 1, 2))
    unique, counts = np.unique(labels, return_counts=True)
    return dict(zip(map(int, unique), counts / len(labels)))


def compute_port_class_divergence_from_reference(df: pd.DataFrame, metric: dict) -> dict:
    """Coarse service-class distribution rather than exact ephemeral ports."""
    a = _port_classes(df)
    b = _port_classes(_load_reference_df(metric))
    return _result("port_class_divergence_from_reference", _tv(a, b),
                   candidate_port_class_proportions=a, reference_port_class_proportions=b,
                   interpretation="IANA port ranges are descriptive, not application identification")
