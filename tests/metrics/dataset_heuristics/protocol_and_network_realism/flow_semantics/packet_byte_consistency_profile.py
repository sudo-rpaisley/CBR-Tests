from pathlib import Path
import pandas as pd
import math


def load_tabular_dataset(dataset_path: Path) -> pd.DataFrame:
    suffix = dataset_path.suffix.lower()
    if suffix == ".csv":
        df = pd.read_csv(dataset_path, skipinitialspace=True, low_memory=False)
    elif suffix == ".tsv":
        df = pd.read_csv(dataset_path, sep="\t", skipinitialspace=True, low_memory=False)
    elif suffix in [".xlsx", ".xls"]:
        df = pd.read_excel(dataset_path)
    else:
        raise ValueError(f"Unsupported tabular dataset format: {suffix}")
    df.columns = df.columns.str.strip()
    return df


def run_packet_byte_consistency_metric(dataset_path: Path, metric: dict) -> tuple[bool, dict]:
    fm = metric.get("input_requirements", {}).get("field_map", {})
    req = ["total_fwd_packets", "total_bwd_packets", "total_len_fwd_packets", "total_len_bwd_packets", "fwd_pkt_len_min", "fwd_pkt_len_mean", "fwd_pkt_len_max", "bwd_pkt_len_min", "bwd_pkt_len_mean", "bwd_pkt_len_max"]
    if any(k not in fm for k in req):
        return False, {"error": "Missing required fields for packet_byte_consistency_profile.", "missing_fields": [k for k in req if k not in fm]}

    df = metric.get("_shared_df")
    if df is None:
        try:
            df = load_tabular_dataset(dataset_path)
        except Exception as exc:
            return False, {"error": f"Failed to load dataset: {exc}"}

    miss = [fm[k] for k in req if fm[k] not in df.columns]
    if miss:
        return False, {"error": "Missing required fields for packet_byte_consistency_profile.", "missing_fields": miss}

    p = metric.get("calculation", {}).get("parameters", {})
    tol = float(p.get("tolerance", 1e-6))
    vtol = float(p.get("variance_tolerance", 1e-3))
    max_examples = int(p.get("max_examples", 10))

    data = pd.DataFrame({k: pd.to_numeric(df[fm[k]], errors="coerce") for k in req})
    checked_mask = data.notna().all(axis=1)
    invalid_numeric_row_count = int((~checked_mask).sum())

    negative_pkt = checked_mask & ((data["total_fwd_packets"] < 0) | (data["total_bwd_packets"] < 0))
    negative_byte = checked_mask & ((data["total_len_fwd_packets"] < 0) | (data["total_len_bwd_packets"] < 0) |
                                    (data[["fwd_pkt_len_min", "fwd_pkt_len_mean", "fwd_pkt_len_max", "bwd_pkt_len_min", "bwd_pkt_len_mean", "bwd_pkt_len_max"]] < 0).any(axis=1))

    order = checked_mask & (
        ~((data["fwd_pkt_len_min"] <= data["fwd_pkt_len_mean"]) & (data["fwd_pkt_len_mean"] <= data["fwd_pkt_len_max"])) |
        ~((data["bwd_pkt_len_min"] <= data["bwd_pkt_len_mean"]) & (data["bwd_pkt_len_mean"] <= data["bwd_pkt_len_max"]))
    )

    zero_nonzero = checked_mask & (
        ((data["total_fwd_packets"] == 0) & (data["total_len_fwd_packets"] > 0)) |
        ((data["total_bwd_packets"] == 0) & (data["total_len_bwd_packets"] > 0))
    )

    exceed = checked_mask & (
        ((data["total_fwd_packets"] > 0) & (data["total_len_fwd_packets"] > data["total_fwd_packets"] * data["fwd_pkt_len_max"] + tol)) |
        ((data["total_bwd_packets"] > 0) & (data["total_len_bwd_packets"] > data["total_bwd_packets"] * data["bwd_pkt_len_max"] + tol))
    )

    varmis = pd.Series(False, index=df.index)
    if "packet_length_std" in fm and fm["packet_length_std"] in df.columns and "packet_length_variance" in fm and fm["packet_length_variance"] in df.columns:
        std = pd.to_numeric(df[fm["packet_length_std"]], errors="coerce")
        var = pd.to_numeric(df[fm["packet_length_variance"]], errors="coerce")
        both = checked_mask & std.notna() & var.notna()
        allowed = pd.Series(vtol, index=df.index)
        allowed = allowed.where(var.abs() * vtol < vtol, var.abs() * vtol)
        varmis = both & ((var - (std ** 2)).abs() > allowed)

    inconsistent_mask = negative_pkt | negative_byte | order | zero_nonzero | exceed | varmis
    checked_row_count = int(checked_mask.sum())
    inconsistent_row_count = int(inconsistent_mask.sum())
    consistent_row_count = checked_row_count - inconsistent_row_count

    examples = []
    if max_examples > 0:
        for idx in df.index[inconsistent_mask][:max_examples]:
            examples.append({"row_index": int(idx), "reason": "packet_byte_inconsistency"})

    ratio = round(consistent_row_count / checked_row_count, 6) if checked_row_count else 0.0
    status = "pass" if ratio >= 0.99 else "warn" if ratio >= 0.95 else "fail"

    return True, {"test_results": {"packet_byte_consistency_profile": {
        "row_count": len(df),
        "checked_row_count": checked_row_count,
        "consistent_row_count": consistent_row_count,
        "inconsistent_row_count": inconsistent_row_count,
        "negative_packet_count": int(negative_pkt.sum()),
        "negative_byte_count": int(negative_byte.sum()),
        "length_order_violation_count": int(order.sum()),
        "zero_packet_nonzero_byte_count": int(zero_nonzero.sum()),
        "byte_total_exceeds_max_possible_count": int(exceed.sum()),
        "variance_std_mismatch_count": int(varmis.sum()),
        "invalid_numeric_row_count": invalid_numeric_row_count,
        "packet_byte_consistency_ratio": ratio,
        "examples": examples,
        "status": status
    }}}
