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


def run_tcp_flag_consistency_metric(dataset_path: Path, metric: dict) -> tuple[bool, dict]:
    field_map = metric.get("input_requirements", {}).get("field_map", {})
    required = ["protocol", "total_fwd_packets", "total_bwd_packets", "syn_flag_count", "ack_flag_count", "fin_flag_count", "rst_flag_count"]
    missing_keys = [k for k in required if k not in field_map]
    if missing_keys:
        return False, {"error": "Missing required fields for tcp_flag_consistency_profile.", "missing_fields": missing_keys}

    df = metric.get("_shared_df")
    if df is None:
        try:
            df = load_tabular_dataset(dataset_path)
        except Exception as exc:
            return False, {"error": f"Failed to load dataset: {exc}"}

    missing_fields = [field_map[k] for k in required if field_map[k] not in df.columns]
    if missing_fields:
        return False, {"error": "Missing required fields for tcp_flag_consistency_profile.", "missing_fields": missing_fields}

    params = metric.get("calculation", {}).get("parameters", {})
    tcp_vals = {str(v).lower() for v in params.get("tcp_protocol_values", [6, "6", "TCP", "tcp"])}
    non_tcp_zero = bool(params.get("non_tcp_flags_must_be_zero", True))
    max_examples = int(params.get("max_examples", 10))

    flag_keys = ["fin_flag_count", "syn_flag_count", "rst_flag_count", "ack_flag_count", "psh_flag_count", "urg_flag_count", "cwe_flag_count", "ece_flag_count"]
    present = [k for k in flag_keys if k in field_map and field_map[k] in df.columns]

    totals = pd.to_numeric(df[field_map["total_fwd_packets"]], errors="coerce") + pd.to_numeric(df[field_map["total_bwd_packets"]], errors="coerce")
    proto = df[field_map["protocol"]].astype(str).str.strip().str.lower()
    is_tcp = proto.isin(tcp_vals)

    flag_df = pd.DataFrame({k: pd.to_numeric(df[field_map[k]], errors="coerce") for k in present})
    invalid_numeric = totals.isna() | flag_df.isna().any(axis=1)
    checked_mask = ~totals.isna()

    negative = (flag_df < 0).any(axis=1)
    exceeds = flag_df.gt(totals, axis=0).any(axis=1)
    bad_flags = negative | exceeds | flag_df.isna().any(axis=1)

    non_zero = (flag_df.fillna(0) > 0).any(axis=1)
    non_tcp_with_flags = (~is_tcp) & non_zero & non_tcp_zero & ~bad_flags & checked_mask

    inconsistent_mask = (bad_flags & checked_mask) | non_tcp_with_flags

    checked_row_count = int(checked_mask.sum())
    if checked_row_count == 0:
        return False, {"error": "No rows could be checked for tcp_flag_consistency_profile."}

    inconsistent_row_count = int(inconsistent_mask.sum())
    consistent_row_count = checked_row_count - inconsistent_row_count

    examples = []
    if max_examples > 0:
        for idx in df.index[inconsistent_mask][:max_examples]:
            reason = "non_tcp_with_tcp_flags" if bool(non_tcp_with_flags.loc[idx]) else "invalid_or_out_of_bounds_flags"
            examples.append({"row_index": int(idx), "reason": reason})

    ratio = round(consistent_row_count / checked_row_count, 6)
    status = "pass" if ratio >= 0.99 else "warn" if ratio >= 0.95 else "fail"

    return True, {"test_results": {"tcp_flag_consistency_profile": {
        "row_count": len(df),
        "checked_row_count": checked_row_count,
        "consistent_row_count": consistent_row_count,
        "inconsistent_row_count": inconsistent_row_count,
        "tcp_row_count": int(is_tcp.sum()),
        "non_tcp_row_count": int((~is_tcp).sum()),
        "tcp_flag_consistency_ratio": ratio,
        "negative_flag_count": int(negative.sum()),
        "flag_exceeds_packet_count": int(exceeds.sum()),
        "non_tcp_with_tcp_flags_count": int(non_tcp_with_flags.sum()),
        "invalid_numeric_row_count": int(invalid_numeric.sum()),
        "examples": examples,
        "status": status
    }}}
