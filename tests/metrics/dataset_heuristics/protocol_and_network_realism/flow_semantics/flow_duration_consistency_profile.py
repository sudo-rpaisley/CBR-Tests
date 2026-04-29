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


def run_flow_duration_consistency_metric(dataset_path: Path, metric: dict) -> tuple[bool, dict]:
    fm = metric.get("input_requirements", {}).get("field_map", {})
    req = ["flow_duration", "flow_iat_mean", "flow_iat_max", "flow_iat_min"]
    if any(k not in fm for k in req):
        return False, {"error": "Missing required fields for flow_duration_consistency_profile.", "missing_fields": [k for k in req if k not in fm]}

    df = metric.get("_shared_df")
    if df is None:
        try:
            df = load_tabular_dataset(dataset_path)
        except Exception as exc:
            return False, {"error": f"Failed to load dataset: {exc}"}

    miss = [fm[k] for k in req if fm[k] not in df.columns]
    if miss:
        return False, {"error": "Missing required fields for flow_duration_consistency_profile.", "missing_fields": miss}

    tol = float(metric.get("calculation", {}).get("parameters", {}).get("tolerance", 1e-6))
    max_examples = int(metric.get("calculation", {}).get("parameters", {}).get("max_examples", 10))

    cols = req + [k for k in ["flow_iat_std", "fwd_iat_total", "bwd_iat_total"] if k in fm and fm[k] in df.columns]
    data = pd.DataFrame({k: pd.to_numeric(df[fm[k]], errors="coerce") for k in cols})

    row_count = len(df)
    required_valid = data[req].notna().all(axis=1)
    invalid_numeric_row_count = int((~required_valid).sum())
    checked_mask = required_valid

    negative_duration = checked_mask & (data["flow_duration"] < 0)
    negative_iat = checked_mask & ((data["flow_iat_min"] < 0) | (data["flow_iat_mean"] < 0) | (data["flow_iat_max"] < 0))
    if "flow_iat_std" in data.columns:
        negative_iat |= checked_mask & data["flow_iat_std"].notna() & (data["flow_iat_std"] < 0)

    iat_order = checked_mask & ~((data["flow_iat_min"] <= data["flow_iat_mean"]) & (data["flow_iat_mean"] <= data["flow_iat_max"]))
    iat_exceeds = checked_mask & (data["flow_iat_max"] > data["flow_duration"] + tol)

    dir_exceeds = pd.Series(False, index=df.index)
    for d in ["fwd_iat_total", "bwd_iat_total"]:
        if d in data.columns:
            dir_exceeds |= checked_mask & data[d].notna() & (data[d] > data["flow_duration"] + tol)

    inconsistent_mask = negative_duration | negative_iat | iat_order | iat_exceeds | dir_exceeds
    checked_row_count = int(checked_mask.sum())
    inconsistent_row_count = int(inconsistent_mask.sum())
    consistent_row_count = checked_row_count - inconsistent_row_count

    examples = []
    if max_examples > 0:
        for idx in df.index[inconsistent_mask][:max_examples]:
            examples.append({"row_index": int(idx), "reason": "duration_iat_violation"})

    ratio = round(consistent_row_count / checked_row_count, 6) if checked_row_count else 0.0
    status = "pass" if ratio >= 0.99 else "warn" if ratio >= 0.95 else "fail"

    return True, {"test_results": {"flow_duration_consistency_profile": {
        "row_count": row_count,
        "checked_row_count": checked_row_count,
        "consistent_row_count": consistent_row_count,
        "inconsistent_row_count": inconsistent_row_count,
        "negative_duration_count": int(negative_duration.sum()),
        "negative_iat_count": int(negative_iat.sum()),
        "iat_order_violation_count": int(iat_order.sum()),
        "iat_exceeds_duration_count": int(iat_exceeds.sum()),
        "direction_iat_exceeds_duration_count": int(dir_exceeds.sum()),
        "invalid_numeric_row_count": invalid_numeric_row_count,
        "flow_duration_consistency_ratio": ratio,
        "examples": examples,
        "status": status
    }}}
