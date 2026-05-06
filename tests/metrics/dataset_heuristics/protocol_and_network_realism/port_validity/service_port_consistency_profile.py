from pathlib import Path
from runner.tabular import load_tabular_dataset


def normalize_port_series(series):
    import pandas as pd

    raw = series.astype("string").str.strip()
    missing_mask = raw.isna() | (raw == "") | (raw.str.lower() == "nan")
    numeric = pd.to_numeric(raw, errors="coerce")
    integer_mask = numeric.notna() & (numeric % 1 == 0)
    in_range_mask = integer_mask & numeric.between(0, 65535)

    status = pd.Series("non_integer", index=series.index, dtype="string")
    status = status.mask(missing_mask, "missing")
    status = status.mask(integer_mask & ~in_range_mask, "out_of_range")
    status = status.mask(in_range_mask, "valid")

    parsed = numeric.where(integer_mask).astype("Int64")
    return status, parsed


def parse_port(value):
    if value is None:
        return "missing", None

    value_str = str(value).strip()
    if value_str == "" or value_str.lower() == "nan":
        return "missing", None

    try:
        numeric_value = float(value_str)
    except ValueError:
        return "non_integer", None

    if not numeric_value.is_integer():
        return "non_integer", None

    port = int(numeric_value)
    if 0 <= port <= 65535:
        return "valid", port
    return "out_of_range", port


def run_service_port_consistency_metric(dataset_path: Path, metric: dict) -> tuple[bool, dict]:
    """
    Heuristic check of expected service ports in tabular flow data.
    This metric is a heuristic, not an absolute validity test. A service-port
    mismatch can be legitimate because services may run on non-standard ports,
    and attack traffic may intentionally target unusual ports.
    """
    params = metric.get("calculation", {}).get("parameters", {})
    input_req = metric.get("input_requirements", {})

    port_fields = input_req.get("port_fields", [])
    if not port_fields:
        return False, {"error": "No port_fields were provided for service_port_consistency_profile."}

    service_name = params.get("service_name")
    expected_ports = params.get("expected_ports", [])
    match_mode = params.get("match_mode", "any_port")
    pass_threshold = float(params.get("pass_threshold", 0.95))
    warn_threshold = float(params.get("warn_threshold", 0.75))
    max_examples = int(params.get("max_examples", 10))

    if not service_name:
        return False, {"error": "service_name is required."}
    if not expected_ports:
        return False, {"error": "expected_ports must not be empty."}

    try:
        expected_port_set = {int(p) for p in expected_ports}
    except Exception:
        return False, {"error": "expected_ports must be integers."}

    if match_mode not in {"any_port", "destination_only", "source_only", "both_ports"}:
        return False, {"error": f"Unsupported match_mode: {match_mode}"}

    df = metric.get("_shared_df")
    if df is None:
        try:
            df = load_tabular_dataset(dataset_path)
        except Exception as exc:
            return False, {"error": f"Failed to load dataset: {exc}"}

    existing_fields = [f for f in port_fields if f in df.columns]
    missing_fields = [f for f in port_fields if f not in df.columns]
    if not existing_fields:
        return False, {"error": "No requested port fields exist in the dataset.", "missing_fields": missing_fields}

    destination_candidates = ["Destination Port", "Dst Port", "dst_port", "destination_port"]
    source_candidates = ["Source Port", "Src Port", "src_port", "source_port"]
    destination_field = next((f for f in destination_candidates if f in existing_fields), None)
    source_field = next((f for f in source_candidates if f in existing_fields), None)

    if match_mode == "destination_only" and destination_field is None:
        return False, {"error": "destination_only match_mode selected but no destination port field exists."}
    if match_mode == "source_only" and source_field is None:
        return False, {"error": "source_only match_mode selected but no source port field exists."}
    if match_mode == "both_ports" and (source_field is None or destination_field is None):
        return False, {"error": "both_ports match_mode requires both source and destination port fields."}

    row_count = int(len(df))
    checked_row_count = 0
    matching_row_count = 0
    mismatching_row_count = 0
    missing_port_row_count = 0
    invalid_port_row_count = 0

    checked_port_count = 0
    valid_port_count = 0
    invalid_port_count = 0
    missing_port_count = 0

    mismatch_examples = []
    invalid_port_examples = []

    parsed_ports = {}
    status_by_field = {}
    for field in existing_fields:
        status, parsed = normalize_port_series(df[field])
        status_by_field[field] = status
        parsed_ports[field] = parsed

        missing_mask = status == "missing"
        valid_mask = status == "valid"
        invalid_mask = ~(missing_mask | valid_mask)

        missing_port_count += int(missing_mask.sum())
        valid_port_count += int(valid_mask.sum())
        invalid_port_count += int(invalid_mask.sum())
        checked_port_count += int((~missing_mask).sum())

    all_missing_mask = None
    any_invalid_mask = None
    for field in existing_fields:
        missing_mask = status_by_field[field] == "missing"
        invalid_mask = ~missing_mask & (status_by_field[field] != "valid")
        all_missing_mask = missing_mask if all_missing_mask is None else (all_missing_mask & missing_mask)
        any_invalid_mask = invalid_mask if any_invalid_mask is None else (any_invalid_mask | invalid_mask)

    missing_port_row_count = int(all_missing_mask.sum())
    invalid_port_row_count = int((~all_missing_mask & any_invalid_mask).sum())
    checked_rows_mask = ~all_missing_mask & ~any_invalid_mask
    checked_row_count = int(checked_rows_mask.sum())

    if match_mode == "any_port":
        match_mask = None
        for field in existing_fields:
            candidate = parsed_ports[field].isin(expected_port_set)
            match_mask = candidate if match_mask is None else (match_mask | candidate)
    elif match_mode == "destination_only":
        match_mask = parsed_ports[destination_field].isin(expected_port_set)
    elif match_mode == "source_only":
        match_mask = parsed_ports[source_field].isin(expected_port_set)
    else:
        match_mask = parsed_ports[source_field].isin(expected_port_set) & parsed_ports[destination_field].isin(expected_port_set)

    matching_row_count = int((checked_rows_mask & match_mask).sum())
    mismatching_row_count = int((checked_rows_mask & ~match_mask).sum())

    invalid_rows = df[~all_missing_mask & any_invalid_mask]
    for idx, row in invalid_rows.head(max_examples).iterrows():
        for field in existing_fields:
            status = status_by_field[field].loc[idx]
            if status not in {"missing", "valid"}:
                invalid_port_examples.append({
                    "row_index": int(idx) if isinstance(idx, int) else str(idx),
                    "field": field,
                    "value": str(row[field]).strip(),
                    "reason": str(status),
                })
                if len(invalid_port_examples) >= max_examples:
                    break
        if len(invalid_port_examples) >= max_examples:
            break

    mismatch_rows = df[checked_rows_mask & ~match_mask]
    for idx, row in mismatch_rows.head(max_examples).iterrows():
        ports = {}
        for field in existing_fields:
            if status_by_field[field].loc[idx] == "valid":
                ports[field] = int(parsed_ports[field].loc[idx])
        mismatch_examples.append({
            "row_index": int(idx) if isinstance(idx, int) else str(idx),
            "ports": ports,
            "reason": "expected_service_port_not_found",
        })

    service_port_match_ratio = round(matching_row_count / checked_row_count, 6) if checked_row_count else 0.0
    service_port_mismatch_ratio = round(mismatching_row_count / checked_row_count, 6) if checked_row_count else 0.0
    invalid_port_row_ratio = round(invalid_port_row_count / row_count, 6) if row_count else 0.0

    if checked_row_count == 0:
        status = "fail"
    elif service_port_match_ratio >= pass_threshold:
        status = "pass"
    elif service_port_match_ratio >= warn_threshold:
        status = "warn"
    else:
        status = "fail"

    if invalid_port_row_count > 0 and status == "pass":
        status = "warn"

    return True, {
        "test_results": {
            "service_port_consistency_profile": {
                "service_name": service_name,
                "expected_ports": sorted(expected_port_set),
                "match_mode": match_mode,
                "port_fields": existing_fields,
                "missing_fields": missing_fields,
                "row_count": row_count,
                "checked_row_count": checked_row_count,
                "matching_row_count": matching_row_count,
                "mismatching_row_count": mismatching_row_count,
                "missing_port_row_count": missing_port_row_count,
                "invalid_port_row_count": invalid_port_row_count,
                "checked_port_count": checked_port_count,
                "valid_port_count": valid_port_count,
                "invalid_port_count": invalid_port_count,
                "missing_port_count": missing_port_count,
                "service_port_match_ratio": service_port_match_ratio,
                "service_port_mismatch_ratio": service_port_mismatch_ratio,
                "invalid_port_row_ratio": invalid_port_row_ratio,
                "mismatch_examples": mismatch_examples,
                "invalid_port_examples": invalid_port_examples,
                "status": status,
            }
        }
    }
