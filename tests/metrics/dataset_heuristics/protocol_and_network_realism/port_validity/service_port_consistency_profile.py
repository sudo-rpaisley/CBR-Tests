from pathlib import Path


def load_tabular_dataset(dataset_path: Path):
    import pandas as pd

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

    for idx, row in df.iterrows():
        row_ports = {}
        row_valid_ports = {}
        row_missing_all = True
        row_has_invalid = False

        for field in existing_fields:
            status, port = parse_port(row[field])
            if status == "missing":
                missing_port_count += 1
                continue

            row_missing_all = False
            checked_port_count += 1

            if status == "valid":
                valid_port_count += 1
                row_ports[field] = port
                row_valid_ports[field] = port
            else:
                invalid_port_count += 1
                row_has_invalid = True
                if len(invalid_port_examples) < max_examples:
                    invalid_port_examples.append({
                        "row_index": int(idx) if isinstance(idx, int) else str(idx),
                        "field": field,
                        "value": str(row[field]).strip(),
                        "reason": status,
                    })

        if row_missing_all:
            missing_port_row_count += 1
            continue

        if row_has_invalid:
            invalid_port_row_count += 1
            continue

        checked_row_count += 1

        if match_mode == "any_port":
            row_matches = any(port in expected_port_set for port in row_valid_ports.values())
        elif match_mode == "destination_only":
            row_matches = row_valid_ports.get(destination_field) in expected_port_set
        elif match_mode == "source_only":
            row_matches = row_valid_ports.get(source_field) in expected_port_set
        else:
            row_matches = (
                row_valid_ports.get(source_field) in expected_port_set
                and row_valid_ports.get(destination_field) in expected_port_set
            )

        if row_matches:
            matching_row_count += 1
        else:
            mismatching_row_count += 1
            if len(mismatch_examples) < max_examples:
                mismatch_examples.append({
                    "row_index": int(idx) if isinstance(idx, int) else str(idx),
                    "ports": row_ports,
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
