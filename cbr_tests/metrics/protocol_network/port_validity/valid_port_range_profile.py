from pathlib import Path

from runner.tabular import load_tabular_dataset


def parse_port(value, valid_min_port: int = 0, valid_max_port: int = 65535):
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
    if valid_min_port <= port <= valid_max_port:
        return "valid", port
    return "out_of_range", port


def classify_port_range(port: int) -> str:
    if 0 <= port <= 1023:
        return "well_known"
    if 1024 <= port <= 49151:
        return "registered"
    if 49152 <= port <= 65535:
        return "dynamic_private"
    return "out_of_range"


def _diagnostic(status: str, *, checked: int, invalid: int, non_integer: int, out_of_range: int,
                zero_count: int, invalid_ratio: float | None, threshold: float,
                valid_min_port: int, valid_max_port: int, examples: list) -> dict:
    evidence = {
        "checked_port_count": checked,
        "invalid_port_count": invalid,
        "non_integer_port_count": non_integer,
        "out_of_range_port_count": out_of_range,
        "zero_port_count": zero_count,
        "invalid_port_ratio": invalid_ratio,
        "valid_range": [valid_min_port, valid_max_port],
    }
    if examples:
        evidence["examples"] = examples[:3]

    if status == "not_applicable":
        return {
            "reason_code": "no_non_missing_ports",
            "summary": "The configured port fields contained no non-missing values to validate.",
            "evidence": evidence,
            "suggestion": "Confirm the port field mappings and whether this dataset contains transport-layer port data.",
        }
    if status == "fail":
        return {
            "reason_code": "invalid_port_ratio_exceeded",
            "summary": (
                f"{invalid} of {checked} checked ports were invalid ({invalid_ratio:.2%}), "
                f"exceeding the configured failure threshold of {threshold:.2%}."
            ),
            "evidence": evidence,
            "suggestion": "Inspect the invalid examples and confirm the source/destination port field mappings and units.",
        }
    if status == "warn":
        return {
            "reason_code": "suspicious_port_values_observed",
            "summary": (
                f"The port fields contained {invalid} invalid value(s) and {zero_count} zero-port value(s), "
                "but the invalid-value rate did not exceed the failure threshold."
            ),
            "evidence": evidence,
            "suggestion": "Review whether zero ports and the listed invalid values are legitimate exporter semantics or data-quality defects.",
        }
    return {
        "reason_code": "port_range_within_policy",
        "summary": f"All {checked} checked port values were within the configured range {valid_min_port}-{valid_max_port}.",
        "evidence": evidence,
    }


def run_valid_port_range_metric(dataset_path: Path, metric: dict) -> tuple[bool, dict]:
    candidate_fields = metric.get("input_requirements", {}).get("candidate_fields", [])
    if not candidate_fields:
        return False, {
            "error": "No candidate_fields were provided for valid_port_range_profile.",
            "reason_code": "invalid_metric_configuration",
        }

    params = metric.get("calculation", {}).get("parameters", {})
    valid_min_port = int(params.get("valid_min_port", 0))
    valid_max_port = int(params.get("valid_max_port", 65535))
    invalid_ratio_fail_threshold = float(params.get("invalid_ratio_fail_threshold", 0.01))
    if valid_min_port < 0 or valid_max_port > 65535 or valid_min_port > valid_max_port:
        return False, {
            "error": "Configured port bounds must satisfy 0 <= valid_min_port <= valid_max_port <= 65535.",
            "reason_code": "invalid_metric_configuration",
        }
    if not 0 <= invalid_ratio_fail_threshold <= 1:
        return False, {
            "error": "invalid_ratio_fail_threshold must be between 0 and 1.",
            "reason_code": "invalid_metric_configuration",
        }

    df = metric.get("_shared_df")
    if df is None:
        try:
            df = load_tabular_dataset(dataset_path)
        except Exception as exc:
            return False, {
                "error": f"Failed to load dataset: {exc}",
                "reason_code": "dataset_load_error",
            }

    existing_fields = [field for field in candidate_fields if field in df.columns]
    missing_fields = [field for field in candidate_fields if field not in df.columns]
    if not existing_fields:
        return False, {
            "error": "None of the requested port fields exist in the dataset.",
            "reason_code": "missing_required_fields",
            "missing_fields": missing_fields or list(candidate_fields),
        }

    row_count = int(len(df))
    checked_port_count = 0
    valid_port_count = 0
    invalid_port_count = 0
    missing_port_count = 0
    non_integer_port_count = 0
    out_of_range_port_count = 0
    zero_port_count = 0
    invalid_row_count = 0

    range_counts = {"well_known": 0, "registered": 0, "dynamic_private": 0}
    field_summaries = []
    invalid_examples = []
    invalid_rows_seen = set()

    for field in existing_fields:
        field_checked_count = 0
        field_valid_count = 0
        field_invalid_count = 0
        field_missing_count = 0
        field_non_integer_count = 0
        field_out_of_range_count = 0
        field_zero_port_count = 0
        field_range_counts = {"well_known": 0, "registered": 0, "dynamic_private": 0}

        for idx, value in df[field].items():
            value_status, parsed_port = parse_port(value, valid_min_port, valid_max_port)

            if value_status == "missing":
                missing_port_count += 1
                field_missing_count += 1
                continue

            checked_port_count += 1
            field_checked_count += 1

            if value_status == "valid":
                valid_port_count += 1
                field_valid_count += 1
                if parsed_port == 0:
                    zero_port_count += 1
                    field_zero_port_count += 1
                port_range = classify_port_range(parsed_port)
                if port_range in range_counts:
                    range_counts[port_range] += 1
                    field_range_counts[port_range] += 1
            else:
                invalid_port_count += 1
                field_invalid_count += 1
                if value_status == "non_integer":
                    non_integer_port_count += 1
                    field_non_integer_count += 1
                elif value_status == "out_of_range":
                    out_of_range_port_count += 1
                    field_out_of_range_count += 1

                if idx not in invalid_rows_seen:
                    invalid_row_count += 1
                    invalid_rows_seen.add(idx)

                if len(invalid_examples) < 10:
                    invalid_examples.append({
                        "row_index": int(idx) if isinstance(idx, int) else str(idx),
                        "field": field,
                        "value": str(value).strip(),
                        "reason": value_status,
                    })

        field_summaries.append({
            "field": field,
            "exists": True,
            "checked_port_count": field_checked_count,
            "valid_port_count": field_valid_count,
            "invalid_port_count": field_invalid_count,
            "missing_port_count": field_missing_count,
            "non_integer_port_count": field_non_integer_count,
            "out_of_range_port_count": field_out_of_range_count,
            "zero_port_count": field_zero_port_count,
            "range_counts": field_range_counts,
            "valid_port_range_ratio": round(field_valid_count / field_checked_count, 6) if field_checked_count else None,
            "invalid_port_ratio": round(field_invalid_count / field_checked_count, 6) if field_checked_count else None,
        })

    valid_port_range_ratio = round(valid_port_count / checked_port_count, 6) if checked_port_count else None
    invalid_port_ratio = round(invalid_port_count / checked_port_count, 6) if checked_port_count else None
    invalid_row_ratio = round(invalid_row_count / row_count, 6) if row_count else None
    zero_port_ratio = round(zero_port_count / checked_port_count, 6) if checked_port_count else None

    if checked_port_count == 0:
        status = "not_applicable"
    elif invalid_port_ratio is not None and invalid_port_ratio > invalid_ratio_fail_threshold:
        status = "fail"
    elif invalid_port_count > 0 or zero_port_count > 0:
        status = "warn"
    else:
        status = "pass"

    diagnostic = _diagnostic(
        status,
        checked=checked_port_count,
        invalid=invalid_port_count,
        non_integer=non_integer_port_count,
        out_of_range=out_of_range_port_count,
        zero_count=zero_port_count,
        invalid_ratio=invalid_port_ratio,
        threshold=invalid_ratio_fail_threshold,
        valid_min_port=valid_min_port,
        valid_max_port=valid_max_port,
        examples=invalid_examples,
    )

    return True, {
        "test_results": {
            "valid_port_range_profile": {
                "row_count": row_count,
                "checked_fields": existing_fields,
                "missing_fields": missing_fields,
                "checked_port_count": checked_port_count,
                "valid_port_count": valid_port_count,
                "invalid_port_count": invalid_port_count,
                "missing_port_count": missing_port_count,
                "non_integer_port_count": non_integer_port_count,
                "out_of_range_port_count": out_of_range_port_count,
                "zero_port_count": zero_port_count,
                "invalid_row_count": invalid_row_count,
                "valid_port_range_ratio": valid_port_range_ratio,
                "invalid_port_ratio": invalid_port_ratio,
                "invalid_row_ratio": invalid_row_ratio,
                "zero_port_ratio": zero_port_ratio,
                "valid_min_port": valid_min_port,
                "valid_max_port": valid_max_port,
                "invalid_ratio_fail_threshold": invalid_ratio_fail_threshold,
                "range_counts": range_counts,
                "field_summaries": field_summaries,
                "invalid_examples": invalid_examples,
                "status": status,
                "diagnostic": diagnostic,
            }
        }
    }
