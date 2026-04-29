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


def classify_port_range(port: int) -> str:
    if 0 <= port <= 1023:
        return "well_known"
    if 1024 <= port <= 49151:
        return "registered"
    if 49152 <= port <= 65535:
        return "dynamic_private"
    return "out_of_range"


def run_valid_port_range_metric(dataset_path: Path, metric: dict) -> tuple[bool, dict]:
    candidate_fields = metric.get("input_requirements", {}).get("candidate_fields", [])
    if not candidate_fields:
        return False, {"error": "No candidate_fields were provided for valid_port_range_profile."}

    df = metric.get("_shared_df")
    if df is None:
        try:
            df = load_tabular_dataset(dataset_path)
        except Exception as exc:
            return False, {"error": f"Failed to load dataset: {exc}"}

    existing_fields = [field for field in candidate_fields if field in df.columns]
    missing_fields = [field for field in candidate_fields if field not in df.columns]

    if not existing_fields:
        return False, {
            "error": "None of the requested port fields exist in the dataset.",
            "missing_fields": missing_fields,
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
            status, parsed_port = parse_port(value)

            if status == "missing":
                missing_port_count += 1
                field_missing_count += 1
                continue

            checked_port_count += 1
            field_checked_count += 1

            if status == "valid":
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

                if status == "non_integer":
                    non_integer_port_count += 1
                    field_non_integer_count += 1
                if status == "out_of_range":
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
                        "reason": status,
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

    if invalid_port_ratio is not None and invalid_port_ratio > 0.01:
        status = "fail"
    elif invalid_port_count > 0 or zero_port_count > 0:
        status = "warn"
    else:
        status = "pass"

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
                "range_counts": range_counts,
                "field_summaries": field_summaries,
                "invalid_examples": invalid_examples,
                "status": status,
            }
        }
    }
