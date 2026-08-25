from pathlib import Path

from runner.tabular import load_tabular_dataset


DEFAULT_SERVICE_FIELD_CANDIDATES = [
    "Service",
    "service",
    "Application",
    "application",
    "Application Name",
    "application_name",
    "Protocol Name",
    "protocol_name",
]


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


def _not_applicable_result(*, service_name: str, expected_ports: list[int], match_mode: str,
                           existing_fields: list[str], missing_fields: list[str], row_count: int,
                           population_basis: str, reason_code: str, summary: str, suggestion: str,
                           service_field: str | None = None, service_values: list[str] | None = None) -> tuple[bool, dict]:
    diagnostic = {
        "reason_code": reason_code,
        "summary": summary,
        "evidence": {
            "row_count": row_count,
            "port_fields": existing_fields,
            "service_field": service_field,
            "service_values": service_values or [],
            "population_basis": population_basis,
        },
        "suggestion": suggestion,
    }
    return True, {
        "test_results": {
            "service_port_consistency_profile": {
                "service_name": service_name,
                "expected_ports": expected_ports,
                "match_mode": match_mode,
                "port_fields": existing_fields,
                "missing_fields": missing_fields,
                "row_count": row_count,
                "population_row_count": 0,
                "population_basis": population_basis,
                "service_field": service_field,
                "service_values": service_values or [],
                "checked_row_count": 0,
                "matching_row_count": 0,
                "mismatching_row_count": 0,
                "service_port_match_ratio": None,
                "service_port_mismatch_ratio": None,
                "mismatch_examples": [],
                "invalid_port_examples": [],
                "status": "not_applicable",
                "diagnostic": diagnostic,
            }
        }
    }


def _diagnostic(status: str, *, service_name: str, checked: int, matching: int, mismatching: int,
                match_ratio: float, pass_threshold: float, warn_threshold: float,
                invalid_rows: int, population_rows: int, population_basis: str,
                expected_ports: list[int], mismatch_examples: list, invalid_examples: list) -> dict:
    evidence = {
        "service_name": service_name,
        "expected_ports": expected_ports,
        "population_basis": population_basis,
        "population_row_count": population_rows,
        "checked_row_count": checked,
        "matching_row_count": matching,
        "mismatching_row_count": mismatching,
        "service_port_match_ratio": match_ratio,
        "pass_threshold": pass_threshold,
        "warn_threshold": warn_threshold,
        "invalid_port_row_count": invalid_rows,
    }
    examples = mismatch_examples[:3] or invalid_examples[:3]
    if examples:
        evidence["examples"] = examples

    if status == "fail":
        return {
            "reason_code": "service_port_match_below_warn_threshold",
            "summary": (
                f"Only {matching} of {checked} selected {service_name} rows matched the expected port set "
                f"({match_ratio:.2%}); this is below the warning threshold of {warn_threshold:.2%}."
            ),
            "evidence": evidence,
            "suggestion": "Inspect mismatching examples and verify both the service population selector and expected-port policy before treating this as unrealistic traffic.",
        }
    if status == "warn":
        if match_ratio < pass_threshold:
            summary = (
                f"{matching} of {checked} selected {service_name} rows matched the expected port set "
                f"({match_ratio:.2%}); this is below the pass threshold of {pass_threshold:.2%} but not below the warning threshold."
            )
            reason_code = "service_port_match_below_pass_threshold"
        else:
            summary = f"Expected service-port usage passed, but {invalid_rows} selected row(s) contained invalid port values."
            reason_code = "invalid_ports_in_service_population"
        return {
            "reason_code": reason_code,
            "summary": summary,
            "evidence": evidence,
            "suggestion": "Review the selected service population and examples; non-standard service ports can be legitimate and should be interpreted with dataset context.",
        }
    return {
        "reason_code": "service_port_consistency_within_policy",
        "summary": (
            f"{matching} of {checked} selected {service_name} rows matched the expected port set "
            f"({match_ratio:.2%}), meeting the pass threshold of {pass_threshold:.2%}."
        ),
        "evidence": evidence,
    }


def run_service_port_consistency_metric(dataset_path: Path, metric: dict) -> tuple[bool, dict]:
    """Heuristically check expected ports only within an independently selected service population.

    A mixed flow dataset must not be treated as though every row belongs to the
    configured service. In ``auto`` mode the metric therefore requires a usable
    service/application field, unless ``assume_dataset_service`` is explicitly
    enabled. A plan can also use ``population_mode=all_rows`` when the dataset is
    known to contain only the named service.
    """
    import pandas as pd

    params = metric.get("calculation", {}).get("parameters", {})
    input_req = metric.get("input_requirements", {})

    port_fields = input_req.get("port_fields", [])
    if not port_fields:
        return False, {
            "error": "No port_fields were provided for service_port_consistency_profile.",
            "reason_code": "invalid_metric_configuration",
        }

    service_name = params.get("service_name")
    expected_ports = params.get("expected_ports", [])
    match_mode = params.get("match_mode", "any_port")
    pass_threshold = float(params.get("pass_threshold", 0.95))
    warn_threshold = float(params.get("warn_threshold", 0.75))
    max_examples = int(params.get("max_examples", 10))
    population_mode = str(params.get("population_mode", "auto")).lower()

    if not service_name:
        return False, {"error": "service_name is required.", "reason_code": "invalid_metric_configuration"}
    if not expected_ports:
        return False, {"error": "expected_ports must not be empty.", "reason_code": "invalid_metric_configuration"}
    try:
        expected_port_set = {int(p) for p in expected_ports}
    except Exception:
        return False, {"error": "expected_ports must be integers.", "reason_code": "invalid_metric_configuration"}
    if any(port < 0 or port > 65535 for port in expected_port_set):
        return False, {"error": "expected_ports must be within 0-65535.", "reason_code": "invalid_metric_configuration"}
    if match_mode not in {"any_port", "destination_only", "source_only", "both_ports"}:
        return False, {"error": f"Unsupported match_mode: {match_mode}", "reason_code": "invalid_metric_configuration"}
    if population_mode not in {"auto", "service_field", "all_rows"}:
        return False, {"error": f"Unsupported population_mode: {population_mode}", "reason_code": "invalid_metric_configuration"}
    if not 0 <= warn_threshold <= pass_threshold <= 1:
        return False, {
            "error": "Thresholds must satisfy 0 <= warn_threshold <= pass_threshold <= 1.",
            "reason_code": "invalid_metric_configuration",
        }

    df = metric.get("_shared_df")
    if df is None:
        try:
            df = load_tabular_dataset(dataset_path)
        except Exception as exc:
            return False, {"error": f"Failed to load dataset: {exc}", "reason_code": "dataset_load_error"}

    existing_fields = [f for f in port_fields if f in df.columns]
    missing_fields = [f for f in port_fields if f not in df.columns]
    if not existing_fields:
        return False, {
            "error": "No requested port fields exist in the dataset.",
            "reason_code": "missing_required_fields",
            "missing_fields": missing_fields or list(port_fields),
        }

    destination_candidates = ["Destination Port", "Dst Port", "dst_port", "destination_port"]
    source_candidates = ["Source Port", "Src Port", "src_port", "source_port"]
    destination_field = next((f for f in destination_candidates if f in existing_fields), None)
    source_field = next((f for f in source_candidates if f in existing_fields), None)

    if match_mode == "destination_only" and destination_field is None:
        return False, {"error": "destination_only match_mode selected but no destination port field exists.", "reason_code": "missing_required_fields"}
    if match_mode == "source_only" and source_field is None:
        return False, {"error": "source_only match_mode selected but no source port field exists.", "reason_code": "missing_required_fields"}
    if match_mode == "both_ports" and (source_field is None or destination_field is None):
        return False, {"error": "both_ports match_mode requires both source and destination port fields.", "reason_code": "missing_required_fields"}

    row_count = int(len(df))
    service_field = params.get("service_field")
    service_values = [str(value).strip().lower() for value in params.get("service_values", [service_name])]
    service_field_candidates = params.get("service_field_candidates", DEFAULT_SERVICE_FIELD_CANDIDATES)
    population_basis = ""

    if population_mode == "all_rows":
        population_mask = pd.Series(True, index=df.index)
        population_basis = "all_rows_explicit"
    else:
        if service_field:
            if service_field not in df.columns:
                return False, {
                    "error": f"Configured service_field does not exist in the dataset: {service_field}",
                    "reason_code": "missing_required_fields",
                    "missing_fields": [service_field],
                }
        else:
            service_field = next((field for field in service_field_candidates if field in df.columns), None)

        if service_field:
            normalized_service = df[service_field].astype("string").str.strip().str.lower()
            population_mask = normalized_service.isin(service_values)
            population_basis = f"service_field:{service_field}"
        elif bool(params.get("assume_dataset_service", False)):
            population_mask = pd.Series(True, index=df.index)
            population_basis = "all_rows_assumed_service"
        else:
            return _not_applicable_result(
                service_name=str(service_name),
                expected_ports=sorted(expected_port_set),
                match_mode=match_mode,
                existing_fields=existing_fields,
                missing_fields=missing_fields,
                row_count=row_count,
                population_basis="service_population_unavailable",
                reason_code="service_population_unavailable",
                summary=(
                    f"No service/application field was available to identify {service_name} rows independently. "
                    f"Applying ports {sorted(expected_port_set)} to every row in a mixed dataset would produce a misleading result."
                ),
                suggestion=(
                    "Map a service/application field, configure service_field/service_values, or explicitly set "
                    "population_mode=all_rows only for a dataset known to contain that service."
                ),
                service_field=None,
                service_values=service_values,
            )

    population_df = df[population_mask]
    population_row_count = int(len(population_df))
    if population_row_count == 0:
        return _not_applicable_result(
            service_name=str(service_name),
            expected_ports=sorted(expected_port_set),
            match_mode=match_mode,
            existing_fields=existing_fields,
            missing_fields=missing_fields,
            row_count=row_count,
            population_basis=population_basis,
            reason_code="service_population_empty",
            summary=f"The service selector found no rows for {service_name}; there is no applicable population to test.",
            suggestion="Check the configured service name/value mapping or mark this metric inapplicable for the dataset.",
            service_field=service_field,
            service_values=service_values,
        )

    checked_port_count = valid_port_count = invalid_port_count = missing_port_count = 0
    parsed_ports = {}
    status_by_field = {}
    for field in existing_fields:
        port_status, parsed = normalize_port_series(population_df[field])
        status_by_field[field] = port_status
        parsed_ports[field] = parsed
        missing_mask = port_status == "missing"
        valid_mask = port_status == "valid"
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

    invalid_port_examples = []
    invalid_rows = population_df[~all_missing_mask & any_invalid_mask]
    for idx, row in invalid_rows.head(max_examples).iterrows():
        for field in existing_fields:
            port_status = status_by_field[field].loc[idx]
            if port_status not in {"missing", "valid"}:
                invalid_port_examples.append({
                    "row_index": int(idx) if isinstance(idx, int) else str(idx),
                    "field": field,
                    "value": str(row[field]).strip(),
                    "reason": str(port_status),
                })
                if len(invalid_port_examples) >= max_examples:
                    break
        if len(invalid_port_examples) >= max_examples:
            break

    mismatch_examples = []
    mismatch_rows = population_df[checked_rows_mask & ~match_mask]
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

    if checked_row_count == 0:
        status = "not_applicable"
        service_port_match_ratio = None
        service_port_mismatch_ratio = None
        diagnostic = {
            "reason_code": "no_valid_service_rows",
            "summary": f"The selected {service_name} population contained no rows with valid port values to compare.",
            "evidence": {
                "population_row_count": population_row_count,
                "missing_port_row_count": missing_port_row_count,
                "invalid_port_row_count": invalid_port_row_count,
            },
            "suggestion": "Inspect the selected rows and port field mappings before interpreting service-port realism.",
        }
    else:
        service_port_match_ratio = round(matching_row_count / checked_row_count, 6)
        service_port_mismatch_ratio = round(mismatching_row_count / checked_row_count, 6)
        if service_port_match_ratio >= pass_threshold:
            status = "pass"
        elif service_port_match_ratio >= warn_threshold:
            status = "warn"
        else:
            status = "fail"
        if invalid_port_row_count > 0 and status == "pass":
            status = "warn"
        diagnostic = _diagnostic(
            status,
            service_name=str(service_name),
            checked=checked_row_count,
            matching=matching_row_count,
            mismatching=mismatching_row_count,
            match_ratio=service_port_match_ratio,
            pass_threshold=pass_threshold,
            warn_threshold=warn_threshold,
            invalid_rows=invalid_port_row_count,
            population_rows=population_row_count,
            population_basis=population_basis,
            expected_ports=sorted(expected_port_set),
            mismatch_examples=mismatch_examples,
            invalid_examples=invalid_port_examples,
        )

    invalid_port_row_ratio = round(invalid_port_row_count / population_row_count, 6) if population_row_count else None

    return True, {
        "test_results": {
            "service_port_consistency_profile": {
                "service_name": service_name,
                "expected_ports": sorted(expected_port_set),
                "match_mode": match_mode,
                "port_fields": existing_fields,
                "missing_fields": missing_fields,
                "row_count": row_count,
                "population_row_count": population_row_count,
                "population_basis": population_basis,
                "service_field": service_field,
                "service_values": service_values,
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
                "pass_threshold": pass_threshold,
                "warn_threshold": warn_threshold,
                "mismatch_examples": mismatch_examples,
                "invalid_port_examples": invalid_port_examples,
                "status": status,
                "diagnostic": diagnostic,
            }
        }
    }
