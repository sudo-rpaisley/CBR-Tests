from pathlib import Path
import pandas as pd


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


def normalise_slice_id(value, case_sensitive: bool, aliases: dict):
    if pd.isna(value):
        return None
    text = str(value).strip()
    if text == "":
        return None
    key = text if case_sensitive else text.lower()
    alias_map = aliases or {}
    if key in alias_map:
        mapped = str(alias_map[key]).strip()
        return mapped if case_sensitive else mapped.lower()
    return key


def run_valid_slice_identifier_metric(dataset_path: Path, metric: dict) -> tuple[bool, dict]:
    """Slice metadata integrity tests are context-dependent. A valid slice identifier only shows that the slice value belongs to the expected vocabulary. Slice identifier consistency checks whether that value is plausible given other row metadata, such as source file, traffic group, or label. A consistency failure should be interpreted as a possible metadata, labelling, merge, or extraction issue, not automatically as proof that the dataset is unusable."""
    input_req = metric.get("input_requirements", {})
    params = metric.get("calculation", {}).get("parameters", {})
    slice_field = input_req.get("slice_field")
    if not slice_field:
        return False, {"error": "slice_field is required."}

    allowed = params.get("allowed_slice_ids", [])
    if not allowed:
        return False, {"error": "allowed_slice_ids is required and must be non-empty."}

    case_sensitive = bool(params.get("case_sensitive", False))
    allow_numeric_equivalents = bool(params.get("allow_numeric_equivalents", True))
    aliases = params.get("slice_aliases", {}) or {}
    if not case_sensitive:
        aliases = {str(k).lower(): v for k, v in aliases.items()}

    missing_policy = params.get("missing_policy", "count_invalid")
    max_examples = int(params.get("max_examples", 10))

    try:
        df = load_tabular_dataset(dataset_path)
    except Exception as exc:
        return False, {"error": f"Failed to load dataset: {exc}"}

    if slice_field not in df.columns:
        return False, {"error": "Slice field does not exist in dataset.", "missing_field": slice_field}

    allowed_norm = set()
    for val in allowed:
        n = normalise_slice_id(val, case_sensitive, aliases)
        if n is not None:
            allowed_norm.add(n)
            if allow_numeric_equivalents:
                try:
                    allowed_norm.add(str(int(float(val))) if case_sensitive else str(int(float(val))).lower())
                except Exception:
                    pass

    row_count = int(len(df))
    checked = valid = invalid = missing = 0
    observed = set()
    examples = []

    for idx, value in df[slice_field].items():
        norm = normalise_slice_id(value, case_sensitive, aliases)
        if norm is None:
            missing += 1
            if missing_policy == "count_invalid":
                checked += 1
                invalid += 1
                if len(examples) < max_examples:
                    examples.append({"row_index": int(idx) if isinstance(idx, int) else str(idx), "value": None, "reason": "missing_slice_id"})
            continue

        observed.add(norm)
        checked += 1

        candidates = {norm}
        if allow_numeric_equivalents:
            try:
                candidates.add(str(int(float(norm))) if case_sensitive else str(int(float(norm))).lower())
            except Exception:
                pass

        if any(c in allowed_norm for c in candidates):
            valid += 1
        else:
            invalid += 1
            if len(examples) < max_examples:
                examples.append({"row_index": int(idx) if isinstance(idx, int) else str(idx), "value": str(value), "reason": "slice_id_not_allowed"})

    if checked == 0:
        return False, {"error": "No slice identifiers were available to check."}

    valid_ratio = round(valid / checked, 6)
    invalid_ratio = round(invalid / checked, 6)
    missing_ratio = round(missing / row_count, 6) if row_count else 0.0
    status = "pass" if valid_ratio >= 0.99 else "warn" if valid_ratio >= 0.95 else "fail"

    return True, {"test_results": {"valid_slice_identifier_profile": {
        "slice_field": slice_field,
        "row_count": row_count,
        "checked_slice_count": checked,
        "valid_slice_count": valid,
        "invalid_slice_count": invalid,
        "missing_slice_count": missing,
        "valid_slice_identifier_ratio": valid_ratio,
        "invalid_slice_identifier_ratio": invalid_ratio,
        "missing_slice_ratio": missing_ratio,
        "allowed_slice_ids": sorted(allowed_norm),
        "observed_slice_ids": sorted(observed),
        "invalid_examples": examples,
        "status": status,
    }}}
