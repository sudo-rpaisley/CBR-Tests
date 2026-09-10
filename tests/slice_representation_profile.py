import pandas as pd


def _normalise(value) -> str | None:
    if pd.isna(value):
        return None
    text = str(value).strip()
    return text if text else None


def _slice_field(metric: dict) -> str:
    return metric.get("input_requirements", {}).get("slice_field", "slice")


def _normalised_slice_series(df: pd.DataFrame, slice_field: str) -> pd.Series:
    if slice_field not in df.columns:
        return pd.Series([None] * len(df), index=df.index, dtype="object")
    return df[slice_field].map(_normalise)


def _observed_slices(df: pd.DataFrame, slice_field: str) -> list[str]:
    series = _normalised_slice_series(df, slice_field)
    return sorted({value for value in series.tolist() if value is not None})


def _unique_normalised(values) -> list[str]:
    output = []
    seen = set()
    for value in values:
        normalised = _normalise(value)
        if normalised is not None and normalised not in seen:
            output.append(normalised)
            seen.add(normalised)
    return output


def compute_per_slice_sample_coverage_ratio(df: pd.DataFrame, metric: dict) -> dict:
    """Measure whether declared expected slices meet a minimum sample count.

    Coverage is undefined without an independently declared expected slice set.
    Observed sample counts/proportions are still returned as descriptive evidence.
    """
    requirements = metric.get("input_requirements", {})
    parameters = metric.get("calculation", {}).get("parameters", {})
    slice_field = _slice_field(metric)
    expected_slices = _unique_normalised(requirements.get("expected_slice_ids", []))
    slice_series = _normalised_slice_series(df, slice_field)
    observed = sorted({value for value in slice_series.tolist() if value is not None})
    eligible_row_count = int(slice_series.notna().sum())
    counts = {slice_id: int((slice_series == slice_id).sum()) for slice_id in observed}

    default_minimum = int(parameters.get("minimum_sample_count", 1))
    if default_minimum < 1:
        default_minimum = 1
    by_slice = parameters.get("minimum_sample_count_by_slice", {})
    if not isinstance(by_slice, dict):
        by_slice = {}

    target_slices = expected_slices or observed
    slice_results = []
    covered_count = 0
    for slice_id in target_slices:
        count = counts.get(slice_id, 0)
        minimum = int(by_slice.get(slice_id, default_minimum)) if expected_slices else None
        covered = (count >= minimum) if minimum is not None else None
        if covered is True:
            covered_count += 1
        slice_results.append(
            {
                "slice_id": slice_id,
                "sample_count": count,
                "sample_proportion": round(count / eligible_row_count, 6) if eligible_row_count else 0.0,
                "minimum_sample_count": minimum,
                "covered": covered,
            }
        )

    coverage_ratio = (
        round(covered_count / len(expected_slices), 6) if expected_slices else None
    )
    return {
        "slices": slice_results,
        "summary": {
            "slice_field": slice_field,
            "eligible_slice_row_count": eligible_row_count,
            "expected_slice_count": len(expected_slices),
            "covered_slice_count": covered_count if expected_slices else None,
            "observed_slice_count": len(observed),
            "unexpected_slices": [value for value in observed if value not in expected_slices] if expected_slices else [],
            "per_slice_sample_coverage_ratio": coverage_ratio,
            "coverage_basis": "declared_expected_slices" if expected_slices else "descriptive_only",
            "reason": None if expected_slices else "expected_slice_ids_required_for_coverage",
        },
    }


def compute_per_slice_feature_coverage_ratio(df: pd.DataFrame, metric: dict) -> dict:
    """Compute row-level non-missing completeness for each feature within each slice."""
    requirements = metric.get("input_requirements", {})
    slice_field = _slice_field(metric)
    candidate_fields = list(dict.fromkeys(requirements.get("candidate_fields", [])))
    slice_series = _normalised_slice_series(df, slice_field)
    observed = sorted({value for value in slice_series.tolist() if value is not None})
    slice_results = []
    slice_ratios = []

    for slice_id in observed:
        mask = slice_series == slice_id
        slice_df = df.loc[mask]
        sample_count = int(mask.sum())
        feature_results = []
        feature_ratios = []
        for field in candidate_fields:
            exists = field in df.columns
            usable_count = int(slice_df[field].notna().sum()) if exists else 0
            ratio = usable_count / sample_count if sample_count else 0.0
            feature_ratios.append(ratio)
            feature_results.append(
                {
                    "field": field,
                    "exists": exists,
                    "usable_sample_count": usable_count,
                    "sample_count": sample_count,
                    "feature_coverage_ratio": round(ratio, 6),
                }
            )
        slice_ratio = (
            sum(feature_ratios) / len(feature_ratios) if feature_ratios else None
        )
        if slice_ratio is not None:
            slice_ratios.append(slice_ratio)
        slice_results.append(
            {
                "slice_id": slice_id,
                "sample_count": sample_count,
                "features": feature_results,
                "expected_feature_count": len(candidate_fields),
                "per_slice_feature_coverage_ratio": round(slice_ratio, 6) if slice_ratio is not None else None,
            }
        )

    overall = sum(slice_ratios) / len(slice_ratios) if slice_ratios else None
    return {
        "slices": slice_results,
        "summary": {
            "slice_field": slice_field,
            "slice_count": len(observed),
            "candidate_feature_count": len(candidate_fields),
            "per_slice_feature_coverage_ratio": round(overall, 6) if overall is not None else None,
            "reason": None if candidate_fields else "candidate_fields_required_for_coverage",
        },
    }


def compute_per_slice_class_coverage_ratio(df: pd.DataFrame, metric: dict) -> dict:
    """Measure expected-class coverage in each observed slice.

    Expected classes must be declared independently; deriving them from the candidate
    dataset would make globally absent classes impossible to detect.
    """
    requirements = metric.get("input_requirements", {})
    slice_field = _slice_field(metric)
    label_field = requirements.get("label_field", "label")
    expected_classes = _unique_normalised(requirements.get("expected_classes", []))
    slice_series = _normalised_slice_series(df, slice_field)
    observed = sorted({value for value in slice_series.tolist() if value is not None})
    slice_results = []
    ratios = []

    for slice_id in observed:
        slice_df = df.loc[slice_series == slice_id]
        observed_classes = (
            sorted(
                {
                    value
                    for value in (_normalise(value) for value in slice_df[label_field])
                    if value is not None
                }
            )
            if label_field in slice_df.columns
            else []
        )
        covered_classes = [label for label in expected_classes if label in observed_classes]
        ratio = (
            len(covered_classes) / len(expected_classes) if expected_classes else None
        )
        if ratio is not None:
            ratios.append(ratio)
        slice_results.append(
            {
                "slice_id": slice_id,
                "observed_classes": observed_classes,
                "covered_class_count": len(covered_classes) if expected_classes else None,
                "expected_class_count": len(expected_classes),
                "missing_classes": [label for label in expected_classes if label not in covered_classes],
                "per_slice_class_coverage_ratio": round(ratio, 6) if ratio is not None else None,
            }
        )

    overall = sum(ratios) / len(ratios) if ratios else None
    return {
        "slices": slice_results,
        "summary": {
            "slice_field": slice_field,
            "label_field": label_field,
            "slice_count": len(observed),
            "expected_class_count": len(expected_classes),
            "per_slice_class_coverage_ratio": round(overall, 6) if overall is not None else None,
            "coverage_basis": "declared_expected_classes" if expected_classes else "descriptive_only",
            "reason": None if expected_classes else "expected_classes_required_for_coverage",
        },
    }


def compute_slice_distribution_imbalance_score(df: pd.DataFrame, metric: dict) -> dict:
    """Report intrinsic slice-distribution skew as the max-minus-min proportion range."""
    slice_field = _slice_field(metric)
    slice_series = _normalised_slice_series(df, slice_field)
    observed = sorted({value for value in slice_series.tolist() if value is not None})
    eligible_row_count = int(slice_series.notna().sum())
    slices = []
    proportions = []
    for slice_id in observed:
        sample_count = int((slice_series == slice_id).sum())
        proportion = sample_count / eligible_row_count if eligible_row_count else 0.0
        proportions.append(proportion)
        slices.append(
            {
                "slice_id": slice_id,
                "sample_count": sample_count,
                "proportion": round(proportion, 6),
            }
        )
    imbalance = max(proportions) - min(proportions) if proportions else None
    return {
        "slices": slices,
        "summary": {
            "slice_field": slice_field,
            "row_count": int(len(df)),
            "eligible_slice_row_count": eligible_row_count,
            "missing_slice_row_count": int(len(df) - eligible_row_count),
            "slice_count": len(observed),
            "slice_distribution_imbalance_score": round(imbalance, 6) if imbalance is not None else None,
            "method": "max_minus_min_observed_slice_proportion",
            "interpretation": "contextual_descriptive_skew",
        },
    }


def compute_cross_slice_duplicate_overlap_ratio(df: pd.DataFrame, metric: dict) -> dict:
    """Measure the fraction of checked signatures that occur in multiple slices."""
    requirements = metric.get("input_requirements", {})
    slice_field = _slice_field(metric)
    subset_fields = requirements.get("subset_fields") or [
        field for field in df.columns if field != slice_field
    ]
    missing_fields = [field for field in subset_fields if field not in df.columns]
    if slice_field not in df.columns or not subset_fields or missing_fields:
        return {
            "summary": {
                "slice_field": slice_field,
                "subset_fields": subset_fields,
                "missing_fields": missing_fields,
                "row_count": int(len(df)),
                "checked_signature_count": 0,
                "overlap_signature_count": 0,
                "cross_slice_duplicate_overlap_ratio": None,
                "reason": "required_fields_missing_or_empty",
            }
        }

    slice_series = _normalised_slice_series(df, slice_field)
    eligible = df.loc[slice_series.notna(), subset_fields].copy()
    eligible["__slice_norm"] = slice_series.loc[slice_series.notna()].values
    if eligible.empty:
        return {
            "summary": {
                "slice_field": slice_field,
                "subset_fields": subset_fields,
                "missing_fields": [],
                "row_count": int(len(df)),
                "eligible_row_count": 0,
                "checked_signature_count": 0,
                "overlap_signature_count": 0,
                "cross_slice_duplicate_overlap_ratio": None,
                "reason": "no_slice_labelled_rows",
            }
        }

    signature_slice_counts = eligible.groupby(subset_fields, dropna=False)["__slice_norm"].nunique()
    checked_signature_count = int(signature_slice_counts.shape[0])
    overlap_signature_count = int((signature_slice_counts > 1).sum())

    row_slice_counts = eligible.groupby(subset_fields, dropna=False)["__slice_norm"].transform("nunique")
    overlap_row_count = int((row_slice_counts > 1).sum())
    eligible_row_count = int(len(eligible))
    return {
        "summary": {
            "slice_field": slice_field,
            "subset_fields": subset_fields,
            "missing_fields": [],
            "row_count": int(len(df)),
            "eligible_row_count": eligible_row_count,
            "checked_signature_count": checked_signature_count,
            "overlap_signature_count": overlap_signature_count,
            "overlap_row_count": overlap_row_count,
            "overlap_row_ratio": round(overlap_row_count / eligible_row_count, 6) if eligible_row_count else None,
            "cross_slice_duplicate_overlap_ratio": (
                round(overlap_signature_count / checked_signature_count, 6)
                if checked_signature_count
                else None
            ),
            "reason": None,
        }
    }


def compute_cross_slice_identifier_leakage_ratio(df: pd.DataFrame, metric: dict) -> dict:
    """Measure cross-slice identifier overlap and label it leakage only under an explicit exclusivity policy."""
    requirements = metric.get("input_requirements", {})
    parameters = metric.get("calculation", {}).get("parameters", {})
    slice_field = _slice_field(metric)
    identifier_fields = list(dict.fromkeys(requirements.get("identifier_fields", [])))
    expect_slice_exclusive = parameters.get("expect_slice_exclusive") is True
    slice_series = _normalised_slice_series(df, slice_field)

    overlap_identifier_count = 0
    unique_identifier_count = 0
    field_results = []
    for field in identifier_fields:
        if field not in df.columns or slice_field not in df.columns:
            field_results.append(
                {
                    "field": field,
                    "exists": field in df.columns,
                    "unique_identifier_count": 0,
                    "overlap_identifier_count": 0,
                    "cross_slice_identifier_overlap_ratio": None,
                    "cross_slice_identifier_leakage_ratio": None,
                    "reason": "required_field_missing",
                }
            )
            continue

        identifier_series = df[field].map(_normalise)
        eligible_mask = identifier_series.notna() & slice_series.notna()
        eligible = pd.DataFrame(
            {
                "identifier": identifier_series.loc[eligible_mask],
                "slice": slice_series.loc[eligible_mask],
            }
        )
        grouped = eligible.groupby("identifier")["slice"].nunique()
        field_unique = int(grouped.shape[0])
        field_overlap = int((grouped > 1).sum())
        unique_identifier_count += field_unique
        overlap_identifier_count += field_overlap
        overlap_ratio = field_overlap / field_unique if field_unique else None
        field_results.append(
            {
                "field": field,
                "exists": True,
                "unique_identifier_count": field_unique,
                "overlap_identifier_count": field_overlap,
                "cross_slice_identifier_overlap_ratio": round(overlap_ratio, 6) if overlap_ratio is not None else None,
                "cross_slice_identifier_leakage_ratio": (
                    round(overlap_ratio, 6) if overlap_ratio is not None and expect_slice_exclusive else None
                ),
                "reason": None if expect_slice_exclusive else "slice_exclusivity_policy_required_for_leakage",
            }
        )

    overall_overlap = (
        overlap_identifier_count / unique_identifier_count if unique_identifier_count else None
    )
    return {
        "fields": field_results,
        "summary": {
            "slice_field": slice_field,
            "identifier_field_count": len(identifier_fields),
            "unique_identifier_count": unique_identifier_count,
            "overlap_identifier_count": overlap_identifier_count,
            "cross_slice_identifier_overlap_ratio": round(overall_overlap, 6) if overall_overlap is not None else None,
            "cross_slice_identifier_leakage_ratio": (
                round(overall_overlap, 6) if overall_overlap is not None and expect_slice_exclusive else None
            ),
            "expect_slice_exclusive": expect_slice_exclusive,
            "reason": (
                None
                if expect_slice_exclusive
                else "slice_exclusivity_policy_required_for_leakage"
            ),
        },
    }
