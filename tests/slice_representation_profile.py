import pandas as pd


def _normalise(value) -> str | None:
    if pd.isna(value):
        return None
    text = str(value).strip()
    return text if text else None


def _slice_field(metric: dict) -> str:
    return metric.get("input_requirements", {}).get("slice_field", "slice")


def _observed_slices(df: pd.DataFrame, slice_field: str) -> list[str]:
    if slice_field not in df.columns:
        return []
    return sorted({value for value in (_normalise(value) for value in df[slice_field]) if value is not None})


def compute_per_slice_sample_coverage_ratio(df: pd.DataFrame, metric: dict) -> dict:
    requirements = metric.get("input_requirements", {})
    slice_field = _slice_field(metric)
    expected_slices = [_normalise(value) for value in requirements.get("expected_slice_ids", [])]
    expected_slices = [value for value in expected_slices if value is not None]
    observed = _observed_slices(df, slice_field)
    target_slices = expected_slices or observed
    covered = [slice_id for slice_id in target_slices if slice_id in observed]
    counts = {slice_id: int((df[slice_field].astype(str) == slice_id).sum()) for slice_id in observed} if slice_field in df.columns else {}
    return {
        "slices": [{"slice_id": slice_id, "sample_count": counts.get(slice_id, 0), "covered": slice_id in covered} for slice_id in target_slices],
        "summary": {
            "slice_field": slice_field,
            "expected_slice_count": len(target_slices),
            "covered_slice_count": len(covered),
            "observed_slice_count": len(observed),
            "per_slice_sample_coverage_ratio": round(len(covered) / len(target_slices), 6) if target_slices else 0.0,
        },
    }


def compute_per_slice_feature_coverage_ratio(df: pd.DataFrame, metric: dict) -> dict:
    requirements = metric.get("input_requirements", {})
    slice_field = _slice_field(metric)
    candidate_fields = requirements.get("candidate_fields", [])
    observed = _observed_slices(df, slice_field)
    slice_results = []
    ratios = []
    for slice_id in observed:
        slice_df = df[df[slice_field].astype(str) == slice_id]
        covered_fields = [field for field in candidate_fields if field in df.columns and slice_df[field].notna().any()]
        ratio = round(len(covered_fields) / len(candidate_fields), 6) if candidate_fields else 0.0
        ratios.append(ratio)
        slice_results.append({
            "slice_id": slice_id,
            "covered_feature_count": len(covered_fields),
            "expected_feature_count": len(candidate_fields),
            "missing_features": [field for field in candidate_fields if field not in covered_fields],
            "per_slice_feature_coverage_ratio": ratio,
        })
    return {
        "slices": slice_results,
        "summary": {
            "slice_field": slice_field,
            "slice_count": len(observed),
            "candidate_feature_count": len(candidate_fields),
            "per_slice_feature_coverage_ratio": round(sum(ratios) / len(ratios), 6) if ratios else 0.0,
        },
    }


def compute_per_slice_class_coverage_ratio(df: pd.DataFrame, metric: dict) -> dict:
    requirements = metric.get("input_requirements", {})
    slice_field = _slice_field(metric)
    label_field = requirements.get("label_field", "label")
    expected_classes = [_normalise(value) for value in requirements.get("expected_classes", [])]
    expected_classes = [value for value in expected_classes if value is not None]
    if not expected_classes and label_field in df.columns:
        expected_classes = sorted({value for value in (_normalise(value) for value in df[label_field]) if value is not None})
    observed = _observed_slices(df, slice_field)
    slice_results = []
    ratios = []
    for slice_id in observed:
        slice_df = df[df[slice_field].astype(str) == slice_id]
        observed_classes = sorted({value for value in (_normalise(value) for value in slice_df[label_field]) if value is not None}) if label_field in slice_df.columns else []
        covered_classes = [label for label in expected_classes if label in observed_classes]
        ratio = round(len(covered_classes) / len(expected_classes), 6) if expected_classes else 0.0
        ratios.append(ratio)
        slice_results.append({
            "slice_id": slice_id,
            "covered_class_count": len(covered_classes),
            "expected_class_count": len(expected_classes),
            "missing_classes": [label for label in expected_classes if label not in covered_classes],
            "per_slice_class_coverage_ratio": ratio,
        })
    return {
        "slices": slice_results,
        "summary": {
            "slice_field": slice_field,
            "label_field": label_field,
            "slice_count": len(observed),
            "expected_class_count": len(expected_classes),
            "per_slice_class_coverage_ratio": round(sum(ratios) / len(ratios), 6) if ratios else 0.0,
        },
    }


def compute_slice_distribution_imbalance_score(df: pd.DataFrame, metric: dict) -> dict:
    slice_field = _slice_field(metric)
    observed = _observed_slices(df, slice_field)
    row_count = int(len(df))
    slices = []
    proportions = []
    for slice_id in observed:
        sample_count = int((df[slice_field].astype(str) == slice_id).sum())
        proportion = sample_count / row_count if row_count else 0.0
        proportions.append(proportion)
        slices.append({"slice_id": slice_id, "sample_count": sample_count, "proportion": round(proportion, 6)})
    imbalance = max(proportions) - min(proportions) if proportions else 0.0
    return {
        "slices": slices,
        "summary": {
            "slice_field": slice_field,
            "row_count": row_count,
            "slice_count": len(observed),
            "slice_distribution_imbalance_score": round(imbalance, 6),
        },
    }


def compute_cross_slice_duplicate_overlap_ratio(df: pd.DataFrame, metric: dict) -> dict:
    requirements = metric.get("input_requirements", {})
    slice_field = _slice_field(metric)
    subset_fields = requirements.get("subset_fields") or [field for field in df.columns if field != slice_field]
    if slice_field not in df.columns or not subset_fields:
        return {"summary": {"slice_field": slice_field, "subset_fields": subset_fields, "row_count": int(len(df)), "cross_slice_duplicate_overlap_ratio": 0.0}}
    duplicate_groups = df.groupby(subset_fields, dropna=False)[slice_field].nunique()
    overlapping_keys = duplicate_groups[duplicate_groups > 1].index
    overlap_mask = df.set_index(subset_fields).index.isin(overlapping_keys)
    overlap_count = int(overlap_mask.sum())
    row_count = int(len(df))
    return {
        "summary": {
            "slice_field": slice_field,
            "subset_fields": subset_fields,
            "row_count": row_count,
            "overlap_row_count": overlap_count,
            "overlap_group_count": int(len(overlapping_keys)),
            "cross_slice_duplicate_overlap_ratio": round(overlap_count / row_count, 6) if row_count else 0.0,
        }
    }


def compute_cross_slice_identifier_leakage_ratio(df: pd.DataFrame, metric: dict) -> dict:
    requirements = metric.get("input_requirements", {})
    slice_field = _slice_field(metric)
    identifier_fields = requirements.get("identifier_fields", [])
    leaked_identifier_count = 0
    unique_identifier_count = 0
    field_results = []
    for field in identifier_fields:
        if field not in df.columns or slice_field not in df.columns:
            field_results.append({"field": field, "exists": field in df.columns, "unique_identifier_count": 0, "leaked_identifier_count": 0, "cross_slice_identifier_leakage_ratio": 0.0})
            continue
        grouped = df.dropna(subset=[field]).groupby(field)[slice_field].nunique()
        field_unique = int(grouped.shape[0])
        field_leaked = int((grouped > 1).sum())
        unique_identifier_count += field_unique
        leaked_identifier_count += field_leaked
        field_results.append({
            "field": field,
            "exists": True,
            "unique_identifier_count": field_unique,
            "leaked_identifier_count": field_leaked,
            "cross_slice_identifier_leakage_ratio": round(field_leaked / field_unique, 6) if field_unique else 0.0,
        })
    return {
        "fields": field_results,
        "summary": {
            "slice_field": slice_field,
            "identifier_field_count": len(identifier_fields),
            "unique_identifier_count": unique_identifier_count,
            "leaked_identifier_count": leaked_identifier_count,
            "cross_slice_identifier_leakage_ratio": round(leaked_identifier_count / unique_identifier_count, 6) if unique_identifier_count else 0.0,
        },
    }
