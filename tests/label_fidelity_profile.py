from math import log2

import pandas as pd


def _normalise(value) -> str | None:
    if pd.isna(value):
        return None
    text = str(value).strip()
    return text if text else None


def _label_field(metric: dict) -> str:
    return metric.get("input_requirements", {}).get("label_field", "label")


def _slice_field(metric: dict) -> str:
    return metric.get("input_requirements", {}).get("slice_field", "slice")


def _timestamp_field(metric: dict) -> str:
    return metric.get("input_requirements", {}).get("timestamp_field", "timestamp")


def _parse_timestamps(df: pd.DataFrame, field: str) -> pd.Series:
    if field not in df.columns:
        return pd.Series([pd.NaT] * len(df), index=df.index)
    return pd.to_datetime(df[field], errors="coerce", utc=True)


def _label_values(metric: dict, key: str) -> set[str]:
    return {value for value in (_normalise(value) for value in metric.get("input_requirements", {}).get(key, [])) if value is not None}


def _observed_labels(series: pd.Series) -> list[str]:
    return sorted({value for value in (_normalise(value) for value in series) if value is not None})


def compute_label_coverage_ratio(df: pd.DataFrame, metric: dict) -> dict:
    label_field = _label_field(metric)
    row_count = int(len(df))
    if label_field not in df.columns:
        labelled_count = 0
    else:
        labelled_count = int(df[label_field].map(lambda value: _normalise(value) is not None).sum())
    return {
        "summary": {
            "label_field": label_field,
            "row_count": row_count,
            "labelled_row_count": labelled_count,
            "missing_label_count": row_count - labelled_count,
            "label_coverage_ratio": round(labelled_count / row_count, 6) if row_count else 0.0,
        }
    }


def compute_per_slice_label_coverage_ratio(df: pd.DataFrame, metric: dict) -> dict:
    label_field = _label_field(metric)
    slice_field = _slice_field(metric)
    if label_field not in df.columns or slice_field not in df.columns:
        return {"slices": [], "summary": {"label_field": label_field, "slice_field": slice_field, "per_slice_label_coverage_ratio": 0.0}}
    slices = []
    ratios = []
    for slice_id, slice_df in df.groupby(slice_field, dropna=False):
        row_count = int(len(slice_df))
        labelled_count = int(slice_df[label_field].map(lambda value: _normalise(value) is not None).sum())
        ratio = round(labelled_count / row_count, 6) if row_count else 0.0
        ratios.append(ratio)
        slices.append({
            "slice_id": _normalise(slice_id),
            "row_count": row_count,
            "labelled_row_count": labelled_count,
            "missing_label_count": row_count - labelled_count,
            "per_slice_label_coverage_ratio": ratio,
        })
    return {
        "slices": slices,
        "summary": {
            "label_field": label_field,
            "slice_field": slice_field,
            "slice_count": len(slices),
            "per_slice_label_coverage_ratio": round(sum(ratios) / len(ratios), 6) if ratios else 0.0,
        },
    }


def _entropy_score(labels: list[str], expected_classes: list[str]) -> float:
    if not labels:
        return 0.0
    classes = expected_classes or sorted(set(labels))
    if len(classes) < 2:
        return 0.0
    counts = {label: 0 for label in classes}
    for label in labels:
        if label in counts:
            counts[label] += 1
    total = sum(counts.values())
    if total == 0:
        return 0.0
    entropy = 0.0
    for count in counts.values():
        if count:
            probability = count / total
            entropy -= probability * log2(probability)
    return entropy / log2(len(classes))


def compute_per_slice_label_entropy_score(df: pd.DataFrame, metric: dict) -> dict:
    label_field = _label_field(metric)
    slice_field = _slice_field(metric)
    expected_classes = list(_label_values(metric, "expected_classes"))
    if label_field not in df.columns or slice_field not in df.columns:
        return {"slices": [], "summary": {"label_field": label_field, "slice_field": slice_field, "per_slice_label_entropy_score": 0.0}}
    scores = []
    slices = []
    for slice_id, slice_df in df.groupby(slice_field, dropna=False):
        labels = [value for value in (_normalise(value) for value in slice_df[label_field]) if value is not None]
        score = round(_entropy_score(labels, expected_classes), 6)
        scores.append(score)
        slices.append({"slice_id": _normalise(slice_id), "label_count": len(labels), "per_slice_label_entropy_score": score})
    return {"slices": slices, "summary": {"label_field": label_field, "slice_field": slice_field, "slice_count": len(slices), "per_slice_label_entropy_score": round(sum(scores) / len(scores), 6) if scores else 0.0}}


def compute_class_imbalance_score(df: pd.DataFrame, metric: dict) -> dict:
    label_field = _label_field(metric)
    expected_classes = sorted(_label_values(metric, "expected_classes"))
    if label_field not in df.columns:
        return {"classes": [], "summary": {"label_field": label_field, "class_imbalance_score": 0.0}}
    labels = [value for value in (_normalise(value) for value in df[label_field]) if value is not None]
    classes = expected_classes or sorted(set(labels))
    total = len(labels)
    proportions = []
    class_results = []
    for label in classes:
        count = labels.count(label)
        proportion = count / total if total else 0.0
        proportions.append(proportion)
        class_results.append({"label": label, "count": count, "proportion": round(proportion, 6)})
    imbalance = max(proportions) - min(proportions) if proportions else 0.0
    return {"classes": class_results, "summary": {"label_field": label_field, "labelled_row_count": total, "class_count": len(classes), "class_imbalance_score": round(imbalance, 6)}}


def _attack_windows(metric: dict) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
    windows = []
    for window in metric.get("input_requirements", {}).get("attack_windows", []):
        start = pd.to_datetime(window.get("start"), errors="coerce", utc=True)
        end = pd.to_datetime(window.get("end"), errors="coerce", utc=True)
        if pd.notna(start) and pd.notna(end):
            windows.append((start, end))
    return windows


def _in_any_window(timestamp: pd.Timestamp, windows: list[tuple[pd.Timestamp, pd.Timestamp]]) -> bool:
    return pd.notna(timestamp) and any(start <= timestamp <= end for start, end in windows)


def compute_attack_window_alignment_score(df: pd.DataFrame, metric: dict) -> dict:
    label_field = _label_field(metric)
    timestamp_field = _timestamp_field(metric)
    attack_labels = _label_values(metric, "attack_label_values")
    windows = _attack_windows(metric)
    timestamps = _parse_timestamps(df, timestamp_field)
    checked = aligned = 0
    for idx, timestamp in timestamps.items():
        label = _normalise(df.at[idx, label_field]) if label_field in df.columns else None
        if label is None or pd.isna(timestamp):
            continue
        expected_attack = _in_any_window(timestamp, windows)
        observed_attack = label in attack_labels
        checked += 1
        if expected_attack == observed_attack:
            aligned += 1
    return {"summary": {"label_field": label_field, "timestamp_field": timestamp_field, "attack_window_count": len(windows), "checked_row_count": checked, "aligned_row_count": aligned, "attack_window_alignment_score": round(aligned / checked, 6) if checked else 0.0}}


def compute_pre_post_attack_label_bleed_ratio(df: pd.DataFrame, metric: dict) -> dict:
    label_field = _label_field(metric)
    timestamp_field = _timestamp_field(metric)
    attack_labels = _label_values(metric, "attack_label_values")
    bleed_seconds = float(metric.get("calculation", {}).get("parameters", {}).get("bleed_window_seconds", 60.0))
    windows = _attack_windows(metric)
    timestamps = _parse_timestamps(df, timestamp_field)
    boundary = bleed = 0
    for idx, timestamp in timestamps.items():
        if pd.isna(timestamp) or _in_any_window(timestamp, windows):
            continue
        near_window = any(0 < abs((timestamp - start).total_seconds()) <= bleed_seconds or 0 < abs((timestamp - end).total_seconds()) <= bleed_seconds for start, end in windows)
        if not near_window:
            continue
        boundary += 1
        label = _normalise(df.at[idx, label_field]) if label_field in df.columns else None
        if label in attack_labels:
            bleed += 1
    return {"summary": {"label_field": label_field, "timestamp_field": timestamp_field, "bleed_window_seconds": bleed_seconds, "boundary_row_count": boundary, "bleed_label_count": bleed, "pre_post_attack_label_bleed_ratio": round(bleed / boundary, 6) if boundary else 0.0}}


def _split_masks(df: pd.DataFrame, metric: dict) -> tuple[pd.Series, pd.Series, str]:
    requirements = metric.get("input_requirements", {})
    split_field = requirements.get("split_field", "split")
    train_values = {str(value).lower() for value in requirements.get("train_values", ["train"])}
    test_values = {str(value).lower() for value in requirements.get("test_values", ["test"])}
    if split_field not in df.columns:
        return pd.Series([False] * len(df), index=df.index), pd.Series([False] * len(df), index=df.index), split_field
    values = df[split_field].astype(str).str.lower()
    return values.isin(train_values), values.isin(test_values), split_field


def compute_train_test_duplicate_overlap_ratio(df: pd.DataFrame, metric: dict) -> dict:
    requirements = metric.get("input_requirements", {})
    subset_fields = requirements.get("subset_fields") or [field for field in df.columns if field != requirements.get("split_field", "split")]
    train_mask, test_mask, split_field = _split_masks(df, metric)
    if not subset_fields:
        return {"summary": {"split_field": split_field, "subset_fields": subset_fields, "train_test_duplicate_overlap_ratio": 0.0}}
    train_keys = set(df.loc[train_mask, subset_fields].drop_duplicates().itertuples(index=False, name=None))
    test_keys = set(df.loc[test_mask, subset_fields].drop_duplicates().itertuples(index=False, name=None))
    overlap = train_keys & test_keys
    denominator = len(train_keys | test_keys)
    return {"summary": {"split_field": split_field, "subset_fields": subset_fields, "train_key_count": len(train_keys), "test_key_count": len(test_keys), "overlap_key_count": len(overlap), "train_test_duplicate_overlap_ratio": round(len(overlap) / denominator, 6) if denominator else 0.0}}


def compute_train_test_identifier_contamination_ratio(df: pd.DataFrame, metric: dict) -> dict:
    requirements = metric.get("input_requirements", {})
    identifier_fields = requirements.get("identifier_fields", [])
    train_mask, test_mask, split_field = _split_masks(df, metric)
    contaminated = total = 0
    fields = []
    for field in identifier_fields:
        if field not in df.columns:
            fields.append({"field": field, "exists": False, "unique_identifier_count": 0, "contaminated_identifier_count": 0, "train_test_identifier_contamination_ratio": 0.0})
            continue
        train_ids = {value for value in (_normalise(value) for value in df.loc[train_mask, field]) if value is not None}
        test_ids = {value for value in (_normalise(value) for value in df.loc[test_mask, field]) if value is not None}
        overlap = train_ids & test_ids
        union = train_ids | test_ids
        total += len(union)
        contaminated += len(overlap)
        fields.append({"field": field, "exists": True, "unique_identifier_count": len(union), "contaminated_identifier_count": len(overlap), "train_test_identifier_contamination_ratio": round(len(overlap) / len(union), 6) if union else 0.0})
    return {"fields": fields, "summary": {"split_field": split_field, "identifier_field_count": len(identifier_fields), "unique_identifier_count": total, "contaminated_identifier_count": contaminated, "train_test_identifier_contamination_ratio": round(contaminated / total, 6) if total else 0.0}}
