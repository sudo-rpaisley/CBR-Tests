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
    return {
        value
        for value in (
            _normalise(value)
            for value in metric.get("input_requirements", {}).get(key, [])
        )
        if value is not None
    }


def _observed_labels(series: pd.Series) -> list[str]:
    return sorted(
        {
            value
            for value in (_normalise(value) for value in series)
            if value is not None
        }
    )


def _valid_slice_groups(df: pd.DataFrame, slice_field: str):
    """Yield normalised, non-missing slice IDs with their row subsets."""
    if slice_field not in df.columns:
        return []
    groups = []
    normalised = [_normalise(value) for value in df[slice_field]]
    slice_ids = sorted({value for value in normalised if value is not None})
    for slice_id in slice_ids:
        indices = [idx for idx, value in zip(df.index, normalised) if value == slice_id]
        groups.append((slice_id, df.loc[indices]))
    return groups


def compute_label_coverage_ratio(df: pd.DataFrame, metric: dict) -> dict:
    """Return labelled rows / rows in the declared label-required population."""
    label_field = _label_field(metric)
    row_count = int(len(df))
    if label_field not in df.columns:
        labelled_count = 0
    else:
        labelled_count = int(
            df[label_field].map(lambda value: _normalise(value) is not None).sum()
        )
    return {
        "summary": {
            "label_field": label_field,
            "row_count": row_count,
            "expected_labelled_row_count": row_count,
            "labelled_row_count": labelled_count,
            "missing_label_count": row_count - labelled_count,
            "label_coverage_ratio": round(labelled_count / row_count, 6)
            if row_count
            else None,
        }
    }


def compute_per_slice_label_coverage_ratio(df: pd.DataFrame, metric: dict) -> dict:
    """Return label completeness for each valid, non-missing slice population."""
    label_field = _label_field(metric)
    slice_field = _slice_field(metric)
    if label_field not in df.columns or slice_field not in df.columns:
        return {
            "slices": [],
            "summary": {
                "label_field": label_field,
                "slice_field": slice_field,
                "slice_count": 0,
                "missing_slice_row_count": int(len(df)) if slice_field not in df.columns else int(df[slice_field].map(lambda value: _normalise(value) is None).sum()),
                "per_slice_label_coverage_ratio": None,
            },
        }

    slices = []
    ratios = []
    missing_slice_row_count = int(
        df[slice_field].map(lambda value: _normalise(value) is None).sum()
    )
    for slice_id, slice_df in _valid_slice_groups(df, slice_field):
        row_count = int(len(slice_df))
        labelled_count = int(
            slice_df[label_field].map(lambda value: _normalise(value) is not None).sum()
        )
        ratio = labelled_count / row_count if row_count else None
        if ratio is not None:
            ratios.append(ratio)
        slices.append(
            {
                "slice_id": slice_id,
                "row_count": row_count,
                "expected_labelled_row_count": row_count,
                "labelled_row_count": labelled_count,
                "missing_label_count": row_count - labelled_count,
                "per_slice_label_coverage_ratio": round(ratio, 6)
                if ratio is not None
                else None,
            }
        )
    return {
        "slices": slices,
        "summary": {
            "label_field": label_field,
            "slice_field": slice_field,
            "slice_count": len(slices),
            "missing_slice_row_count": missing_slice_row_count,
            "per_slice_label_coverage_ratio": round(sum(ratios) / len(ratios), 6)
            if ratios
            else None,
        },
    }


def _entropy_score(labels: list[str], expected_classes: list[str]) -> tuple[float | None, list[str], str]:
    """Return normalised Shannon entropy, unexpected labels and class-universe source."""
    if not labels:
        return None, [], "expected" if expected_classes else "observed"

    observed_classes = sorted(set(labels))
    if expected_classes:
        classes = sorted(set(expected_classes))
        unexpected = sorted(set(observed_classes) - set(classes))
        if unexpected:
            return None, unexpected, "expected"
        universe_source = "expected"
    else:
        classes = observed_classes
        unexpected = []
        universe_source = "observed"

    if len(classes) < 2:
        return 0.0, unexpected, universe_source

    counts = {label: 0 for label in classes}
    for label in labels:
        counts[label] += 1
    total = sum(counts.values())
    if total == 0:
        return None, unexpected, universe_source

    entropy = 0.0
    for count in counts.values():
        if count:
            probability = count / total
            entropy -= probability * log2(probability)
    return entropy / log2(len(classes)), unexpected, universe_source


def compute_per_slice_label_entropy_score(df: pd.DataFrame, metric: dict) -> dict:
    """Return normalised Shannon entropy per valid slice; interpretation is contextual."""
    label_field = _label_field(metric)
    slice_field = _slice_field(metric)
    expected_classes = sorted(_label_values(metric, "expected_classes"))
    if label_field not in df.columns or slice_field not in df.columns:
        return {
            "slices": [],
            "summary": {
                "label_field": label_field,
                "slice_field": slice_field,
                "slice_count": 0,
                "expected_classes": expected_classes,
                "per_slice_label_entropy_score": None,
            },
        }

    scores = []
    slices = []
    for slice_id, slice_df in _valid_slice_groups(df, slice_field):
        labels = [
            value
            for value in (_normalise(value) for value in slice_df[label_field])
            if value is not None
        ]
        score, unexpected, universe_source = _entropy_score(labels, expected_classes)
        if score is not None:
            scores.append(score)
        slices.append(
            {
                "slice_id": slice_id,
                "label_count": len(labels),
                "missing_label_count": int(len(slice_df)) - len(labels),
                "class_universe_source": universe_source,
                "unexpected_labels": unexpected,
                "per_slice_label_entropy_score": round(score, 6)
                if score is not None
                else None,
            }
        )
    return {
        "slices": slices,
        "summary": {
            "label_field": label_field,
            "slice_field": slice_field,
            "slice_count": len(slices),
            "expected_classes": expected_classes,
            "interpretation": "contextual_label_diversity_not_direct_realism",
            "per_slice_label_entropy_score": round(sum(scores) / len(scores), 6)
            if scores
            else None,
        },
    }


def compute_class_imbalance_score(df: pd.DataFrame, metric: dict) -> dict:
    """Return max-minus-min class-proportion skew over an independently declared class universe."""
    label_field = _label_field(metric)
    expected_classes = sorted(_label_values(metric, "expected_classes"))
    if label_field not in df.columns:
        return {
            "classes": [],
            "summary": {
                "label_field": label_field,
                "expected_classes": expected_classes,
                "class_imbalance_score": None,
                "reason": "label_field_missing",
            },
        }

    labels = [
        value
        for value in (_normalise(value) for value in df[label_field])
        if value is not None
    ]
    observed_classes = sorted(set(labels))
    if not expected_classes:
        return {
            "classes": [
                {
                    "label": label,
                    "count": labels.count(label),
                    "proportion": round(labels.count(label) / len(labels), 6)
                    if labels
                    else None,
                }
                for label in observed_classes
            ],
            "summary": {
                "label_field": label_field,
                "labelled_row_count": len(labels),
                "observed_classes": observed_classes,
                "expected_classes": [],
                "class_imbalance_score": None,
                "reason": "expected_classes_required_for_canonical_imbalance",
            },
        }

    unexpected = sorted(set(observed_classes) - set(expected_classes))
    if unexpected:
        return {
            "classes": [],
            "summary": {
                "label_field": label_field,
                "labelled_row_count": len(labels),
                "expected_classes": expected_classes,
                "unexpected_labels": unexpected,
                "class_imbalance_score": None,
                "reason": "unexpected_labels_outside_declared_class_universe",
            },
        }

    total = len(labels)
    proportions = []
    class_results = []
    for label in expected_classes:
        count = labels.count(label)
        proportion = count / total if total else 0.0
        proportions.append(proportion)
        class_results.append(
            {"label": label, "count": count, "proportion": round(proportion, 6)}
        )
    imbalance = max(proportions) - min(proportions) if proportions and total else None
    return {
        "classes": class_results,
        "summary": {
            "label_field": label_field,
            "labelled_row_count": total,
            "class_count": len(expected_classes),
            "expected_classes": expected_classes,
            "interpretation": "contextual_class_skew_not_direct_realism",
            "class_imbalance_score": round(imbalance, 6)
            if imbalance is not None
            else None,
        },
    }


def _attack_windows(metric: dict) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
    windows = []
    for window in metric.get("input_requirements", {}).get("attack_windows", []):
        start = pd.to_datetime(window.get("start"), errors="coerce", utc=True)
        end = pd.to_datetime(window.get("end"), errors="coerce", utc=True)
        if pd.notna(start) and pd.notna(end) and start <= end:
            windows.append((start, end))
    return windows


def _in_any_window(
    timestamp: pd.Timestamp, windows: list[tuple[pd.Timestamp, pd.Timestamp]]
) -> bool:
    return pd.notna(timestamp) and any(start <= timestamp <= end for start, end in windows)


def compute_attack_window_alignment_score(df: pd.DataFrame, metric: dict) -> dict:
    """Compare observed attack labels with the binary attack state defined by ground-truth windows."""
    label_field = _label_field(metric)
    timestamp_field = _timestamp_field(metric)
    attack_labels = _label_values(metric, "attack_label_values")
    windows = _attack_windows(metric)
    timestamps = _parse_timestamps(df, timestamp_field)

    if not attack_labels or not windows:
        return {
            "summary": {
                "label_field": label_field,
                "timestamp_field": timestamp_field,
                "attack_window_count": len(windows),
                "attack_label_values": sorted(attack_labels),
                "configuration_complete": False,
                "attack_window_alignment_score": None,
                "attack_window_precision": None,
                "attack_window_recall": None,
            }
        }

    checked = aligned = true_attack = observed_attack_count = in_window_count = 0
    missing_label_count = invalid_timestamp_count = 0
    for idx, timestamp in timestamps.items():
        label = _normalise(df.at[idx, label_field]) if label_field in df.columns else None
        if label is None:
            missing_label_count += 1
            continue
        if pd.isna(timestamp):
            invalid_timestamp_count += 1
            continue
        expected_attack = _in_any_window(timestamp, windows)
        observed_attack = label in attack_labels
        checked += 1
        if expected_attack:
            in_window_count += 1
        if observed_attack:
            observed_attack_count += 1
        if expected_attack and observed_attack:
            true_attack += 1
        if expected_attack == observed_attack:
            aligned += 1

    return {
        "summary": {
            "label_field": label_field,
            "timestamp_field": timestamp_field,
            "attack_window_count": len(windows),
            "attack_label_values": sorted(attack_labels),
            "configuration_complete": True,
            "checked_row_count": checked,
            "aligned_row_count": aligned,
            "missing_label_count": missing_label_count,
            "invalid_timestamp_count": invalid_timestamp_count,
            "attack_labelled_row_count": observed_attack_count,
            "in_window_row_count": in_window_count,
            "attack_labelled_in_window_count": true_attack,
            "attack_window_alignment_score": round(aligned / checked, 6)
            if checked
            else None,
            "attack_window_precision": round(true_attack / observed_attack_count, 6)
            if observed_attack_count
            else None,
            "attack_window_recall": round(true_attack / in_window_count, 6)
            if in_window_count
            else None,
        }
    }


def compute_pre_post_attack_label_bleed_ratio(df: pd.DataFrame, metric: dict) -> dict:
    """Measure attack labels among label-observable rows in declared pre/post attack buffers."""
    label_field = _label_field(metric)
    timestamp_field = _timestamp_field(metric)
    attack_labels = _label_values(metric, "attack_label_values")
    bleed_seconds = float(
        metric.get("calculation", {})
        .get("parameters", {})
        .get("bleed_window_seconds", 60.0)
    )
    windows = _attack_windows(metric)
    timestamps = _parse_timestamps(df, timestamp_field)

    if not attack_labels or not windows or bleed_seconds <= 0:
        return {
            "summary": {
                "label_field": label_field,
                "timestamp_field": timestamp_field,
                "bleed_window_seconds": bleed_seconds,
                "attack_window_count": len(windows),
                "configuration_complete": False,
                "pre_post_attack_label_bleed_ratio": None,
            }
        }

    boundary = bleed = missing_boundary_labels = 0
    for idx, timestamp in timestamps.items():
        if pd.isna(timestamp) or _in_any_window(timestamp, windows):
            continue
        near_window = any(
            0 < abs((timestamp - start).total_seconds()) <= bleed_seconds
            or 0 < abs((timestamp - end).total_seconds()) <= bleed_seconds
            for start, end in windows
        )
        if not near_window:
            continue
        label = _normalise(df.at[idx, label_field]) if label_field in df.columns else None
        if label is None:
            missing_boundary_labels += 1
            continue
        boundary += 1
        if label in attack_labels:
            bleed += 1
    return {
        "summary": {
            "label_field": label_field,
            "timestamp_field": timestamp_field,
            "bleed_window_seconds": bleed_seconds,
            "attack_window_count": len(windows),
            "configuration_complete": True,
            "boundary_row_count": boundary,
            "missing_boundary_label_count": missing_boundary_labels,
            "bleed_label_count": bleed,
            "pre_post_attack_label_bleed_ratio": round(bleed / boundary, 6)
            if boundary
            else None,
        }
    }


def _split_masks(df: pd.DataFrame, metric: dict) -> tuple[pd.Series, pd.Series, str]:
    requirements = metric.get("input_requirements", {})
    split_field = requirements.get("split_field", "split")
    train_values = {
        str(value).strip().lower()
        for value in requirements.get("train_values", ["train"])
    }
    test_values = {
        str(value).strip().lower()
        for value in requirements.get("test_values", ["test"])
    }
    if split_field not in df.columns:
        return (
            pd.Series([False] * len(df), index=df.index),
            pd.Series([False] * len(df), index=df.index),
            split_field,
        )
    values = df[split_field].astype(str).str.strip().str.lower()
    return values.isin(train_values), values.isin(test_values), split_field


def compute_train_test_duplicate_overlap_ratio(df: pd.DataFrame, metric: dict) -> dict:
    """Return the fraction of distinct test signatures that are already present in train."""
    requirements = metric.get("input_requirements", {})
    subset_fields = requirements.get("subset_fields") or [
        field for field in df.columns if field != requirements.get("split_field", "split")
    ]
    train_mask, test_mask, split_field = _split_masks(df, metric)
    if not subset_fields or any(field not in df.columns for field in subset_fields):
        return {
            "summary": {
                "split_field": split_field,
                "subset_fields": subset_fields,
                "train_test_duplicate_overlap_ratio": None,
                "reason": "duplicate_signature_fields_missing",
            }
        }
    train_keys = set(
        df.loc[train_mask, subset_fields]
        .drop_duplicates()
        .itertuples(index=False, name=None)
    )
    test_keys = set(
        df.loc[test_mask, subset_fields]
        .drop_duplicates()
        .itertuples(index=False, name=None)
    )
    overlap = train_keys & test_keys
    union = train_keys | test_keys
    return {
        "summary": {
            "split_field": split_field,
            "subset_fields": subset_fields,
            "train_key_count": len(train_keys),
            "test_key_count": len(test_keys),
            "overlap_key_count": len(overlap),
            "train_test_duplicate_overlap_ratio": round(len(overlap) / len(test_keys), 6)
            if test_keys
            else None,
            "train_test_duplicate_jaccard": round(len(overlap) / len(union), 6)
            if union
            else None,
        }
    }


def compute_train_test_identifier_contamination_ratio(df: pd.DataFrame, metric: dict) -> dict:
    """Measure test identifiers already seen in train when entity-disjoint evaluation is required."""
    requirements = metric.get("input_requirements", {})
    identifier_fields = requirements.get("identifier_fields", [])
    entity_disjoint_expected = bool(requirements.get("entity_disjoint_expected", False))
    train_mask, test_mask, split_field = _split_masks(df, metric)

    contaminated = total_test = total_union = 0
    fields = []
    for field in identifier_fields:
        if field not in df.columns:
            fields.append(
                {
                    "field": field,
                    "exists": False,
                    "test_identifier_count": 0,
                    "overlap_identifier_count": 0,
                    "train_test_identifier_overlap_ratio": None,
                    "train_test_identifier_contamination_ratio": None,
                }
            )
            continue
        train_ids = {
            value
            for value in (_normalise(value) for value in df.loc[train_mask, field])
            if value is not None
        }
        test_ids = {
            value
            for value in (_normalise(value) for value in df.loc[test_mask, field])
            if value is not None
        }
        overlap = train_ids & test_ids
        union = train_ids | test_ids
        total_test += len(test_ids)
        total_union += len(union)
        contaminated += len(overlap)
        overlap_ratio = len(overlap) / len(test_ids) if test_ids else None
        fields.append(
            {
                "field": field,
                "exists": True,
                "train_identifier_count": len(train_ids),
                "test_identifier_count": len(test_ids),
                "unique_identifier_count": len(union),
                "overlap_identifier_count": len(overlap),
                "train_test_identifier_overlap_ratio": round(overlap_ratio, 6)
                if overlap_ratio is not None
                else None,
                "train_test_identifier_contamination_ratio": round(overlap_ratio, 6)
                if entity_disjoint_expected and overlap_ratio is not None
                else None,
            }
        )

    overlap_ratio = contaminated / total_test if total_test else None
    return {
        "fields": fields,
        "summary": {
            "split_field": split_field,
            "identifier_field_count": len(identifier_fields),
            "entity_disjoint_expected": entity_disjoint_expected,
            "test_identifier_count": total_test,
            "unique_identifier_count": total_union,
            "overlap_identifier_count": contaminated,
            "train_test_identifier_overlap_ratio": round(overlap_ratio, 6)
            if overlap_ratio is not None
            else None,
            "train_test_identifier_contamination_ratio": round(overlap_ratio, 6)
            if entity_disjoint_expected and overlap_ratio is not None
            else None,
            "reason": None
            if entity_disjoint_expected
            else "entity_disjoint_expectation_required_for_contamination_claim",
        },
    }
