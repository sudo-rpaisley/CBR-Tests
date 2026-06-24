import pandas as pd

from tests.data_quality_profile import compute_duplicate_row_ratio, compute_missing_value_ratio
from tests.spearman_profile import compute_spearman_profile, validate_spearman_candidate_fields


def test_compute_missing_value_ratio_uses_candidate_fields():
    df = pd.DataFrame({
        "a": [1, None, 3],
        "b": [None, None, 3],
        "ignored": [None, None, None],
    })
    metric = {"input_requirements": {"candidate_fields": ["a", "b", "missing"]}}

    result = compute_missing_value_ratio(df, metric)

    assert result["summary"] == {
        "row_count": 3,
        "field_count": 2,
        "total_cells": 6,
        "missing_cells": 3,
        "missing_value_ratio": 0.5,
    }
    assert [field["field"] for field in result["fields"]] == ["a", "b"]


def test_compute_duplicate_row_ratio_counts_repeated_rows_after_first():
    df = pd.DataFrame({
        "src": ["a", "a", "b", "b", "b"],
        "dst": ["x", "x", "y", "y", "z"],
        "payload": [1, 2, 3, 4, 5],
    })
    metric = {"input_requirements": {"subset_fields": ["src", "dst"]}}

    result = compute_duplicate_row_ratio(df, metric)

    assert result["summary"]["duplicate_row_count"] == 2
    assert result["summary"]["duplicate_group_count"] == 2
    assert result["summary"]["duplicate_row_ratio"] == 0.4


def test_compute_spearman_profile_reports_rank_correlation():
    df = pd.DataFrame({
        "x": [1, 2, 3, 4],
        "y": [10, 20, 30, 40],
        "constant": [7, 7, 7, 7],
    })

    validation, runnable_fields, validated_df = validate_spearman_candidate_fields(df, ["x", "y", "constant", "missing"])
    result = compute_spearman_profile(validated_df, runnable_fields)

    assert runnable_fields == ["x", "y"]
    assert validation[0]["usable_for_spearman"] is True
    assert validation[2]["reason"] == "constant_column"
    assert validation[3]["reason"] == "missing_column"
    assert result["summary"]["pair_count"] == 1
    assert result["summary"]["mean_absolute_correlation"] == 1.0
    assert result["matrix"]["x"]["y"] == 1.0
