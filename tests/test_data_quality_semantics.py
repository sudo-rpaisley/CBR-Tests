import pandas as pd

from cbr_tests.metrics.data_quality import compute_duplicate_row_ratio


def test_duplicate_ratio_reports_full_row_basis_when_all_columns_define_identity():
    df = pd.DataFrame({"a": [1, 1, 2], "b": ["x", "x", "y"]})
    result = compute_duplicate_row_ratio(df, {})["summary"]

    assert result["duplicate_definition"] == "full_row"
    assert result["subset_field_names"] == "a, b"
    assert result["duplicate_row_count"] == 1
    assert result["duplicate_row_ratio"] == 1 / 3
    assert result["runnable"] is True


def test_duplicate_ratio_labels_configured_subset_as_signature_not_full_row():
    df = pd.DataFrame({"a": [1, 1, 2], "b": ["x", "different", "y"]})
    metric = {"input_requirements": {"subset_fields": ["a"]}}
    result = compute_duplicate_row_ratio(df, metric)["summary"]

    assert result["duplicate_definition"] == "configured_signature"
    assert result["subset_field_count"] == 1
    assert result["subset_field_names"] == "a"
    assert result["duplicate_row_count"] == 1
    assert result["duplicate_row_ratio"] == 1 / 3


def test_duplicate_ratio_is_not_runnable_without_any_available_identity_fields():
    df = pd.DataFrame(index=range(2))
    result = compute_duplicate_row_ratio(df, {})["summary"]

    assert result["duplicate_definition"] == "no_runnable_signature"
    assert result["duplicate_row_ratio"] is None
    assert result["runnable"] is False
