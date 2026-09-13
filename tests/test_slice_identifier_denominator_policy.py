import pandas as pd

from runner.metric_catalog import build_metric_catalog
from tests.metrics.dataset_heuristics.protocol_and_network_realism.slice_metadata_integrity.valid_slice_identifier_profile import (
    run_valid_slice_identifier_metric,
)
from tests.metrics.dataset_heuristics.protocol_and_network_realism.slice_metadata_integrity.slice_identifier_consistency_profile import (
    run_slice_identifier_consistency_metric,
)


def test_valid_slice_identifier_excludes_missing_by_default():
    df = pd.DataFrame({"slice": ["embb", "URLLC", "other", None]})
    metric = {
        "_shared_df": df,
        "input_requirements": {"slice_field": "slice"},
        "calculation": {"parameters": {"allowed_slice_ids": ["embb", "urllc"]}},
    }

    ok, payload = run_valid_slice_identifier_metric(None, metric)
    result = payload["test_results"]["valid_slice_identifier_profile"]

    assert ok is True
    assert result["checked_slice_count"] == 3
    assert result["valid_slice_count"] == 2
    assert result["invalid_slice_count"] == 1
    assert result["missing_slice_count"] == 1
    assert result["valid_slice_identifier_ratio"] == 0.666667
    assert result["missing_policy"] == "exclude_missing"


def test_valid_slice_identifier_can_reproduce_legacy_count_invalid_policy():
    df = pd.DataFrame({"slice": ["embb", "URLLC", "other", None]})
    metric = {
        "_shared_df": df,
        "input_requirements": {"slice_field": "slice"},
        "calculation": {
            "parameters": {
                "allowed_slice_ids": ["embb", "urllc"],
                "missing_policy": "count_invalid",
            }
        },
    }

    ok, payload = run_valid_slice_identifier_metric(None, metric)
    result = payload["test_results"]["valid_slice_identifier_profile"]

    assert ok is True
    assert result["checked_slice_count"] == 4
    assert result["valid_slice_identifier_ratio"] == 0.5


def test_slice_consistency_excludes_unmatched_and_missing_rows_by_default():
    df = pd.DataFrame(
        {
            "service": ["video", "video", "voice", "video", "video"],
            "slice": ["embb", "urllc", "urllc", "embb", None],
        }
    )
    metric = {
        "_shared_df": df,
        "input_requirements": {"slice_field": "slice", "context_fields": ["service"]},
        "calculation": {
            "parameters": {
                "rules": [
                    {
                        "when_field": "service",
                        "operator": "equals",
                        "value": "video",
                        "expected_slice_ids": ["embb"],
                    }
                ]
            }
        },
    }

    ok, payload = run_slice_identifier_consistency_metric(None, metric)
    result = payload["test_results"]["slice_identifier_consistency_profile"]

    assert ok is True
    assert result["checked_row_count"] == 3
    assert result["consistent_row_count"] == 2
    assert result["inconsistent_row_count"] == 1
    assert result["missing_slice_count"] == 1
    assert result["unmatched_context_row_count"] == 1
    assert result["slice_identifier_consistency_ratio"] == 0.666667
    assert result["missing_policy"] == "exclude_missing"


def test_new_plan_templates_do_not_inherit_legacy_slice_denominators():
    entries = build_metric_catalog(
        metric_ids=[
            "valid_slice_identifier_profile",
            "slice_identifier_consistency_profile",
        ]
    )
    by_id = {entry["metric_id"]: entry for entry in entries}

    for metric_id in by_id:
        template = by_id[metric_id]["template"]
        assert template is not None
        assert template["calculation"]["parameters"]["missing_policy"] == "exclude_missing"
