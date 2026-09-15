from __future__ import annotations

import json
from importlib import metadata
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

import run_plan as run_plan_module
from cbr_tests.metrics.data_quality import compute_missing_value_ratio
from cbr_tests.metrics.statistical import (
    compute_energy_distance,
    compute_ks_feature_divergence,
    compute_maximum_mean_discrepancy,
    compute_wasserstein_feature_distance,
)
from runner.dispatch import (
    run_distance_correlation_metric,
    run_pearson_metric,
    run_spearman_metric,
    run_tabular_metric,
)
from runner.provenance import software_manifest


@pytest.mark.parametrize(
    ("compute_metric", "result_key"),
    [
        (compute_ks_feature_divergence, "ks_statistic"),
        (compute_wasserstein_feature_distance, "wasserstein_distance"),
        (compute_energy_distance, "energy_distance"),
        (compute_maximum_mean_discrepancy, "maximum_mean_discrepancy"),
    ],
)
def test_all_ordered_half_drift_metrics_sample_the_true_halves(compute_metric, result_key):
    """Large traces must be split before sampling, for every ordered-half metric."""
    dataframe = pd.DataFrame({"feature": [0.0] * 2000 + [100.0] * 2000})
    metric = {
        "input_requirements": {"candidate_fields": ["feature"]},
        "calculation": {
            "parameters": {
                "minimum_sample_size": 2,
                "max_sample_size": 1000,
            }
        },
    }

    result = compute_metric(dataframe, metric)
    field = result["fields"][0]

    assert field["population_a_count"] == 2000
    assert field["population_b_count"] == 2000
    assert field["sample_a_count"] == 1000
    assert field["sample_b_count"] == 1000
    assert field[result_key] is not None
    assert field[result_key] > 0.0
    assert (
        result["summary"]["sampling_policy"]
        == "split_full_usable_sequence_then_evenly_sample_each_half"
    )


def test_numpy_version_is_recorded_in_software_provenance():
    software = software_manifest()

    assert "numpy" in software["dependencies"]
    assert software["dependencies"]["numpy"] == metadata.version("numpy")


def test_shared_dataframe_remains_unchanged_across_representative_metrics():
    """Metric dispatch may share a dataframe, but metrics must treat it as read-only."""
    dataframe = pd.DataFrame(
        {
            "left": ["1", "2", "3", "4"],
            "right": ["4", "3", "2", "1"],
            "label": ["x", None, "x", "y"],
        }
    )
    original = dataframe.copy(deep=True)

    def unexpected_loader(_path: Path):
        raise AssertionError("shared dataframe path should not reload the dataset")

    dependency_metric = {
        "input_requirements": {
            "candidate_fields": ["left", "right"],
            "minimum_runnable_fields": 2,
        },
        "calculation": {"parameters": {"max_sample_size": 4}},
    }

    pearson_ok, _ = run_pearson_metric(
        Path("unused.csv"), dependency_metric, unexpected_loader, shared_df=dataframe
    )
    spearman_ok, _ = run_spearman_metric(
        Path("unused.csv"), dependency_metric, unexpected_loader, shared_df=dataframe
    )
    distance_ok, _ = run_distance_correlation_metric(
        Path("unused.csv"), dependency_metric, unexpected_loader, shared_df=dataframe
    )
    ordinary_ok, _ = run_tabular_metric(
        Path("unused.csv"),
        {"input_requirements": {"candidate_fields": ["label"]}},
        unexpected_loader,
        dataframe,
        "missing_value_ratio",
        compute_missing_value_ratio,
    )

    assert pearson_ok is True
    assert spearman_ok is True
    assert distance_ok is True
    assert ordinary_ok is True
    pd.testing.assert_frame_equal(dataframe, original, check_dtype=True)


def _write_tiny_plan_and_dataset(tmp_path: Path) -> tuple[Path, Path]:
    dataset = tmp_path / "dataset.csv"
    dataset.write_text("value\n1\n2\n3\n", encoding="utf-8")

    plan = {
        "plan_meta": {
            "plan_id": "timing-regression-plan",
            "name": "Timing Regression Plan",
            "version": "1.0.0",
        },
        "applicability": {
            "dataset_formats": ["csv"],
            "requires_numeric_fields": False,
            "minimum_numeric_fields": 0,
        },
        "execution_policy": {
            "fail_fast": True,
            "allow_skips": False,
            "sample_mode": "full",
        },
        "metrics": [
            {
                "metric_id": "column_quality_profile",
                "label": "Column Quality Profile",
                "taxonomy_path": ["test", "column_quality"],
                "enabled": True,
                "input_requirements": {"candidate_fields": ["value"]},
                "calculation": {
                    "method": "column_quality_profile",
                    "parameters": {},
                },
                "field_requirements": {
                    "required": ["value"],
                    "optional": [],
                },
            }
        ],
    }
    plan_file = tmp_path / "plan.json"
    plan_file.write_text(json.dumps(plan), encoding="utf-8")
    return plan_file, dataset


def _run_args(plan_file: Path, dataset: Path, output: Path) -> SimpleNamespace:
    return SimpleNamespace(
        case=str(plan_file),
        dataset=str(dataset),
        output=str(output),
        case_id="timing-regression-case",
        field_translation=None,
        taxonomy_file=None,
        taxonomy_strict=False,
        force_output=False,
        display="quiet",
        no_update_field_translation=True,
        yes_field_translation_sidecar=False,
        field_translation_dry_run=False,
        field_translation_report=None,
        field_translation_text_report=None,
        field_translation_markdown_report=None,
        dataset_summary=False,
        refresh_dataset_summary=False,
        workers=1,
    )


def _assert_complete_phase_timings(outcome: dict) -> None:
    timings = outcome["provenance"]["phase_timings_seconds"]
    expected = {
        "run_context",
        "field_translation_and_preflight",
        "provenance_hashing",
        "dataset_loading",
        "dataset_summary",
        "metric_handler_setup",
        "metric_execution",
        "total_before_output",
    }

    assert expected.issubset(timings)
    for name in expected:
        assert isinstance(timings[name], (int, float))
        assert timings[name] >= 0.0
    assert timings["total_before_output"] >= timings["metric_execution"]


def test_successful_run_persists_complete_phase_timings(tmp_path: Path):
    plan_file, dataset = _write_tiny_plan_and_dataset(tmp_path)
    output = tmp_path / "outcome.json"

    result = run_plan_module.run_once(_run_args(plan_file, dataset, output))

    assert result["status"] == "success"
    outcome = json.loads(output.read_text(encoding="utf-8"))
    _assert_complete_phase_timings(outcome)


def test_serial_fail_fast_republishes_completed_phase_timings(tmp_path: Path, monkeypatch):
    """The early fail-fast write must be replaced after the execution timer closes."""
    plan_file, dataset = _write_tiny_plan_and_dataset(tmp_path)
    output = tmp_path / "failed-outcome.json"

    def failing_handlers(_shared_df, _loader, _translation):
        return {
            "column_quality_profile": lambda _dataset, _metric: (
                False,
                {"error": "deliberate regression-test failure"},
            )
        }

    monkeypatch.setattr(run_plan_module, "build_metric_handlers", failing_handlers)

    result = run_plan_module.run_once(_run_args(plan_file, dataset, output))

    assert result["status"] == "failed"
    outcome = json.loads(output.read_text(encoding="utf-8"))
    assert outcome["status"] == "failed"
    _assert_complete_phase_timings(outcome)
