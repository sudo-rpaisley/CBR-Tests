from __future__ import annotations

from types import SimpleNamespace

from runner.friendly_tui import (
    batch_review_lines,
    batch_visible_field_names,
    build_friendly_single_fields,
    single_review_lines,
    single_setup_issues,
    visible_single_fields,
)
from runner.tui import apply_tui_fields


def _args(**overrides):
    values = dict(
        case=None,
        dataset=None,
        output=None,
        case_id="ad_hoc_case",
        display="interactive",
        workers=None,
        experiment_mode=False,
        dataset_summary=True,
        refresh_dataset_summary=False,
        taxonomy_file=None,
        taxonomy_strict=False,
        field_translation=None,
        no_update_field_translation=False,
        yes_field_translation_sidecar=False,
        field_translation_dry_run=False,
        field_translation_report=None,
        field_translation_text_report=None,
        field_translation_markdown_report=None,
        tui=True,
    )
    values.update(overrides)
    return SimpleNamespace(**values)


def test_friendly_single_defaults_to_only_essential_fields(tmp_path):
    (tmp_path / "cases").mkdir()
    (tmp_path / "cases" / "case.json").write_text(
        '{"case_id":"case-1","dataset":{"path":"sample.csv"},"test_plan":{"path":"../plans/plan.json"}}',
        encoding="utf-8",
    )
    fields = build_friendly_single_fields(_args(), repo_root=tmp_path)

    visible = visible_single_fields(fields, show_advanced=False)

    assert [field.name for field in visible] == [
        "case",
        "dataset",
        "experiment_mode",
        "field_translation_dry_run",
        "display",
    ]
    assert len(visible_single_fields(fields, show_advanced=True)) > len(visible)


def test_friendly_single_exposes_strict_experiment_mode_without_changing_runner_contract(tmp_path):
    fields = build_friendly_single_fields(_args(experiment_mode=True), repo_root=tmp_path)
    experiment = next(field for field in fields if field.name == "experiment_mode")

    assert experiment.value is True
    assert "canonical final-experiment contract" in experiment.help

    args = apply_tui_fields(_args(), fields)
    assert args.experiment_mode is True


def test_direct_plan_requires_dataset_before_review(tmp_path):
    (tmp_path / "plans").mkdir()
    (tmp_path / "plans" / "plan.json").write_text(
        '{"plan_meta":{"plan_id":"p1","name":"Plan one"},"metrics":[]}',
        encoding="utf-8",
    )
    fields = build_friendly_single_fields(_args(case="plans/plan.json"), repo_root=tmp_path)

    assert "Choose a dataset when running a plan directly." in single_setup_issues(fields, tmp_path)

    dataset = tmp_path / "sample.csv"
    dataset.write_text("a\n1\n", encoding="utf-8")
    next(field for field in fields if field.name == "dataset").value = "sample.csv"
    assert single_setup_issues(fields, tmp_path) == []


def test_case_can_supply_its_own_dataset(tmp_path):
    (tmp_path / "cases").mkdir()
    (tmp_path / "cases" / "case.json").write_text(
        '{"case_id":"c1","dataset":{"path":"sample.csv"},"test_plan":{"path":"plan.json"}}',
        encoding="utf-8",
    )
    fields = build_friendly_single_fields(_args(case="cases/case.json"), repo_root=tmp_path)

    assert single_setup_issues(fields, tmp_path) == []
    assert "Dataset: Provided by case" in single_review_lines(fields, tmp_path)


def test_batch_defaults_to_essential_fields_and_keeps_advanced_options_available():
    essentials = batch_visible_field_names(False)
    advanced = batch_visible_field_names(True)

    assert essentials == ("name", "datasets", "references", "metric_policy")
    assert "workers" not in essentials
    assert "workers" in advanced
    assert "fail_fast" in advanced


def test_batch_review_summarises_comparison_matrix(tmp_path):
    candidate = str(tmp_path / "candidate.csv")
    reference = str(tmp_path / "reference.csv")
    lines = batch_review_lines(
        {
            "name": "Final matrix",
            "datasets": [candidate],
            "references": [reference],
            "per_dataset_metrics": False,
            "workers": None,
            "display": "compact",
        }
    )

    assert "Batch: Final matrix" in lines
    assert "Jobs: 1" in lines
    assert "Metric policy: Common metrics across every job" in lines
    assert "Workers: Automatic" in lines
