from datetime import datetime
from types import SimpleNamespace

import pytest

from runner.tui import _field_mapping_choices, _result_lines, apply_tui_fields, build_default_tui_fields, build_result_sections, default_outcome_path, describe_tui_field, detected_max_workers, list_file_browser_entries, save_field_mappings, validate_required_run_args


def _args(**overrides):
    defaults = dict(
        case=None,
        dataset=None,
        output=None,
        case_id="ad_hoc_case",
        display="compact",
        workers=None,
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
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def test_build_default_tui_fields_discovers_case_and_plan_choices(tmp_path):
    (tmp_path / "cases").mkdir()
    (tmp_path / "plans").mkdir()
    (tmp_path / "cases" / "case_a.json").write_text("{}", encoding="utf-8")
    (tmp_path / "plans" / "plan_a.json").write_text("{}", encoding="utf-8")

    fields = build_default_tui_fields(_args(), repo_root=tmp_path)
    case_field = next(field for field in fields if field.name == "case")

    assert case_field.value == "cases/case_a.json"
    assert case_field.choices == ("cases/case_a.json", "plans/plan_a.json")


def test_apply_tui_fields_converts_blank_optional_values_and_worker_count():
    args = _args()
    fields = build_default_tui_fields(args)
    values = {field.name: field for field in fields}
    values["case"].value = "plans/example.json"
    values["workers"].value = "3"
    values["taxonomy_file"].value = ""
    values["taxonomy_strict"].value = True

    updated = apply_tui_fields(args, fields)

    assert updated.case == "plans/example.json"
    assert updated.workers == 3
    assert updated.taxonomy_file is None
    assert updated.taxonomy_strict is True
    assert updated.tui is False


def test_validate_required_run_args_rejects_missing_case():
    with pytest.raises(SystemExit):
        validate_required_run_args(_args(case=None))


def test_dataset_field_uses_file_browser():
    fields = build_default_tui_fields(_args())
    dataset_field = next(field for field in fields if field.name == "dataset")

    assert dataset_field.kind == "file"
    assert dataset_field.label == "Dataset file"
    assert dataset_field.section == "Required inputs"
    assert "Browse" in dataset_field.help


def test_list_file_browser_entries_sorts_directories_first_and_skips_internal_dirs(tmp_path):
    (tmp_path / "z_data.csv").write_text("a,b\n", encoding="utf-8")
    (tmp_path / "datasets").mkdir()
    (tmp_path / "datasets" / "sample.csv").write_text("a,b\n", encoding="utf-8")
    (tmp_path / ".git").mkdir()

    entries = list_file_browser_entries(tmp_path, tmp_path)

    assert [entry.label for entry in entries] == ["datasets/", "z_data.csv"]
    assert entries[0].is_dir is True
    assert entries[1].is_dir is False


def test_describe_tui_field_explains_selected_field_actions():
    fields = build_default_tui_fields(_args())
    dataset_field = next(field for field in fields if field.name == "dataset")

    description = describe_tui_field(dataset_field)

    assert description[0] == "Section: Required inputs"
    assert description[1] == "Selected: Dataset file"
    assert any("CSV/TSV/XLSX/PCAP" in line for line in description)
    assert description[-1] == "How: Press Enter to browse files, or press e to type/paste a path."


def test_worker_field_shows_detected_max_workers():
    fields = build_default_tui_fields(_args())
    worker_field = next(field for field in fields if field.name == "workers")

    description = describe_tui_field(worker_field)

    assert detected_max_workers() >= 1
    assert "detected maximum" in worker_field.help
    assert description[-1] == "How: Press Enter or e to type a value."


def test_post_dry_run_result_lines_offer_run_action():
    lines = _result_lines({"dry_run": True, "status": "ready", "output_path": "out.json", "metrics_total": 2, "skipped_count": 0}, _args())

    assert lines[0] == "Dry run complete"
    assert "r: run now using these settings" in lines
    assert "Enter/m: back to setup menu" in lines


def test_post_dry_run_result_lines_show_attention_for_skips():
    lines = _result_lines({"dry_run": True, "status": "needs_attention", "output_path": "out.json", "metrics_total": 2, "skipped_count": 1}, _args())

    assert "Attention: 1 metric(s) skipped or blocked by missing fields" in lines


def test_default_outcome_path_uses_plan_title_and_timestamp(tmp_path):
    (tmp_path / "plans").mkdir()
    plan = tmp_path / "plans" / "example_plan.json"
    plan.write_text('{"plan_meta": {"name": "DeepSecure Flow Feature Plan"}}', encoding="utf-8")

    value = default_outcome_path(
        "plans/example_plan.json",
        repo_root=tmp_path,
        now=datetime(2026, 8, 26, 10, 45, 30),
    )

    assert value == "outcomes/outcome_deepsecure_flow_feature_plan_2026-08-26_10-45-30.json"


def test_default_outcome_path_uses_referenced_plan_title_for_case(tmp_path):
    (tmp_path / "plans").mkdir()
    (tmp_path / "cases").mkdir()
    (tmp_path / "plans" / "example_plan.json").write_text(
        '{"plan_meta": {"name": "Reference Fidelity Experiment"}}',
        encoding="utf-8",
    )
    (tmp_path / "cases" / "case_example.json").write_text(
        '{"case_id": "case_example", "test_plan": {"path": "../plans/example_plan.json"}}',
        encoding="utf-8",
    )

    value = default_outcome_path(
        "cases/case_example.json",
        repo_root=tmp_path,
        now=datetime(2026, 8, 26, 10, 45, 30),
    )

    assert value == "outcomes/outcome_reference_fidelity_experiment_2026-08-26_10-45-30.json"


def test_default_output_is_auto_managed_unless_explicit(tmp_path):
    (tmp_path / "plans").mkdir()
    (tmp_path / "plans" / "example_plan.json").write_text(
        '{"plan_meta": {"name": "Example Plan"}}',
        encoding="utf-8",
    )

    automatic = build_default_tui_fields(_args(), repo_root=tmp_path)
    explicit = build_default_tui_fields(_args(output="outcomes/custom.json"), repo_root=tmp_path)

    automatic_output = next(field for field in automatic if field.name == "output")
    explicit_output = next(field for field in explicit if field.name == "output")
    assert automatic_output.auto is True
    assert automatic_output.value.startswith("outcomes/outcome_example_plan_")
    assert explicit_output.auto is False
    assert explicit_output.value == "outcomes/custom.json"


def test_report_fields_explain_each_report_format():
    fields = build_default_tui_fields(_args())
    json_report = next(field for field in fields if field.name == "field_translation_report")
    text_report = next(field for field in fields if field.name == "field_translation_text_report")
    markdown_report = next(field for field in fields if field.name == "field_translation_markdown_report")

    assert "Machine-readable" in json_report.help
    assert "Plain-language" in text_report.help
    assert "Markdown" in markdown_report.help


def test_result_sections_include_expandable_human_readable_metric_results(tmp_path):
    output = tmp_path / "outcome.json"
    output.write_text(
        '{"metric_results": [{"metric_id": "m1", "status": "failed", "error": "boom", "elapsed_seconds": 1.25}, {"metric_id": "m2", "status": "success", "elapsed_seconds": 2.0}], "test_results": {"m2": {"summary": {"ratio": 0.95}}}}',
        encoding="utf-8",
    )

    sections = build_result_sections({"dry_run": False, "status": "failed", "output_path": str(output), "metrics_total": 2, "skipped_count": 0})

    assert sections[0].title == "Summary"
    assert sections[1].title == "Human-readable metric results (2)"
    assert "m1 | status=failed | elapsed=1.2s | detail=boom" in sections[1].lines
    assert "m2 | status=success | elapsed=2.0s | ratio=0.95" in sections[1].lines
    assert any(section.title == "Successful metrics (1)" for section in sections)
    assert any(section.title == "Failed metrics (1)" for section in sections)


def test_field_mapping_choices_remove_already_selected_columns():
    choices = _field_mapping_choices(["Src IP", "Dst IP", "Protocol"], {"Source IP": "Src IP", "Destination IP": ""}, "Destination IP")

    assert "" in choices
    assert "Src IP" not in choices
    assert "Dst IP" in choices
    assert "Protocol" in choices


def test_save_field_mappings_updates_test_to_dataset_fields(tmp_path):
    path = tmp_path / "data.field_translation.json"
    save_field_mappings(path, {"Source IP": "Src IP", "Destination IP": "Dst IP"})

    payload = __import__("json").loads(path.read_text(encoding="utf-8"))

    assert payload["test_to_dataset_fields"] == {"Destination IP": "Dst IP", "Source IP": "Src IP"}
