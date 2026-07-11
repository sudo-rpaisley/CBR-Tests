from types import SimpleNamespace

import pytest

from runner.tui import apply_tui_fields, build_default_tui_fields, list_file_browser_entries, validate_required_run_args


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
    assert "browse" in dataset_field.help


def test_list_file_browser_entries_sorts_directories_first_and_skips_internal_dirs(tmp_path):
    (tmp_path / "z_data.csv").write_text("a,b\n", encoding="utf-8")
    (tmp_path / "datasets").mkdir()
    (tmp_path / "datasets" / "sample.csv").write_text("a,b\n", encoding="utf-8")
    (tmp_path / ".git").mkdir()

    entries = list_file_browser_entries(tmp_path, tmp_path)

    assert [entry.label for entry in entries] == ["datasets/", "z_data.csv"]
    assert entries[0].is_dir is True
    assert entries[1].is_dir is False
