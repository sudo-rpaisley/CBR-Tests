import json
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def _write_plan(path: Path):
    path.write_text(
        json.dumps(
            {
                "plan_meta": {"plan_id": "field-translation-test", "name": "Field Translation Test"},
                "metrics": [
                    {
                        "metric_id": "missing_value_ratio",
                        "taxonomy_path": ["dataset_metrics", "quality", "missing_value_ratio"],
                        "input_requirements": {"candidate_fields": ["Canonical A"]},
                        "field_requirements": {"required": ["Canonical A"], "optional": []},
                    }
                ],
            }
        ),
        encoding="utf-8",
    )


def test_field_translation_dry_run_creates_sidecar_and_report(tmp_path):
    dataset = tmp_path / "dataset.csv"
    dataset.write_text("a\n1\n", encoding="utf-8")
    plan = tmp_path / "plan.json"
    report = tmp_path / "report.json"
    text_report = tmp_path / "report.txt"
    markdown_report = tmp_path / "report.md"
    output = tmp_path / "out.json"
    _write_plan(plan)

    result = subprocess.run(
        [
            sys.executable,
            str(REPO_ROOT / "run_plan.py"),
            "--case",
            str(plan),
            "--dataset",
            str(dataset),
            "--output",
            str(output),
            "--field-translation-dry-run",
            "--field-translation-report",
            str(report),
            "--field-translation-text-report",
            str(text_report),
            "--field-translation-markdown-report",
            str(markdown_report),
        ],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        check=True,
    )

    sidecar = tmp_path / "dataset.field_translation.json"
    sidecar_payload = json.loads(sidecar.read_text(encoding="utf-8"))
    report_payload = json.loads(report.read_text(encoding="utf-8"))
    text_report_body = text_report.read_text(encoding="utf-8")
    markdown_report_body = markdown_report.read_text(encoding="utf-8")
    assert sidecar_payload["schema_version"] == 1
    assert sidecar_payload["test_to_dataset_fields"] == {"Canonical A": ""}
    assert report_payload["sidecar_status"] == "created"
    assert report_payload["unused_dataset_columns"] == ["a"]
    assert report_payload["skipped_metrics"] == [
        {"metric_id": "missing_value_ratio", "reason": "missing_field_mappings", "missing_fields": ["Canonical A"]}
    ]
    assert "[SKIPPED] missing_value_ratio" in text_report_body
    assert "| `missing_value_ratio` | skipped | Canonical A | - |" in markdown_report_body
    assert "Field translation report" in result.stdout
    assert "Dry run complete" in result.stdout
    assert not output.exists()


def test_no_update_field_translation_does_not_create_sidecar(tmp_path):
    dataset = tmp_path / "dataset.csv"
    dataset.write_text("a\n1\n", encoding="utf-8")
    plan = tmp_path / "plan.json"
    output = tmp_path / "out.json"
    _write_plan(plan)

    subprocess.run(
        [
            sys.executable,
            str(REPO_ROOT / "run_plan.py"),
            "--case",
            str(plan),
            "--dataset",
            str(dataset),
            "--output",
            str(output),
            "--field-translation-dry-run",
            "--no-update-field-translation",
        ],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        check=True,
    )

    assert not (tmp_path / "dataset.field_translation.json").exists()
