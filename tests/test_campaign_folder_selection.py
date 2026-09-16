from __future__ import annotations

import json
from pathlib import Path

import pytest

from runner.campaign_tui import _resolve_matrix_selection


def _write_batch_for_plan_folder(root: Path, *, manifest_name: str = "matrix_batch.json") -> tuple[Path, Path]:
    plans = root / "plans"
    plan_dir = plans / "matrix_batch_plans"
    plan_dir.mkdir(parents=True)
    for index in range(1, 3):
        (plan_dir / f"{index:02d}_plan.json").write_text("{}", encoding="utf-8")

    manifest = plans / manifest_name
    payload = {
        "schema_version": 1,
        "batch_meta": {
            "batch_id": "matrix",
            "name": "Matrix",
            "metric_policy": "common_across_all_datasets",
            "comparison_mode": "candidate_reference_matrix",
        },
        "jobs": [
            {
                "job_id": f"matrix-{index:02d}",
                "dataset_path": f"datasets/candidate-{index}.pcapng",
                "reference_dataset_path": "datasets/reference.pcapng",
                "plan_path": f"plans/matrix_batch_plans/{index:02d}_plan.json",
            }
            for index in range(1, 3)
        ],
    }
    manifest.write_text(json.dumps(payload), encoding="utf-8")
    return manifest, plan_dir


def test_matrix_selection_accepts_generated_plan_folder(tmp_path):
    manifest, plan_dir = _write_batch_for_plan_folder(tmp_path)

    selected = _resolve_matrix_selection(tmp_path, str(plan_dir.relative_to(tmp_path)))

    assert selected == manifest.resolve()


def test_plan_folder_resolution_does_not_depend_on_manifest_filename(tmp_path):
    manifest, plan_dir = _write_batch_for_plan_folder(tmp_path, manifest_name="final-pcap-comparisons.json")

    selected = _resolve_matrix_selection(tmp_path, str(plan_dir.relative_to(tmp_path)))

    assert selected == manifest.resolve()


def test_matrix_selection_still_accepts_batch_manifest_directly(tmp_path):
    manifest, _ = _write_batch_for_plan_folder(tmp_path)

    selected = _resolve_matrix_selection(tmp_path, str(manifest.relative_to(tmp_path)))

    assert selected == manifest.resolve()


def test_orphan_plan_folder_is_rejected_instead_of_guessing_matrix_metadata(tmp_path):
    orphan = tmp_path / "plans" / "orphan_batch_plans"
    orphan.mkdir(parents=True)
    (orphan / "01_plan.json").write_text("{}", encoding="utf-8")

    with pytest.raises(ValueError, match="No batch manifest was found"):
        _resolve_matrix_selection(tmp_path, str(orphan.relative_to(tmp_path)))
