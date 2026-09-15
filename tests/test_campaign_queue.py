from __future__ import annotations

import argparse
import json
from pathlib import Path

import pytest

from run_campaign import _build_batch_command, _preflight_campaign
from runner.campaign import build_campaign, load_campaign, write_campaign
from runner.unified_tui import _menu_items


def _write_batch(path: Path, batch_id: str, job_count: int) -> Path:
    payload = {
        "schema_version": 1,
        "batch_meta": {
            "batch_id": batch_id,
            "name": batch_id.replace("-", " ").title(),
            "metric_policy": "common_across_all_datasets",
            "comparison_mode": "candidate_reference_matrix",
        },
        "jobs": [
            {
                "job_id": f"{batch_id}-{index:02d}",
                "dataset_path": f"datasets/candidate-{index}.csv",
                "plan_path": f"plans/{batch_id}-{index:02d}.json",
            }
            for index in range(1, job_count + 1)
        ],
    }
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_build_campaign_preserves_matrix_order_and_counts_jobs(tmp_path):
    first = _write_batch(tmp_path / "first_batch.json", "first", 3)
    second = _write_batch(tmp_path / "second_batch.json", "second", 5)
    third = _write_batch(tmp_path / "third_batch.json", "third", 2)

    campaign = build_campaign(
        name="Final comparison campaign",
        batch_paths=[first, second, third],
        repo_root=tmp_path,
    )

    assert campaign["campaign_meta"]["matrix_count"] == 3
    assert campaign["campaign_meta"]["job_count"] == 10
    assert [matrix["batch_id"] for matrix in campaign["matrices"]] == ["first", "second", "third"]
    assert [matrix["position"] for matrix in campaign["matrices"]] == [1, 2, 3]
    assert len({matrix["queue_id"] for matrix in campaign["matrices"]}) == 3


def test_same_batch_can_be_queued_more_than_once_with_distinct_queue_ids(tmp_path):
    batch = _write_batch(tmp_path / "repeat_batch.json", "repeat", 2)
    campaign = build_campaign(
        name="Repeated matrix",
        batch_paths=[batch, batch],
        repo_root=tmp_path,
    )

    assert [matrix["batch_id"] for matrix in campaign["matrices"]] == ["repeat", "repeat"]
    assert campaign["matrices"][0]["queue_id"] != campaign["matrices"][1]["queue_id"]


def test_campaign_manifest_round_trip_and_duplicate_queue_guard(tmp_path):
    batch = _write_batch(tmp_path / "one_batch.json", "one", 1)
    payload = build_campaign(name="One", batch_paths=[batch], repo_root=tmp_path)
    manifest = tmp_path / "campaign.json"
    write_campaign(manifest, payload)

    loaded = load_campaign(manifest)
    assert loaded["campaign_meta"]["campaign_id"] == "one"

    loaded["matrices"].append(dict(loaded["matrices"][0]))
    manifest.write_text(json.dumps(loaded), encoding="utf-8")
    with pytest.raises(ValueError, match="Duplicate campaign queue_id"):
        load_campaign(manifest)


def test_campaign_preflight_checks_every_matrix_before_outputs_start(tmp_path):
    first = _write_batch(tmp_path / "first_batch.json", "first", 1)
    second = _write_batch(tmp_path / "second_batch.json", "second", 1)
    campaign = build_campaign(
        name="Preflight",
        batch_paths=[first, second],
        repo_root=tmp_path,
    )

    assert _preflight_campaign(tmp_path, campaign["matrices"], experiment_mode=False) == (2, 0)

    second.write_text("{}", encoding="utf-8")
    with pytest.raises(ValueError, match="Not a supported CBR-Tests batch manifest"):
        _preflight_campaign(tmp_path, campaign["matrices"], experiment_mode=False)


def test_campaign_batch_command_passes_resume_retry_and_experiment_controls(tmp_path):
    args = argparse.Namespace(
        display="quiet",
        workers=2,
        experiment_mode=True,
        batch_fail_fast=False,
        no_update_field_translation=True,
        yes_field_translation_sidecar=False,
        no_dataset_summary=True,
        refresh_dataset_summary=False,
    )
    command = _build_batch_command(
        repo_root=tmp_path,
        batch_path=tmp_path / "matrix.json",
        output_dir=tmp_path / "out",
        args=args,
        resume=True,
        retry_failed=True,
    )

    assert "--experiment-mode" in command
    assert "--resume" in command
    assert "--retry-failed" in command
    assert "--no-update-field-translation" in command
    assert "--no-dataset-summary" in command
    assert command[command.index("--workers") + 1] == "2"


def test_toolbox_exposes_campaign_builder_and_runner():
    keys = [item.key for item in _menu_items()]
    assert "campaign_run" in keys
    assert "campaign_build" in keys
    assert keys.index("batch") < keys.index("campaign_run") < keys.index("build_plan")
    assert keys.index("build_plan") < keys.index("campaign_build") < keys.index("field_mapping")
