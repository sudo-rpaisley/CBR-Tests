from __future__ import annotations

from argparse import Namespace
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

import run_batch
from runner.batch_state import (
    batch_manifest_fingerprint,
    build_initial_batch_state,
    default_batch_state_path,
    load_batch_state,
    replace_result,
    result_map,
    validate_resume_state,
    write_batch_state,
)


def _batch_payload(tmp_path: Path) -> tuple[Path, dict]:
    dataset_a = tmp_path / "candidate-a.csv"
    dataset_b = tmp_path / "candidate-b.csv"
    plan_a = tmp_path / "plan-a.json"
    plan_b = tmp_path / "plan-b.json"
    dataset_a.write_text("a\n1\n", encoding="utf-8")
    dataset_b.write_text("a\n2\n", encoding="utf-8")
    plan_a.write_text("{}\n", encoding="utf-8")
    plan_b.write_text("{}\n", encoding="utf-8")
    batch = {
        "schema_version": 1,
        "batch_meta": {
            "batch_id": "resume-test",
            "name": "Resume test",
            "dataset_count": 2,
            "reference_dataset_count": 0,
            "metric_policy": "common_across_all_datasets",
        },
        "output_directory": str(tmp_path / "outcomes" / "resume-test"),
        "jobs": [
            {
                "job_id": "resume-test-01",
                "dataset_path": str(dataset_a),
                "plan_path": str(plan_a),
            },
            {
                "job_id": "resume-test-02",
                "dataset_path": str(dataset_b),
                "plan_path": str(plan_b),
            },
        ],
    }
    batch_path = tmp_path / "batch.json"
    batch_path.write_text(json.dumps(batch), encoding="utf-8")
    return batch_path, batch


def _args(batch_path: Path, *, resume: bool = False, retry_failed: bool = False) -> Namespace:
    return Namespace(
        batch=str(batch_path),
        output_dir=None,
        workers=1,
        display="quiet",
        force_output=False,
        fail_fast=False,
        resume=resume,
        retry_failed=retry_failed,
        no_update_field_translation=True,
        yes_field_translation_sidecar=False,
        no_dataset_summary=True,
        refresh_dataset_summary=False,
    )


def test_batch_state_round_trip_and_manifest_validation(tmp_path):
    batch_path, batch = _batch_payload(tmp_path)
    output_dir = tmp_path / "outcomes" / "resume-test"
    state = build_initial_batch_state(
        batch_path=batch_path,
        batch=batch,
        output_dir=output_dir,
        run_timestamp="2026-09-02_12-30-00",
    )
    state_path = default_batch_state_path(output_dir)
    write_batch_state(state_path, state)

    loaded = load_batch_state(state_path)
    validate_resume_state(loaded, batch_path=batch_path, batch=batch)
    assert loaded["batch_manifest_sha256"] == batch_manifest_fingerprint(batch)
    assert loaded["status"] == "running"

    result = {"job_id": "resume-test-01", "outcome_status": "success"}
    replace_result(loaded, result)
    replace_result(loaded, {**result, "attempt": 2})
    assert result_map(loaded)["resume-test-01"]["attempt"] == 2
    assert len(loaded["results"]) == 1


def test_resume_validation_rejects_changed_manifest(tmp_path):
    batch_path, batch = _batch_payload(tmp_path)
    state = build_initial_batch_state(
        batch_path=batch_path,
        batch=batch,
        output_dir=tmp_path / "outcomes",
        run_timestamp="2026-09-02_12-30-00",
    )
    changed = json.loads(json.dumps(batch))
    changed["jobs"][0]["dataset_path"] = str(tmp_path / "different.csv")

    with pytest.raises(ValueError, match="manifest changed"):
        validate_resume_state(state, batch_path=batch_path, batch=changed)


def test_interrupted_batch_resumes_without_rerunning_completed_job(monkeypatch, tmp_path):
    batch_path, _batch = _batch_payload(tmp_path)
    monkeypatch.setattr(run_batch, "__file__", str(tmp_path / "run_batch.py"))

    first_args = _args(batch_path)
    monkeypatch.setattr(run_batch, "parse_args", lambda: first_args)
    first_commands: list[list[str]] = []

    def first_run(command, cwd, check, env):
        first_commands.append(command)
        if len(first_commands) == 2:
            raise KeyboardInterrupt
        output = Path(command[command.index("--output") + 1])
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps({"status": "success"}), encoding="utf-8")
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(run_batch.subprocess, "run", first_run)
    assert run_batch.main() == 130

    output_dir = tmp_path / "outcomes" / "resume-test"
    checkpoint = load_batch_state(default_batch_state_path(output_dir))
    assert checkpoint["status"] == "interrupted"
    assert len(checkpoint["results"]) == 1
    assert checkpoint["results"][0]["job_id"] == "resume-test-01"
    assert checkpoint["current_job"]["job_id"] == "resume-test-02"
    assert checkpoint["current_job"]["attempt"] == 1

    second_args = _args(batch_path, resume=True)
    monkeypatch.setattr(run_batch, "parse_args", lambda: second_args)
    resumed_commands: list[list[str]] = []

    def resumed_run(command, cwd, check, env):
        resumed_commands.append(command)
        output = Path(command[command.index("--output") + 1])
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps({"status": "success"}), encoding="utf-8")
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(run_batch.subprocess, "run", resumed_run)
    assert run_batch.main() == 0
    assert len(resumed_commands) == 1
    resumed_output = Path(resumed_commands[0][resumed_commands[0].index("--output") + 1])
    assert "retry02" in resumed_output.name

    completed = load_batch_state(default_batch_state_path(output_dir))
    assert completed["status"] == "completed"
    assert len(completed["results"]) == 2
    assert {result["job_id"] for result in completed["results"]} == {
        "resume-test-01",
        "resume-test-02",
    }
