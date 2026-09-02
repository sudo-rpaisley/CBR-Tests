from __future__ import annotations

from argparse import Namespace
from pathlib import Path
from types import SimpleNamespace

import pytest

from runner import tui_batch, unified_tui


def test_comparison_job_count_excludes_self_comparisons(tmp_path):
    candidate_a = tmp_path / "candidate-a.csv"
    candidate_b = tmp_path / "candidate-b.csv"
    reference = tmp_path / "reference.csv"

    count = tui_batch.comparison_job_count(
        [str(candidate_a), str(candidate_b)],
        [str(candidate_a), str(reference)],
    )

    assert count == 3


def test_build_batch_spec_normalises_and_validates():
    spec = tui_batch.build_batch_spec(
        name="  Experiment A  ",
        datasets=["a.csv", "a.csv", "b.csv"],
        references=["r.csv", "r.csv"],
        workers=2,
        display="compact",
    )

    assert spec["name"] == "Experiment A"
    assert spec["datasets"] == ["a.csv", "b.csv"]
    assert spec["references"] == ["r.csv"]
    assert spec["workers"] == 2


@pytest.mark.parametrize(
    "kwargs, message",
    [
        ({"name": "", "datasets": ["a.csv"]}, "Batch name is required"),
        ({"name": "x", "datasets": []}, "Select at least one candidate"),
        ({"name": "x", "datasets": ["a.csv"], "workers": 0}, "Worker count must be at least 1"),
    ],
)
def test_build_batch_spec_rejects_invalid_values(kwargs, message):
    with pytest.raises(ValueError, match=message):
        tui_batch.build_batch_spec(**kwargs)


def test_unified_tui_routes_batch_mode(monkeypatch):
    args = Namespace(tui=True)
    monkeypatch.setattr(unified_tui.curses, "wrapper", lambda fn: "batch")
    monkeypatch.setattr(
        unified_tui,
        "launch_batch_tui",
        lambda incoming, repo_root=None: {"name": "matrix", "datasets": ["a.csv"], "references": ["r.csv"]},
    )

    returned = unified_tui.launch_unified_tui(args, repo_root=Path("."))

    assert returned is args
    assert args.tui is False
    assert args.tui_batch_spec["name"] == "matrix"


def test_unified_tui_routes_single_mode(monkeypatch):
    args = Namespace(tui=True)
    expected = Namespace(tui=False, case="plan.json")
    monkeypatch.setattr(unified_tui.curses, "wrapper", lambda fn: "single")
    monkeypatch.setattr(unified_tui, "launch_tui", lambda incoming, repo_root=None: expected)

    assert unified_tui.launch_unified_tui(args) is expected


def test_execute_batch_spec_uses_existing_batch_pipeline(monkeypatch, tmp_path):
    dataset = tmp_path / "candidate.csv"
    reference = tmp_path / "reference.csv"
    dataset.write_text("a\n1\n", encoding="utf-8")
    reference.write_text("a\n1\n", encoding="utf-8")

    created = {}

    def fake_create_batch(**kwargs):
        created.update(kwargs)
        kwargs["output_path"].parent.mkdir(parents=True, exist_ok=True)
        kwargs["output_path"].write_text("{}\n", encoding="utf-8")
        return kwargs["output_path"]

    import create_plan

    monkeypatch.setattr(create_plan, "_create_batch", fake_create_batch)
    monkeypatch.setattr(create_plan, "_slug", lambda value: "experiment")
    monkeypatch.setattr(
        create_plan,
        "_deduplicate_dataset_values",
        lambda values: [Path(value).expanduser().resolve() for value in dict.fromkeys(values)],
    )

    commands = []

    def fake_run(command, cwd, check):
        commands.append((command, cwd, check))
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(tui_batch.subprocess, "run", fake_run)

    result = tui_batch.execute_batch_spec(
        {
            "name": "Experiment",
            "datasets": [str(dataset)],
            "references": [str(reference)],
            "workers": 3,
            "display": "compact",
            "force": True,
            "dataset_summary": False,
            "refresh_dataset_summary": True,
            "fail_fast": True,
        },
        repo_root=tmp_path,
    )

    assert created["dataset_paths"] == [dataset.resolve()]
    assert created["reference_dataset_paths"] == [reference.resolve()]
    assert created["per_dataset_metrics"] is False
    assert result["status"] == "completed"
    assert result["job_count"] == 1

    command = commands[0][0]
    assert str(tmp_path / "run_batch.py") in command
    assert "--batch" in command
    assert "--workers" in command and "3" in command
    assert "--yes-field-translation-sidecar" in command
    assert "--no-dataset-summary" in command
    assert "--refresh-dataset-summary" in command
    assert "--fail-fast" in command
