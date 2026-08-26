from __future__ import annotations

import sys
from pathlib import Path

from runner.run_plan_helpers import parse_run_plan_args
from runner.tui import build_default_tui_fields


def test_dataset_summary_cli_defaults_enabled(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["run_plan.py", "--case", "plans/deepsecure_plan.json"])
    args = parse_run_plan_args()
    assert args.dataset_summary is True
    assert args.refresh_dataset_summary is False


def test_dataset_summary_cli_can_disable_and_force_refresh(monkeypatch):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_plan.py",
            "--case",
            "plans/deepsecure_plan.json",
            "--no-dataset-summary",
            "--refresh-dataset-summary",
        ],
    )
    args = parse_run_plan_args()
    assert args.dataset_summary is False
    assert args.refresh_dataset_summary is True


def test_tui_exposes_dataset_summary_controls(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["run_plan.py", "--case", "plans/deepsecure_plan.json"])
    args = parse_run_plan_args()
    fields = build_default_tui_fields(args, repo_root=Path.cwd())
    by_name = {field.name: field for field in fields}
    assert by_name["dataset_summary"].value is True
    assert by_name["dataset_summary"].section == "Dataset summary"
    assert by_name["refresh_dataset_summary"].value is False
