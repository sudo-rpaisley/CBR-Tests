from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys

from cbr_tests.plan_migration import migrate_plan_to_canonical_ids
from runner.experiment_contract import FINAL_EXPERIMENT_CONTRACT


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_run_plan_experiment_mode_writes_contract_to_provenance(tmp_path: Path):
    source_plan = json.loads(
        (REPO_ROOT / "examples/quickstart/plan.json").read_text(encoding="utf-8")
    )
    plan, changes = migrate_plan_to_canonical_ids(source_plan)
    assert changes
    plan["execution_policy"]["allow_skips"] = False

    plan_path = tmp_path / "canonical_plan.json"
    output_path = tmp_path / "outcome.json"
    plan_path.write_text(json.dumps(plan, indent=2) + "\n", encoding="utf-8")

    completed = subprocess.run(
        [
            sys.executable,
            str(REPO_ROOT / "run_plan.py"),
            "--case",
            str(plan_path),
            "--dataset",
            str(REPO_ROOT / "examples/quickstart/sample.csv"),
            "--output",
            str(output_path),
            "--case-id",
            "experiment_mode_integration",
            "--experiment-mode",
            "--workers",
            "1",
            "--display",
            "quiet",
            "--no-update-field-translation",
            "--no-dataset-summary",
        ],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stdout + "\n" + completed.stderr
    outcome = json.loads(output_path.read_text(encoding="utf-8"))
    assert outcome["status"] == "success"
    contract = outcome["provenance"]["experiment_contract"]
    assert contract["contract"] == FINAL_EXPERIMENT_CONTRACT
    assert contract["canonical_metric_ids"] is True
    assert contract["compatibility_only_profiles"] is False
    assert contract["sample_mode"] == "full"
    assert contract["allow_skips"] is False


def test_run_plan_experiment_mode_rejects_historical_quickstart_plan(tmp_path: Path):
    output_path = tmp_path / "should_not_exist.json"

    completed = subprocess.run(
        [
            sys.executable,
            str(REPO_ROOT / "run_plan.py"),
            "--case",
            str(REPO_ROOT / "examples/quickstart/plan.json"),
            "--dataset",
            str(REPO_ROOT / "examples/quickstart/sample.csv"),
            "--output",
            str(output_path),
            "--case-id",
            "experiment_mode_rejection",
            "--experiment-mode",
            "--workers",
            "1",
            "--display",
            "quiet",
            "--no-update-field-translation",
            "--no-dataset-summary",
        ],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert completed.returncode != 0
    combined = completed.stdout + "\n" + completed.stderr
    assert "Final experiment plans must use canonical metric IDs" in combined
    assert not output_path.exists()
