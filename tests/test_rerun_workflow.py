import json
from pathlib import Path
from types import SimpleNamespace

import pytest

import cbr_tests.rerun_workflow as rerun_workflow
from cbr_tests.rerun_workflow import (
    build_run_plan_command,
    outcome_dataset_sha256,
    run_and_compare,
)


def test_build_run_plan_command_uses_normal_runner_and_passes_options(tmp_path):
    repo = tmp_path / "repo"
    command = build_run_plan_command(
        repo_root=repo,
        plan_path=tmp_path / "plan.json",
        dataset_path=tmp_path / "bucket.pcapng",
        output_path=tmp_path / "out.json",
        case_id="bucket-2-overhaul",
        display="quiet",
        workers=2,
        extra_args=["--no-update-field-translation"],
    )

    assert command[1] == str(repo / "run_plan.py")
    assert command[command.index("--case") + 1].endswith("plan.json")
    assert command[command.index("--dataset") + 1].endswith("bucket.pcapng")
    assert command[command.index("--case-id") + 1] == "bucket-2-overhaul"
    assert command[command.index("--workers") + 1] == "2"
    assert command[-1] == "--no-update-field-translation"


def test_outcome_dataset_sha256_reads_normal_run_provenance():
    assert outcome_dataset_sha256(
        {"provenance": {"dataset": {"sha256": "abc123"}}}
    ) == "abc123"
    assert outcome_dataset_sha256({}) is None


def test_run_and_compare_archives_baseline_and_reuses_dataset_digest(tmp_path, monkeypatch):
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "run_plan.py").write_text("# normal runner placeholder\n")

    baseline = tmp_path / "baseline.json"
    baseline.write_text(
        json.dumps(
            {
                "status": "success",
                "case_id": "ad_hoc_case",
                "test_results": {"metric": {"score": 1.0, "status": "pass"}},
            }
        )
    )
    baseline.with_name("baseline_summary.md").write_text("old summary\n")
    plan = tmp_path / "plan.json"
    plan.write_text('{"plan_meta":{"plan_id":"bucket-plan"}}\n')
    dataset = tmp_path / "Bucket_2.pcapng"
    dataset.write_bytes(b"pretend this is a very large packet capture")
    record_dir = tmp_path / "record"

    def fake_run(command, **kwargs):
        if command[0] == "git":
            return SimpleNamespace(returncode=0, stdout="deadbeef\n", stderr="")

        output = Path(command[command.index("--output") + 1])
        output.write_text(
            json.dumps(
                {
                    "status": "success",
                    "case_id": "ad_hoc_case",
                    "test_results": {
                        "metric": {
                            "score": 0.5,
                            "status": "warn",
                            "decision_rule": {"provenance": "framework-default"},
                        }
                    },
                    "provenance": {"dataset": {"sha256": "dataset-from-run"}},
                }
            )
        )
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(rerun_workflow.subprocess, "run", fake_run)
    original_sha256 = rerun_workflow.file_sha256

    def guarded_sha256(path):
        if Path(path).resolve() == dataset.resolve():
            raise AssertionError("dataset should not be hashed a second time")
        return original_sha256(Path(path))

    monkeypatch.setattr(rerun_workflow, "file_sha256", guarded_sha256)

    manifest = run_and_compare(
        repo_root=repo,
        baseline_path=baseline,
        plan_path=plan,
        dataset_path=dataset,
        record_dir=record_dir,
        case_id="ad_hoc_case",
        display="quiet",
    )

    assert (record_dir / "baseline_pre_overhaul.json").exists()
    assert (record_dir / "baseline_summary.md").read_text() == "old summary\n"
    assert (record_dir / "outcome_post_overhaul.json").exists()
    assert (record_dir / "comparison.json").exists()
    assert (record_dir / "comparison.md").exists()
    assert (record_dir / "rerun_manifest.json").exists()

    assert manifest["cbr_tests_commit"] == "deadbeef"
    assert manifest["inputs"]["dataset_sha256"] == "dataset-from-run"
    assert manifest["inputs"]["dataset_sha256_source"] == "post_overhaul_outcome.provenance.dataset.sha256"
    assert manifest["comparison_summary"]["high_impact"] >= 2
    assert manifest["before_status"] == "success"
    assert manifest["after_status"] == "success"

    archived = json.loads((record_dir / "baseline_pre_overhaul.json").read_text())
    original = json.loads(baseline.read_text())
    assert archived == original


def test_run_and_compare_refuses_to_replace_existing_record_without_force(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "run_plan.py").write_text("# placeholder\n")
    baseline = tmp_path / "baseline.json"
    baseline.write_text("{}\n")
    plan = tmp_path / "plan.json"
    plan.write_text("{}\n")
    dataset = tmp_path / "dataset.csv"
    dataset.write_text("a\n1\n")
    record_dir = tmp_path / "record"
    record_dir.mkdir()
    (record_dir / "comparison.json").write_text("{}\n")

    with pytest.raises(FileExistsError):
        run_and_compare(
            repo_root=repo,
            baseline_path=baseline,
            plan_path=plan,
            dataset_path=dataset,
            record_dir=record_dir,
        )
