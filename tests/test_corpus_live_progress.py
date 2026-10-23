"""Progress/ETA tests for long-running recursive GAN corpus experiments."""
from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path

from runner import corpus_progress
from runner.corpus_monitor_tui import monitor_lines, read_progress
from runner.corpus_perturbation import atomic_json


NOW = datetime(2026, 10, 23, 12, 0, tzinfo=timezone.utc)


def _source(tmp_path: Path):
    output = tmp_path / "gan-run"
    complete = {
        "case_id": "Group/a.pcap", "held_out_name": "a.pcap",
        "folder": "Group", "status": "completed", "finished_job_count": 3,
        "finished_results_count": 3, "attempts": [{
            "started_at": "2026-10-23T11:00:00+00:00",
            "finished_at": "2026-10-23T11:20:00+00:00",
            "status": "completed",
        }],
    }
    active_dir = output / "cases" / "Group" / "b.pcap" / "attempt_001"
    active = {
        "case_id": "Group/b.pcap", "held_out_name": "b.pcap",
        "folder": "Group", "status": "running",
        "output_dir": str(active_dir),
        "attempts": [{
            "started_at": "2026-10-23T11:50:00+00:00",
            "status": "running",
            "path": str(active_dir),
        }],
    }
    pending = {
        "case_id": "Other/c.pcap", "held_out_name": "c.pcap",
        "folder": "Other", "status": "pending", "attempts": [],
    }
    campaign = {
        "campaign_id": "test", "status": "running",
        "started_at": "2026-10-23T11:00:00+00:00",
        "settings": {"seeds": [42], "percentages": [0, 10, 20]},
        "cases": [complete, active, pending],
    }
    eval_dir = active_dir / "seed_42" / "evaluation"
    first_outcome = eval_dir / "result0.json"
    atomic_json(first_outcome, {"metric_results": [{"metric_id": "m"}]})
    atomic_json(eval_dir / "batch_state.json", {
        "results": [{
            "job_id": "0", "output_path": str(first_outcome),
            "process_return_code": 0, "outcome_status": "success",
            "started_at": "2026-10-23T11:58:40+00:00",
            "finished_at": "2026-10-23T11:59:20+00:00",
        }],
        "current_job": {
            "job_id": "1", "job_index": 2,
            "dataset_path": str(active_dir / "seed_42" / "captures" / "gan_010.pcap"),
            "output_path": str(eval_dir / "result1.json"),
            "started_at": "2026-10-23T11:59:40+00:00",
        },
    })
    atomic_json(active_dir / "experiment.json", {
        "runs": [{
            "seed": 42, "batch_output_dir": str(eval_dir),
            "captures": [{
                "dataset_path": str(active_dir / "seed_42" / "captures" / "gan_010.pcap"),
                "requested_pct": 10, "actual_packet_pct": 10,
            }],
        }],
    })
    atomic_json(active_dir / "stage.json", {
        "stage": "evaluating", "details": {"seed": 42},
    })
    atomic_json(output / "campaign.json", campaign)
    return output, campaign


def test_progress_shows_actual_campaign_group_case_and_job_with_observed_eta(tmp_path):
    output, campaign = _source(tmp_path)
    status = corpus_progress.build_snapshot(campaign, output_dir=output, now=NOW)
    assert status["campaign"]["total_groups"] == 2
    assert status["campaign"]["completed_groups"] == 0
    assert status["campaign"]["total_cases"] == 3
    assert status["campaign"]["total_jobs"] == 9
    assert status["campaign"]["evaluated_jobs"] == 4
    assert status["campaign"]["available_result_jobs"] == 4
    assert status["campaign"]["completed_cases"] == 1
    assert status["campaign"]["eta_seconds"] == 2400
    assert status["campaign"]["eta_sample_count"] == 1
    assert status["group"]["name"] == "Group"
    assert status["group"]["index"] == 1
    assert status["case"]["group_position"] == 2
    assert status["case"]["campaign_position"] == 2
    assert status["group"]["total_cases"] == 2
    assert status["group"]["evaluated_jobs"] == 4
    assert status["group"]["eta_seconds"] == 1200
    assert status["case"]["stage"] == "evaluating"
    assert status["case"]["evaluated_jobs"] == 1
    assert status["case"]["available_result_jobs"] == 1
    assert status["case"]["eta_seconds"] == 600
    assert status["job"]["index"] == 2
    assert status["job"]["perturbation_pct"] == 10
    assert status["job"]["elapsed_seconds"] == 20
    assert status["job"]["eta_seconds"] == 20
    assert status["job"]["observed_job_samples"] == 1
    lines = monitor_lines(status)
    assert any("CAMPAIGN:" in line and "4/9" in line for line in lines)
    assert any("GROUP " in line and "Group" in line for line in lines)
    assert any("HELD-OUT PCAP" in line for line in lines)
    assert any("CURRENT JOB:" in line for line in lines)
    assert any("Finished job outcomes available" in line for line in lines)


def test_eta_is_unknown_until_completed_samples_exist(tmp_path):
    output, campaign = _source(tmp_path)
    campaign["cases"] = campaign["cases"][1:]
    snapshot = corpus_progress.build_snapshot(campaign, output_dir=output, now=NOW)
    assert snapshot["campaign"]["eta_seconds"] is None
    assert snapshot["group"]["eta_seconds"] is None
    assert snapshot["case"]["eta_seconds"] is None
    assert "estimating" in corpus_progress.format_duration(None)
    assert "estimating" in "\n".join(monitor_lines(snapshot))


def test_persisted_progress_can_be_read_without_running_experiment(tmp_path):
    output, _ = _source(tmp_path)
    result = corpus_progress.publish_snapshot(output)
    assert result["campaign"]["total_cases"] == 3
    assert read_progress(output)["campaign_id"] == "test"
    assert corpus_progress.progress_line(1, 4, width=4) == "[#---] 1/4 (25.0%)"


def test_terminal_progress_has_no_running_case_or_stale_eta(tmp_path):
    output, campaign = _source(tmp_path)
    for case in campaign["cases"]:
        case["status"] = "failed" if case["held_out_name"] == "b.pcap" else "completed"
    campaign["status"] = "needs_attention"
    snapshot = corpus_progress.build_snapshot(campaign, output_dir=output, now=NOW)
    assert snapshot["case"] is None
    assert snapshot["job"] is None
    assert snapshot["campaign"]["failed_cases"] == 1
    assert snapshot["campaign"]["eta_seconds"] == 0

def test_monitor_reports_final_export_and_stale_worker_heartbeat(tmp_path):
    output, campaign = _source(tmp_path)
    campaign["phase"] = "exporting"
    for case in campaign["cases"]:
        case["status"] = "completed"
    snapshot = corpus_progress.build_snapshot(campaign, output_dir=output, now=NOW)
    assert snapshot["phase"] == "exporting"
    snapshot["updated_at"] = "2000-01-01T00:00:00+00:00"
    messages = "\n".join(monitor_lines(snapshot))
    assert "Finalising full CSV/JSONL exports" in messages
    assert "heartbeat" in messages
