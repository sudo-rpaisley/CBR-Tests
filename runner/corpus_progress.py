"""Observable, persisted progress for recursive GAN campaigns.

Source of truth: immutable per-job outcomes and atomic batch/campaign states.
Progress JSON is a cheap derived snapshot, safe to read from another SSH
session. ETA is sample-based and remains unavailable until observed timings
support it; no invented speed or completion time is reported.
"""
from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import threading
import time

from runner.corpus_perturbation import atomic_json


TERMINAL = {"completed", "prepared", "failed"}
STAGES = {
    "preparing": "Preparing metric plan",
    "extracting": "Extracting training flows",
    "training": "Training CTGAN",
    "generating": "Generating GAN captures",
    "validating": "Validating generated captures",
    "evaluating": "Evaluating comparison jobs",
    "finished": "Finished held-out capture",
    "failed": "Failed",
}


def _utc() -> datetime:
    return datetime.now(timezone.utc)


def _parse(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)
    except (TypeError, ValueError):
        return None


def _read(path: Path) -> dict:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError):
        return {}


def _seconds(start: str | None, end: str | None) -> float | None:
    left, right = _parse(start), _parse(end)
    return max(0.0, (right - left).total_seconds()) if left and right else None


def _average(values: list[float]) -> float | None:
    values = [x for x in values if x >= 0]
    return sum(values) / len(values) if values else None


def _case_duration(case: dict) -> float | None:
    if case.get("status") not in TERMINAL:
        return None
    attempts = case.get("attempts", [])
    if not attempts:
        return None
    # Failed attempts are not representative of a full case ETA.
    if case.get("status") not in {"completed", "prepared"}:
        return None
    attempt = attempts[-1]
    return _seconds(attempt.get("started_at"), attempt.get("finished_at"))


def _case_batch_progress(case: dict) -> dict:
    """Read only the active case's small experiment and batch-state files."""
    folder = case.get("output_dir")
    empty = {"completed": 0, "with_results": 0, "failed": 0, "active_job": None,
             "seed": None, "current_percentage": None, "job_samples_seconds": []}
    if not folder:
        return empty
    experiment = _read(Path(folder) / "experiment.json")
    for run in experiment.get("runs", []):
        state = _read(Path(run["batch_output_dir"]) / "batch_state.json")
        lookup = {
            str(Path(cap["dataset_path"]).resolve()): cap
            for cap in run.get("captures", [])
        }
        results = state.get("results", [])
        empty["completed"] += len(results)
        for result in results:
            outcome_path = Path(str(result.get("output_path", "")))
            if outcome_path.is_file():
                empty["with_results"] += 1
            if result.get("process_return_code") not in (None, 0) or result.get(
                "outcome_status"
            ) in {"failed", "error", "cancelled"}:
                empty["failed"] += 1
            duration = _seconds(result.get("started_at"), result.get("finished_at"))
            if duration is not None and duration > 0:
                empty["job_samples_seconds"].append(duration)
        current = state.get("current_job")
        if isinstance(current, dict) and current.get("job_id"):
            capture = lookup.get(str(Path(current.get("dataset_path", "")).resolve()), {})
            empty["active_job"] = current
            empty["seed"] = run.get("seed")
            empty["current_percentage"] = capture.get("requested_pct")
    return empty


def build_snapshot(campaign: dict, *, output_dir: Path, now: datetime | None = None) -> dict:
    """Calculate campaign, group, held-out and comparison-job progress/ETAs."""
    now = now or _utc()
    cases = campaign.get("cases", [])
    settings = campaign.get("settings", {})
    levels = len(settings.get("percentages", ()))
    seeds = len(settings.get("seeds", ()))
    per_case = levels * seeds
    total_jobs = len(cases) * per_case
    completed = [case for case in cases if case.get("status") in {"completed", "prepared"}]
    failed = [case for case in cases if case.get("status") == "failed"]
    active = next((case for case in cases if case.get("status") == "running"), None)
    group_name = str(active.get("folder")) if active else ""
    group = [case for case in cases if str(case.get("folder")) == group_name] if active else []
    group_names = list(dict.fromkeys(str(c.get("folder")) for c in cases))
    completed_groups = sum(
        all(c.get("status") in TERMINAL for c in cases if str(c.get("folder")) == name)
        for name in group_names
    )
    group_index = (group_names.index(group_name) + 1) if active else None
    stage_data = (
        _read(Path(active["output_dir"]) / "stage.json")
        if active and active.get("output_dir") else {}
    )
    # Read actual packet-level progress from the streaming generator,
    # independently of the elapsed training / comparison-job counters.
    if active and stage_data.get("stage") == "generating":
        details = stage_data.setdefault("details", {})
        seed = details.get("seed")
        if seed is not None:
            progress_file = (
                Path(active["output_dir"]) / f"seed_{seed}" /
                "captures" / "generation_progress.json"
            )
            generator_status = _read(progress_file)
            # Do not show stale packet counters from the previous level.
            if generator_status and (
                generator_status.get("percentage") in (None, details.get("percentage"))
            ):
                details.update(generator_status)
    batch = _case_batch_progress(active) if active else {
        "completed": 0, "with_results": 0, "failed": 0,
        "active_job": None, "seed": None, "current_percentage": None,
        "job_samples_seconds": [],
    }
    completed_prior = sum(int(c.get("finished_job_count", per_case)) for c in completed)
    failures_prior = sum(int(c.get("finished_job_count", 0)) for c in failed)
    evaluated = min(total_jobs, completed_prior + failures_prior + batch["completed"])
    all_measured = sum(int(c.get("finished_results_count", per_case)) for c in completed)
    all_measured += sum(int(c.get("finished_results_count", 0)) for c in failed)
    all_measured += batch["with_results"]
    durations = [v for c in completed if (v := _case_duration(c)) is not None]
    rate = _average(durations)
    remaining_case_units = max(0, len(cases) - len(completed) - len(failed))
    campaign_eta = round(max(0, rate * remaining_case_units), 1) if rate is not None else None
    group_complete = sum(c.get("status") in TERMINAL for c in group)
    group_duration = [v for c in group if (v := _case_duration(c)) is not None]
    group_rate = _average(group_duration) or rate
    group_eta = (
        round(max(0, group_rate * (len(group) - group_complete)), 1)
        if group_rate is not None else None
    )
    case_start = (active.get("attempts") or [{}])[-1].get("started_at") if active else None
    case_elapsed = _seconds(case_start, now.isoformat()) if case_start else None
    # A case may have completed a few comparison jobs but not training. Use
    # observed complete-case timings for its remaining part, when available.
    case_eta = max(0, round(rate - case_elapsed, 1)) if rate is not None and case_elapsed is not None else None

    current_job = batch["active_job"]
    job_elapsed = _seconds(current_job.get("started_at"), now.isoformat()) if current_job else None
    job_average = _average(batch["job_samples_seconds"])
    job_eta = max(0, round(job_average - job_elapsed, 1)) if job_average and job_elapsed is not None else None

    status = campaign.get("status", "unknown")
    return {
        "schema_version": 1,
        "campaign_id": campaign.get("campaign_id"),
        "updated_at": now.isoformat(),
        "status": status,
        "phase": campaign.get("phase", "evaluating"),
        "abort_reason": campaign.get("abort_reason"),
        "elapsed_seconds": _seconds(campaign.get("started_at"), now.isoformat()),
        "campaign": {
            "total_groups": len(group_names),
            "completed_groups": completed_groups,
            "total_cases": len(cases),
            "completed_cases": len(completed),
            "failed_cases": len(failed),
            "evaluated_jobs": evaluated,
            "available_result_jobs": all_measured,
            "total_jobs": total_jobs,
            "eta_seconds": campaign_eta if status == "running" else 0,
            "eta_sample_count": len(durations),
        },
        "group": {
            "name": group_name,
            "index": group_index,
            "total_groups": len(group_names),
            "total_cases": len(group),
            "completed_cases": group_complete,
            "failed_cases": sum(c.get("status") == "failed" for c in group),
            "evaluated_jobs": sum(
                int(c.get("finished_job_count", per_case))
                for c in group if c.get("status") in TERMINAL
            ) + (batch["completed"] if active else 0),
            "total_jobs": len(group) * per_case,
            "eta_seconds": group_eta,
        } if active else None,
        "case": {
            "name": active.get("held_out_name"),
            "folder": group_name,
            "case_id": active.get("case_id"),
            "group_position": group.index(active) + 1,
            "campaign_position": cases.index(active) + 1,
            "stage": stage_data.get("stage", "preparing"),
            "stage_details": stage_data.get("details", {}),
            "started_at": case_start,
            "elapsed_seconds": case_elapsed,
            "eta_seconds": case_eta,
            "evaluated_jobs": batch["completed"],
            "available_result_jobs": batch["with_results"],
            "failed_jobs": batch["failed"],
            "total_jobs": per_case,
            "seed": batch["seed"] if batch["seed"] is not None else stage_data.get("details", {}).get("seed"),
        } if active else None,
        "job": {
            "name": Path(current_job.get("dataset_path", "")).name,
            "index": current_job.get("job_index"),
            "total_in_seed": levels,
            "seed": batch["seed"],
            "perturbation_pct": batch["current_percentage"],
            "started_at": current_job.get("started_at"),
            "elapsed_seconds": job_elapsed,
            "eta_seconds": job_eta,
            "observed_job_samples": len(batch["job_samples_seconds"]),
        } if current_job else None,
        "log_path": str(output_dir.with_suffix(".log")),
        "archive_path": str(output_dir),
    }


def publish_snapshot(output_dir: Path) -> dict:
    output_dir = Path(output_dir)
    campaign = _read(output_dir / "campaign.json")
    if not campaign:
        return {}
    snapshot = build_snapshot(campaign, output_dir=output_dir)
    atomic_json(output_dir / "progress.json", snapshot)
    return snapshot


def monitor_progress_file(output_dir: Path, stop: threading.Event, *, interval: float = 1.5) -> None:
    """Periodically copy authoritative job checkpoints to a tiny status JSON."""
    while not stop.is_set():
        try:
            publish_snapshot(output_dir)
        except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError):
            # The underlying experiment proceeds if a checkpoint is briefly
            # mid-write or a progress snapshot cannot be assembled.
            pass
        stop.wait(interval)
    try:
        publish_snapshot(output_dir)
    except (OSError, ValueError, TypeError, KeyError):
        pass


def format_duration(seconds: float | None) -> str:
    if seconds is None:
        return "estimating (needs completed samples)"
    value = max(0, round(seconds))
    if value >= 86400:
        return f"{value // 86400}d {(value % 86400) // 3600}h"
    if value >= 3600:
        return f"{value // 3600}h {(value % 3600) // 60}m"
    return f"{value // 60}m {value % 60:02d}s"


def progress_line(done: int, total: int, width: int = 26) -> str:
    width = max(3, min(60, width))
    ratio = max(0, min(1, done / total)) if total else 0
    filled = round(width * ratio)
    return f"[{'#' * filled}{'-' * (width - filled)}] {done}/{total} ({100 * ratio:.1f}%)"
