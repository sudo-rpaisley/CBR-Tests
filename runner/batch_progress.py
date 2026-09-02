from __future__ import annotations

import os
from collections.abc import Mapping


_BATCH_ENV_PREFIX = "CBR_BATCH_"


def _as_int(value: object, default: int = 0) -> int:
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return default


def render_batch_progress(completed: int, total: int, width: int = 30) -> str:
    """Return a compact completion bar for a sequential dataset batch."""
    total = max(1, int(total))
    completed = min(max(0, int(completed)), total)
    width = max(10, int(width))
    filled = int(width * completed / total)
    bar = "#" * filled + "-" * (width - filled)
    percent = int((completed / total) * 100)
    return f"[{bar}] {percent:3d}% ({completed}/{total} complete)"


def build_batch_progress_lines(
    *,
    batch_name: str,
    batch_id: str,
    current_job: int,
    total_jobs: int,
    completed_jobs: int,
    failed_jobs: int,
    candidate_name: str,
    reference_name: str | None = None,
) -> list[str]:
    """Build the persistent batch context shown above a child run's metric progress."""
    total_jobs = max(1, int(total_jobs))
    completed_jobs = min(max(0, int(completed_jobs)), total_jobs)
    failed_jobs = min(max(0, int(failed_jobs)), completed_jobs)
    current_job = min(max(1, int(current_job)), total_jobs)
    successful_jobs = max(0, completed_jobs - failed_jobs)
    waiting_jobs = max(0, total_jobs - completed_jobs - 1)
    label = str(batch_name or batch_id or "batch")

    lines = [
        f"Batch: {label} ({batch_id})" if batch_id and batch_id != label else f"Batch: {label}",
        (
            f"Batch progress: {render_batch_progress(completed_jobs, total_jobs)} "
            f"| running job {current_job}/{total_jobs}"
        ),
        (
            f"Batch results so far: {successful_jobs} successful | {failed_jobs} needing attention "
            f"| {waiting_jobs} waiting after current"
        ),
    ]
    comparison = f"Candidate: {candidate_name}"
    if reference_name:
        comparison += f" | Reference: {reference_name}"
    lines.append(comparison)
    return lines


def build_batch_child_environment(
    base_environment: Mapping[str, str] | None,
    *,
    batch_name: str,
    batch_id: str,
    current_job: int,
    total_jobs: int,
    completed_jobs: int,
    failed_jobs: int,
    candidate_name: str,
    reference_name: str | None = None,
) -> dict[str, str]:
    """Return an environment carrying batch progress into a child run_plan process."""
    environment = dict(base_environment if base_environment is not None else os.environ)
    values = {
        "NAME": batch_name,
        "ID": batch_id,
        "JOB_INDEX": current_job,
        "JOB_TOTAL": total_jobs,
        "COMPLETED": completed_jobs,
        "FAILED": failed_jobs,
        "CANDIDATE": candidate_name,
        "REFERENCE": reference_name or "",
    }
    for key, value in values.items():
        environment[f"{_BATCH_ENV_PREFIX}{key}"] = str(value)
    return environment


def batch_progress_lines_from_environment(
    environment: Mapping[str, str] | None = None,
) -> list[str]:
    """Read optional batch context from the environment for a child run dashboard."""
    environment = environment if environment is not None else os.environ
    total_jobs = _as_int(environment.get(f"{_BATCH_ENV_PREFIX}JOB_TOTAL"), 0)
    if total_jobs <= 0:
        return []

    return build_batch_progress_lines(
        batch_name=str(environment.get(f"{_BATCH_ENV_PREFIX}NAME") or ""),
        batch_id=str(environment.get(f"{_BATCH_ENV_PREFIX}ID") or ""),
        current_job=_as_int(environment.get(f"{_BATCH_ENV_PREFIX}JOB_INDEX"), 1),
        total_jobs=total_jobs,
        completed_jobs=_as_int(environment.get(f"{_BATCH_ENV_PREFIX}COMPLETED"), 0),
        failed_jobs=_as_int(environment.get(f"{_BATCH_ENV_PREFIX}FAILED"), 0),
        candidate_name=str(environment.get(f"{_BATCH_ENV_PREFIX}CANDIDATE") or "unknown"),
        reference_name=str(environment.get(f"{_BATCH_ENV_PREFIX}REFERENCE") or "") or None,
    )
