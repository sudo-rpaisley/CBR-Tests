from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import subprocess
import sys

from runner.batch_progress import (
    build_batch_child_environment,
    build_batch_progress_lines,
    render_batch_progress,
)
from runner.batch_reports import write_comparison_reports


BATCH_SCHEMA_VERSION = 1
ATTENTION_STATUSES = {"failed", "error", "cancelled", "not_written"}


def _slug(value: str) -> str:
    value = re.sub(r"[^A-Za-z0-9]+", "_", value).strip("_").lower()
    return value or "dataset"


def _resolve_repo_path(repo_root: Path, value: str) -> Path:
    path = Path(value).expanduser()
    return path.resolve() if path.is_absolute() else (repo_root / path).resolve()


def load_batch(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError("Batch manifest must be a JSON object.")
    if payload.get("schema_version") != BATCH_SCHEMA_VERSION:
        raise ValueError(
            f"Unsupported batch schema version: {payload.get('schema_version')!r}; "
            f"expected {BATCH_SCHEMA_VERSION}."
        )
    meta = payload.get("batch_meta")
    if not isinstance(meta, dict) or not str(meta.get("batch_id", "")).strip():
        raise ValueError("Batch manifest must include batch_meta.batch_id.")
    jobs = payload.get("jobs")
    if not isinstance(jobs, list) or not jobs:
        raise ValueError("Batch manifest must include a non-empty jobs list.")
    for index, job in enumerate(jobs):
        if not isinstance(job, dict):
            raise ValueError(f"jobs[{index}] must be an object.")
        for field in ("job_id", "dataset_path", "plan_path"):
            if not str(job.get(field, "")).strip():
                raise ValueError(f"jobs[{index}].{field} is required.")
    return payload


def _read_outcome_status(path: Path) -> str:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return "unknown"
    return str(payload.get("status") or "unknown")


def _write_batch_summary(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
        handle.write("\n")
    return path


def _result_needs_attention(result: dict) -> bool:
    return (
        int(result.get("process_return_code", 0)) != 0
        or str(result.get("outcome_status", "unknown")) in ATTENTION_STATUSES
    )


def _print_batch_position(
    *,
    meta: dict,
    current_job: int,
    total_jobs: int,
    results: list[dict],
    candidate_name: str,
    reference_name: str | None,
) -> None:
    failed_count = sum(1 for result in results if _result_needs_attention(result))
    lines = build_batch_progress_lines(
        batch_name=str(meta.get("name") or meta["batch_id"]),
        batch_id=str(meta["batch_id"]),
        current_job=current_job,
        total_jobs=total_jobs,
        completed_jobs=len(results),
        failed_jobs=failed_count,
        candidate_name=candidate_name,
        reference_name=reference_name,
    )
    for line in lines[1:3]:
        print(line)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a multi-dataset CBR-Tests batch sequentially."
    )
    parser.add_argument("--batch", required=True, help="Batch manifest created by create_plan.py")
    parser.add_argument(
        "--output-dir",
        help="Override the batch output directory stored in the manifest",
    )
    parser.add_argument("--workers", type=int, default=None, help="Worker count passed to each dataset run")
    parser.add_argument(
        "--display",
        choices=("compact", "full", "quiet", "interactive"),
        default="compact",
        help="Display mode passed to each dataset run",
    )
    parser.add_argument(
        "--force-output",
        action="store_true",
        help="Allow replacement if a generated outcome path already exists",
    )
    parser.add_argument(
        "--fail-fast",
        action="store_true",
        help="Stop the dataset batch after the first process/run failure",
    )
    parser.add_argument(
        "--no-update-field-translation",
        action="store_true",
        help="Do not create or update field-translation sidecars during batch execution",
    )
    parser.add_argument(
        "--yes-field-translation-sidecar",
        action="store_true",
        help="Permit sidecar creation/update without an extra prompt for each dataset",
    )
    parser.add_argument(
        "--no-dataset-summary",
        action="store_true",
        help="Suppress dataset summary sidecars for every job",
    )
    parser.add_argument(
        "--refresh-dataset-summary",
        action="store_true",
        help="Refresh dataset summary sidecars for every job",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    repo_root = Path(__file__).resolve().parent
    batch_path = Path(args.batch).expanduser().resolve()
    batch = load_batch(batch_path)
    meta = batch["batch_meta"]
    jobs = batch["jobs"]
    total_jobs = len(jobs)

    output_value = args.output_dir or batch.get("output_directory") or str(
        Path("outcomes") / str(meta["batch_id"])
    )
    output_dir = _resolve_repo_path(repo_root, str(output_value))
    output_dir.mkdir(parents=True, exist_ok=True)

    batch_started_at = datetime.now(timezone.utc)
    timestamp = batch_started_at.strftime("%Y-%m-%d_%H-%M-%S")
    results: list[dict] = []

    print("=" * 88)
    print(f"Batch: {meta.get('name', meta['batch_id'])} ({meta['batch_id']})")
    print(f"Jobs: {total_jobs}")
    print(f"Candidate datasets: {meta.get('dataset_count', total_jobs)}")
    if meta.get("reference_dataset_count"):
        print(f"Reference datasets: {meta.get('reference_dataset_count')}")
    print(f"Metric policy: {meta.get('metric_policy', 'unspecified')}")
    print("Execution: sequential")
    print(f"Outputs: {output_dir}")
    print(f"Batch progress: {render_batch_progress(0, total_jobs)}")
    print("=" * 88)

    for index, job in enumerate(jobs, start=1):
        dataset_path = _resolve_repo_path(repo_root, str(job["dataset_path"]))
        plan_path = _resolve_repo_path(repo_root, str(job["plan_path"]))
        dataset_slug = _slug(dataset_path.stem)
        reference_value = job.get("reference_dataset_path")
        reference_path = _resolve_repo_path(repo_root, str(reference_value)) if reference_value else None
        reference_slug = f"_vs_{_slug(reference_path.stem)}" if reference_path is not None else ""
        output_path = output_dir / f"outcome_{index:02d}_{dataset_slug}{reference_slug}_{timestamp}.json"

        print()
        print("-" * 88)
        _print_batch_position(
            meta=meta,
            current_job=index,
            total_jobs=total_jobs,
            results=results,
            candidate_name=dataset_path.name,
            reference_name=reference_path.name if reference_path is not None else None,
        )
        print(f"Current job: {index}/{total_jobs} — {dataset_path.name}")
        if reference_path is not None:
            print(f"Reference: {reference_path.name}")
        print(f"Plan: {plan_path}")
        print(f"Output: {output_path}")
        print("-" * 88)

        command = [
            sys.executable,
            str(repo_root / "run_plan.py"),
            "--case",
            str(plan_path),
            "--dataset",
            str(dataset_path),
            "--output",
            str(output_path),
            "--case-id",
            str(job["job_id"]),
            "--display",
            args.display,
        ]
        if args.workers is not None:
            command.extend(["--workers", str(args.workers)])
        if args.force_output:
            command.append("--force-output")
        if args.no_update_field_translation:
            command.append("--no-update-field-translation")
        if args.yes_field_translation_sidecar:
            command.append("--yes-field-translation-sidecar")
        if args.no_dataset_summary:
            command.append("--no-dataset-summary")
        if args.refresh_dataset_summary:
            command.append("--refresh-dataset-summary")

        failed_before_current = sum(1 for result in results if _result_needs_attention(result))
        child_environment = build_batch_child_environment(
            os.environ,
            batch_name=str(meta.get("name") or meta["batch_id"]),
            batch_id=str(meta["batch_id"]),
            current_job=index,
            total_jobs=total_jobs,
            completed_jobs=len(results),
            failed_jobs=failed_before_current,
            candidate_name=dataset_path.name,
            reference_name=reference_path.name if reference_path is not None else None,
        )

        started_at = datetime.now(timezone.utc)
        completed = subprocess.run(
            command,
            cwd=repo_root,
            check=False,
            env=child_environment,
        )
        finished_at = datetime.now(timezone.utc)
        outcome_status = _read_outcome_status(output_path) if output_path.exists() else "not_written"
        result = {
            "job_id": job["job_id"],
            "dataset_path": str(dataset_path),
            "reference_dataset_path": str(reference_path) if reference_path is not None else None,
            "plan_path": str(plan_path),
            "output_path": str(output_path),
            "process_return_code": completed.returncode,
            "outcome_status": outcome_status,
            "started_at": started_at.isoformat(),
            "finished_at": finished_at.isoformat(),
        }
        results.append(result)
        failed_count = sum(1 for item in results if _result_needs_attention(item))
        successful_count = len(results) - failed_count

        print(
            f"Batch progress: {render_batch_progress(len(results), total_jobs)} "
            f"| {successful_count} successful | {failed_count} needing attention"
        )

        if completed.returncode != 0:
            print(f"Batch job process failed with return code {completed.returncode}.")
            if args.fail_fast:
                print("Batch fail-fast is enabled; remaining datasets will not be started.")
                break
        elif outcome_status in {"failed", "error", "cancelled"}:
            print(f"Dataset run completed with outcome status: {outcome_status}")
            if args.fail_fast:
                print("Batch fail-fast is enabled; remaining datasets will not be started.")
                break
        else:
            print(f"Dataset run completed with outcome status: {outcome_status}")

    batch_finished_at = datetime.now(timezone.utc)
    failed_jobs = [result for result in results if _result_needs_attention(result)]

    comparison_reports: dict = {}
    comparison_report_error: str | None = None
    try:
        comparison_reports = write_comparison_reports(
            output_dir=output_dir,
            timestamp=timestamp,
            batch_meta=meta,
            results=results,
        )
    except Exception as exc:  # Supplementary reporting must never hide the authoritative batch result.
        comparison_report_error = str(exc)
        print(f"WARNING: Comparison CSV/Markdown reports could not be generated: {exc}")

    summary = {
        "schema_version": 1,
        "batch_id": meta["batch_id"],
        "batch_name": meta.get("name"),
        "batch_manifest": str(batch_path),
        "started_at": batch_started_at.isoformat(),
        "finished_at": batch_finished_at.isoformat(),
        "requested_job_count": total_jobs,
        "completed_job_count": len(results),
        "failed_job_count": len(failed_jobs),
        "status": "completed" if not failed_jobs and len(results) == total_jobs else "needs_attention",
        "results": results,
        "comparison_reports": comparison_reports,
    }
    if comparison_report_error:
        summary["comparison_report_error"] = comparison_report_error

    summary_path = output_dir / f"batch_summary_{timestamp}.json"
    _write_batch_summary(summary_path, summary)

    print()
    print("=" * 88)
    print(f"Batch status: {summary['status']}")
    print(f"Batch progress: {render_batch_progress(len(results), total_jobs)}")
    print(f"Completed jobs: {len(results)}/{total_jobs}")
    print(f"Jobs needing attention: {len(failed_jobs)}")
    print(f"Batch summary: {summary_path}")
    if comparison_reports:
        print(f"Comparison overview CSV: {comparison_reports['comparison_overview_csv']}")
        print(f"Comparison long CSV: {comparison_reports['comparison_long_csv']}")
        print(f"Comparison Markdown: {comparison_reports['comparison_markdown']}")
        print(f"Metric matrices: {comparison_reports['comparison_matrices_directory']}")
    print("=" * 88)
    return 1 if failed_jobs or len(results) != total_jobs else 0


if __name__ == "__main__":
    raise SystemExit(main())
