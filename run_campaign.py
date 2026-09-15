from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
import sys
from typing import Any

from runner.campaign import (
    CAMPAIGN_STATE_SCHEMA_VERSION,
    campaign_state_path,
    file_sha256,
    load_campaign,
    load_campaign_state,
    resolve_repo_path,
    slug,
    write_campaign_state,
)


ATTENTION_BATCH_STATUSES = {"needs_attention", "interrupted", "failed", "error", "cancelled", "unknown"}


def _read_batch_status(output_dir: Path) -> str:
    state_path = output_dir / "batch_state.json"
    if not state_path.is_file():
        return "unknown"
    try:
        payload = json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return "unknown"
    return str(payload.get("status") or "unknown")


def _matrix_needs_attention(result: dict[str, Any]) -> bool:
    return int(result.get("process_return_code", 0)) != 0 or str(result.get("batch_status") or "unknown") != "completed"


def _result_map(state: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        str(result["queue_id"]): result
        for result in state.get("results", [])
        if isinstance(result, dict) and result.get("queue_id")
    }


def _replace_result(state: dict[str, Any], result: dict[str, Any]) -> None:
    queue_id = str(result["queue_id"])
    results = [
        existing
        for existing in state.get("results", [])
        if not isinstance(existing, dict) or str(existing.get("queue_id")) != queue_id
    ]
    results.append(result)
    order = {
        str(matrix["queue_id"]): index
        for index, matrix in enumerate(state.get("matrix_queue", []))
        if isinstance(matrix, dict) and matrix.get("queue_id")
    }
    results.sort(key=lambda item: order.get(str(item.get("queue_id")), 10**9))
    state["results"] = results


def _matrix_output_dir(campaign_output_dir: Path, index: int, matrix: dict[str, Any]) -> Path:
    return campaign_output_dir / f"{index:03d}_{slug(str(matrix['batch_id']))}"


def _build_batch_command(
    *,
    repo_root: Path,
    batch_path: Path,
    output_dir: Path,
    args: argparse.Namespace,
    resume: bool,
    retry_failed: bool,
) -> list[str]:
    command = [
        sys.executable,
        str(repo_root / "run_batch.py"),
        "--batch",
        str(batch_path),
        "--output-dir",
        str(output_dir),
        "--display",
        str(args.display),
    ]
    if args.workers is not None:
        command.extend(["--workers", str(args.workers)])
    if args.experiment_mode:
        command.append("--experiment-mode")
    if args.batch_fail_fast:
        command.append("--fail-fast")
    if args.no_update_field_translation:
        command.append("--no-update-field-translation")
    if args.yes_field_translation_sidecar:
        command.append("--yes-field-translation-sidecar")
    if args.no_dataset_summary:
        command.append("--no-dataset-summary")
    if args.refresh_dataset_summary:
        command.append("--refresh-dataset-summary")
    if resume:
        command.append("--resume")
    if retry_failed:
        command.append("--retry-failed")
    return command


def _initial_state(
    *,
    campaign_path: Path,
    campaign: dict[str, Any],
    output_dir: Path,
) -> dict[str, Any]:
    meta = campaign["campaign_meta"]
    now = datetime.now(timezone.utc).isoformat()
    return {
        "schema_version": CAMPAIGN_STATE_SCHEMA_VERSION,
        "campaign_id": meta["campaign_id"],
        "campaign_name": meta.get("name"),
        "campaign_manifest": str(campaign_path),
        "campaign_manifest_sha256": file_sha256(campaign_path),
        "output_directory": str(output_dir.resolve()),
        "status": "running",
        "started_at": now,
        "matrix_queue": campaign["matrices"],
        "current_matrix": None,
        "results": [],
    }


def _validate_resume_state(
    state: dict[str, Any],
    *,
    campaign_path: Path,
    campaign: dict[str, Any],
    output_dir: Path,
) -> None:
    if str(state.get("campaign_id")) != str(campaign["campaign_meta"]["campaign_id"]):
        raise SystemExit("error: campaign checkpoint belongs to a different campaign ID")
    if str(state.get("campaign_manifest_sha256")) != file_sha256(campaign_path):
        raise SystemExit("error: campaign manifest changed since the checkpoint was created")
    if Path(str(state.get("output_directory") or "")).expanduser().resolve() != output_dir.resolve():
        raise SystemExit("error: campaign checkpoint belongs to a different output directory")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run an ordered queue of CBR-Tests comparison matrices sequentially."
    )
    parser.add_argument("--campaign", required=True, help="Comparison campaign manifest JSON")
    parser.add_argument("--output-dir", help="Override the campaign output directory")
    parser.add_argument("--workers", type=int, default=None, help="Worker count passed to every batch")
    parser.add_argument(
        "--display",
        choices=("compact", "full", "quiet", "interactive"),
        default="compact",
        help="Display mode passed to each batch and dataset run",
    )
    parser.add_argument("--experiment-mode", action="store_true", help="Run every matrix under the strict final-experiment contract")
    parser.add_argument("--resume", action="store_true", help="Resume an existing campaign checkpoint")
    parser.add_argument("--retry-failed", action="store_true", help="With --resume, retry matrices/batch jobs needing attention")
    parser.add_argument("--fail-fast", action="store_true", help="Stop the campaign after the first matrix needing attention")
    parser.add_argument("--batch-fail-fast", action="store_true", help="Also stop each individual matrix after its first failed dataset job")
    parser.add_argument("--no-update-field-translation", action="store_true")
    parser.add_argument("--yes-field-translation-sidecar", action="store_true")
    parser.add_argument("--no-dataset-summary", action="store_true")
    parser.add_argument("--refresh-dataset-summary", action="store_true")
    args = parser.parse_args()
    if args.retry_failed and not args.resume:
        parser.error("--retry-failed requires --resume")
    return args


def main() -> int:
    args = parse_args()
    repo_root = Path(__file__).resolve().parent
    campaign_path = Path(args.campaign).expanduser().resolve()
    campaign = load_campaign(campaign_path)
    meta = campaign["campaign_meta"]
    matrices = campaign["matrices"]
    output_value = args.output_dir or campaign.get("output_directory") or str(Path("outcomes") / str(meta["campaign_id"]))
    output_dir = resolve_repo_path(repo_root, str(output_value))
    output_dir.mkdir(parents=True, exist_ok=True)
    state_path = campaign_state_path(output_dir)

    if args.resume:
        if not state_path.is_file():
            raise SystemExit(f"error: no campaign checkpoint exists at {state_path}")
        state = load_campaign_state(state_path)
        _validate_resume_state(state, campaign_path=campaign_path, campaign=campaign, output_dir=output_dir)
        prior_results = _result_map(state)
        current = state.get("current_matrix") if isinstance(state.get("current_matrix"), dict) else None
        print(f"Resuming campaign checkpoint: {len(prior_results)}/{len(matrices)} matrices previously attempted")
    else:
        if state_path.exists():
            raise SystemExit(
                f"error: campaign checkpoint already exists at {state_path}; use --resume or choose a new --output-dir"
            )
        state = _initial_state(campaign_path=campaign_path, campaign=campaign, output_dir=output_dir)
        write_campaign_state(state_path, state)
        prior_results = {}
        current = None

    results: list[dict[str, Any]] = []
    interrupted = False

    print("=" * 92)
    print(f"Comparison campaign: {meta.get('name') or meta['campaign_id']}")
    print(f"Matrices queued: {len(matrices)}")
    print(f"Comparison jobs queued: {sum(int(matrix.get('job_count') or 0) for matrix in matrices)}")
    print("Execution: sequential matrices; each matrix keeps its own batch checkpoint")
    print(f"Campaign outputs: {output_dir}")
    print(f"Campaign checkpoint: {state_path}")
    print("=" * 92)

    for index, matrix in enumerate(matrices, start=1):
        queue_id = str(matrix["queue_id"])
        prior = prior_results.get(queue_id)
        should_retry = bool(prior and args.retry_failed and _matrix_needs_attention(prior))
        if prior is not None and not should_retry:
            results.append(prior)
            print(f"[{index}/{len(matrices)}] Keeping prior matrix result: {matrix.get('batch_name') or matrix['batch_id']} ({prior.get('batch_status')})")
            continue

        batch_path = resolve_repo_path(repo_root, str(matrix["batch_path"]))
        matrix_output = _matrix_output_dir(output_dir, index, matrix)
        matrix_output.mkdir(parents=True, exist_ok=True)
        child_state_exists = (matrix_output / "batch_state.json").is_file()
        was_interrupted_current = bool(current and str(current.get("queue_id")) == queue_id)
        child_resume = bool(args.resume and child_state_exists and (should_retry or was_interrupted_current or prior is None))
        child_retry = bool(should_retry and child_resume)

        print()
        print("-" * 92)
        print(f"Campaign matrix {index}/{len(matrices)}: {matrix.get('batch_name') or matrix['batch_id']}")
        print(f"Matrix jobs: {matrix.get('job_count', '?')}")
        print(f"Batch manifest: {batch_path}")
        print(f"Matrix output: {matrix_output}")
        if child_resume:
            print("Matrix checkpoint: resume" + (" + retry attention jobs" if child_retry else ""))
        print("-" * 92)

        state["status"] = "running"
        state["current_matrix"] = {
            "queue_id": queue_id,
            "position": index,
            "batch_id": matrix["batch_id"],
            "batch_path": str(batch_path),
            "output_directory": str(matrix_output),
            "started_at": datetime.now(timezone.utc).isoformat(),
        }
        write_campaign_state(state_path, state)

        command = _build_batch_command(
            repo_root=repo_root,
            batch_path=batch_path,
            output_dir=matrix_output,
            args=args,
            resume=child_resume,
            retry_failed=child_retry,
        )
        started_at = datetime.now(timezone.utc)
        try:
            completed = subprocess.run(command, cwd=repo_root, check=False)
        except KeyboardInterrupt:
            interrupted = True
            state["status"] = "interrupted"
            state["interrupted_at"] = datetime.now(timezone.utc).isoformat()
            write_campaign_state(state_path, state)
            print("\nCampaign interrupted. Matrix and job checkpoints have been preserved.")
            break

        finished_at = datetime.now(timezone.utc)
        batch_status = _read_batch_status(matrix_output)
        result = {
            "queue_id": queue_id,
            "position": index,
            "batch_id": matrix["batch_id"],
            "batch_name": matrix.get("batch_name"),
            "batch_path": str(batch_path),
            "output_directory": str(matrix_output),
            "job_count": matrix.get("job_count"),
            "process_return_code": completed.returncode,
            "batch_status": batch_status,
            "started_at": started_at.isoformat(),
            "finished_at": finished_at.isoformat(),
        }
        _replace_result(state, result)
        state["current_matrix"] = None
        write_campaign_state(state_path, state)
        results.append(result)

        status_text = "completed" if not _matrix_needs_attention(result) else f"needs attention ({batch_status})"
        print(f"Campaign progress: {len(results)}/{len(matrices)} matrices attempted — current matrix {status_text}")
        if _matrix_needs_attention(result) and args.fail_fast:
            print("Campaign fail-fast is enabled; later matrices will not be started.")
            break

    finished_at = datetime.now(timezone.utc)
    attention = [result for result in results if _matrix_needs_attention(result)]
    if interrupted:
        status = "interrupted"
    elif not attention and len(results) == len(matrices):
        status = "completed"
    else:
        status = "needs_attention"

    state["status"] = status
    state["results"] = results
    state["last_invocation_finished_at"] = finished_at.isoformat()
    if status == "completed":
        state["finished_at"] = finished_at.isoformat()
        state["current_matrix"] = None
    write_campaign_state(state_path, state)

    total_jobs = sum(int(matrix.get("job_count") or 0) for matrix in matrices)
    completed_jobs = sum(int(result.get("job_count") or 0) for result in results if result.get("batch_status") == "completed")
    summary = {
        "schema_version": 1,
        "campaign_id": meta["campaign_id"],
        "campaign_name": meta.get("name"),
        "campaign_manifest": str(campaign_path),
        "campaign_state": str(state_path),
        "status": status,
        "matrix_count": len(matrices),
        "attempted_matrix_count": len(results),
        "attention_matrix_count": len(attention),
        "queued_job_count": total_jobs,
        "jobs_in_completed_matrices": completed_jobs,
        "finished_at": finished_at.isoformat(),
        "results": results,
    }
    summary_path = output_dir / "campaign_summary.json"
    write_campaign_state(summary_path, summary)

    print()
    print("=" * 92)
    print(f"Campaign status: {status}")
    print(f"Matrices attempted: {len(results)}/{len(matrices)}")
    print(f"Matrices needing attention: {len(attention)}")
    print(f"Campaign summary: {summary_path}")
    print(f"Campaign checkpoint: {state_path}")
    if status == "interrupted":
        print(f"Resume: python run_campaign.py --campaign {campaign_path} --resume")
    elif attention:
        print(f"Retry attention matrices: python run_campaign.py --campaign {campaign_path} --resume --retry-failed")
    print("=" * 92)

    if interrupted:
        return 130
    return 1 if attention or len(results) != len(matrices) else 0


if __name__ == "__main__":
    raise SystemExit(main())
