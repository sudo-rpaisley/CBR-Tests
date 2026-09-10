"""Reproducible representative rerun workflow for metric-conformance experiments."""

from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

from cbr_tests.outcome_comparison import compare_outcomes, load_outcome, render_markdown


def file_sha256(path: Path) -> str:
    """Return the SHA-256 digest of one file."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def current_git_commit(repo_root: Path) -> str | None:
    """Return HEAD when the working copy is a Git repository."""
    try:
        completed = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repo_root,
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return None
    value = completed.stdout.strip()
    return value or None


def build_run_plan_command(
    *,
    repo_root: Path,
    plan_path: Path,
    dataset_path: Path,
    output_path: Path,
    case_id: str,
    display: str = "compact",
    workers: int | None = None,
    extra_args: Sequence[str] = (),
) -> list[str]:
    """Build the canonical ``run_plan.py`` invocation for one rerun."""
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
        case_id,
        "--display",
        display,
    ]
    if workers is not None:
        command.extend(["--workers", str(workers)])
    command.extend(str(arg) for arg in extra_args)
    return command


def _copy_baseline_companion(baseline_path: Path, record_dir: Path) -> Path | None:
    candidates = [
        baseline_path.with_suffix(".md"),
        baseline_path.with_name(baseline_path.stem + "_summary.md"),
        baseline_path.with_name(baseline_path.stem + ".summary.md"),
    ]
    for candidate in candidates:
        if candidate.exists() and candidate.is_file():
            destination = record_dir / "baseline_summary.md"
            shutil.copy2(candidate, destination)
            return destination
    return None


def run_and_compare(
    *,
    repo_root: Path,
    baseline_path: Path,
    plan_path: Path,
    dataset_path: Path,
    record_dir: Path,
    case_id: str = "ad_hoc_case",
    display: str = "compact",
    workers: int | None = None,
    extra_args: Sequence[str] = (),
    force: bool = False,
) -> dict:
    """Archive a baseline, rerun a plan, compare outcomes, and write a manifest.

    The authoritative baseline is copied before execution. The original baseline is
    never modified. The post-overhaul outcome is produced by the normal
    ``run_plan.py`` path rather than by a special test harness.
    """
    repo_root = repo_root.resolve()
    baseline_path = baseline_path.resolve()
    plan_path = plan_path.resolve()
    dataset_path = dataset_path.resolve()
    record_dir = record_dir.resolve()

    for label, path in (
        ("baseline outcome", baseline_path),
        ("plan", plan_path),
        ("dataset", dataset_path),
    ):
        if not path.exists() or not path.is_file():
            raise FileNotFoundError(f"{label} does not exist or is not a file: {path}")

    runner = repo_root / "run_plan.py"
    if not runner.exists():
        raise FileNotFoundError(f"run_plan.py was not found under repository root: {repo_root}")

    record_dir.mkdir(parents=True, exist_ok=True)
    baseline_copy = record_dir / "baseline_pre_overhaul.json"
    rerun_output = record_dir / "outcome_post_overhaul.json"
    comparison_json = record_dir / "comparison.json"
    comparison_markdown = record_dir / "comparison.md"
    manifest_path = record_dir / "rerun_manifest.json"

    protected_outputs = [baseline_copy, rerun_output, comparison_json, comparison_markdown, manifest_path]
    existing = [path for path in protected_outputs if path.exists()]
    if existing and not force:
        joined = ", ".join(str(path) for path in existing)
        raise FileExistsError(
            "Rerun record already contains generated files; use force=True/--force to replace them: "
            + joined
        )

    shutil.copy2(baseline_path, baseline_copy)
    baseline_summary_copy = _copy_baseline_companion(baseline_path, record_dir)

    command = build_run_plan_command(
        repo_root=repo_root,
        plan_path=plan_path,
        dataset_path=dataset_path,
        output_path=rerun_output,
        case_id=case_id,
        display=display,
        workers=workers,
        extra_args=extra_args,
    )
    if force:
        command.append("--force-output")

    started_at = datetime.now(timezone.utc)
    completed = subprocess.run(command, cwd=repo_root, check=False)
    finished_at = datetime.now(timezone.utc)

    if not rerun_output.exists():
        raise RuntimeError(
            f"run_plan.py returned {completed.returncode} and did not produce the expected outcome: {rerun_output}"
        )

    before = load_outcome(baseline_copy)
    after = load_outcome(rerun_output)
    comparison = compare_outcomes(before, after)
    comparison_json.write_text(
        json.dumps(comparison, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    comparison_markdown.write_text(render_markdown(comparison), encoding="utf-8")

    manifest = {
        "schema_version": 1,
        "workflow": "metric-conformance-representative-rerun",
        "repository_root": str(repo_root),
        "cbr_tests_commit": current_git_commit(repo_root),
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "process_return_code": completed.returncode,
        "case_id": case_id,
        "display": display,
        "workers": workers,
        "command": command,
        "inputs": {
            "baseline_original": str(baseline_path),
            "baseline_archived": str(baseline_copy),
            "baseline_sha256": file_sha256(baseline_copy),
            "baseline_summary_archived": str(baseline_summary_copy) if baseline_summary_copy else None,
            "plan": str(plan_path),
            "plan_sha256": file_sha256(plan_path),
            "dataset": str(dataset_path),
            "dataset_sha256": file_sha256(dataset_path),
        },
        "outputs": {
            "post_overhaul_outcome": str(rerun_output),
            "post_overhaul_outcome_sha256": file_sha256(rerun_output),
            "comparison_json": str(comparison_json),
            "comparison_markdown": str(comparison_markdown),
        },
        "comparison_summary": comparison["summary"],
        "before_status": before.get("status"),
        "after_status": after.get("status"),
    }
    manifest_path.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return manifest
