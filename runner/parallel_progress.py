from __future__ import annotations

import time
from pathlib import Path

from runner.live_rendering import render_live_taxonomy
from runner.progress import render_overall_progress_line, print_live_status
from runner.run_display import compact_overall_progress_line
from runner.run_plan_helpers import update_live_header


def build_parallel_progress_callback(
    *,
    plan: dict,
    case_id: str,
    dataset_path: Path,
    output_path: Path,
    metrics: list[dict],
    shared_tabular_df,
    mode: str,
    completed_statuses: dict[str, str],
    completed_durations: dict[str, float],
    default_metric_predictions: dict[str, float],
    run_start_perf: float,
    display_mode: str,
    display_max_lines: int | None,
    run_state,
):
    """Build the live progress callback used by parallel metric execution."""
    running_started_at: dict[str, float] = {}

    def _parallel_progress(event, completed, total, pending, metric_id, ok, running_ids, elapsed_seconds):
        active_running = set(running_ids or [])
        if event == "stopping":
            for metric in metrics:
                mid = metric["metric_id"]
                if mid not in completed_statuses:
                    completed_statuses[mid] = "stopping"
        for metric in metrics:
            mid = metric["metric_id"]
            if mid in completed_statuses:
                continue
            if mid in active_running:
                completed_statuses[mid] = "running"
                running_started_at.setdefault(mid, time.perf_counter())
                run_state.mark_running(mid)
            elif completed_statuses.get(mid) == "running":
                completed_statuses[mid] = "pending"
                running_started_at.pop(mid, None)
        if event == "completed" and metric_id:
            completed_statuses[metric_id] = "success" if ok else "failed"
            running_started_at.pop(metric_id, None)
            if elapsed_seconds is not None:
                completed_durations[metric_id] = float(elapsed_seconds)
            run_state.mark_completed(
                metric_id,
                "success" if ok else "failed",
                elapsed_seconds=float(elapsed_seconds) if elapsed_seconds is not None else None,
            )
        running_elapsed = {
            mid: (time.perf_counter() - started_at)
            for mid, started_at in running_started_at.items()
        }
        overall_header = render_overall_progress_line(max(1, completed), total, time.perf_counter() - run_start_perf, None)
        compact_overall_header = compact_overall_progress_line(overall_header)
        update_live_header([
            f"Run Title: {plan['plan_meta']['name']} ({plan['plan_meta']['plan_id']})",
            f"Case ID: {case_id}",
            _dataset_summary_line(shared_tabular_df, total),
            f"Source Path: {dataset_path}",
            f"Destination Output: {output_path}",
        ], [
            f"Status: {'Stopping' if event == 'stopping' else f'Running ({mode})'}",
            f"Overall Progress: {completed}/{total} metrics completed",
            compact_overall_header,
        ])
        print_live_status(
            render_live_taxonomy(
                metrics,
                metric_id if metric_id else "parallel_batch",
                completed_statuses,
                completed_durations,
                default_metric_predictions,
                max(20.0, float(total)),
                elapsed=(time.perf_counter() - run_start_perf),
                completed=False,
                running_elapsed=running_elapsed,
                display_mode=display_mode,
                max_lines=display_max_lines,
                run_state=run_state,
            ),
            "",
            None,
        )

    return _parallel_progress


def _dataset_summary_line(shared_tabular_df, total: int) -> str:
    if shared_tabular_df is None:
        return f"Metrics: {total}"
    return f"Rows: {len(shared_tabular_df):,} | Columns: {shared_tabular_df.shape[1]}"
