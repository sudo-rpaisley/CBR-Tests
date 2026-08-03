from __future__ import annotations

import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, TimeoutError, wait
from datetime import datetime, timezone
from pathlib import Path

from runner.live_rendering import render_interactive_run_state
from runner.progress import (
    colorize_status,
    print_live_status,
    render_metric_activity_bar,
    render_overall_progress_line,
)
from runner.telemetry import RunState


def _metric_status_line(
    metric_id: str,
    status: str,
    duration: float | None = None,
    elapsed: float | None = None,
    expected: float | None = None,
) -> str:
    status_text = colorize_status(status)
    if status == "running" and elapsed is not None and expected is not None:
        return (
            f"{metric_id} [{status_text} | {elapsed:.1f}/{expected:.0f}s ] "
            f"[{render_metric_activity_bar(elapsed, expected_seconds=expected)}]"
        )
    if duration is not None:
        return f"{metric_id} [{status_text} | run time {duration:.1f}s]"
    return f"{metric_id} [{status_text}]"


def _metric_display_status(metric_id: str, current_metric_id: str, completed_statuses: dict[str, str]) -> str:
    if metric_id in completed_statuses:
        return completed_statuses[metric_id]
    if metric_id == current_metric_id:
        return "running"
    return "pending"


def render_compact_run_state(
    run_state: RunState,
    default_predictions: dict[str, float],
    running_elapsed: dict[str, float] | None = None,
    max_lines: int | None = 24,
) -> str:
    """Render a compact dashboard from centralized run telemetry."""
    running_elapsed = running_elapsed or {}
    status_counts = run_state.status_counts()
    lines = [
        "Taxonomy summary",
        "  " + " | ".join(f"{name}: {count}" for name, count in status_counts.items() if count),
        "",
        "Branches",
    ]
    for branch, counts in run_state.branch_summaries().items():
        detail = ", ".join(f"{key} {value}" for key, value in counts.items() if key != "total" and value)
        prefix = "▾" if counts.get("running") or counts.get("failed") or counts.get("skipped") else "▸"
        lines.append(f"  {prefix} {branch}: {detail} / {counts['total']} total")

    attention = run_state.attention_metrics()
    if attention:
        lines.extend(["", "Active / attention"])
        for metric in attention[:8]:
            elapsed = running_elapsed.get(metric.metric_id)
            expected = None
            if metric.status == "running":
                elapsed = elapsed or 0.0
                expected = max(default_predictions.get(metric.metric_id, 20.0), elapsed + 1.0)
            line = "  " + _metric_status_line(
                metric.metric_id,
                metric.status,
                duration=metric.elapsed_seconds,
                elapsed=elapsed,
                expected=expected,
            )
            if metric.missing_fields:
                line += f" | missing: {', '.join(metric.missing_fields)}"
            lines.append(line)

    recent = run_state.recent_completed(limit=5)
    if recent:
        lines.extend(["", "Recently completed"])
        lines.extend(
            "  " + _metric_status_line(metric.metric_id, metric.status, duration=metric.elapsed_seconds)
            for metric in recent
        )

    if run_state.events:
        lines.extend(["", "Recent events"])
        for event in run_state.events[-3:]:
            lines.append(f"  {event.timestamp.strftime('%H:%M:%S')} {event.message}")

    total_lines = len(lines)
    if max_lines is not None and max_lines > 0 and total_lines > max_lines:
        visible = max(1, max_lines - 1)
        hidden = total_lines - visible
        lines = lines[:visible] + [f"... {hidden} lines hidden; use --display full for all metrics."]
    return "\n".join(lines)


def render_compact_taxonomy(
    metrics: list[dict],
    current_metric_id: str,
    completed_statuses: dict[str, str],
    completed_durations: dict[str, float],
    default_predictions: dict[str, float],
    predicted_metric_total: float,
    elapsed: float | None = None,
    running_elapsed: dict[str, float] | None = None,
    max_lines: int | None = 24,
) -> str:
    """Render a screen-friendly taxonomy summary with attention items expanded."""
    branch_counts: dict[str, dict[str, int]] = {}
    status_counts = {
        "success": 0,
        "running": 0,
        "failed": 0,
        "pending": 0,
        "stopping": 0,
        "cancelled": 0,
    }
    attention: list[str] = []
    recent: list[str] = []
    running_elapsed = running_elapsed or {}
    predicted_metric_total = max(1.0, predicted_metric_total)

    for metric in metrics:
        metric_id = metric.get("metric_id", "unknown_metric")
        path = metric.get("taxonomy_path", []) or ["uncategorized"]
        branch = str(path[0])
        status = _metric_display_status(metric_id, current_metric_id, completed_statuses)
        if status == "running" and elapsed is not None and metric_id == current_metric_id and metric_id not in running_elapsed:
            run_elapsed = elapsed
        else:
            run_elapsed = running_elapsed.get(metric_id, 0.0)
        status_counts[status] = status_counts.get(status, 0) + 1
        branch_counts.setdefault(
            branch,
            {
                "total": 0,
                "success": 0,
                "running": 0,
                "failed": 0,
                "pending": 0,
                "stopping": 0,
                "cancelled": 0,
            },
        )
        branch_counts[branch]["total"] += 1
        branch_counts[branch][status] = branch_counts[branch].get(status, 0) + 1

        if status in {"running", "failed", "stopping", "cancelled"}:
            expected = max(
                completed_durations.get(metric_id, default_predictions.get(metric_id, predicted_metric_total)),
                run_elapsed + 1.0,
            )
            attention.append(
                "  "
                + _metric_status_line(
                    metric_id,
                    status,
                    completed_durations.get(metric_id),
                    run_elapsed,
                    expected,
                )
            )
        elif status == "success":
            recent.append("  " + _metric_status_line(metric_id, status, completed_durations.get(metric_id)))

    lines = [
        "Taxonomy summary",
        "  " + " | ".join(f"{name}: {count}" for name, count in status_counts.items() if count),
        "",
        "Branches",
    ]
    for branch, counts in branch_counts.items():
        detail = ", ".join(f"{key} {value}" for key, value in counts.items() if key != "total" and value)
        prefix = "▾" if counts.get("running") or counts.get("failed") else "▸"
        lines.append(f"  {prefix} {branch}: {detail} / {counts['total']} total")

    if attention:
        lines.extend(["", "Active / attention"] + attention[:8])
    if recent:
        lines.extend(["", "Recently completed"] + recent[-5:])

    total_lines = len(lines)
    if max_lines is not None and max_lines > 0 and total_lines > max_lines:
        visible = max(1, max_lines - 1)
        hidden = total_lines - visible
        lines = lines[:visible] + [f"... {hidden} lines hidden; use --display full for all metrics."]
    return "\n".join(lines)


def render_live_taxonomy(
    metrics: list[dict],
    current_metric_id: str,
    completed_statuses: dict[str, str],
    completed_durations: dict[str, float],
    default_predictions: dict[str, float],
    predicted_metric_total: float,
    elapsed: float | None = None,
    completed: bool = False,
    running_elapsed: dict[str, float] | None = None,
    display_mode: str = "full",
    max_lines: int | None = None,
    run_state: RunState | None = None,
) -> str:
    if display_mode == "interactive" and run_state is not None:
        return render_interactive_run_state(
            run_state,
            default_predictions,
            running_elapsed=running_elapsed,
            max_lines=max_lines,
        )
    if display_mode == "compact" and run_state is not None:
        return render_compact_run_state(
            run_state,
            default_predictions,
            running_elapsed=running_elapsed,
            max_lines=max_lines or 24,
        )
    if display_mode in {"compact", "interactive"}:
        return render_compact_taxonomy(
            metrics,
            current_metric_id,
            completed_statuses,
            completed_durations,
            default_predictions,
            predicted_metric_total,
            elapsed=elapsed,
            running_elapsed=running_elapsed,
            max_lines=max_lines or 24,
        )
    lines: list[str] = []
    printed_nodes: set[tuple[str, ...]] = set()
    predicted_metric_total = max(1.0, predicted_metric_total)
    if elapsed is not None:
        predicted_metric_total = max(predicted_metric_total, elapsed)
    for metric in metrics:
        path = metric.get("taxonomy_path", [])
        for depth in range(len(path)):
            node_tuple = tuple(path[: depth + 1])
            if node_tuple in printed_nodes:
                continue
            printed_nodes.add(node_tuple)
            lines.append(f"{'  ' * depth}↳ {path[depth]}")
        metric_id = metric.get("metric_id", "unknown_metric")
        metric_prediction = completed_durations.get(
            metric_id,
            default_predictions.get(metric_id, predicted_metric_total),
        )
        if metric_id in completed_statuses and completed_statuses[metric_id] == "running":
            run_elapsed = (running_elapsed or {}).get(metric_id, 0.0)
            expected = max(metric_prediction, run_elapsed + 1.0)
            suffix = (
                f" [{colorize_status('running')} | {run_elapsed:.1f}/{expected:.0f}s ] "
                f"[{render_metric_activity_bar(run_elapsed, expected_seconds=expected)}]"
            )
        elif metric_id in completed_statuses:
            run_time = completed_durations.get(metric_id)
            status_text = colorize_status(completed_statuses[metric_id])
            suffix = f" [{status_text} | run time {run_time:.1f}s]" if run_time is not None else f" [{status_text}]"
        elif metric_id == current_metric_id:
            if completed:
                suffix = f" [{colorize_status('success')}] | done in {elapsed:.1f}s"
            elif elapsed is not None:
                suffix = (
                    f" [{colorize_status('running')} | {elapsed:.1f}/{predicted_metric_total:.0f}s ] "
                    f"[{render_metric_activity_bar(elapsed, expected_seconds=predicted_metric_total)}]"
                )
            else:
                suffix = f" [{colorize_status('running')}]"
        else:
            suffix = f" [{colorize_status('pending')} | 0.0/{metric_prediction:.0f}s]"
        lines.append(f"{'  ' * len(path)}↳ {metric_id}{suffix}")
    return "\n".join(lines)


def run_metric_with_heartbeat(
    dataset_path: Path,
    metric: dict,
    metrics: list[dict],
    completed_statuses: dict[str, str],
    completed_durations: dict[str, float],
    current: int,
    total: int,
    shutdown_requested: dict,
    run_start_perf: float | None,
    metric_handlers: dict,
    default_predictions: dict[str, float],
    display_mode: str = "full",
    max_lines: int | None = None,
):
    metric_id = metric.get("metric_id", "unknown_metric")
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(lambda: metric_handlers[metric_id](dataset_path, metric))
        heartbeat_start = time.perf_counter()
        smoothed_total = 20.0
        while True:
            try:
                result = future.result(timeout=1.0)
                elapsed = time.perf_counter() - heartbeat_start
                instant_total = max(elapsed + 1.0, elapsed * 1.2, 20.0)
                smoothed_total = max(elapsed, 0.7 * smoothed_total + 0.3 * instant_total)
                task_line = render_live_taxonomy(
                    metrics,
                    metric_id,
                    completed_statuses,
                    completed_durations,
                    default_predictions,
                    smoothed_total,
                    elapsed,
                    completed=True,
                    display_mode=display_mode,
                    max_lines=max_lines,
                )
                run_elapsed = (time.perf_counter() - run_start_perf) if run_start_perf is not None else None
                overall_line = render_overall_progress_line(current, total, run_elapsed, elapsed)
                print_live_status(task_line, overall_line, None)
                return result
            except TimeoutError:
                elapsed = time.perf_counter() - heartbeat_start
                instant_total = max(elapsed + 1.0, elapsed * 1.2, 20.0)
                smoothed_total = max(elapsed, 0.7 * smoothed_total + 0.3 * instant_total)
                task_line = render_live_taxonomy(
                    metrics,
                    metric_id,
                    completed_statuses,
                    completed_durations,
                    default_predictions,
                    smoothed_total,
                    elapsed,
                    display_mode=display_mode,
                    max_lines=max_lines,
                )
                run_elapsed = (time.perf_counter() - run_start_perf) if run_start_perf is not None else None
                overall_line = render_overall_progress_line(max(0, current - 1), total, run_elapsed, elapsed)
                warning_line = None
                if shutdown_requested.get("requested"):
                    warning_line = "Stop requested. Cancelling current task and pending tasks..."
                print_live_status(task_line, overall_line, warning_line)


def auto_worker_count(num_metrics: int) -> int:
    import os

    cpu = os.cpu_count() or 2
    return max(1, min(num_metrics, cpu - 1 if cpu > 2 else 1))


def _not_run_payload(status: str, reason: str) -> dict:
    now = datetime.now(timezone.utc).isoformat()
    return {
        "status": status,
        "reason": reason,
        "started_at": now,
        "finished_at": now,
        "elapsed_seconds": 0.0,
    }


def run_metrics_parallel(
    dataset_path: Path,
    metrics: list[dict],
    metric_handlers: dict,
    workers: int,
    progress_callback=None,
    control_state: dict | None = None,
    fail_fast: bool = False,
) -> list[tuple[int, bool, dict]]:
    """Run metrics with bounded submission and deterministic result records.

    At most ``workers`` metrics are submitted at once. When fail-fast is enabled,
    a failed metric stops new submissions. Already-running work is allowed to
    finish because Python threads cannot be safely terminated; metrics that were
    never started are explicitly marked ``not_run_fail_fast``.

    Cancellation returns promptly, attempts to cancel queued futures, and marks
    all unfinished or unsubmitted metrics ``not_run_cancelled``.
    """
    results: list[tuple[int, bool, dict]] = []
    workers = max(1, int(workers))
    executor = ThreadPoolExecutor(max_workers=workers)
    future_to_index: dict[Future, int] = {}
    next_index = 0
    stop_submitting = False
    stop_status: str | None = None
    stop_reason: str | None = None

    def _timed_call(metric_id: str, metric: dict):
        started_at = datetime.now(timezone.utc)
        started_perf = time.perf_counter()
        try:
            ok, payload = metric_handlers[metric_id](dataset_path, metric)
        except Exception as exc:  # noqa: BLE001
            ok, payload = False, {"error": str(exc)}
        finished_at = datetime.now(timezone.utc)
        elapsed = round(time.perf_counter() - started_perf, 6)
        if isinstance(payload, dict):
            payload = dict(payload)
        else:
            payload = {"value": payload}
        payload.setdefault("elapsed_seconds", elapsed)
        payload.setdefault("started_at", started_at.isoformat())
        payload.setdefault("finished_at", finished_at.isoformat())
        return ok, payload

    def _submit_available() -> None:
        nonlocal next_index
        while not stop_submitting and len(future_to_index) < workers and next_index < len(metrics):
            metric = metrics[next_index]
            future = executor.submit(_timed_call, metric["metric_id"], metric)
            future_to_index[future] = next_index
            next_index += 1

    try:
        _submit_available()
        while future_to_index:
            if control_state and control_state.get("cancel_requested"):
                stop_submitting = True
                stop_status = "not_run_cancelled"
                stop_reason = "run_cancelled"
                if progress_callback is not None:
                    progress_callback(
                        "stopping",
                        len(results),
                        len(metrics),
                        len(future_to_index) + (len(metrics) - next_index),
                        None,
                        None,
                        [],
                        None,
                    )
                for future, idx in list(future_to_index.items()):
                    if future.cancel():
                        results.append(
                            (idx, False, _not_run_payload("not_run_cancelled", "run_cancelled"))
                        )
                        del future_to_index[future]
                break

            if control_state and control_state.get("pause_requested"):
                if progress_callback is not None:
                    progress_callback(
                        "paused",
                        len(results),
                        len(metrics),
                        len(future_to_index) + (len(metrics) - next_index),
                        None,
                        None,
                        [],
                        None,
                    )
                time.sleep(0.2)
                continue

            done, _ = wait(set(future_to_index), timeout=1.0, return_when=FIRST_COMPLETED)
            running_ids = [
                metrics[idx].get("metric_id", "unknown_metric")
                for future, idx in future_to_index.items()
                if future.running()
            ]
            if not done:
                if progress_callback is not None:
                    progress_callback(
                        "heartbeat",
                        len(results),
                        len(metrics),
                        len(future_to_index) + (len(metrics) - next_index),
                        None,
                        None,
                        running_ids,
                        None,
                    )
                continue

            failed_in_batch = False
            for future in done:
                idx = future_to_index.pop(future)
                metric_id = metrics[idx].get("metric_id", "unknown_metric")
                try:
                    ok, payload = future.result()
                except Exception as exc:  # noqa: BLE001
                    ok, payload = False, {"error": str(exc)}
                results.append((idx, ok, payload))
                failed_in_batch = failed_in_batch or not ok
                if progress_callback is not None:
                    elapsed_seconds = payload.get("elapsed_seconds") if isinstance(payload, dict) else None
                    progress_callback(
                        "completed",
                        len(results),
                        len(metrics),
                        len(future_to_index) + (len(metrics) - next_index),
                        metric_id,
                        ok,
                        running_ids,
                        elapsed_seconds,
                    )

            if fail_fast and failed_in_batch:
                stop_submitting = True
                stop_status = "not_run_fail_fast"
                stop_reason = "previous_metric_failed"
                for future, idx in list(future_to_index.items()):
                    if future.cancel():
                        results.append(
                            (idx, False, _not_run_payload("not_run_fail_fast", "previous_metric_failed"))
                        )
                        del future_to_index[future]

            if not stop_submitting:
                _submit_available()

        if stop_submitting:
            status = stop_status or "not_run_fail_fast"
            reason = stop_reason or "previous_metric_failed"
            for idx in range(next_index, len(metrics)):
                results.append((idx, False, _not_run_payload(status, reason)))

            if status == "not_run_cancelled":
                for future, idx in list(future_to_index.items()):
                    results.append((idx, False, _not_run_payload(status, reason)))
                    del future_to_index[future]
            else:
                while future_to_index:
                    done, _ = wait(set(future_to_index), return_when=FIRST_COMPLETED)
                    for future in done:
                        idx = future_to_index.pop(future)
                        try:
                            ok, payload = future.result()
                        except Exception as exc:  # noqa: BLE001
                            ok, payload = False, {"error": str(exc)}
                        results.append((idx, ok, payload))
    finally:
        executor.shutdown(wait=False, cancel_futures=True)

    results.sort(key=lambda item: item[0])
    return results
