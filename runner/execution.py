from __future__ import annotations

import inspect
import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, TimeoutError, wait
from datetime import datetime, timezone
from pathlib import Path

from runner.live_rendering import render_live_taxonomy
from runner.progress import print_live_status, render_overall_progress_line


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
    run_state=None,
):
    metric_id = metric.get("metric_id", "unknown_metric")
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(lambda: metric_handlers[metric_id](dataset_path, metric))
        heartbeat_start = time.perf_counter()
        smoothed_total = 20.0
        while True:
            try:
                result = future.result(timeout=1.0)
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
                    run_state=run_state,
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
        "reason_code": reason,
        "started_at": now,
        "finished_at": now,
        "elapsed_seconds": 0.0,
    }


def _notify_progress(progress_callback, *args, payload=None) -> None:
    """Call progress callbacks without breaking the historical eight-argument API."""
    if progress_callback is None:
        return
    try:
        signature = inspect.signature(progress_callback)
        parameters = list(signature.parameters.values())
        accepts_varargs = any(p.kind == inspect.Parameter.VAR_POSITIONAL for p in parameters)
        positional = [
            p for p in parameters
            if p.kind in {inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD}
        ]
        accepts_payload = accepts_varargs or len(positional) >= len(args) + 1
    except (TypeError, ValueError):
        accepts_payload = False

    if accepts_payload:
        progress_callback(*args, payload)
    else:
        progress_callback(*args)


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
            ok, payload = False, {
                "error": str(exc),
                "reason_code": "execution_exception",
            }
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
                _notify_progress(
                    progress_callback,
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
                        results.append((idx, False, _not_run_payload("not_run_cancelled", "run_cancelled")))
                        del future_to_index[future]
                break

            if control_state and control_state.get("pause_requested"):
                _notify_progress(
                    progress_callback,
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
                _notify_progress(
                    progress_callback,
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
                    ok, payload = False, {
                        "error": str(exc),
                        "reason_code": "execution_exception",
                    }
                results.append((idx, ok, payload))
                failed_in_batch = failed_in_batch or not ok
                elapsed_seconds = payload.get("elapsed_seconds") if isinstance(payload, dict) else None
                _notify_progress(
                    progress_callback,
                    "completed",
                    len(results),
                    len(metrics),
                    len(future_to_index) + (len(metrics) - next_index),
                    metric_id,
                    ok,
                    running_ids,
                    elapsed_seconds,
                    payload=payload,
                )

            if fail_fast and failed_in_batch:
                stop_submitting = True
                stop_status = "not_run_fail_fast"
                stop_reason = "previous_metric_failed"
                for future, idx in list(future_to_index.items()):
                    if future.cancel():
                        results.append((idx, False, _not_run_payload("not_run_fail_fast", "previous_metric_failed")))
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
                            ok, payload = False, {
                                "error": str(exc),
                                "reason_code": "execution_exception",
                            }
                        results.append((idx, ok, payload))
    finally:
        executor.shutdown(wait=False, cancel_futures=True)

    results.sort(key=lambda item: item[0])
    return results
