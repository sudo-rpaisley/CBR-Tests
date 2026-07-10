from __future__ import annotations
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from concurrent.futures import wait, FIRST_COMPLETED
from pathlib import Path

from runner.live_rendering import render_live_taxonomy
from runner.progress import render_overall_progress_line, print_live_status


def run_metric_with_heartbeat(dataset_path: Path, metric: dict, metrics: list[dict], completed_statuses: dict[str, str], completed_durations: dict[str, float], current: int, total: int, shutdown_requested: dict, run_start_perf: float | None, metric_handlers: dict, default_predictions: dict[str, float], display_mode: str = "full", max_lines: int | None = None):
    metric_id = metric.get('metric_id', 'unknown_metric')
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
                task_line = render_live_taxonomy(metrics, metric_id, completed_statuses, completed_durations, default_predictions, smoothed_total, elapsed, completed=True, display_mode=display_mode, max_lines=max_lines)
                run_elapsed = (time.perf_counter() - run_start_perf) if run_start_perf is not None else None
                overall_line = render_overall_progress_line(current, total, run_elapsed, elapsed)
                print_live_status(task_line, overall_line, None)
                return result
            except TimeoutError:
                elapsed = time.perf_counter() - heartbeat_start
                instant_total = max(elapsed + 1.0, elapsed * 1.2, 20.0)
                smoothed_total = max(elapsed, 0.7 * smoothed_total + 0.3 * instant_total)
                task_line = render_live_taxonomy(metrics, metric_id, completed_statuses, completed_durations, default_predictions, smoothed_total, elapsed, display_mode=display_mode, max_lines=max_lines)
                run_elapsed = (time.perf_counter() - run_start_perf) if run_start_perf is not None else None
                overall_line = render_overall_progress_line(max(0, current - 1), total, run_elapsed, elapsed)
                warning_line = None
                if shutdown_requested.get('requested'):
                    warning_line = 'Stop requested. Cancelling current task and pending tasks...'
                print_live_status(task_line, overall_line, warning_line)


def auto_worker_count(num_metrics: int) -> int:
    import os
    cpu = os.cpu_count() or 2
    return max(1, min(num_metrics, cpu - 1 if cpu > 2 else 1))


def run_metrics_parallel(dataset_path: Path, metrics: list[dict], metric_handlers: dict, workers: int, progress_callback=None, control_state: dict | None = None) -> list[tuple[int, bool, dict]]:
    results: list[tuple[int, bool, dict]] = []
    executor = ThreadPoolExecutor(max_workers=workers)
    try:
        def _timed_call(metric_id: str, dataset_path: Path, metric: dict):
            started = time.perf_counter()
            ok, payload = metric_handlers[metric_id](dataset_path, metric)
            elapsed = round(time.perf_counter() - started, 6)
            if isinstance(payload, dict):
                payload = dict(payload)
                payload.setdefault("elapsed_seconds", elapsed)
            else:
                payload = {"value": payload, "elapsed_seconds": elapsed}
            return ok, payload

        fut_map = {
            executor.submit(_timed_call, m["metric_id"], dataset_path, m): i
            for i, m in enumerate(metrics)
        }
        pending = set(fut_map.keys())
        while pending:
            if control_state and control_state.get("cancel_requested"):
                for fut in pending:
                    fut.cancel()
                if progress_callback is not None:
                    progress_callback("stopping", len(results), len(metrics), len(pending), None, None, [], None)
                executor.shutdown(wait=False, cancel_futures=True)
                break
            if control_state and control_state.get("pause_requested"):
                if progress_callback is not None:
                    progress_callback("paused", len(results), len(metrics), len(pending), None, None, [], None)
                time.sleep(0.2)
                continue
            done, pending = wait(pending, timeout=1.0, return_when=FIRST_COMPLETED)
            running_ids = []
            for fut in fut_map:
                if fut.done():
                    continue
                if fut.running():
                    idx = fut_map[fut]
                    metric_id = metrics[idx].get("metric_id", "unknown_metric")
                    running_ids.append(metric_id)
            if not done:
                if progress_callback is not None:
                    progress_callback("heartbeat", len(results), len(metrics), len(pending), None, None, running_ids, None)
                continue
            for fut in done:
                idx = fut_map[fut]
                metric_id = metrics[idx].get("metric_id", "unknown_metric")
                try:
                    ok, payload = fut.result()
                except Exception as exc:  # noqa: BLE001
                    ok, payload = False, {"error": str(exc)}
                results.append((idx, ok, payload))
                if progress_callback is not None:
                    elapsed_seconds = payload.get("elapsed_seconds") if isinstance(payload, dict) else None
                    progress_callback("completed", len(results), len(metrics), len(pending), metric_id, ok, running_ids, elapsed_seconds)
    finally:
        executor.shutdown(wait=False, cancel_futures=True)
    results.sort(key=lambda t: t[0])
    return results
