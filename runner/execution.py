from __future__ import annotations
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from pathlib import Path

from runner.progress import render_metric_activity_bar, render_overall_progress_line, print_live_status


def render_live_taxonomy(metrics: list[dict], current_metric_id: str, completed_statuses: dict[str, str], completed_durations: dict[str, float], default_predictions: dict[str, float], predicted_metric_total: float, elapsed: float | None = None, completed: bool = False) -> str:
    lines: list[str] = []
    printed_nodes: set[tuple[str, ...]] = set()
    predicted_metric_total = max(1.0, predicted_metric_total)
    if elapsed is not None:
        predicted_metric_total = max(predicted_metric_total, elapsed)
    for metric in metrics:
        path = metric.get('taxonomy_path', [])
        for depth in range(len(path)):
            node_tuple = tuple(path[: depth + 1])
            if node_tuple in printed_nodes:
                continue
            printed_nodes.add(node_tuple)
            lines.append(f"{'  ' * depth}↳ {path[depth]}")
        metric_id = metric.get('metric_id', 'unknown_metric')
        metric_prediction = completed_durations.get(metric_id, default_predictions.get(metric_id, predicted_metric_total))
        if metric_id in completed_statuses:
            run_time = completed_durations.get(metric_id)
            suffix = f" [{completed_statuses[metric_id]} | run time {run_time:.1f}s]" if run_time is not None else f" [{completed_statuses[metric_id]}]"
        elif metric_id == current_metric_id:
            if completed:
                suffix = f" [success] | done in {elapsed:.1f}s"
            elif elapsed is not None:
                suffix = f" [running | {elapsed:.1f}/{predicted_metric_total:.0f}s ] [{render_metric_activity_bar(elapsed, expected_seconds=predicted_metric_total)}]"
            else:
                suffix = ' [running]'
        else:
            suffix = f" [pending | 0.0/{metric_prediction:.0f}s]"
        lines.append(f"{'  ' * len(path)}↳ {metric_id}{suffix}")
    return '\n'.join(lines)


def run_metric_with_heartbeat(dataset_path: Path, metric: dict, metrics: list[dict], completed_statuses: dict[str, str], completed_durations: dict[str, float], current: int, total: int, shutdown_requested: dict, run_start_perf: float | None, metric_handlers: dict, default_predictions: dict[str, float]):
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
                task_line = render_live_taxonomy(metrics, metric_id, completed_statuses, completed_durations, default_predictions, smoothed_total, elapsed, completed=True)
                run_elapsed = (time.perf_counter() - run_start_perf) if run_start_perf is not None else None
                overall_line = render_overall_progress_line(current, total, run_elapsed, elapsed)
                print_live_status(task_line, overall_line, None)
                return result
            except TimeoutError:
                elapsed = time.perf_counter() - heartbeat_start
                instant_total = max(elapsed + 1.0, elapsed * 1.2, 20.0)
                smoothed_total = max(elapsed, 0.7 * smoothed_total + 0.3 * instant_total)
                task_line = render_live_taxonomy(metrics, metric_id, completed_statuses, completed_durations, default_predictions, smoothed_total, elapsed)
                run_elapsed = (time.perf_counter() - run_start_perf) if run_start_perf is not None else None
                overall_line = render_overall_progress_line(max(0, current - 1), total, run_elapsed, elapsed)
                warning_line = None
                if shutdown_requested.get('requested'):
                    warning_line = 'Stop requested. Cancelling current task and pending tasks...'
                print_live_status(task_line, overall_line, warning_line)
