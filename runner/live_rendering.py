from __future__ import annotations

import shutil

from runner.progress import colorize_status, render_metric_activity_bar
from runner.telemetry import RunState

ATTENTION_STATUSES = {"running", "error", "fail", "warn", "not_applicable", "skipped", "stopping", "cancelled"}
SUCCESS_STATUSES = {"success", "pass"}


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


def _diagnostic_suffix(metric) -> str:
    if metric.summary:
        if metric.reason_code:
            return f" | {metric.reason_code}: {metric.summary}"
        return f" | {metric.summary}"
    if metric.error:
        return f" | error: {metric.error}"
    if metric.missing_fields:
        return f" | missing: {', '.join(metric.missing_fields)}"
    return ""


def _has_branch_attention(counts: dict[str, int]) -> bool:
    return any(counts.get(status, 0) for status in ATTENTION_STATUSES)


def _terminal_width(value: str) -> int:
    return len(value)


def _clip(value: str, width: int) -> str:
    if width <= 0:
        return ""
    if _terminal_width(value) <= width:
        return value
    if width <= 1:
        return "…"
    return value[: width - 1] + "…"


def _tui_border(width: int, left: str, fill: str, right: str, title: str = "") -> str:
    if title:
        label = f" {title} "
        available = max(0, width - len(left) - len(right))
        label = _clip(label, available)
        remaining = max(0, available - len(label))
        return left + label + fill * remaining + right
    return left + fill * max(0, width - len(left) - len(right)) + right


def _tui_row(content: str, width: int) -> str:
    inner_width = max(0, width - 4)
    return "│ " + _clip(content, inner_width).ljust(inner_width) + " │"


def _progress_bar(completed: int, total: int, width: int) -> str:
    total = max(total, 1)
    width = max(width, 3)
    filled = int(width * min(completed, total) / total)
    return "█" * filled + "░" * (width - filled)


def render_interactive_run_state(
    run_state: RunState,
    default_predictions: dict[str, float],
    running_elapsed: dict[str, float] | None = None,
    max_lines: int | None = None,
) -> str:
    running_elapsed = running_elapsed or {}
    terminal_size = shutil.get_terminal_size(fallback=(120, 32))
    width = max(72, terminal_size.columns)
    height = max_lines or terminal_size.lines
    counts = run_state.status_counts()
    total = max(len(run_state.metrics), 1)
    completed = sum(
        count for status, count in counts.items()
        if status not in {"pending", "running", "stopping"}
    )
    progress_width = max(10, min(40, width - 36))
    count_summary = " | ".join(f"{name}: {count}" for name, count in counts.items() if count)

    lines = [
        _tui_border(width, "┌", "─", "┐", "CBR Test Runner TUI"),
        _tui_row(f"Case: {run_state.case_id} | Plan: {run_state.plan_id} ({run_state.plan_name})", width),
        _tui_row(f"Dataset: {run_state.dataset_path}", width),
        _tui_row(f"Output:  {run_state.output_path}", width),
        _tui_row(
            f"Overall: [{_progress_bar(completed, total, progress_width)}] "
            f"{completed}/{total} | {count_summary}",
            width,
        ),
        _tui_border(width, "├", "─", "┤", "Branches"),
    ]

    for branch, branch_counts in list(run_state.branch_summaries().items())[:8]:
        detail = ", ".join(
            f"{key} {value}" for key, value in branch_counts.items() if key != "total" and value
        )
        prefix = "▾" if _has_branch_attention(branch_counts) else "▸"
        lines.append(_tui_row(f"{prefix} {branch}: {detail} / {branch_counts['total']} total", width))

    attention = run_state.attention_metrics()
    if attention:
        lines.append(_tui_border(width, "├", "─", "┤", "Active / Attention"))
        for metric in attention[:8]:
            status = metric.display_status
            elapsed = running_elapsed.get(metric.metric_id)
            expected = None
            if status == "running":
                elapsed = elapsed or 0.0
                expected = max(default_predictions.get(metric.metric_id, 20.0), elapsed + 1.0)
            line = _metric_status_line(
                metric.metric_id,
                status,
                duration=metric.elapsed_seconds,
                elapsed=elapsed,
                expected=expected,
            )
            line += _diagnostic_suffix(metric)
            lines.append(_tui_row(line, width))
            if metric.suggestion:
                lines.append(_tui_row(f"  action: {metric.suggestion}", width))

    recent = run_state.recent_completed(limit=6)
    if recent:
        lines.append(_tui_border(width, "├", "─", "┤", "Recently Completed"))
        for metric in recent:
            lines.append(
                _tui_row(
                    _metric_status_line(metric.metric_id, metric.display_status, duration=metric.elapsed_seconds),
                    width,
                )
            )

    if run_state.events:
        lines.append(_tui_border(width, "├", "─", "┤", "Recent Events"))
        for event in run_state.events[-5:]:
            lines.append(_tui_row(f"{event.timestamp.strftime('%H:%M:%S')} {event.message}", width))

    lines.append(_tui_border(width, "└", "─", "┘"))
    if height and len(lines) > height:
        hidden = len(lines) - height + 1
        lines = lines[: max(1, height - 1)] + [
            _tui_row(f"… {hidden} lines hidden; resize terminal or use --display full", width)
        ]
    return "\n".join(lines)


def render_compact_run_state(
    run_state: RunState,
    default_predictions: dict[str, float],
    running_elapsed: dict[str, float] | None = None,
    max_lines: int | None = 24,
) -> str:
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
        prefix = "▾" if _has_branch_attention(counts) else "▸"
        lines.append(f"  {prefix} {branch}: {detail} / {counts['total']} total")

    attention = run_state.attention_metrics()
    if attention:
        lines.extend(["", "Active / attention"])
        for metric in attention[:8]:
            status = metric.display_status
            elapsed = running_elapsed.get(metric.metric_id)
            expected = None
            if status == "running":
                elapsed = elapsed or 0.0
                expected = max(default_predictions.get(metric.metric_id, 20.0), elapsed + 1.0)
            line = "  " + _metric_status_line(
                metric.metric_id,
                status,
                duration=metric.elapsed_seconds,
                elapsed=elapsed,
                expected=expected,
            )
            line += _diagnostic_suffix(metric)
            lines.append(line)

    recent = run_state.recent_completed(limit=5)
    if recent:
        lines.extend(["", "Recently completed"])
        lines.extend(
            "  " + _metric_status_line(metric.metric_id, metric.display_status, duration=metric.elapsed_seconds)
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
    branch_counts: dict[str, dict[str, int]] = {}
    status_counts: dict[str, int] = {}
    attention: list[str] = []
    recent: list[str] = []
    running_elapsed = running_elapsed or {}
    predicted_metric_total = max(1.0, predicted_metric_total)

    for metric in metrics:
        metric_id = metric.get("metric_id", "unknown_metric")
        path = metric.get("taxonomy_path", []) or ["uncategorized"]
        branch = str(path[0])
        status = _metric_display_status(metric_id, current_metric_id, completed_statuses)
        run_elapsed = running_elapsed.get(metric_id, 0.0)
        if status == "running" and elapsed is not None and metric_id == current_metric_id and metric_id not in running_elapsed:
            run_elapsed = elapsed
        status_counts[status] = status_counts.get(status, 0) + 1
        branch_counts.setdefault(branch, {"total": 0})
        branch_counts[branch]["total"] += 1
        branch_counts[branch][status] = branch_counts[branch].get(status, 0) + 1

        if status in ATTENTION_STATUSES:
            expected = max(
                completed_durations.get(metric_id, default_predictions.get(metric_id, predicted_metric_total)),
                run_elapsed + 1.0,
            )
            attention.append(
                "  " + _metric_status_line(
                    metric_id,
                    status,
                    completed_durations.get(metric_id),
                    run_elapsed,
                    expected,
                )
            )
        elif status in SUCCESS_STATUSES:
            recent.append("  " + _metric_status_line(metric_id, status, completed_durations.get(metric_id)))

    lines = [
        "Taxonomy summary",
        "  " + " | ".join(f"{name}: {count}" for name, count in status_counts.items() if count),
        "",
        "Branches",
    ]
    for branch, counts in branch_counts.items():
        detail = ", ".join(f"{key} {value}" for key, value in counts.items() if key != "total" and value)
        prefix = "▾" if _has_branch_attention(counts) else "▸"
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
        return render_interactive_run_state(run_state, default_predictions, running_elapsed=running_elapsed, max_lines=max_lines)
    if display_mode == "compact" and run_state is not None:
        return render_compact_run_state(run_state, default_predictions, running_elapsed=running_elapsed, max_lines=max_lines or 24)
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
        metric_prediction = completed_durations.get(metric_id, default_predictions.get(metric_id, predicted_metric_total))
        metric_state = run_state.metrics.get(metric_id) if run_state is not None else None
        if metric_state is not None and metric_state.status != "pending":
            status = metric_state.display_status
            run_time = metric_state.elapsed_seconds
            suffix = f" [{colorize_status(status)} | run time {run_time:.1f}s]" if run_time is not None else f" [{colorize_status(status)}]"
            suffix += _diagnostic_suffix(metric_state)
        elif metric_id in completed_statuses and completed_statuses[metric_id] == "running":
            run_elapsed = (running_elapsed or {}).get(metric_id, 0.0)
            expected = max(metric_prediction, run_elapsed + 1.0)
            suffix = (
                f" [{colorize_status('running')} | {run_elapsed:.1f}/{expected:.0f}s ] "
                f"[{render_metric_activity_bar(run_elapsed, expected_seconds=expected)}]"
            )
        elif metric_id in completed_statuses:
            run_time = completed_durations.get(metric_id)
            status = completed_statuses[metric_id]
            suffix = f" [{colorize_status(status)} | run time {run_time:.1f}s]" if run_time is not None else f" [{colorize_status(status)}]"
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
