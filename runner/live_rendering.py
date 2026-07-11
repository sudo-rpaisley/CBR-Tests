from __future__ import annotations

import atexit
import select
import shutil
import sys
import termios
import tty

from runner.telemetry import RunState
from runner.progress import colorize_status, render_metric_activity_bar


_KEYBOARD_ENABLED = False
_OLD_TERMIOS = None
_INTERACTIVE_STATE = {"selected_branch": 0, "expanded_branches": set()}


def enable_interactive_keyboard() -> None:
    global _KEYBOARD_ENABLED, _OLD_TERMIOS
    if _KEYBOARD_ENABLED or not sys.stdin.isatty():
        return
    _OLD_TERMIOS = termios.tcgetattr(sys.stdin.fileno())
    tty.setcbreak(sys.stdin.fileno())
    _KEYBOARD_ENABLED = True
    atexit.register(disable_interactive_keyboard)


def disable_interactive_keyboard() -> None:
    global _KEYBOARD_ENABLED, _OLD_TERMIOS
    if not _KEYBOARD_ENABLED or _OLD_TERMIOS is None:
        return
    termios.tcsetattr(sys.stdin.fileno(), termios.TCSADRAIN, _OLD_TERMIOS)
    _KEYBOARD_ENABLED = False


def _read_key() -> str | None:
    if not _KEYBOARD_ENABLED:
        return None
    readable, _, _ = select.select([sys.stdin], [], [], 0)
    if not readable:
        return None
    char = sys.stdin.read(1)
    if char == "\x1b":
        readable, _, _ = select.select([sys.stdin], [], [], 0)
        if readable and sys.stdin.read(1) == "[":
            readable, _, _ = select.select([sys.stdin], [], [], 0)
            if readable:
                code = sys.stdin.read(1)
                return {"A": "up", "B": "down"}.get(code)
        return "escape"
    if char in {"k", "K"}:
        return "up"
    if char in {"j", "J"}:
        return "down"
    if char in {"\r", "\n", " "}:
        return "toggle"
    return None


def _update_interactive_state(branches: list[str]) -> None:
    if not branches:
        return
    key = _read_key()
    if key == "up":
        _INTERACTIVE_STATE["selected_branch"] = max(0, int(_INTERACTIVE_STATE["selected_branch"]) - 1)
    elif key == "down":
        _INTERACTIVE_STATE["selected_branch"] = min(len(branches) - 1, int(_INTERACTIVE_STATE["selected_branch"]) + 1)
    elif key == "toggle":
        selected = branches[int(_INTERACTIVE_STATE["selected_branch"])]
        expanded = _INTERACTIVE_STATE["expanded_branches"]
        if selected in expanded:
            expanded.remove(selected)
        else:
            expanded.add(selected)
    if int(_INTERACTIVE_STATE["selected_branch"]) >= len(branches):
        _INTERACTIVE_STATE["selected_branch"] = len(branches) - 1


def _metric_status_line(
    metric_id: str,
    status: str,
    duration: float | None = None,
    elapsed: float | None = None,
    expected: float | None = None,
) -> str:
    status_text = colorize_status(status)
    if status == "running" and elapsed is not None and expected is not None:
        return f"{metric_id} [{status_text} | {elapsed:.1f}/{expected:.0f}s ] [{render_metric_activity_bar(elapsed, expected_seconds=expected)}]"
    if duration is not None:
        return f"{metric_id} [{status_text} | run time {duration:.1f}s]"
    return f"{metric_id} [{status_text}]"


def _metric_display_status(metric_id: str, current_metric_id: str, completed_statuses: dict[str, str]) -> str:
    if metric_id in completed_statuses:
        return completed_statuses[metric_id]
    if metric_id == current_metric_id:
        return "running"
    return "pending"


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
    """Render an ANSI-friendly dashboard for ``--display interactive``.

    This avoids optional third-party TUI dependencies while still providing a
    full-screen dashboard with boxed sections, progress, active metrics, branch
    summaries, and recent events.
    """
    running_elapsed = running_elapsed or {}
    terminal_size = shutil.get_terminal_size(fallback=(120, 32))
    width = max(72, terminal_size.columns)
    height = max_lines or terminal_size.lines
    counts = run_state.status_counts()
    total = max(len(run_state.metrics), 1)
    completed = sum(counts.get(status, 0) for status in ("success", "failed", "skipped", "cancelled"))
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

    branch_summaries = run_state.branch_summaries()
    branch_names = list(branch_summaries)
    _INTERACTIVE_STATE["expanded_branches"].update(
        branch
        for branch, branch_counts in branch_summaries.items()
        if branch_counts.get("running") or branch_counts.get("failed") or branch_counts.get("skipped")
    )
    _update_interactive_state(branch_names)
    selected_branch = int(_INTERACTIVE_STATE["selected_branch"])
    expanded_branches = _INTERACTIVE_STATE["expanded_branches"]
    lines.append(_tui_row("Controls: ↑/↓ select branch • Enter/Space expand/collapse • Ctrl-C cancel", width))
    for index, (branch, branch_counts) in enumerate(branch_summaries.items()):
        detail = ", ".join(f"{key} {value}" for key, value in branch_counts.items() if key != "total" and value)
        prefix = "▾" if branch in expanded_branches else "▸"
        selector = ">" if index == selected_branch else " "
        lines.append(_tui_row(f"{selector} {prefix} {branch}: {detail} / {branch_counts['total']} total", width))
        if branch in expanded_branches:
            branch_metrics = [metric for metric in run_state.metrics.values() if metric.branch == branch]
            for metric in branch_metrics[:10]:
                elapsed = running_elapsed.get(metric.metric_id)
                expected = None
                if metric.status == "running":
                    elapsed = elapsed or 0.0
                    expected = max(default_predictions.get(metric.metric_id, 20.0), elapsed + 1.0)
                metric_line = "    " + _metric_status_line(
                    metric.metric_id,
                    metric.status,
                    duration=metric.elapsed_seconds,
                    elapsed=elapsed,
                    expected=expected,
                )
                if metric.missing_fields:
                    metric_line += f" | missing: {', '.join(metric.missing_fields)}"
                if metric.error:
                    metric_line += f" | error: {metric.error}"
                lines.append(_tui_row(metric_line, width))
            if len(branch_metrics) > 10:
                lines.append(_tui_row(f"    … {len(branch_metrics) - 10} more metrics in this branch", width))

    attention = run_state.attention_metrics()
    if attention:
        lines.append(_tui_border(width, "├", "─", "┤", "Active / Attention"))
        for metric in attention[:8]:
            elapsed = running_elapsed.get(metric.metric_id)
            expected = None
            if metric.status == "running":
                elapsed = elapsed or 0.0
                expected = max(default_predictions.get(metric.metric_id, 20.0), elapsed + 1.0)
            line = _metric_status_line(
                metric.metric_id,
                metric.status,
                duration=metric.elapsed_seconds,
                elapsed=elapsed,
                expected=expected,
            )
            if metric.missing_fields:
                line += f" | missing: {', '.join(metric.missing_fields)}"
            if metric.error:
                line += f" | error: {metric.error}"
            lines.append(_tui_row(line, width))

    recent = run_state.recent_completed(limit=6)
    if recent:
        lines.append(_tui_border(width, "├", "─", "┤", "Recently Completed"))
        for metric in recent:
            lines.append(
                _tui_row(
                    _metric_status_line(metric.metric_id, metric.status, duration=metric.elapsed_seconds),
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
        lines.extend("  " + _metric_status_line(metric.metric_id, metric.status, duration=metric.elapsed_seconds) for metric in recent)

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
    status_counts = {"success": 0, "running": 0, "failed": 0, "pending": 0, "stopping": 0, "cancelled": 0}
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
            {"total": 0, "success": 0, "running": 0, "failed": 0, "pending": 0, "stopping": 0, "cancelled": 0},
        )
        branch_counts[branch]["total"] += 1
        branch_counts[branch][status] = branch_counts[branch].get(status, 0) + 1

        if status in {"running", "failed", "stopping", "cancelled"}:
            expected = max(
                completed_durations.get(metric_id, default_predictions.get(metric_id, predicted_metric_total)),
                run_elapsed + 1.0,
            )
            attention.append("  " + _metric_status_line(metric_id, status, completed_durations.get(metric_id), run_elapsed, expected))
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
        path = metric.get('taxonomy_path', [])
        for depth in range(len(path)):
            node_tuple = tuple(path[: depth + 1])
            if node_tuple in printed_nodes:
                continue
            printed_nodes.add(node_tuple)
            lines.append(f"{'  ' * depth}↳ {path[depth]}")
        metric_id = metric.get('metric_id', 'unknown_metric')
        metric_prediction = completed_durations.get(metric_id, default_predictions.get(metric_id, predicted_metric_total))
        if metric_id in completed_statuses and completed_statuses[metric_id] == "running":
            run_elapsed = (running_elapsed or {}).get(metric_id, 0.0)
            expected = max(metric_prediction, run_elapsed + 1.0)
            suffix = f" [{colorize_status('running')} | {run_elapsed:.1f}/{expected:.0f}s ] [{render_metric_activity_bar(run_elapsed, expected_seconds=expected)}]"
        elif metric_id in completed_statuses:
            run_time = completed_durations.get(metric_id)
            status_text = colorize_status(completed_statuses[metric_id])
            suffix = f" [{status_text} | run time {run_time:.1f}s]" if run_time is not None else f" [{status_text}]"
        elif metric_id == current_metric_id:
            if completed:
                suffix = f" [{colorize_status('success')}] | done in {elapsed:.1f}s"
            elif elapsed is not None:
                suffix = f" [{colorize_status('running')} | {elapsed:.1f}/{predicted_metric_total:.0f}s ] [{render_metric_activity_bar(elapsed, expected_seconds=predicted_metric_total)}]"
            else:
                suffix = f" [{colorize_status('running')}]"
        else:
            suffix = f" [{colorize_status('pending')} | 0.0/{metric_prediction:.0f}s]"
        lines.append(f"{'  ' * len(path)}↳ {metric_id}{suffix}")
    return '\n'.join(lines)
