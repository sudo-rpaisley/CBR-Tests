from __future__ import annotations
import os
import sys


def render_metric_activity_bar(elapsed: float, expected_seconds: float = 60.0, width: int = 12) -> str:
    if width < 3:
        width = 3
    if expected_seconds <= 0:
        expected_seconds = 60.0
    progress = min(elapsed / expected_seconds, 0.99)
    filled = max(1, int(progress * width))
    return "#" * filled + "-" * (width - filled)


def render_overall_progress_line(current: int, total: int, run_elapsed: float | None = None, in_metric_elapsed: float | None = None) -> str:
    total = max(total, 1)
    width = 30
    filled = int(width * current / total)
    bar = "#" * filled + "-" * (width - filled)
    pct = int((current / total) * 100)
    suffix = ""
    if run_elapsed is not None:
        metric_fraction = 0.0
        if in_metric_elapsed is not None:
            metric_fraction = min(in_metric_elapsed / 60.0, 0.99)
        progress_fraction = min(((current - 1) + metric_fraction) / total, 0.999)
        if progress_fraction > 0:
            predicted_total = run_elapsed / progress_fraction
            suffix = f" | {int(run_elapsed)}/{int(predicted_total)}s"
    return f"Overall  [{bar}] {pct:3d}% ({current}/{total}){suffix}"


def print_live_status(task_line: str, overall_line: str, warning_line: str | None = None) -> None:
    if not sys.stdout.isatty():
        if warning_line is not None:
            print(f"{overall_line} | {warning_line}")
        return
    if os.environ.get("TERM", "").lower() in {"", "dumb"}:
        return
    block_lines = task_line.splitlines() + [overall_line]
    if warning_line is not None:
        block_lines.append(warning_line)
    print("\x1b[H\x1b[J", end="")
    print("\n".join(block_lines), end="", flush=True)
