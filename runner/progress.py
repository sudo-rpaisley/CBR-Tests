from __future__ import annotations
import os
import sys

ANSI_RESET = "\x1b[0m"
ANSI_COLORS = {
    "success": "\x1b[32m",
    "running": "\x1b[34m",
    "pending": "\x1b[33m",
    "stopping": "\x1b[33m",
    "failed": "\x1b[31m",
    "cancelled": "\x1b[31m",
}
LIVE_HEADER_LINES: list[str] = []
LIVE_RENDERED_LINE_COUNT = 0


def supports_color() -> bool:
    return sys.stdout.isatty() and os.environ.get("TERM", "").lower() not in {"", "dumb"} and "NO_COLOR" not in os.environ


def colorize_status(status: str) -> str:
    if not supports_color():
        return status
    color = ANSI_COLORS.get(status.lower())
    if not color:
        return status
    return f"{color}{status}{ANSI_RESET}"


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
    global LIVE_RENDERED_LINE_COUNT
    header_block = "\n".join(LIVE_HEADER_LINES).rstrip()
    overall_line = overall_line or ""
    if not sys.stdout.isatty() or not supports_color():
        if header_block:
            print(header_block)
        print(task_line)
        if overall_line:
            print(overall_line)
        if warning_line is not None:
            print(warning_line)
        LIVE_RENDERED_LINE_COUNT = 0
        return
    block_lines = task_line.splitlines()
    if overall_line:
        block_lines.append(overall_line)
    if warning_line is not None:
        block_lines.append(warning_line)
    lines_to_render = []
    if header_block:
        lines_to_render.extend(header_block.splitlines())
    lines_to_render.extend(block_lines)
    if LIVE_RENDERED_LINE_COUNT > 0:
        print(f"\x1b[{LIVE_RENDERED_LINE_COUNT}F", end="")
        for _ in range(LIVE_RENDERED_LINE_COUNT):
            print("\x1b[2K\x1b[1E", end="")
        print(f"\x1b[{LIVE_RENDERED_LINE_COUNT}F", end="")
    print("\n".join(lines_to_render), end="", flush=True)
    LIVE_RENDERED_LINE_COUNT = max(1, len(lines_to_render))


def set_live_header(lines: list[str]) -> None:
    global LIVE_HEADER_LINES
    LIVE_HEADER_LINES = list(lines)
