from __future__ import annotations

import os
import re
import sys
from datetime import datetime, timezone

from runner.progress import set_live_output_enabled
from runner.run_plan_helpers import build_title_box_lines

DISPLAY_FALLBACK_NOTICE = "Interactive display requested; using compact live view until the Textual TUI is available."


def configure_display(args) -> tuple[bool, str, int | None]:
    """Configure live output and return render-enabled, mode, and max-line settings."""
    live_render_enabled = (
        args.display != "quiet"
        and sys.stdout.isatty()
        and os.environ.get("TERM", "").lower() not in {"", "dumb"}
    )
    display_mode = "compact" if args.display == "interactive" else args.display
    display_max_lines = 24 if display_mode == "compact" else None
    set_live_output_enabled(args.display != "quiet")
    if args.display == "interactive":
        print(DISPLAY_FALLBACK_NOTICE)
    return live_render_enabled, display_mode, display_max_lines


def compact_overall_progress_line(overall_header: str) -> str:
    """Return the short version of the overall progress line used in live headers."""
    compact = overall_header.replace("Overall  ", "", 1)
    return re.sub(r"\s+\(\d+/\d+\)", "", compact, count=1)


def print_title_box(lines: list[str]) -> None:
    """Print a boxed title/header block."""
    for line in build_title_box_lines(lines):
        print(line)


def print_phase_status(phase: str, detail: str = "") -> None:
    """Print a timestamped phase status line."""
    timestamp = datetime.now(timezone.utc).strftime("%H:%M:%S")
    suffix = f" | {detail}" if detail else ""
    print(f"[{timestamp}] {phase}{suffix}")
