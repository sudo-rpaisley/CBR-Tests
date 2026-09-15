from __future__ import annotations

import curses
from pathlib import Path

from runner.friendly_tui import launch_friendly_batch_tui, launch_single_tui


# Preserve these historical module-level hooks so existing tests and callers can
# monkeypatch them without knowing which presentation layer is currently used.
launch_tui = launch_single_tui
launch_batch_tui = launch_friendly_batch_tui

TUI_MODES = ("Single dataset run", "Batch / comparison run")


def _safe_addstr(stdscr, y: int, x: int, text: str, attr: int = 0) -> None:
    height, width = stdscr.getmaxyx()
    if y < 0 or y >= height or x < 0 or x >= width:
        return
    available = max(0, width - x - 1)
    if available <= 0:
        return
    try:
        stdscr.addstr(y, x, text[:available], attr)
    except curses.error:
        pass


def _choose_mode_curses(stdscr) -> str | None:
    curses.curs_set(0)
    selected = 0
    descriptions = (
        "Configure and run one dataset using a case or plan.",
        "Select several candidate datasets and optional references, then build the comparison matrix automatically.",
    )
    while True:
        stdscr.erase()
        height, width = stdscr.getmaxyx()
        _safe_addstr(stdscr, 0, 0, "CBR Tests", curses.A_BOLD)
        _safe_addstr(stdscr, 1, 0, "What would you like to do?")
        _safe_addstr(stdscr, 2, 0, "↑/↓ move   Enter select   q/Esc quit")
        for index, mode in enumerate(TUI_MODES):
            row = 4 + index * 4
            marker = ">" if index == selected else " "
            attr = curses.A_REVERSE if index == selected else curses.A_NORMAL
            _safe_addstr(stdscr, row, 0, f"{marker} {mode}", attr)
            _safe_addstr(stdscr, row + 1, 4, descriptions[index])
            if index == 0:
                _safe_addstr(stdscr, row + 2, 4, "Best for checking or running one prepared experiment plan.")
            else:
                _safe_addstr(stdscr, row + 2, 4, "Best for experiment matrices and reference comparisons.")
        if height > 14:
            _safe_addstr(stdscr, height - 2, 0, "The next screen shows only essential settings by default; press a for advanced options.")

        key = stdscr.getch()
        if key in (ord("q"), ord("Q"), 27):
            return None
        if key in (curses.KEY_UP, ord("k")):
            selected = max(0, selected - 1)
        elif key in (curses.KEY_DOWN, ord("j")):
            selected = min(len(TUI_MODES) - 1, selected + 1)
        elif key in (10, 13):
            return "single" if selected == 0 else "batch"


def launch_unified_tui(args, repo_root: Path | None = None):
    """Launch the guided single-run or batch/comparison terminal UI."""

    mode = curses.wrapper(_choose_mode_curses)
    if mode is None:
        raise SystemExit("TUI cancelled")
    if mode == "single":
        return launch_tui(args, repo_root=repo_root)

    args.tui_batch_spec = launch_batch_tui(args, repo_root=repo_root)
    args.tui = False
    return args
