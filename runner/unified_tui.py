from __future__ import annotations

import curses
from pathlib import Path

from runner.tui import launch_tui
from runner.tui_batch import launch_batch_tui


TUI_MODES = ("Single dataset run", "Batch / comparison run")


def _choose_mode_curses(stdscr) -> str | None:
    curses.curs_set(0)
    selected = 0
    while True:
        stdscr.erase()
        height, width = stdscr.getmaxyx()
        stdscr.addstr(0, 0, "CBR Test Runner"[: width - 1], curses.A_BOLD)
        stdscr.addstr(1, 0, "Choose how you want to run the tests."[: width - 1])
        stdscr.addstr(2, 0, "↑/↓ move  Enter select  q quit"[: width - 1])
        descriptions = (
            "Run one dataset using an existing case or plan.",
            "Select multiple candidate datasets and optional references, build the comparison matrix, and run it.",
        )
        for index, mode in enumerate(TUI_MODES):
            row = 4 + index * 3
            marker = ">" if index == selected else " "
            attr = curses.A_REVERSE if index == selected else curses.A_NORMAL
            if row < height:
                stdscr.addstr(row, 0, f"{marker} {mode}"[: width - 1], attr)
            if row + 1 < height:
                stdscr.addstr(row + 1, 4, descriptions[index][: max(1, width - 5)])
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
    """Launch the single-run TUI or the batch/comparison TUI from one entry point."""

    mode = curses.wrapper(_choose_mode_curses)
    if mode is None:
        raise SystemExit("TUI cancelled")
    if mode == "single":
        return launch_tui(args, repo_root=repo_root)

    args.tui_batch_spec = launch_batch_tui(args, repo_root=repo_root)
    args.tui = False
    return args
