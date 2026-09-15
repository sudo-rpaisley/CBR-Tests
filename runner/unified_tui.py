from __future__ import annotations

import curses
from pathlib import Path

from runner.campaign_tui import campaign_toolbox_items, run_campaign_tool_action
from runner.fixed_plan_matrix_tui import fixed_plan_matrix_toolbox_item, launch_fixed_plan_matrix_builder
from runner.friendly_tui import launch_friendly_batch_tui, launch_single_tui
from runner.toolbox_tui import ToolboxItem, run_tool_action, toolbox_items


# Preserve these historical module-level hooks so existing tests and callers can
# monkeypatch them without knowing which presentation layer is currently used.
launch_tui = launch_single_tui
launch_batch_tui = launch_friendly_batch_tui


def _menu_items() -> tuple[ToolboxItem, ...]:
    """Return user-facing toolbox entries, including guided workflow shortcuts."""

    items = list(toolbox_items())
    campaign_items = {item.key: item for item in campaign_toolbox_items()}

    batch_index = next((index for index, item in enumerate(items) if item.key == "batch"), 1)
    items.insert(batch_index + 1, campaign_items["campaign_run"])

    build_index = next((index for index, item in enumerate(items) if item.key == "build_plan"), len(items) - 1)
    items.insert(build_index + 1, fixed_plan_matrix_toolbox_item())
    items.insert(build_index + 2, campaign_items["campaign_build"])

    shortcut = ToolboxItem(
        "field_mapping",
        "Validate / map dataset fields",
        "Run field-translation preflight and map missing canonical fields before an experiment.",
        "Prepare experiments",
    )
    build_index = next((index for index, item in enumerate(items) if item.key == "campaign_build"), len(items) - 1)
    items.insert(build_index + 1, shortcut)
    return tuple(items)


TUI_MODES = tuple(item.title for item in _menu_items())


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


def _tool_rows() -> list[tuple[int | None, str, str]]:
    rows: list[tuple[int | None, str, str]] = []
    previous_group: str | None = None
    for index, item in enumerate(_menu_items()):
        if item.group != previous_group:
            rows.append((None, item.group.upper(), ""))
            previous_group = item.group
        rows.append((index, item.title, item.description))
    return rows


def _choose_mode_curses(stdscr) -> str | None:
    curses.curs_set(0)
    selected = 0
    items = _menu_items()
    rows = _tool_rows()
    while True:
        stdscr.erase()
        height, _ = stdscr.getmaxyx()
        _safe_addstr(stdscr, 0, 0, "CBR Tests Toolbox", curses.A_BOLD)
        _safe_addstr(stdscr, 1, 0, "Run experiments, build plans, map datasets, queue matrices, inspect results and access maintenance tools from one place.")
        _safe_addstr(stdscr, 2, 0, "↑/↓ move   Enter open   PgUp/PgDn page   q/Esc quit")

        selected_row = next((i for i, (index, _, _) in enumerate(rows) if index == selected), 0)
        visible_height = max(1, height - 5)
        start = min(max(0, selected_row - visible_height + 1), max(0, len(rows) - visible_height))
        for screen_row, (index, title, description) in enumerate(rows[start : start + visible_height], start=4):
            if index is None:
                _safe_addstr(stdscr, screen_row, 0, title, curses.A_BOLD)
                continue
            marker = ">" if index == selected else " "
            attr = curses.A_REVERSE if index == selected else curses.A_NORMAL
            _safe_addstr(stdscr, screen_row, 0, f"{marker} {title}", attr)
            if description and screen_row + 1 < height:
                pass

        if items:
            _safe_addstr(stdscr, height - 1, 0, items[selected].description, curses.A_BOLD)

        key = stdscr.getch()
        if key in (ord("q"), ord("Q"), 27):
            return None
        if key in (curses.KEY_UP, ord("k")):
            selected = max(0, selected - 1)
        elif key in (curses.KEY_DOWN, ord("j")):
            selected = min(len(items) - 1, selected + 1)
        elif key == curses.KEY_PPAGE:
            selected = max(0, selected - visible_height)
        elif key == curses.KEY_NPAGE:
            selected = min(len(items) - 1, selected + visible_height)
        elif key in (10, 13):
            return items[selected].key


def _pause_after_tool() -> None:
    try:
        input("\nPress Enter to return to the CBR Tests Toolbox...")
    except EOFError:
        pass


def launch_unified_tui(args, repo_root: Path | None = None):
    """Launch the CBR-Tests toolbox and return only when a run workflow is selected."""

    root = (repo_root or Path.cwd()).expanduser().resolve()
    campaign_actions = {item.key for item in campaign_toolbox_items()}
    while True:
        mode = curses.wrapper(_choose_mode_curses)
        if mode is None:
            raise SystemExit("TUI cancelled")
        if mode == "single":
            return launch_tui(args, repo_root=root)
        if mode == "field_mapping":
            args.field_translation_dry_run = True
            return launch_tui(args, repo_root=root)
        if mode == "batch":
            args.tui_batch_spec = launch_batch_tui(args, repo_root=root)
            args.tui = False
            return args

        try:
            if mode == "fixed_pcap_matrix":
                launch_fixed_plan_matrix_builder(root)
            elif mode in campaign_actions:
                run_campaign_tool_action(mode, root)
            else:
                run_tool_action(mode, root)
        except (OSError, ValueError, RuntimeError) as exc:
            print(f"\nTool failed: {exc}")
        _pause_after_tool()
