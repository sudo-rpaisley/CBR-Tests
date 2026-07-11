from __future__ import annotations

import curses
import os
from curses import textpad
from dataclasses import dataclass
from pathlib import Path


DISPLAY_MODES = ("compact", "full", "quiet", "interactive")


def detected_max_workers() -> int:
    return max(1, os.cpu_count() or 1)


@dataclass
class FileBrowserEntry:
    label: str
    path: Path
    is_dir: bool


@dataclass
class TuiField:
    name: str
    label: str
    kind: str
    value: object = None
    choices: tuple[str, ...] = ()
    help: str = ""
    section: str = "Run setup"


def _discover_files(root: Path, patterns: tuple[str, ...]) -> tuple[str, ...]:
    paths: list[str] = []
    for pattern in patterns:
        paths.extend(str(path.relative_to(root)) for path in root.glob(pattern) if path.is_file())
    return tuple(sorted(dict.fromkeys(paths)))


def _display_path(path: Path, root: Path) -> str:
    path = path.expanduser()
    try:
        return str(path.resolve().relative_to(root.resolve()))
    except ValueError:
        return str(path)


def list_file_browser_entries(directory: Path, root: Path) -> list[FileBrowserEntry]:
    directory = directory.expanduser().resolve()
    root = root.expanduser().resolve()
    entries: list[FileBrowserEntry] = []
    if directory != root and directory.parent != directory:
        entries.append(FileBrowserEntry("../", directory.parent, True))
    for path in sorted(directory.iterdir(), key=lambda item: (not item.is_dir(), item.name.lower())):
        if path.name in {".git", "__pycache__"}:
            continue
        suffix = "/" if path.is_dir() else ""
        entries.append(FileBrowserEntry(f"{path.name}{suffix}", path, path.is_dir()))
    return entries


def build_default_tui_fields(args, repo_root: Path | None = None) -> list[TuiField]:
    root = repo_root or Path.cwd()
    case_choices = _discover_files(root, ("cases/*.json", "plans/*.json"))
    taxonomy_choices = ("",) + _discover_files(root, ("taxonomy/*.json", "plans/*taxonomy*.json"))
    translation_choices = ("",) + _discover_files(root, ("examples/field_translations/*.json",))
    return [
        TuiField("case", "Case or plan JSON", "choice", args.case or (case_choices[0] if case_choices else ""), case_choices, "Pick a ready-to-run case, or pick a direct plan and provide dataset/output below.", "Required inputs"),
        TuiField("dataset", "Dataset file", "file", args.dataset or "", (), "Browse to the CSV/TSV/XLSX/PCAP file to test. Case files may already provide this.", "Required inputs"),
        TuiField("output", "Outcome JSON", "text", args.output or "outcomes/outcome_tui.json", (), "Where the run result JSON should be written. Case files may already provide this.", "Required inputs"),
        TuiField("case_id", "Ad-hoc case ID", "text", args.case_id or "ad_hoc_case", (), "Label written to the outcome when you run a plan directly instead of a case.", "Required inputs"),
        TuiField("display", "Live display mode", "choice", args.display or "interactive", DISPLAY_MODES, "Choose how much progress detail to show after the run starts.", "Execution"),
        TuiField("workers", "Worker count", "int", "" if args.workers is None else str(args.workers), (), "Leave blank for automatic worker selection; enter 1 for serial execution. The detected maximum is shown beside this field.", "Execution"),
        TuiField("taxonomy_file", "Metric order file", "choice", args.taxonomy_file or "", taxonomy_choices, "Optional taxonomy JSON that controls metric ordering.", "Taxonomy"),
        TuiField("taxonomy_strict", "Require taxonomy coverage", "bool", bool(args.taxonomy_strict), (), "When enabled, fail if the taxonomy order omits enabled metrics.", "Taxonomy"),
        TuiField("field_translation", "Field translation JSON", "choice", args.field_translation or "", translation_choices, "Optional mapping from dataset column names to canonical test field names.", "Field translation"),
        TuiField("no_update_field_translation", "Never update sidecar", "bool", bool(args.no_update_field_translation), (), "Do not create or modify dataset sidecar translation templates.", "Field translation"),
        TuiField("yes_field_translation_sidecar", "Auto-create sidecar", "bool", bool(args.yes_field_translation_sidecar), (), "Allow sidecar template creation/update without an extra prompt.", "Field translation"),
        TuiField("field_translation_dry_run", "Validate fields only", "bool", bool(args.field_translation_dry_run), (), "Check field mappings and reports, then stop before running metrics.", "Field translation"),
        TuiField("field_translation_report", "Field report JSON", "text", args.field_translation_report or "", (), "Optional machine-readable field translation validation report path.", "Reports"),
        TuiField("field_translation_text_report", "Field report text", "text", args.field_translation_text_report or "", (), "Optional human-readable field translation report path.", "Reports"),
        TuiField("field_translation_markdown_report", "Field report Markdown", "text", args.field_translation_markdown_report or "", (), "Optional Markdown field translation report path.", "Reports"),
    ]


def apply_tui_fields(args, fields: list[TuiField]):
    values = {field.name: field.value for field in fields}
    for key, value in values.items():
        if key == "workers":
            value = None if str(value).strip() == "" else int(value)
        elif value == "" and key not in {"case", "dataset", "output", "case_id", "display"}:
            value = None
        setattr(args, key, value)
    args.tui = False
    return args


def validate_required_run_args(args) -> None:
    if not args.case:
        raise SystemExit("error: --case is required unless selected in --tui")


def _format_value(field: TuiField) -> str:
    if field.kind == "bool":
        return "Yes" if field.value else "No"
    if field.name == "workers":
        value = str(field.value or "").strip() or "auto"
        return f"{value} / max {detected_max_workers()} detected"
    value = str(field.value or "")
    return value if value else "(blank)"


def field_action_hint(field: TuiField) -> str:
    if field.kind == "bool":
        return "Press Space or Enter to toggle Yes/No."
    if field.kind == "choice":
        return "Press Enter to cycle through available choices."
    if field.kind == "file":
        return "Press Enter to browse files, or press e to type/paste a path."
    return "Press Enter or e to type a value."


def describe_tui_field(field: TuiField) -> list[str]:
    return [
        f"Section: {field.section}",
        f"Selected: {field.label}",
        f"Does: {field.help}",
        f"How: {field_action_hint(field)}",
    ]


def _edit_text(stdscr, y: int, x: int, initial: str, width: int) -> str:
    win = curses.newwin(1, max(4, width), y, x)
    win.addstr(0, 0, initial[: width - 1])
    curses.curs_set(1)
    box = textpad.Textbox(win, insert_mode=True)
    value = box.edit().strip()
    curses.curs_set(0)
    return value


def _initial_browser_directory(initial: str, root: Path) -> Path:
    if initial:
        candidate = Path(initial).expanduser()
        if not candidate.is_absolute():
            candidate = root / candidate
        if candidate.is_file():
            return candidate.parent.resolve()
        if candidate.is_dir():
            return candidate.resolve()
    return root.resolve()


def _browse_file(stdscr, root: Path, initial: str) -> str | None:
    root = root.expanduser().resolve()
    current_dir = _initial_browser_directory(initial, root)
    selected = 0
    while True:
        stdscr.erase()
        height, width = stdscr.getmaxyx()
        entries = list_file_browser_entries(current_dir, root)
        if selected >= len(entries):
            selected = max(0, len(entries) - 1)
        stdscr.addstr(0, 0, "Dataset file explorer"[: width - 1], curses.A_BOLD)
        stdscr.addstr(1, 0, "Pick the dataset file to test. Directories end with /."[: width - 1])
        stdscr.addstr(2, 0, f"Directory: {_display_path(current_dir, root)}"[: width - 1])
        stdscr.addstr(3, 0, "↑/↓ move  Enter open/select  Backspace parent  e type path  q cancel"[: width - 1])
        visible_rows = max(1, height - 6)
        start = min(max(0, selected - visible_rows + 1), max(0, len(entries) - visible_rows))
        for row, entry in enumerate(entries[start : start + visible_rows], start=5):
            index = start + row - 5
            marker = ">" if index == selected else " "
            attr = curses.A_REVERSE if index == selected else curses.A_NORMAL
            stdscr.addstr(row, 0, f"{marker} {entry.label}"[: width - 1], attr)
        if not entries:
            stdscr.addstr(5, 0, "No files in this directory."[: width - 1])
        key = stdscr.getch()
        if key in (ord("q"), 27):
            return None
        if key in (ord("e"),):
            return _edit_text(stdscr, min(height - 2, 5), 0, initial, max(8, width - 1))
        if key in (curses.KEY_BACKSPACE, 127, 8):
            if current_dir != root and current_dir.parent != current_dir:
                current_dir = current_dir.parent.resolve()
                selected = 0
        elif key in (curses.KEY_UP, ord("k")):
            selected = max(0, selected - 1)
        elif key in (curses.KEY_DOWN, ord("j")):
            selected = min(max(0, len(entries) - 1), selected + 1)
        elif key in (10, 13) and entries:
            entry = entries[selected]
            if entry.is_dir:
                current_dir = entry.path.resolve()
                selected = 0
            else:
                return _display_path(entry.path, root)


def _run_curses(stdscr, fields: list[TuiField]) -> list[TuiField] | None:
    curses.curs_set(0)
    selected = 0
    repo_root = Path.cwd()
    message = "↑/↓ move  Enter act on selected field  e type paths/text  Space toggle  r run  q quit"
    while True:
        stdscr.erase()
        height, width = stdscr.getmaxyx()
        stdscr.addstr(0, 0, "CBR Test Runner TUI"[: width - 1], curses.A_BOLD)
        stdscr.addstr(1, 0, "Configure the run from top to bottom, then press r to start."[: width - 1])
        stdscr.addstr(2, 0, message[: width - 1])
        footer_height = 5
        visible_rows = max(1, height - footer_height - 4)
        start = min(max(0, selected - visible_rows + 1), max(0, len(fields) - visible_rows))
        for row, field in enumerate(fields[start : start + visible_rows], start=4):
            index = start + row - 4
            marker = ">" if index == selected else " "
            line = f"{marker} [{field.section}] {field.label:24} {_format_value(field)}"
            attr = curses.A_REVERSE if index == selected else curses.A_NORMAL
            stdscr.addstr(row, 0, line[: width - 1], attr)
        footer_start = max(4, height - footer_height)
        stdscr.hline(footer_start, 0, curses.ACS_HLINE, max(1, width - 1))
        for offset, line in enumerate(describe_tui_field(fields[selected]) if fields else []):
            stdscr.addstr(footer_start + offset + 1, 0, line[: width - 1])
        key = stdscr.getch()
        if key in (ord("q"), 27):
            return None
        if key == ord("r"):
            return fields
        if key in (curses.KEY_UP, ord("k")):
            selected = max(0, selected - 1)
        elif key in (curses.KEY_DOWN, ord("j")):
            selected = min(len(fields) - 1, selected + 1)
        elif key == ord(" ") and fields[selected].kind == "bool":
            fields[selected].value = not fields[selected].value
        elif key == ord("e") and fields[selected].kind in {"file", "text", "int"}:
            field = fields[selected]
            field.value = _edit_text(stdscr, min(height - 2, selected - start + 3), 29, str(field.value or ""), max(8, width - 30))
        elif key in (10, 13):
            field = fields[selected]
            if field.kind == "bool":
                field.value = not field.value
            elif field.kind == "choice" and field.choices:
                idx = field.choices.index(field.value) if field.value in field.choices else -1
                field.value = field.choices[(idx + 1) % len(field.choices)]
            elif field.kind == "file":
                browsed = _browse_file(stdscr, repo_root, str(field.value or ""))
                if browsed is not None:
                    field.value = browsed
            else:
                field.value = _edit_text(stdscr, min(height - 2, selected - start + 3), 29, str(field.value or ""), max(8, width - 30))


def launch_tui(args, repo_root: Path | None = None):
    fields = build_default_tui_fields(args, repo_root=repo_root)
    selected = curses.wrapper(_run_curses, fields)
    if selected is None:
        raise SystemExit("TUI cancelled")
    return apply_tui_fields(args, selected)


def _result_lines(result: dict | None, args) -> list[str]:
    result = result or {}
    title = "Dry run complete" if result.get("dry_run") else "Run complete"
    lines = [title]
    if result.get("status"):
        lines.append(f"Status: {result['status']}")
    if result.get("output_path"):
        lines.append(f"Outcome: {result['output_path']}")
    if result.get("metrics_total") is not None:
        lines.append(f"Metrics: {result.get('metrics_total', 0)} total")
    if result.get("skipped_count"):
        lines.append(f"Attention: {result['skipped_count']} metric(s) skipped or blocked by missing fields")
    elif result.get("dry_run"):
        lines.append("Dry-run validation did not report missing required fields.")
    lines.append("")
    lines.append("Enter/m: back to setup menu")
    if result.get("dry_run"):
        lines.append("r: run now using these settings")
    lines.append("q: quit program")
    return lines


def _confirm_run_after_errors(stdscr, skipped_count: int) -> bool:
    while True:
        stdscr.erase()
        height, width = stdscr.getmaxyx()
        lines = [
            "Dry run found issues",
            f"{skipped_count} metric(s) may be skipped or blocked by missing fields.",
            "Run anyway?",
            "y: yes, run metrics now",
            "n/q/Esc: no, return to results",
        ]
        for row, line in enumerate(lines[:height]):
            stdscr.addstr(row, 0, line[: width - 1], curses.A_BOLD if row == 0 else curses.A_NORMAL)
        key = stdscr.getch()
        if key in (ord("y"), ord("Y")):
            return True
        if key in (ord("n"), ord("N"), ord("q"), 27):
            return False


def _post_run_curses(stdscr, result: dict | None, args) -> str:
    while True:
        stdscr.erase()
        height, width = stdscr.getmaxyx()
        lines = _result_lines(result, args)
        for row, line in enumerate(lines[:height]):
            attr = curses.A_BOLD if row == 0 else curses.A_NORMAL
            stdscr.addstr(row, 0, line[: width - 1], attr)
        key = stdscr.getch()
        if key in (10, 13, ord("m"), ord("M")):
            return "menu"
        if key in (ord("q"), ord("Q"), 27):
            return "quit"
        if result and result.get("dry_run") and key in (ord("r"), ord("R")):
            skipped_count = int(result.get("skipped_count") or 0)
            if skipped_count and not _confirm_run_after_errors(stdscr, skipped_count):
                continue
            return "run"


def show_post_run_menu(result: dict | None, args) -> str:
    return curses.wrapper(_post_run_curses, result, args)
