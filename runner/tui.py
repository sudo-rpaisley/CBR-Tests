from __future__ import annotations

import curses
from curses import textpad
from dataclasses import dataclass
from pathlib import Path


DISPLAY_MODES = ("compact", "full", "quiet", "interactive")


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
        TuiField("case", "Case / plan", "choice", args.case or (case_choices[0] if case_choices else ""), case_choices, "Required. Choose a case JSON or plan JSON."),
        TuiField("dataset", "Dataset", "file", args.dataset or "", (), "Required for plan JSON; press Enter to browse files or e to type."),
        TuiField("output", "Output", "text", args.output or "outcomes/outcome_tui.json", (), "Required for plan JSON; case JSON may provide it."),
        TuiField("case_id", "Case id", "text", args.case_id or "ad_hoc_case", (), "Used when running a plan JSON directly."),
        TuiField("display", "Display", "choice", args.display or "interactive", DISPLAY_MODES, "Live output mode after launch."),
        TuiField("workers", "Workers", "int", "" if args.workers is None else str(args.workers), (), "Blank auto-selects workers; 1 forces serial."),
        TuiField("taxonomy_file", "Taxonomy file", "choice", args.taxonomy_file or "", taxonomy_choices, "Optional metric order file."),
        TuiField("taxonomy_strict", "Strict taxonomy", "bool", bool(args.taxonomy_strict), (), "Fail if enabled metrics are absent from taxonomy order."),
        TuiField("field_translation", "Field translation", "choice", args.field_translation or "", translation_choices, "Optional dataset field mapping JSON."),
        TuiField("no_update_field_translation", "No sidecar update", "bool", bool(args.no_update_field_translation), (), "Do not create/update sidecar templates."),
        TuiField("yes_field_translation_sidecar", "Auto sidecar yes", "bool", bool(args.yes_field_translation_sidecar), (), "Create/update sidecar templates without prompting."),
        TuiField("field_translation_dry_run", "Translation dry-run", "bool", bool(args.field_translation_dry_run), (), "Validate translations and exit before metrics run."),
        TuiField("field_translation_report", "JSON report", "text", args.field_translation_report or "", (), "Optional field translation validation JSON report."),
        TuiField("field_translation_text_report", "Text report", "text", args.field_translation_text_report or "", (), "Optional human-readable field translation report."),
        TuiField("field_translation_markdown_report", "Markdown report", "text", args.field_translation_markdown_report or "", (), "Optional Markdown field translation report."),
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
        return "[x]" if field.value else "[ ]"
    return str(field.value or "")


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
        stdscr.addstr(1, 0, f"Directory: {_display_path(current_dir, root)}"[: width - 1])
        stdscr.addstr(2, 0, "↑/↓ move  Enter open/select  Backspace parent  e type path  q cancel"[: width - 1])
        visible_rows = max(1, height - 5)
        start = min(max(0, selected - visible_rows + 1), max(0, len(entries) - visible_rows))
        for row, entry in enumerate(entries[start : start + visible_rows], start=4):
            index = start + row - 4
            marker = ">" if index == selected else " "
            attr = curses.A_REVERSE if index == selected else curses.A_NORMAL
            stdscr.addstr(row, 0, f"{marker} {entry.label}"[: width - 1], attr)
        if not entries:
            stdscr.addstr(4, 0, "No files in this directory."[: width - 1])
        key = stdscr.getch()
        if key in (ord("q"), 27):
            return None
        if key in (ord("e"),):
            return _edit_text(stdscr, min(height - 2, 4), 0, initial, max(8, width - 1))
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
    message = "↑/↓ move  Enter edit/cycle/browse  e type path  Space toggle  r run  q quit"
    while True:
        stdscr.erase()
        height, width = stdscr.getmaxyx()
        stdscr.addstr(0, 0, "CBR Test Runner TUI"[: width - 1], curses.A_BOLD)
        stdscr.addstr(1, 0, message[: width - 1])
        visible_rows = max(1, height - 5)
        start = min(max(0, selected - visible_rows + 1), max(0, len(fields) - visible_rows))
        for row, field in enumerate(fields[start : start + visible_rows], start=3):
            marker = ">" if start + row - 3 == selected else " "
            line = f"{marker} {field.label:24} {_format_value(field)}"
            attr = curses.A_REVERSE if start + row - 3 == selected else curses.A_NORMAL
            stdscr.addstr(row, 0, line[: width - 1], attr)
        help_line = fields[selected].help if fields else ""
        stdscr.addstr(height - 1, 0, help_line[: width - 1])
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
