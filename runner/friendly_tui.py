from __future__ import annotations

import curses
import json
from pathlib import Path
from typing import Any

from runner.tui import (
    DISPLAY_MODES,
    TuiField,
    _display_path,
    _edit_text,
    _format_value,
    apply_tui_fields,
    build_default_tui_fields,
    default_outcome_path,
    describe_tui_field,
    list_file_browser_entries,
)
from runner.tui_batch import (
    SUPPORTED_DATASET_SUFFIXES,
    _format_list,
    _multi_file_browser,
    build_batch_spec,
    comparison_job_count,
    default_batch_run_id,
)


_SINGLE_ESSENTIAL_FIELDS = (
    "case",
    "dataset",
    "experiment_mode",
    "field_translation_dry_run",
    "display",
)
_BATCH_ESSENTIAL_FIELDS = (
    "name",
    "datasets",
    "references",
    "metric_policy",
)


def build_friendly_single_fields(args, repo_root: Path | None = None) -> list[TuiField]:
    """Build the normal run fields plus the strict final-experiment switch.

    The existing field model is deliberately reused so this UI is only a presentation
    layer over the established runner configuration.
    """

    fields = build_default_tui_fields(args, repo_root=repo_root)
    experiment = TuiField(
        "experiment_mode",
        "Strict experiment mode",
        "bool",
        bool(getattr(args, "experiment_mode", False)),
        (),
        (
            "Require the canonical final-experiment contract before metrics run. "
            "Use this for authoritative experiment runs; historical plans may be rejected."
        ),
        "Run safety",
    )
    insert_at = next((index + 1 for index, field in enumerate(fields) if field.name == "dataset"), 0)
    fields.insert(insert_at, experiment)
    return fields


def visible_single_fields(fields: list[TuiField], show_advanced: bool) -> list[TuiField]:
    if show_advanced:
        return list(fields)
    wanted = set(_SINGLE_ESSENTIAL_FIELDS)
    return [field for field in fields if field.name in wanted]


def _field_by_name(fields: list[TuiField], name: str) -> TuiField | None:
    return next((field for field in fields if field.name == name), None)


def _field_value(fields: list[TuiField], name: str, default: object = "") -> object:
    field = _field_by_name(fields, name)
    return field.value if field is not None else default


def _resolve_from_root(value: str, root: Path) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = root / path
    return path.resolve()


def _selection_kind(case_value: str, root: Path) -> str:
    if not case_value:
        return "missing"
    path = _resolve_from_root(case_value, root)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError):
        return "unknown"
    if not isinstance(payload, dict):
        return "unknown"
    if isinstance(payload.get("plan_meta"), dict):
        return "plan"
    if payload.get("case_id") or isinstance(payload.get("test_plan"), dict):
        return "case"
    return "unknown"


def single_setup_issues(fields: list[TuiField], repo_root: Path | None = None) -> list[str]:
    """Return concise setup problems that should be fixed before starting a run."""

    root = (repo_root or Path.cwd()).expanduser().resolve()
    issues: list[str] = []
    case_value = str(_field_value(fields, "case") or "").strip()
    dataset_value = str(_field_value(fields, "dataset") or "").strip()

    if not case_value:
        issues.append("Choose a case or plan.")
    else:
        case_path = _resolve_from_root(case_value, root)
        if not case_path.is_file():
            issues.append("The selected case or plan does not exist.")
        elif _selection_kind(case_value, root) == "plan" and not dataset_value:
            issues.append("Choose a dataset when running a plan directly.")

    if dataset_value:
        dataset_path = _resolve_from_root(dataset_value, root)
        if not dataset_path.is_file():
            issues.append("The selected dataset does not exist.")
        elif dataset_path.suffix.lower() not in SUPPORTED_DATASET_SUFFIXES:
            issues.append(f"Unsupported dataset type: {dataset_path.suffix or '(none)'}.")

    workers = str(_field_value(fields, "workers") or "").strip()
    if workers:
        try:
            if int(workers) < 1:
                raise ValueError
        except ValueError:
            issues.append("Worker count must be a positive integer or blank for automatic selection.")
    return issues


def single_review_lines(fields: list[TuiField], repo_root: Path | None = None) -> list[str]:
    root = (repo_root or Path.cwd()).expanduser().resolve()
    case_value = str(_field_value(fields, "case") or "")
    dataset_value = str(_field_value(fields, "dataset") or "")
    kind = _selection_kind(case_value, root)
    kind_label = {"case": "Case", "plan": "Plan", "unknown": "Selection", "missing": "Selection"}[kind]
    workers = str(_field_value(fields, "workers") or "").strip() or "Automatic"
    dataset = dataset_value or ("Provided by case" if kind == "case" else "Not selected")
    return [
        f"{kind_label}: {case_value or 'Not selected'}",
        f"Dataset: {dataset}",
        f"Output: {_field_value(fields, 'output') or 'Automatic'}",
        f"Strict experiment mode: {'ON' if _field_value(fields, 'experiment_mode', False) else 'off'}",
        f"Field validation only: {'ON' if _field_value(fields, 'field_translation_dry_run', False) else 'off'}",
        f"Display: {_field_value(fields, 'display') or 'interactive'}",
        f"Workers: {workers}",
    ]


def _safe_addstr(stdscr, y: int, x: int, text: str, attr: int = 0) -> None:
    height, width = stdscr.getmaxyx()
    if y < 0 or y >= height or x < 0 or x >= width:
        return
    available = max(0, width - x - 1)
    if available <= 0:
        return
    try:
        stdscr.addstr(y, x, str(text)[:available], attr)
    except curses.error:
        pass


def _initialise_colours() -> None:
    if not curses.has_colors():
        return
    try:
        curses.start_color()
        curses.use_default_colors()
        curses.init_pair(1, curses.COLOR_GREEN, -1)
        curses.init_pair(2, curses.COLOR_YELLOW, -1)
        curses.init_pair(3, curses.COLOR_CYAN, -1)
    except curses.error:
        pass


def _status_attr(ok: bool) -> int:
    if curses.has_colors():
        return curses.color_pair(1 if ok else 2) | curses.A_BOLD
    return curses.A_BOLD


def _choice_dialog(stdscr, title: str, choices: tuple[str, ...], current: object) -> str | None:
    if not choices:
        return None
    selected = choices.index(current) if current in choices else 0
    while True:
        stdscr.erase()
        height, width = stdscr.getmaxyx()
        _safe_addstr(stdscr, 0, 0, title, curses.A_BOLD)
        _safe_addstr(stdscr, 1, 0, "↑/↓ choose   Enter select   q/Esc cancel")
        visible_height = max(1, height - 4)
        start = min(max(0, selected - visible_height + 1), max(0, len(choices) - visible_height))
        for row, value in enumerate(choices[start : start + visible_height], start=3):
            index = start + row - 3
            label = value or "(none)"
            marker = ">" if index == selected else " "
            attr = curses.A_REVERSE if index == selected else curses.A_NORMAL
            _safe_addstr(stdscr, row, 0, f"{marker} {label}", attr)
        key = stdscr.getch()
        if key in (ord("q"), ord("Q"), 27):
            return None
        if key in (curses.KEY_UP, ord("k")):
            selected = max(0, selected - 1)
        elif key in (curses.KEY_DOWN, ord("j")):
            selected = min(len(choices) - 1, selected + 1)
        elif key in (curses.KEY_PPAGE,):
            selected = max(0, selected - visible_height)
        elif key in (curses.KEY_NPAGE,):
            selected = min(len(choices) - 1, selected + visible_height)
        elif key in (10, 13):
            return choices[selected]


def _dataset_browser_entries(directory: Path, root: Path):
    return [
        entry
        for entry in list_file_browser_entries(directory, root)
        if entry.is_dir or entry.path.suffix.lower() in SUPPORTED_DATASET_SUFFIXES
    ]


def _friendly_dataset_browser(stdscr, root: Path, initial: str) -> str | None:
    root = root.expanduser().resolve()
    initial_path = _resolve_from_root(initial, root) if initial else None
    if initial_path and initial_path.is_file():
        current_dir = initial_path.parent
    elif initial_path and initial_path.is_dir():
        current_dir = initial_path
    elif (root / "datasets").is_dir():
        current_dir = (root / "datasets").resolve()
    else:
        current_dir = root
    selected = 0
    message = "↑/↓ move   Enter open/select   Backspace parent   e path   R repo   H home   M /media   q cancel"

    while True:
        stdscr.erase()
        height, width = stdscr.getmaxyx()
        entries = _dataset_browser_entries(current_dir, root)
        selected = min(selected, max(0, len(entries) - 1))
        _safe_addstr(stdscr, 0, 0, "Choose dataset", curses.A_BOLD)
        _safe_addstr(stdscr, 1, 0, message)
        _safe_addstr(stdscr, 2, 0, f"Directory: {_display_path(current_dir, root)}")
        _safe_addstr(stdscr, 3, 0, "Only supported dataset files are shown.")
        visible_height = max(1, height - 6)
        start = min(max(0, selected - visible_height + 1), max(0, len(entries) - visible_height))
        for row, entry in enumerate(entries[start : start + visible_height], start=5):
            index = start + row - 5
            marker = ">" if index == selected else " "
            attr = curses.A_REVERSE if index == selected else curses.A_NORMAL
            _safe_addstr(stdscr, row, 0, f"{marker} {entry.label}", attr)
        if not entries:
            _safe_addstr(stdscr, 5, 0, "No supported datasets in this directory.")

        key = stdscr.getch()
        if key in (ord("q"), ord("Q"), 27):
            return None
        if key == ord("R"):
            current_dir = root
            selected = 0
            continue
        if key == ord("H"):
            current_dir = Path.home().expanduser().resolve()
            selected = 0
            continue
        if key == ord("M"):
            media = Path("/media")
            if media.is_dir():
                current_dir = media.resolve()
                selected = 0
            continue
        if key == ord("e"):
            typed = _edit_text(stdscr, min(height - 2, 4), 0, str(current_dir), max(8, width - 1)).strip()
            if not typed:
                continue
            path = Path(typed).expanduser()
            if not path.is_absolute():
                path = current_dir / path
            path = path.resolve()
            if path.is_dir():
                current_dir = path
                selected = 0
            elif path.is_file() and path.suffix.lower() in SUPPORTED_DATASET_SUFFIXES:
                return _display_path(path, root)
            else:
                message = "That path is not a supported dataset file or directory."
            continue
        if key in (curses.KEY_BACKSPACE, 127, 8):
            if current_dir.parent != current_dir:
                current_dir = current_dir.parent.resolve()
                selected = 0
            continue
        if key in (curses.KEY_UP, ord("k")):
            selected = max(0, selected - 1)
            continue
        if key in (curses.KEY_DOWN, ord("j")):
            selected = min(max(0, len(entries) - 1), selected + 1)
            continue
        if key in (10, 13) and entries:
            entry = entries[selected]
            if entry.is_dir:
                current_dir = entry.path.resolve()
                selected = 0
            else:
                return _display_path(entry.path, root)


def _rows_for_fields(fields: list[TuiField]) -> list[tuple[int | None, str]]:
    rows: list[tuple[int | None, str]] = []
    previous_section: str | None = None
    for index, field in enumerate(fields):
        if field.section != previous_section:
            rows.append((None, field.section.upper()))
            previous_section = field.section
        rows.append((index, f"  {field.label:28} {_format_value(field)}"))
    return rows


def _refresh_auto_output(fields: list[TuiField], root: Path) -> None:
    output = _field_by_name(fields, "output")
    case = _field_by_name(fields, "case")
    if output is not None and output.auto:
        output.value = default_outcome_path(str(case.value or "") if case else None, repo_root=root)


def _review_single(stdscr, fields: list[TuiField], root: Path) -> bool:
    issues = single_setup_issues(fields, root)
    if issues:
        return False
    while True:
        stdscr.erase()
        _safe_addstr(stdscr, 0, 0, "Review run", curses.A_BOLD)
        _safe_addstr(stdscr, 1, 0, "Check the important settings before the run starts.")
        for row, line in enumerate(single_review_lines(fields, root), start=3):
            _safe_addstr(stdscr, row, 2, line)
        row = 3 + len(single_review_lines(fields, root)) + 1
        _safe_addstr(stdscr, row, 0, "Enter/r start run   b/Esc back", curses.A_BOLD)
        key = stdscr.getch()
        if key in (10, 13, ord("r"), ord("R")):
            return True
        if key in (ord("b"), ord("B"), ord("q"), ord("Q"), 27):
            return False


def _edit_single_field(stdscr, field: TuiField, fields: list[TuiField], root: Path) -> None:
    height, width = stdscr.getmaxyx()
    if field.kind == "bool":
        field.value = not bool(field.value)
        return
    if field.kind == "choice":
        chosen = _choice_dialog(stdscr, f"Choose {field.label.lower()}", field.choices, field.value)
        if chosen is not None:
            field.value = chosen
            if field.name == "case":
                _refresh_auto_output(fields, root)
        return
    if field.kind == "file":
        chosen = _friendly_dataset_browser(stdscr, root, str(field.value or ""))
        if chosen is not None:
            field.value = chosen
        return
    field.value = _edit_text(stdscr, min(height - 2, 5), 0, str(field.value or ""), max(8, width - 1))
    if field.name == "output":
        field.auto = False


def _single_setup_curses(stdscr, fields: list[TuiField], root: Path) -> list[TuiField] | None:
    curses.curs_set(0)
    _initialise_colours()
    selected = 0
    show_advanced = False
    message = ""

    while True:
        stdscr.erase()
        height, width = stdscr.getmaxyx()
        visible = visible_single_fields(fields, show_advanced)
        if not visible:
            return None
        selected = min(selected, len(visible) - 1)
        issues = single_setup_issues(fields, root)

        _safe_addstr(stdscr, 0, 0, "CBR Tests — Single dataset", curses.A_BOLD)
        _safe_addstr(
            stdscr,
            1,
            0,
            "Set the essentials below. Advanced options stay hidden unless you need them.",
        )
        status = "READY TO REVIEW" if not issues else f"NEEDS ATTENTION — {issues[0]}"
        _safe_addstr(stdscr, 2, 0, status, _status_attr(not issues))
        mode = "ADVANCED" if show_advanced else "ESSENTIALS"
        _safe_addstr(
            stdscr,
            3,
            0,
            f"↑/↓ move   Enter edit   Space toggle   a {('essentials' if show_advanced else 'advanced')}   r review/run   q quit   [{mode}]",
        )
        if message:
            _safe_addstr(stdscr, 4, 0, message, curses.A_BOLD)

        rows = _rows_for_fields(visible)
        selected_row = next((index for index, (field_index, _) in enumerate(rows) if field_index == selected), 0)
        content_top = 6
        footer_height = 5
        visible_height = max(1, height - content_top - footer_height)
        scroll = min(max(0, selected_row - visible_height + 1), max(0, len(rows) - visible_height))
        for screen_row, (field_index, text) in enumerate(rows[scroll : scroll + visible_height], start=content_top):
            if field_index is None:
                _safe_addstr(stdscr, screen_row, 0, text, curses.A_BOLD)
            else:
                marker = ">" if field_index == selected else " "
                attr = curses.A_REVERSE if field_index == selected else curses.A_NORMAL
                _safe_addstr(stdscr, screen_row, 0, f"{marker}{text}", attr)

        footer = max(content_top, height - footer_height)
        try:
            stdscr.hline(footer, 0, curses.ACS_HLINE, max(1, width - 1))
        except curses.error:
            pass
        selected_field = visible[selected]
        help_lines = describe_tui_field(selected_field)
        _safe_addstr(stdscr, footer + 1, 0, f"{selected_field.label}: {selected_field.help}")
        _safe_addstr(stdscr, footer + 2, 0, help_lines[-1].removeprefix("How: "))
        if bool(_field_value(fields, "experiment_mode", False)):
            _safe_addstr(stdscr, footer + 3, 0, "Strict experiment mode is ON: non-canonical/historical plans can be rejected.", _status_attr(True))

        key = stdscr.getch()
        message = ""
        if key in (ord("q"), ord("Q"), 27):
            return None
        if key in (curses.KEY_UP, ord("k")):
            selected = max(0, selected - 1)
            continue
        if key in (curses.KEY_DOWN, ord("j")):
            selected = min(len(visible) - 1, selected + 1)
            continue
        if key in (ord("a"), ord("A")):
            current_name = visible[selected].name
            show_advanced = not show_advanced
            new_visible = visible_single_fields(fields, show_advanced)
            selected = next((i for i, item in enumerate(new_visible) if item.name == current_name), 0)
            continue
        if key in (ord("r"), ord("R")):
            _refresh_auto_output(fields, root)
            issues = single_setup_issues(fields, root)
            if issues:
                message = issues[0]
                continue
            if _review_single(stdscr, fields, root):
                return fields
            continue
        if key == ord(" ") and visible[selected].kind == "bool":
            visible[selected].value = not bool(visible[selected].value)
            continue
        if key in (10, 13):
            _edit_single_field(stdscr, visible[selected], fields, root)
            continue
        if key in (ord("e"), ord("E")) and visible[selected].kind in {"file", "text", "int"}:
            field = visible[selected]
            if field.kind == "file":
                value = _edit_text(stdscr, min(height - 2, 5), 0, str(field.value or ""), max(8, width - 1))
                if value:
                    field.value = value
            else:
                field.value = _edit_text(stdscr, min(height - 2, 5), 0, str(field.value or ""), max(8, width - 1))
                if field.name == "output":
                    field.auto = False


def launch_single_tui(args, repo_root: Path | None = None):
    root = (repo_root or Path.cwd()).expanduser().resolve()
    fields = build_friendly_single_fields(args, repo_root=root)
    selected = curses.wrapper(_single_setup_curses, fields, root)
    if selected is None:
        raise SystemExit("TUI cancelled")
    return apply_tui_fields(args, selected)


def batch_visible_field_names(show_advanced: bool) -> tuple[str, ...]:
    if not show_advanced:
        return _BATCH_ESSENTIAL_FIELDS
    return (
        "name",
        "datasets",
        "references",
        "metric_policy",
        "workers",
        "display",
        "force",
        "dataset_summary",
        "refresh_dataset_summary",
        "fail_fast",
    )


def batch_review_lines(state: dict[str, Any]) -> list[str]:
    candidates = list(state.get("datasets") or [])
    references = list(state.get("references") or [])
    jobs = comparison_job_count(candidates, references)
    policy = "Per-job runnable metrics" if state.get("per_dataset_metrics") else "Common metrics across every job"
    return [
        f"Batch: {state.get('name') or 'Not named'}",
        f"Candidates: {len(candidates)} — {_format_list(candidates)}",
        f"References: {len(references)} — {_format_list(references)}",
        f"Jobs: {jobs}",
        f"Metric policy: {policy}",
        f"Workers: {state.get('workers') if state.get('workers') is not None else 'Automatic'}",
        f"Display: {state.get('display') or 'compact'}",
    ]


def _review_batch(stdscr, state: dict[str, Any]) -> bool:
    while True:
        stdscr.erase()
        _safe_addstr(stdscr, 0, 0, "Review batch / comparison", curses.A_BOLD)
        _safe_addstr(stdscr, 1, 0, "The comparison matrix below will be created and run.")
        lines = batch_review_lines(state)
        for row, line in enumerate(lines, start=3):
            _safe_addstr(stdscr, row, 2, line)
        _safe_addstr(stdscr, 3 + len(lines) + 1, 0, "Enter/r start batch   b/Esc back", curses.A_BOLD)
        key = stdscr.getch()
        if key in (10, 13, ord("r"), ord("R")):
            return True
        if key in (ord("b"), ord("B"), ord("q"), ord("Q"), 27):
            return False


def _batch_value(state: dict[str, Any], name: str) -> str:
    if name == "name":
        return str(state.get("name") or "")
    if name == "datasets":
        return _format_list(list(state.get("datasets") or []))
    if name == "references":
        return _format_list(list(state.get("references") or []))
    if name == "metric_policy":
        return "Per-job runnable metrics" if state.get("per_dataset_metrics") else "Common metrics across every job"
    if name == "workers":
        return str(state.get("workers") if state.get("workers") is not None else "Automatic")
    if name == "display":
        return str(state.get("display") or "compact")
    return "ON" if state.get(name) else "off"


def _batch_label(name: str) -> str:
    return {
        "name": "Batch name",
        "datasets": "Candidate datasets",
        "references": "Reference datasets",
        "metric_policy": "Metric policy",
        "workers": "Worker count",
        "display": "Live display",
        "force": "Replace existing outputs",
        "dataset_summary": "Dataset summaries",
        "refresh_dataset_summary": "Refresh summaries",
        "fail_fast": "Stop after first failed job",
    }[name]


def _batch_help(name: str) -> str:
    return {
        "name": "A readable name used to identify this experiment batch.",
        "datasets": "The candidate datasets you want to assess.",
        "references": "Optional independent real/reference datasets used for comparisons.",
        "metric_policy": "Use one common metric set for comparability, or the runnable set for each job.",
        "workers": "Leave automatic unless you have a reason to override concurrency.",
        "display": "Choose how much live progress information is drawn while jobs run.",
        "force": "Allow existing generated batch/output files to be replaced.",
        "dataset_summary": "Create or reuse descriptive dataset summary sidecars.",
        "refresh_dataset_summary": "Rebuild summaries even when the dataset hash still matches.",
        "fail_fast": "Stop the batch after the first failed job instead of continuing.",
    }[name]


def _friendly_batch_setup_curses(stdscr, initial: dict[str, Any], root: Path) -> dict[str, Any] | None:
    curses.curs_set(0)
    _initialise_colours()
    state = dict(initial)
    selected = 0
    show_advanced = False
    message = ""

    while True:
        stdscr.erase()
        height, width = stdscr.getmaxyx()
        fields = batch_visible_field_names(show_advanced)
        selected = min(selected, len(fields) - 1)
        candidates = list(state.get("datasets") or [])
        references = list(state.get("references") or [])
        jobs = comparison_job_count(candidates, references)
        ready = bool(str(state.get("name") or "").strip() and candidates and (jobs > 0 or not references))

        _safe_addstr(stdscr, 0, 0, "CBR Tests — Batch / comparison", curses.A_BOLD)
        _safe_addstr(stdscr, 1, 0, "Pick candidate datasets and optional references; the comparison matrix is built automatically.")
        status = f"READY — {jobs} job(s)" if ready else "NEEDS ATTENTION — select at least one candidate dataset"
        _safe_addstr(stdscr, 2, 0, status, _status_attr(ready))
        mode = "ADVANCED" if show_advanced else "ESSENTIALS"
        _safe_addstr(
            stdscr,
            3,
            0,
            f"↑/↓ move   Enter edit   Space toggle   a {('essentials' if show_advanced else 'advanced')}   r review/run   q quit   [{mode}]",
        )
        if message:
            _safe_addstr(stdscr, 4, 0, message, curses.A_BOLD)

        content_top = 6
        max_rows = max(1, height - content_top - 6)
        start = min(max(0, selected - max_rows + 1), max(0, len(fields) - max_rows))
        for row, name in enumerate(fields[start : start + max_rows], start=content_top):
            index = start + row - content_top
            marker = ">" if index == selected else " "
            attr = curses.A_REVERSE if index == selected else curses.A_NORMAL
            _safe_addstr(stdscr, row, 0, f"{marker} {_batch_label(name):28} {_batch_value(state, name)}", attr)

        footer = max(content_top, height - 5)
        try:
            stdscr.hline(footer, 0, curses.ACS_HLINE, max(1, width - 1))
        except curses.error:
            pass
        current = fields[selected]
        _safe_addstr(stdscr, footer + 1, 0, _batch_help(current))
        _safe_addstr(stdscr, footer + 2, 0, f"Run ID: {state.get('run_id')}   Candidates: {len(candidates)}   References: {len(references)}   Jobs: {jobs}", curses.A_BOLD)
        _safe_addstr(stdscr, footer + 3, 0, "Self-comparisons are excluded automatically.")

        key = stdscr.getch()
        message = ""
        if key in (ord("q"), ord("Q"), 27):
            return None
        if key in (curses.KEY_UP, ord("k")):
            selected = max(0, selected - 1)
            continue
        if key in (curses.KEY_DOWN, ord("j")):
            selected = min(len(fields) - 1, selected + 1)
            continue
        if key in (ord("a"), ord("A")):
            current_name = fields[selected]
            show_advanced = not show_advanced
            new_fields = batch_visible_field_names(show_advanced)
            selected = new_fields.index(current_name) if current_name in new_fields else 0
            continue
        if key in (ord("r"), ord("R")):
            try:
                spec = build_batch_spec(
                    name=str(state.get("name") or ""),
                    run_id=str(state.get("run_id") or ""),
                    datasets=candidates,
                    references=references,
                    per_dataset_metrics=bool(state.get("per_dataset_metrics")),
                    workers=state.get("workers"),
                    display=str(state.get("display") or "compact"),
                    force=bool(state.get("force")),
                    dataset_summary=bool(state.get("dataset_summary", True)),
                    refresh_dataset_summary=bool(state.get("refresh_dataset_summary")),
                    fail_fast=bool(state.get("fail_fast")),
                )
            except ValueError as exc:
                message = str(exc)
                continue
            if _review_batch(stdscr, state):
                return spec
            continue

        field = fields[selected]
        if key == ord(" ") and field in {"force", "dataset_summary", "refresh_dataset_summary", "fail_fast"}:
            state[field] = not bool(state.get(field))
            continue
        if key not in (10, 13):
            continue

        if field == "name":
            state["name"] = _edit_text(stdscr, min(height - 2, 5), 0, str(state.get("name") or ""), max(8, width - 1))
        elif field == "datasets":
            chosen = _multi_file_browser(stdscr, root, candidates, title="Select candidate datasets", allow_empty=False)
            if chosen is not None:
                state["datasets"] = chosen
        elif field == "references":
            chosen = _multi_file_browser(stdscr, root, references, title="Select independent reference datasets", allow_empty=True)
            if chosen is not None:
                state["references"] = chosen
        elif field == "metric_policy":
            state["per_dataset_metrics"] = not bool(state.get("per_dataset_metrics"))
        elif field == "workers":
            raw = _edit_text(stdscr, min(height - 2, 5), 0, "" if state.get("workers") is None else str(state["workers"]), max(8, width - 1)).strip()
            if not raw:
                state["workers"] = None
            else:
                try:
                    value = int(raw)
                    if value < 1:
                        raise ValueError
                    state["workers"] = value
                except ValueError:
                    message = "Worker count must be a positive integer or blank for automatic selection."
        elif field == "display":
            chosen = _choice_dialog(stdscr, "Choose live display mode", DISPLAY_MODES, state.get("display") or "compact")
            if chosen is not None:
                state["display"] = chosen
        elif field in {"force", "dataset_summary", "refresh_dataset_summary", "fail_fast"}:
            state[field] = not bool(state.get(field))


def launch_friendly_batch_tui(args, repo_root: Path | None = None) -> dict[str, Any]:
    root = (repo_root or Path.cwd()).expanduser().resolve()
    initial_dataset = getattr(args, "dataset", None)
    if isinstance(initial_dataset, (list, tuple)):
        datasets = [str(value) for value in initial_dataset if value]
    elif initial_dataset:
        datasets = [str(initial_dataset)]
    else:
        datasets = []
    initial = {
        "name": "comparison-batch",
        "run_id": default_batch_run_id(),
        "datasets": datasets,
        "references": [],
        "per_dataset_metrics": False,
        "workers": getattr(args, "workers", None),
        "display": getattr(args, "display", None) or "compact",
        "force": False,
        "dataset_summary": bool(getattr(args, "dataset_summary", True)),
        "refresh_dataset_summary": bool(getattr(args, "refresh_dataset_summary", False)),
        "fail_fast": False,
    }
    selected = curses.wrapper(_friendly_batch_setup_curses, initial, root)
    if selected is None:
        raise SystemExit("Batch TUI cancelled")
    return selected
