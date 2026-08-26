from __future__ import annotations

import curses
import json
import os
import re
from curses import textpad
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


DISPLAY_MODES = ("compact", "full", "quiet", "interactive")


def _outcome_filename_slug(value: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9]+", "_", value).strip("_").lower()
    return slug or "run"


def _selected_run_title(case_path: str | None, repo_root: Path) -> str:
    if not case_path:
        return "tui_run"

    selected = Path(case_path).expanduser()
    if not selected.is_absolute():
        selected = repo_root / selected

    try:
        payload = json.loads(selected.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError):
        payload = {}

    if isinstance(payload, dict):
        plan_meta = payload.get("plan_meta")
        if isinstance(plan_meta, dict) and plan_meta.get("name"):
            return str(plan_meta["name"])
        for key in ("title", "name"):
            if payload.get(key):
                return str(payload[key])

        test_plan = payload.get("test_plan")
        if isinstance(test_plan, dict) and test_plan.get("path"):
            plan_path = Path(str(test_plan["path"])).expanduser()
            if not plan_path.is_absolute():
                plan_path = selected.parent / plan_path
            try:
                plan_payload = json.loads(plan_path.resolve().read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError, TypeError):
                plan_payload = {}
            plan_meta = plan_payload.get("plan_meta") if isinstance(plan_payload, dict) else None
            if isinstance(plan_meta, dict) and plan_meta.get("name"):
                return str(plan_meta["name"])

        dataset = payload.get("dataset")
        if isinstance(dataset, dict) and dataset.get("name"):
            return str(dataset["name"])

    stem = Path(case_path).stem
    if stem.endswith("_plan"):
        stem = stem[:-5]
    return stem or "tui_run"


def default_outcome_path(
    case_path: str | None = None,
    *,
    repo_root: Path | None = None,
    now: datetime | None = None,
) -> str:
    root = (repo_root or Path.cwd()).expanduser().resolve()
    title = _selected_run_title(case_path, root)
    timestamp = (now or datetime.now()).strftime("%Y-%m-%d_%H-%M-%S")
    return str(Path("outcomes") / f"outcome_{_outcome_filename_slug(title)}_{timestamp}.json")


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
    auto: bool = False


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
    selected_case = args.case or (case_choices[0] if case_choices else "")
    return [
        TuiField("case", "Case or plan JSON", "choice", selected_case, case_choices, "Pick a ready-to-run case, or pick a direct plan and provide dataset/output below.", "Required inputs"),
        TuiField("dataset", "Dataset file", "file", args.dataset or "", (), "Browse to the CSV/TSV/XLSX/PCAP file to test. Case files may already provide this.", "Required inputs"),
        TuiField("output", "Outcome JSON", "text", args.output or default_outcome_path(selected_case or None, repo_root=root), (), "Where the run result JSON should be written. By default this follows the selected plan/case title and receives a fresh date/time suffix when the run starts; edit it only when you want a custom filename.", "Required inputs", auto=args.output is None),
        TuiField("case_id", "Ad-hoc case ID", "text", args.case_id or "ad_hoc_case", (), "Label written to the outcome when you run a plan directly instead of a case.", "Required inputs"),
        TuiField("dataset_summary", "Dataset summary sidecar", "bool", bool(getattr(args, "dataset_summary", True)), (), "Create or reuse a hash-validated Markdown summary beside the dataset. Enabled by default.", "Dataset summary"),
        TuiField("refresh_dataset_summary", "Force summary refresh", "bool", bool(getattr(args, "refresh_dataset_summary", False)), (), "Regenerate the dataset summary even when the existing sidecar hash and schema are still current.", "Dataset summary"),
        TuiField("display", "Live display mode", "choice", args.display or "interactive", DISPLAY_MODES, "Choose how much progress detail to show after the run starts.", "Execution"),
        TuiField("workers", "Worker count", "int", "" if args.workers is None else str(args.workers), (), "Leave blank for automatic worker selection; enter 1 for serial execution. The detected maximum is shown beside this field.", "Execution"),
        TuiField("taxonomy_file", "Metric order file", "choice", args.taxonomy_file or "", taxonomy_choices, "Optional taxonomy JSON that controls metric ordering.", "Taxonomy"),
        TuiField("taxonomy_strict", "Strict taxonomy order", "bool", bool(args.taxonomy_strict), (), "Use this only when you provide a metric order file and want the run to fail if any enabled metric is missing from that order.", "Taxonomy"),
        TuiField("field_translation", "Field translation JSON", "choice", args.field_translation or "", translation_choices, "Optional mapping from dataset column names to canonical test field names.", "Field translation"),
        TuiField("no_update_field_translation", "Never update sidecar", "bool", bool(args.no_update_field_translation), (), "Do not create or modify dataset sidecar translation templates.", "Field translation"),
        TuiField("yes_field_translation_sidecar", "Auto-create sidecar", "bool", bool(args.yes_field_translation_sidecar), (), "Allow sidecar template creation/update without an extra prompt.", "Field translation"),
        TuiField("field_translation_dry_run", "Dry run: validate fields only", "bool", bool(args.field_translation_dry_run), (), "Check field mappings/reports first and stop before metrics. The results screen can start the real run afterward.", "Field translation"),
        TuiField("field_translation_report", "Mapping report JSON", "text", args.field_translation_report or "", (), "Machine-readable report for automation: available columns, detected mappings, missing required fields, and skipped metrics.", "Reports"),
        TuiField("field_translation_text_report", "Mapping report text", "text", args.field_translation_text_report or "", (), "Plain-language report for quick terminal review or sharing in logs.", "Reports"),
        TuiField("field_translation_markdown_report", "Mapping report Markdown", "text", args.field_translation_markdown_report or "", (), "Markdown report for documentation, GitHub issues, or review notes.", "Reports"),
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
            output_field = next((item for item in fields if item.name == "output"), None)
            case_field = next((item for item in fields if item.name == "case"), None)
            if output_field is not None and output_field.auto:
                output_field.value = default_outcome_path(str(case_field.value or "") if case_field else None, repo_root=repo_root)
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
            if field.name == "output":
                field.auto = False
        elif key in (10, 13):
            field = fields[selected]
            if field.kind == "bool":
                field.value = not field.value
            elif field.kind == "choice" and field.choices:
                idx = field.choices.index(field.value) if field.value in field.choices else -1
                field.value = field.choices[(idx + 1) % len(field.choices)]
                if field.name == "case":
                    output_field = next((item for item in fields if item.name == "output"), None)
                    if output_field is not None and output_field.auto:
                        output_field.value = default_outcome_path(str(field.value or ""), repo_root=repo_root)
            elif field.kind == "file":
                browsed = _browse_file(stdscr, repo_root, str(field.value or ""))
                if browsed is not None:
                    field.value = browsed
            else:
                field.value = _edit_text(stdscr, min(height - 2, selected - start + 3), 29, str(field.value or ""), max(8, width - 30))
                if field.name == "output":
                    field.auto = False


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



def _field_mapping_choices(dataset_columns: list[str], mappings: dict[str, str], field: str) -> list[str]:
    used = {value for key, value in mappings.items() if key != field and value}
    current = mappings.get(field, "")
    choices = [""]
    choices.extend(column for column in dataset_columns if column not in used or column == current)
    return choices


def save_field_mappings(path: Path, mappings: dict[str, str]) -> None:
    payload = {}
    if path.exists():
        payload = json.loads(path.read_text(encoding="utf-8"))
    existing = payload.get("test_to_dataset_fields", {}) if isinstance(payload, dict) else {}
    existing.update(mappings)
    payload.update({"schema_version": payload.get("schema_version", 1), "test_to_dataset_fields": dict(sorted(existing.items()))})
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _field_mapping_curses(stdscr, result: dict) -> str:
    missing_fields = list(result.get("missing_fields") or [])
    dataset_columns = list(result.get("dataset_columns") or [])
    translation_path = Path(result.get("field_translation_path") or "field_translation.json")
    mappings = {field: "" for field in missing_fields}
    selected = 0
    message = "↑/↓ field  Enter cycle dataset column  s save  q cancel"
    while True:
        stdscr.erase()
        height, width = stdscr.getmaxyx()
        stdscr.addstr(0, 0, "Map missing fields"[: width - 1], curses.A_BOLD)
        stdscr.addstr(1, 0, "Choose one dataset column for each required canonical field."[: width - 1])
        stdscr.addstr(2, 0, message[: width - 1])
        if not missing_fields:
            stdscr.addstr(4, 0, "No missing fields were reported."[: width - 1])
        elif not dataset_columns:
            stdscr.addstr(4, 0, "No dataset columns are available to choose from."[: width - 1])
        visible_rows = max(1, height - 6)
        start = min(max(0, selected - visible_rows + 1), max(0, len(missing_fields) - visible_rows))
        for row, field in enumerate(missing_fields[start : start + visible_rows], start=4):
            index = start + row - 4
            value = mappings.get(field) or "(unmapped)"
            marker = ">" if index == selected else " "
            attr = curses.A_REVERSE if index == selected else curses.A_NORMAL
            stdscr.addstr(row, 0, f"{marker} {field:32} ← {value}"[: width - 1], attr)
        used_count = len([value for value in mappings.values() if value])
        stdscr.addstr(height - 2, 0, f"Mapped {used_count}/{len(missing_fields)} | output: {translation_path}"[: width - 1])
        stdscr.addstr(height - 1, 0, "Columns already selected disappear from the other dropdowns."[: width - 1])
        key = stdscr.getch()
        if key in (ord("q"), ord("Q"), 27):
            return "cancel"
        if key in (ord("s"), ord("S")):
            save_field_mappings(translation_path, {field: value for field, value in mappings.items() if value})
            return "saved"
        if not missing_fields:
            continue
        if key in (curses.KEY_UP, ord("k")):
            selected = max(0, selected - 1)
        elif key in (curses.KEY_DOWN, ord("j")):
            selected = min(len(missing_fields) - 1, selected + 1)
        elif key in (10, 13):
            field = missing_fields[selected]
            choices = _field_mapping_choices(dataset_columns, mappings, field)
            current = mappings.get(field, "")
            current_index = choices.index(current) if current in choices else 0
            mappings[field] = choices[(current_index + 1) % len(choices)]


def show_field_mapping_menu(result: dict) -> str:
    return curses.wrapper(_field_mapping_curses, result)


def _confirm_run_after_errors(stdscr, skipped_count: int, result: dict | None = None) -> str:
    while True:
        stdscr.erase()
        height, width = stdscr.getmaxyx()
        lines = [
            "Dry run found issues",
            f"{skipped_count} metric(s) may be skipped or blocked by missing fields.",
            "Run anyway?",
            "m: map missing fields first",
            "y: yes, run metrics now",
            "n/q/Esc: no, return to results",
        ]
        for row, line in enumerate(lines[:height]):
            stdscr.addstr(row, 0, line[: width - 1], curses.A_BOLD if row == 0 else curses.A_NORMAL)
        key = stdscr.getch()
        if key in (ord("m"), ord("M")):
            return "map"
        if key in (ord("y"), ord("Y")):
            return "run"
        if key in (ord("n"), ord("N"), ord("q"), 27):
            return "back"



@dataclass
class ResultSection:
    title: str
    lines: list[str]
    expanded: bool = False


def _load_outcome_payload(output_path: str | None) -> dict:
    if not output_path:
        return {}
    path = Path(output_path)
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _scalar_summary_items(payload: object, limit: int = 4) -> list[str]:
    if not isinstance(payload, dict):
        return []
    source = payload.get("summary") if isinstance(payload.get("summary"), dict) else payload
    items: list[str] = []
    for key, value in source.items():
        if isinstance(value, (str, int, float, bool)) or value is None:
            items.append(f"{key}={value}")
        if len(items) >= limit:
            break
    return items


def _test_result_for_metric(test_results: dict, metric_id: str) -> object:
    if metric_id in test_results:
        return test_results[metric_id]
    for value in test_results.values():
        if isinstance(value, dict) and metric_id in value:
            return value[metric_id]
    return None


def _metric_result_line(metric: dict, test_results: dict) -> str:
    metric_id = metric.get("metric_id", "unknown_metric")
    status = metric.get("status", "unknown")
    elapsed = metric.get("elapsed_seconds")
    parts = [metric_id, f"status={status}"]
    if elapsed is not None:
        parts.append(f"elapsed={float(elapsed):.1f}s")
    reason = metric.get("reason") or metric.get("error") or metric.get("message")
    if reason:
        parts.append(f"detail={reason}")
    summary_items = _scalar_summary_items(_test_result_for_metric(test_results, metric_id))
    if summary_items:
        parts.append("; ".join(summary_items))
    return " | ".join(parts)


def _outcome_result_sections(output_path: str | None) -> tuple[list[str], list[str], list[str], list[str]]:
    payload = _load_outcome_payload(output_path)
    metric_results = payload.get("metric_results", []) if isinstance(payload, dict) else []
    test_results = payload.get("test_results", {}) if isinstance(payload, dict) else {}
    readable: list[str] = []
    failed: list[str] = []
    skipped: list[str] = []
    success: list[str] = []
    for metric in metric_results:
        line = _metric_result_line(metric, test_results)
        readable.append(line)
        status = metric.get("status", "unknown")
        if status == "failed":
            failed.append(line)
        elif status == "skipped":
            skipped.append(line)
        elif status == "success":
            success.append(line)
    if not readable and isinstance(test_results, dict):
        readable = [f"{key}: {', '.join(_scalar_summary_items(value)) or 'result available'}" for key, value in test_results.items()]
    return readable, success, failed, skipped


def build_result_sections(result: dict | None) -> list[ResultSection]:
    result = result or {}
    summary = [line for line in _result_lines(result, None) if line and not line.endswith("program") and not line.startswith("Enter/") and not line.startswith("r:")]
    readable, success, failed, skipped = _outcome_result_sections(result.get("output_path"))
    sections = [ResultSection("Summary", summary, True)]
    if readable:
        sections.append(ResultSection(f"Human-readable metric results ({len(readable)})", readable, False))
    if success:
        sections.append(ResultSection(f"Successful metrics ({len(success)})", success, False))
    if failed:
        sections.append(ResultSection(f"Failed metrics ({len(failed)})", failed, False))
    if skipped:
        sections.append(ResultSection(f"Skipped metrics ({len(skipped)})", skipped, False))
    if result.get("dry_run"):
        dry_run_lines = [
            "This was a validation-only pass; metrics were not executed.",
            "Press r to run now with these settings.",
        ]
        if result.get("skipped_count"):
            dry_run_lines.append("Because issues were found, pressing r will ask for confirmation first.")
        sections.append(ResultSection("Dry-run next steps", dry_run_lines, True))
    return sections


def _visible_result_rows(sections: list[ResultSection]) -> list[tuple[int | None, str]]:
    rows: list[tuple[int | None, str]] = []
    for index, section in enumerate(sections):
        marker = "▾" if section.expanded else "▸"
        rows.append((index, f"{marker} {section.title}"))
        if section.expanded:
            rows.extend((None, f"  {line}") for line in section.lines)
    return rows


def _post_run_curses(stdscr, result: dict | None, args) -> str:
    sections = build_result_sections(result)
    selected_section = 0
    scroll = 0
    while True:
        stdscr.erase()
        height, width = stdscr.getmaxyx()
        stdscr.addstr(0, 0, "Run results"[: width - 1], curses.A_BOLD)
        stdscr.addstr(1, 0, "↑/↓ scroll  Enter expand/collapse  m menu  q quit"[: width - 1])
        if result and result.get("dry_run"):
            stdscr.addstr(2, 0, "r run now (confirmation required if issues were found)"[: width - 1])
        rows = _visible_result_rows(sections)
        selected_row = next((i for i, (section_index, _) in enumerate(rows) if section_index == selected_section), 0)
        visible_height = max(1, height - 4)
        scroll = min(max(0, selected_row - visible_height + 1), max(0, len(rows) - visible_height))
        for row_number, (section_index, line) in enumerate(rows[scroll : scroll + visible_height], start=4):
            attr = curses.A_REVERSE if section_index == selected_section else curses.A_NORMAL
            stdscr.addstr(row_number, 0, line[: width - 1], attr)
        key = stdscr.getch()
        if key in (ord("m"), ord("M")):
            return "menu"
        if key in (ord("q"), ord("Q"), 27):
            return "quit"
        if key in (curses.KEY_UP, ord("k")):
            selected_section = max(0, selected_section - 1)
        elif key in (curses.KEY_DOWN, ord("j")):
            selected_section = min(len(sections) - 1, selected_section + 1)
        elif key in (10, 13):
            sections[selected_section].expanded = not sections[selected_section].expanded
        elif result and result.get("dry_run") and key in (ord("r"), ord("R")):
            skipped_count = int(result.get("skipped_count") or 0)
            if skipped_count:
                decision = _confirm_run_after_errors(stdscr, skipped_count, result)
                if decision == "map":
                    show_field_mapping_menu(result or {})
                    continue
                if decision != "run":
                    continue
            return "run"


def show_post_run_menu(result: dict | None, args) -> str:
    return curses.wrapper(_post_run_curses, result, args)
