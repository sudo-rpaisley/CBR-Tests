from __future__ import annotations

import curses
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import subprocess
import sys
from typing import Any

from runner.tui import (
    DISPLAY_MODES,
    _display_path,
    _edit_text,
    _initial_browser_directory,
    detected_max_workers,
    list_file_browser_entries,
)


SUPPORTED_DATASET_SUFFIXES = frozenset({".csv", ".tsv", ".xlsx", ".xls", ".pcap", ".pcapng"})


def is_supported_dataset_path(path: Path | str) -> bool:
    return Path(path).suffix.lower() in SUPPORTED_DATASET_SUFFIXES


def default_batch_run_id(now: datetime | None = None) -> str:
    return (now or datetime.now()).strftime("%Y%m%d-%H%M%S")


@dataclass
class BatchTuiSpec:
    name: str
    run_id: str
    datasets: list[str]
    references: list[str]
    per_dataset_metrics: bool = False
    workers: int | None = None
    display: str = "compact"
    force: bool = False
    dataset_summary: bool = True
    refresh_dataset_summary: bool = False
    fail_fast: bool = False

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "run_id": self.run_id,
            "datasets": list(self.datasets),
            "references": list(self.references),
            "per_dataset_metrics": self.per_dataset_metrics,
            "workers": self.workers,
            "display": self.display,
            "force": self.force,
            "dataset_summary": self.dataset_summary,
            "refresh_dataset_summary": self.refresh_dataset_summary,
            "fail_fast": self.fail_fast,
        }


def comparison_job_count(datasets: list[str], references: list[str]) -> int:
    """Return the runnable candidate/reference job count, excluding self-comparisons."""

    if not references:
        return len(dict.fromkeys(datasets))
    candidate_paths = list(dict.fromkeys(str(Path(value).expanduser().resolve()) for value in datasets))
    reference_paths = list(dict.fromkeys(str(Path(value).expanduser().resolve()) for value in references))
    return sum(candidate != reference for candidate in candidate_paths for reference in reference_paths)


def _validate_dataset_suffixes(values: list[str], *, label: str) -> None:
    invalid = [value for value in values if not is_supported_dataset_path(value)]
    if invalid:
        supported = ", ".join(sorted(SUPPORTED_DATASET_SUFFIXES))
        raise ValueError(
            f"Unsupported {label} dataset type: {invalid[0]}. Supported extensions: {supported}"
        )


def build_batch_spec(
    *,
    name: str,
    datasets: list[str],
    references: list[str] | None = None,
    run_id: str | None = None,
    per_dataset_metrics: bool = False,
    workers: int | None = None,
    display: str = "compact",
    force: bool = False,
    dataset_summary: bool = True,
    refresh_dataset_summary: bool = False,
    fail_fast: bool = False,
) -> dict[str, Any]:
    """Validate and normalise the batch settings selected in the TUI."""

    cleaned_name = str(name).strip()
    if not cleaned_name:
        raise ValueError("Batch name is required.")
    cleaned_run_id = str(run_id or default_batch_run_id()).strip()
    if not cleaned_run_id:
        raise ValueError("Batch run ID is required.")
    candidates = list(dict.fromkeys(str(value).strip() for value in datasets if str(value).strip()))
    refs = list(dict.fromkeys(str(value).strip() for value in (references or []) if str(value).strip()))
    if not candidates:
        raise ValueError("Select at least one candidate dataset.")
    _validate_dataset_suffixes(candidates, label="candidate")
    _validate_dataset_suffixes(refs, label="reference")
    if display not in DISPLAY_MODES:
        raise ValueError(f"Unknown display mode: {display}")
    if workers is not None and int(workers) < 1:
        raise ValueError("Worker count must be at least 1 when supplied.")
    if refs and comparison_job_count(candidates, refs) == 0:
        raise ValueError("Every selected comparison is a self-comparison. Select an independent reference dataset.")
    return BatchTuiSpec(
        name=cleaned_name,
        run_id=cleaned_run_id,
        datasets=candidates,
        references=refs,
        per_dataset_metrics=bool(per_dataset_metrics),
        workers=None if workers is None else int(workers),
        display=display,
        force=bool(force),
        dataset_summary=bool(dataset_summary),
        refresh_dataset_summary=bool(refresh_dataset_summary),
        fail_fast=bool(fail_fast),
    ).as_dict()


def _normalise_initial_paths(values: list[str], root: Path) -> tuple[list[str], set[Path]]:
    ordered: list[str] = []
    selected: set[Path] = set()
    for value in values:
        path = Path(value).expanduser()
        if not path.is_absolute():
            path = root / path
        resolved = path.resolve()
        if resolved in selected:
            continue
        selected.add(resolved)
        ordered.append(_display_path(resolved, root))
    return ordered, selected


def _toggle_selected(path: Path, selected: set[Path], ordered: list[str], root: Path) -> None:
    resolved = path.resolve()
    display = _display_path(resolved, root)
    if resolved in selected:
        selected.remove(resolved)
        if display in ordered:
            ordered.remove(display)
    else:
        selected.add(resolved)
        if display not in ordered:
            ordered.append(display)


def _browser_entries(current_dir: Path, root: Path):
    return [
        entry
        for entry in list_file_browser_entries(current_dir, root)
        if entry.is_dir or is_supported_dataset_path(entry.path)
    ]


def _typed_browser_path(value: str, *, current_dir: Path, root: Path) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = current_dir / path
    return path.resolve()


def _multi_file_browser(
    stdscr,
    root: Path,
    initial_values: list[str],
    *,
    title: str,
    allow_empty: bool,
) -> list[str] | None:
    root = root.expanduser().resolve()
    initial_dir = root / "datasets" if (root / "datasets").is_dir() else root
    current_dir = _initial_browser_directory(str(initial_dir), root)
    ordered, selected_paths = _normalise_initial_paths(initial_values, root)
    selected_index = 0
    default_message = (
        "Space toggle  Enter open/toggle  d done  c clear  e path  R repo  H home  M /media  q cancel"
    )
    message = default_message

    while True:
        stdscr.erase()
        height, width = stdscr.getmaxyx()
        entries = _browser_entries(current_dir, root)
        if selected_index >= len(entries):
            selected_index = max(0, len(entries) - 1)

        stdscr.addstr(0, 0, title[: width - 1], curses.A_BOLD)
        stdscr.addstr(1, 0, message[: width - 1])
        stdscr.addstr(2, 0, f"Directory: {_display_path(current_dir, root)}"[: width - 1])
        stdscr.addstr(3, 0, f"Selected files: {len(ordered)}"[: width - 1])
        visible_rows = max(1, height - 7)
        start = min(max(0, selected_index - visible_rows + 1), max(0, len(entries) - visible_rows))
        for row, entry in enumerate(entries[start : start + visible_rows], start=5):
            index = start + row - 5
            cursor = ">" if index == selected_index else " "
            if entry.is_dir:
                checked = "   "
            else:
                checked = "[x]" if entry.path.resolve() in selected_paths else "[ ]"
            attr = curses.A_REVERSE if index == selected_index else curses.A_NORMAL
            stdscr.addstr(row, 0, f"{cursor} {checked} {entry.label}"[: width - 1], attr)

        footer = f"{len(ordered)} selected | files: {', '.join(sorted(SUPPORTED_DATASET_SUFFIXES))}"
        if ordered:
            preview = ", ".join(Path(value).name for value in ordered[-3:])
            footer += f" | latest: {preview}"
        stdscr.addstr(height - 1, 0, footer[: width - 1])
        key = stdscr.getch()

        if key in (ord("q"), ord("Q"), 27):
            return None
        if key in (ord("d"), ord("D")):
            if ordered or allow_empty:
                return ordered
            message = "Select at least one supported dataset before pressing d."
            continue
        if key in (ord("c"), ord("C")):
            ordered.clear()
            selected_paths.clear()
            message = default_message
            continue
        if key == ord("R"):
            current_dir = root
            selected_index = 0
            message = default_message
            continue
        if key == ord("H"):
            current_dir = Path.home().expanduser().resolve()
            selected_index = 0
            message = default_message
            continue
        if key == ord("M"):
            media = Path("/media")
            if media.is_dir():
                current_dir = media.resolve()
                selected_index = 0
                message = default_message
            else:
                message = "/media is not available on this system."
            continue
        if key == ord("e"):
            typed = _edit_text(stdscr, min(height - 2, 4), 0, str(current_dir), max(8, width - 1)).strip()
            if not typed:
                message = default_message
                continue
            target = _typed_browser_path(typed, current_dir=current_dir, root=root)
            if target.is_dir():
                current_dir = target
                selected_index = 0
                message = default_message
            elif target.is_file() and is_supported_dataset_path(target):
                _toggle_selected(target, selected_paths, ordered, root)
                current_dir = target.parent
                selected_index = 0
                message = f"Selected: {target.name}"
            elif target.is_file():
                message = f"Unsupported dataset type: {target.suffix or '(no extension)'}"
            else:
                message = f"Path does not exist: {target}"
            continue
        if key in (curses.KEY_BACKSPACE, 127, 8):
            if current_dir.parent != current_dir:
                current_dir = current_dir.parent.resolve()
                selected_index = 0
            continue
        if key in (curses.KEY_UP, ord("k")):
            selected_index = max(0, selected_index - 1)
            continue
        if key in (curses.KEY_DOWN, ord("j")):
            selected_index = min(max(0, len(entries) - 1), selected_index + 1)
            continue
        if not entries:
            continue

        entry = entries[selected_index]
        if key == ord(" ") and not entry.is_dir:
            _toggle_selected(entry.path, selected_paths, ordered, root)
            message = default_message
        elif key in (10, 13):
            if entry.is_dir:
                current_dir = entry.path.resolve()
                selected_index = 0
            else:
                _toggle_selected(entry.path, selected_paths, ordered, root)
            message = default_message


def _format_list(values: list[str]) -> str:
    if not values:
        return "(none)"
    names = [Path(value).name for value in values]
    if len(names) <= 2:
        return ", ".join(names)
    return f"{names[0]}, {names[1]} +{len(names) - 2} more"


def _batch_setup_curses(stdscr, initial: dict[str, Any], root: Path) -> dict[str, Any] | None:
    curses.curs_set(0)
    fields = [
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
    ]
    selected = 0
    state = dict(initial)
    message = "↑/↓ move  Enter edit/select  Space toggle  r run batch  q cancel"

    while True:
        stdscr.erase()
        height, width = stdscr.getmaxyx()
        candidates = list(state.get("datasets") or [])
        references = list(state.get("references") or [])
        jobs = comparison_job_count(candidates, references)
        policy = "Per-job runnable metrics" if state.get("per_dataset_metrics") else "Common metrics across every job"
        workers = state.get("workers")
        values = {
            "name": state.get("name") or "",
            "datasets": _format_list(candidates),
            "references": _format_list(references),
            "metric_policy": policy,
            "workers": f"{workers if workers is not None else 'auto'} / max {detected_max_workers()} detected",
            "display": state.get("display") or "compact",
            "force": "Yes" if state.get("force") else "No",
            "dataset_summary": "Yes" if state.get("dataset_summary", True) else "No",
            "refresh_dataset_summary": "Yes" if state.get("refresh_dataset_summary") else "No",
            "fail_fast": "Yes" if state.get("fail_fast") else "No",
        }
        labels = {
            "name": "Batch name",
            "datasets": "Candidate datasets",
            "references": "Reference datasets",
            "metric_policy": "Metric policy",
            "workers": "Worker count",
            "display": "Live display",
            "force": "Replace existing batch",
            "dataset_summary": "Dataset summaries",
            "refresh_dataset_summary": "Refresh summaries",
            "fail_fast": "Stop after first failed job",
        }

        stdscr.addstr(0, 0, "CBR Tests — Batch / comparison run"[: width - 1], curses.A_BOLD)
        stdscr.addstr(1, 0, "Select several candidates and optional independent references."[: width - 1])
        stdscr.addstr(2, 0, message[: width - 1])
        for row, key_name in enumerate(fields, start=4):
            marker = ">" if row - 4 == selected else " "
            attr = curses.A_REVERSE if row - 4 == selected else curses.A_NORMAL
            line = f"{marker} {labels[key_name]:28} {values[key_name]}"
            if row < height - 6:
                stdscr.addstr(row, 0, line[: width - 1], attr)

        summary_row = max(4, height - 5)
        stdscr.hline(summary_row, 0, curses.ACS_HLINE, max(1, width - 1))
        stdscr.addstr(summary_row + 1, 0, f"Run ID: {state.get('run_id')}"[: width - 1], curses.A_BOLD)
        stdscr.addstr(summary_row + 2, 0, f"Candidates: {len(candidates)} | References: {len(references)} | Jobs: {jobs}"[: width - 1], curses.A_BOLD)
        stdscr.addstr(summary_row + 3, 0, "Batch mode automatically builds per-job plans using all structurally runnable tests."[: width - 1])
        stdscr.addstr(summary_row + 4, 0, "Reference self-comparisons are excluded automatically."[: width - 1])

        key = stdscr.getch()
        if key in (ord("q"), ord("Q"), 27):
            return None
        if key == ord("r"):
            try:
                return build_batch_spec(
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
        if key in (curses.KEY_UP, ord("k")):
            selected = max(0, selected - 1)
            continue
        if key in (curses.KEY_DOWN, ord("j")):
            selected = min(len(fields) - 1, selected + 1)
            continue

        field = fields[selected]
        if key == ord(" ") and field in {"force", "dataset_summary", "refresh_dataset_summary", "fail_fast"}:
            state[field] = not bool(state.get(field))
            continue
        if key not in (10, 13):
            continue

        if field == "name":
            state["name"] = _edit_text(stdscr, min(height - 2, selected + 4), 31, str(state.get("name") or ""), max(8, width - 32))
        elif field == "datasets":
            chosen = _multi_file_browser(
                stdscr,
                root,
                candidates,
                title="Select candidate datasets",
                allow_empty=False,
            )
            if chosen is not None:
                state["datasets"] = chosen
        elif field == "references":
            chosen = _multi_file_browser(
                stdscr,
                root,
                references,
                title="Select independent reference datasets",
                allow_empty=True,
            )
            if chosen is not None:
                state["references"] = chosen
        elif field == "metric_policy":
            state["per_dataset_metrics"] = not bool(state.get("per_dataset_metrics"))
        elif field == "workers":
            raw = _edit_text(
                stdscr,
                min(height - 2, selected + 4),
                31,
                "" if state.get("workers") is None else str(state["workers"]),
                max(8, width - 32),
            ).strip()
            if not raw:
                state["workers"] = None
            else:
                try:
                    parsed = int(raw)
                    if parsed < 1:
                        raise ValueError
                    state["workers"] = parsed
                except ValueError:
                    message = "Worker count must be a positive integer or blank for auto."
        elif field == "display":
            current = str(state.get("display") or "compact")
            index = DISPLAY_MODES.index(current) if current in DISPLAY_MODES else 0
            state["display"] = DISPLAY_MODES[(index + 1) % len(DISPLAY_MODES)]
        elif field in {"force", "dataset_summary", "refresh_dataset_summary", "fail_fast"}:
            state[field] = not bool(state.get(field))


def launch_batch_tui(args, repo_root: Path | None = None) -> dict[str, Any]:
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
    selected = curses.wrapper(_batch_setup_curses, initial, root)
    if selected is None:
        raise SystemExit("Batch TUI cancelled")
    return selected


def execute_batch_spec(spec: dict[str, Any], repo_root: Path | None = None) -> dict[str, Any]:
    """Create and execute the batch selected in the TUI using the standard batch pipeline."""

    from create_plan import _create_batch, _deduplicate_dataset_values, _slug

    root = (repo_root or Path.cwd()).expanduser().resolve()
    validated = build_batch_spec(
        name=str(spec.get("name") or ""),
        run_id=str(spec.get("run_id") or default_batch_run_id()),
        datasets=list(spec.get("datasets") or []),
        references=list(spec.get("references") or []),
        per_dataset_metrics=bool(spec.get("per_dataset_metrics")),
        workers=spec.get("workers"),
        display=str(spec.get("display") or "compact"),
        force=bool(spec.get("force")),
        dataset_summary=bool(spec.get("dataset_summary", True)),
        refresh_dataset_summary=bool(spec.get("refresh_dataset_summary")),
        fail_fast=bool(spec.get("fail_fast")),
    )
    plan_id = f"{_slug(validated['name'])}-{validated['run_id']}"
    dataset_paths = _deduplicate_dataset_values(validated["datasets"])
    reference_paths = _deduplicate_dataset_values(validated["references"])
    batch_path = root / "plans" / f"{plan_id}_batch.json"

    _create_batch(
        plan_id=plan_id,
        name=validated["name"],
        description=f"Batch created from the CBR Test Runner TUI. Run ID: {validated['run_id']}.",
        dataset_paths=dataset_paths,
        field_translation_path=None,
        include_metric_ids=None,
        exclude_metric_ids=None,
        reference_dataset_paths=reference_paths,
        service_port_configuration=None,
        output_path=batch_path,
        force=validated["force"],
        per_dataset_metrics=validated["per_dataset_metrics"],
        interactive=False,
    )

    command = [
        sys.executable,
        str(root / "run_batch.py"),
        "--batch",
        str(batch_path),
        "--display",
        validated["display"],
        "--yes-field-translation-sidecar",
    ]
    if validated["workers"] is not None:
        command.extend(["--workers", str(validated["workers"])])
    if validated["force"]:
        command.append("--force-output")
    if not validated["dataset_summary"]:
        command.append("--no-dataset-summary")
    if validated["refresh_dataset_summary"]:
        command.append("--refresh-dataset-summary")
    if validated["fail_fast"]:
        command.append("--fail-fast")

    completed = subprocess.run(command, cwd=root, check=False)
    return {
        "status": "completed" if completed.returncode == 0 else "needs_attention",
        "process_return_code": completed.returncode,
        "batch_path": str(batch_path),
        "run_id": validated["run_id"],
        "candidate_count": len(dataset_paths),
        "reference_count": len(reference_paths),
        "job_count": comparison_job_count(validated["datasets"], validated["references"]),
    }
