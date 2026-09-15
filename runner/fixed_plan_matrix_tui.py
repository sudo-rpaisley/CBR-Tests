from __future__ import annotations

import curses
from pathlib import Path
import subprocess
import sys

from runner.fixed_plan_matrix import build_fixed_plan_matrix
from runner.friendly_tui import _edit_text, _safe_addstr, _status_attr
from runner.toolbox_tui import ToolboxItem, _json_browser
from runner.tui_batch import _format_list, _multi_file_browser, comparison_job_count


PCAP_SUFFIXES = {".pcap", ".pcapng"}


def fixed_plan_matrix_toolbox_item() -> ToolboxItem:
    return ToolboxItem(
        "fixed_pcap_matrix",
        "Map PCAP datasets to existing plan",
        "Reuse one validated PCAP plan and map candidate/reference captures onto it without regenerating the scientific plan.",
        "Prepare experiments",
    )


def _pcap_only(values: list[str], root: Path, *, label: str) -> list[str]:
    output: list[str] = []
    for value in values:
        path = Path(value).expanduser()
        if not path.is_absolute():
            path = root / path
        path = path.resolve()
        if path.suffix.lower() not in PCAP_SUFFIXES:
            raise ValueError(f"{label} must be PCAP/PCAPNG: {path.name}")
        output.append(str(path))
    return output


def _matrix_review_curses(stdscr, state: dict) -> dict | None:
    curses.curs_set(0)
    selected = 0
    fields = ("name", "output", "run_now")
    message = ""
    while True:
        stdscr.erase()
        candidates = list(state.get("candidates") or [])
        references = list(state.get("references") or [])
        jobs = comparison_job_count(candidates, references)
        ready = bool(str(state.get("name") or "").strip() and str(state.get("output") or "").strip() and candidates)
        _safe_addstr(stdscr, 0, 0, "CBR Tests — Map PCAP datasets to existing plan", curses.A_BOLD)
        _safe_addstr(stdscr, 1, 0, "The scientific plan stays fixed; this screen only defines dataset bindings.")
        _safe_addstr(stdscr, 2, 0, "READY TO SAVE" if ready else "NEEDS ATTENTION", _status_attr(ready))
        _safe_addstr(stdscr, 3, 0, "↑/↓ move   Enter edit/toggle   s save   q cancel")
        if message:
            _safe_addstr(stdscr, 4, 0, message, curses.A_BOLD)

        lines = [
            f"Plan: {state.get('plan')}",
            f"Candidates: {len(candidates)} — {_format_list(candidates)}",
            f"References: {len(references)} — {_format_list(references)}",
            f"Jobs: {jobs}",
        ]
        for row, line in enumerate(lines, start=6):
            _safe_addstr(stdscr, row, 0, line)

        values = {
            "name": str(state.get("name") or ""),
            "output": str(state.get("output") or ""),
            "run_now": "Yes" if state.get("run_now") else "No",
        }
        labels = {
            "name": "Matrix name",
            "output": "Batch manifest",
            "run_now": "Run immediately after save",
        }
        for row, field in enumerate(fields, start=12):
            marker = ">" if row - 12 == selected else " "
            attr = curses.A_REVERSE if row - 12 == selected else curses.A_NORMAL
            _safe_addstr(stdscr, row, 0, f"{marker} {labels[field]:30} {values[field]}", attr)

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
        if key in (ord("s"), ord("S")):
            if ready:
                return state
            message = "Enter a matrix name and output path before saving."
            continue
        if key not in (10, 13, ord(" ")):
            continue
        field = fields[selected]
        height, width = stdscr.getmaxyx()
        if field in {"name", "output"}:
            state[field] = _edit_text(stdscr, min(height - 2, 16), 0, str(state.get(field) or ""), max(8, width - 1)).strip()
        else:
            state["run_now"] = not bool(state.get("run_now"))


def launch_fixed_plan_matrix_builder(repo_root: Path | None = None) -> Path | None:
    root = (repo_root or Path.cwd()).expanduser().resolve()
    plan = curses.wrapper(_json_browser, root, title="Choose the fixed PCAP plan", initial_dir="plans")
    if plan is None:
        return None
    candidates = curses.wrapper(
        _multi_file_browser,
        root,
        [],
        title="Select candidate PCAP/PCAPNG datasets",
        allow_empty=False,
    )
    if not candidates:
        return None
    references = curses.wrapper(
        _multi_file_browser,
        root,
        [],
        title="Select reference PCAP/PCAPNG datasets (optional)",
        allow_empty=True,
    )
    if references is None:
        return None

    candidates = _pcap_only(candidates, root, label="Candidate dataset")
    references = _pcap_only(references, root, label="Reference dataset")
    plan_path = Path(plan).expanduser()
    if not plan_path.is_absolute():
        plan_path = root / plan_path
    default_name = f"{plan_path.stem.replace('_', ' ').replace('-', ' ')} matrix"
    default_output = root / "plans" / f"{plan_path.stem}_mapped_batch.json"
    state = {
        "plan": str(plan),
        "candidates": candidates,
        "references": references,
        "name": default_name,
        "output": str(default_output.relative_to(root)),
        "run_now": False,
    }
    selected = curses.wrapper(_matrix_review_curses, state)
    if selected is None:
        return None

    written = build_fixed_plan_matrix(
        name=str(selected["name"]),
        plan_path=plan,
        candidate_paths=candidates,
        reference_paths=references,
        output_path=str(selected["output"]),
        overwrite=False,
        repo_root=root,
    )
    print(f"Mapped PCAP matrix written: {written}")
    if selected.get("run_now"):
        completed = subprocess.run(
            [sys.executable, str(root / "run_batch.py"), "--batch", str(written)],
            cwd=root,
            check=False,
        )
        if completed.returncode != 0:
            raise RuntimeError(f"Mapped matrix run returned {completed.returncode}")
    else:
        print(f"Run it with: python run_batch.py --batch {written}")
    return written
