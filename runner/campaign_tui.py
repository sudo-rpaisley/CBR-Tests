from __future__ import annotations

import curses
import json
from pathlib import Path
import subprocess
import sys
from typing import Any

from runner.campaign import (
    build_campaign,
    campaign_state_path,
    load_campaign,
    resolve_repo_path,
    slug,
    validate_batch_manifest,
    write_campaign,
)
from runner.friendly_tui import _choice_dialog, _edit_text, _initialise_colours, _safe_addstr, _status_attr
from runner.toolbox_tui import ToolboxItem, _json_browser


CAMPAIGN_TOOLBOX_ITEMS = (
    ToolboxItem(
        "campaign_run",
        "Run comparison campaign",
        "Run an ordered queue of independent comparison matrices with campaign-level resume/retry.",
        "Run experiments",
    ),
    ToolboxItem(
        "campaign_build",
        "Build comparison campaign",
        "Queue several prepared batch/comparison matrices to run one after another.",
        "Prepare experiments",
    ),
)


def campaign_toolbox_items() -> tuple[ToolboxItem, ...]:
    return CAMPAIGN_TOOLBOX_ITEMS


def _automatic_output(name: str) -> str:
    return str(Path("campaigns") / f"{slug(name)}_campaign.json")


def _resolve(root: Path, value: str) -> Path:
    return resolve_repo_path(root, value)


def _campaign_review_lines(state: dict[str, Any], root: Path) -> list[str]:
    batches = list(state.get("batches") or [])
    jobs = 0
    names: list[str] = []
    for value in batches:
        path = _resolve(root, str(value))
        try:
            batch = validate_batch_manifest(path)
        except (OSError, ValueError, json.JSONDecodeError):
            names.append(Path(str(value)).name)
            continue
        meta = batch["batch_meta"]
        names.append(str(meta.get("name") or meta["batch_id"]))
        jobs += len(batch["jobs"])
    return [
        f"Campaign: {state.get('name') or 'Not named'}",
        f"Matrices queued: {len(batches)}",
        f"Comparison jobs queued: {jobs}",
        f"Order: {' → '.join(names) if names else 'No matrices selected'}",
        f"Manifest: {state.get('output') or _automatic_output(str(state.get('name') or 'comparison-campaign'))}",
    ]


def _campaign_issues(state: dict[str, Any], root: Path) -> list[str]:
    issues: list[str] = []
    if not str(state.get("name") or "").strip():
        issues.append("Enter a campaign name.")
    batches = list(state.get("batches") or [])
    if not batches:
        issues.append("Add at least one batch/comparison matrix.")
    for value in batches:
        path = _resolve(root, str(value))
        if not path.is_file():
            issues.append(f"Batch manifest does not exist: {value}")
            continue
        try:
            validate_batch_manifest(path)
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            issues.append(str(exc))
    return issues


def _review_campaign(stdscr, state: dict[str, Any], root: Path) -> bool:
    while True:
        stdscr.erase()
        _safe_addstr(stdscr, 0, 0, "Review comparison campaign", curses.A_BOLD)
        _safe_addstr(stdscr, 1, 0, "Matrices run strictly in the order shown below.")
        lines = _campaign_review_lines(state, root)
        for row, line in enumerate(lines, start=3):
            _safe_addstr(stdscr, row, 2, line)
        _safe_addstr(stdscr, 3 + len(lines) + 1, 0, "Enter/b build campaign   Esc back", curses.A_BOLD)
        key = stdscr.getch()
        if key in (10, 13, ord("b"), ord("B")):
            return True
        if key in (27, ord("q"), ord("Q")):
            return False


def _campaign_builder_curses(stdscr, root: Path) -> dict[str, Any] | None:
    curses.curs_set(0)
    _initialise_colours()
    state: dict[str, Any] = {
        "name": "",
        "description": "",
        "batches": [],
        "output": "campaigns/comparison-campaign_campaign.json",
        "output_auto": True,
        "force": False,
    }
    selected = 0
    message = ""

    while True:
        stdscr.erase()
        height, width = stdscr.getmaxyx()
        batches = list(state["batches"])
        selected = min(selected, max(0, len(batches) - 1))
        issues = _campaign_issues(state, root)
        ready = not issues

        _safe_addstr(stdscr, 0, 0, "CBR Tests — Comparison campaign builder", curses.A_BOLD)
        _safe_addstr(stdscr, 1, 0, "Queue independent matrices; each matrix finishes before the next starts.")
        status = "READY TO BUILD" if ready else f"NEEDS ATTENTION — {issues[0]}"
        _safe_addstr(stdscr, 2, 0, status, _status_attr(ready))
        _safe_addstr(stdscr, 3, 0, "n name   a add matrix   x remove   U/D reorder   d description   o output   f overwrite   b build   q quit")
        _safe_addstr(stdscr, 4, 0, f"Name: {state['name'] or '(not set)'}")
        _safe_addstr(stdscr, 5, 0, f"Output: {state['output']}   Replace existing: {'ON' if state['force'] else 'off'}")
        _safe_addstr(stdscr, 6, 0, f"Matrices queued: {len(batches)}")
        if message:
            _safe_addstr(stdscr, 7, 0, message, curses.A_BOLD)

        start_row = 9
        visible = max(1, height - start_row - 2)
        scroll = min(max(0, selected - visible + 1), max(0, len(batches) - visible))
        for row, value in enumerate(batches[scroll : scroll + visible], start=start_row):
            index = scroll + row - start_row
            path = _resolve(root, str(value))
            try:
                batch = validate_batch_manifest(path)
                meta = batch["batch_meta"]
                label = f"{index + 1:02d}. {meta.get('name') or meta['batch_id']} — {len(batch['jobs'])} jobs"
            except Exception:
                label = f"{index + 1:02d}. {value} — INVALID"
            marker = ">" if index == selected else " "
            attr = curses.A_REVERSE if index == selected else curses.A_NORMAL
            _safe_addstr(stdscr, row, 0, f"{marker} {label}", attr)
        if not batches:
            _safe_addstr(stdscr, start_row, 2, "No matrices queued. Press a to add a saved *_batch.json manifest.")

        key = stdscr.getch()
        message = ""
        if key in (ord("q"), ord("Q"), 27):
            return None
        if key in (curses.KEY_UP, ord("k")):
            selected = max(0, selected - 1)
            continue
        if key in (curses.KEY_DOWN, ord("j")):
            selected = min(max(0, len(batches) - 1), selected + 1)
            continue
        if key in (ord("n"), ord("N")):
            value = _edit_text(stdscr, min(height - 2, 5), 0, str(state["name"]), max(8, width - 1)).strip()
            if value:
                state["name"] = value
                if state.get("output_auto"):
                    state["output"] = _automatic_output(value)
            continue
        if key == ord("d"):
            state["description"] = _edit_text(stdscr, min(height - 2, 5), 0, str(state["description"]), max(8, width - 1)).strip()
            continue
        if key in (ord("a"), ord("A")):
            chosen = _json_browser(stdscr, root, title="Choose batch/comparison matrix", initial_dir="plans")
            if chosen is None:
                continue
            try:
                validate_batch_manifest(_resolve(root, chosen))
            except Exception as exc:
                message = f"Not a valid batch manifest: {exc}"
                continue
            state["batches"].append(chosen)
            selected = len(state["batches"]) - 1
            continue
        if key in (ord("x"), ord("X")) and batches:
            state["batches"].pop(selected)
            selected = min(selected, max(0, len(state["batches"]) - 1))
            continue
        if key == ord("U") and batches and selected > 0:
            state["batches"][selected - 1], state["batches"][selected] = state["batches"][selected], state["batches"][selected - 1]
            selected -= 1
            continue
        if key == ord("D") and batches and selected < len(batches) - 1:
            state["batches"][selected + 1], state["batches"][selected] = state["batches"][selected], state["batches"][selected + 1]
            selected += 1
            continue
        if key in (ord("o"), ord("O")):
            value = _edit_text(stdscr, min(height - 2, 5), 0, str(state["output"]), max(8, width - 1)).strip()
            if value:
                state["output"] = value
                state["output_auto"] = False
            continue
        if key in (ord("f"), ord("F")):
            state["force"] = not bool(state["force"])
            continue
        if key in (ord("b"), ord("B")):
            issues = _campaign_issues(state, root)
            if issues:
                message = issues[0]
                continue
            if _review_campaign(stdscr, state, root):
                return state


def launch_campaign_builder(repo_root: Path | None = None) -> dict[str, Any] | None:
    root = (repo_root or Path.cwd()).expanduser().resolve()
    state = curses.wrapper(_campaign_builder_curses, root)
    if state is None:
        return None
    batch_paths = [_resolve(root, str(value)) for value in state["batches"]]
    payload = build_campaign(
        name=str(state["name"]),
        batch_paths=batch_paths,
        repo_root=root,
        description=str(state.get("description") or ""),
    )
    output = _resolve(root, str(state["output"]))
    written = write_campaign(output, payload, overwrite=bool(state.get("force")))
    print(f"Campaign written: {written}")
    print(f"Matrices queued: {payload['campaign_meta']['matrix_count']}")
    print(f"Comparison jobs queued: {payload['campaign_meta']['job_count']}")
    return {"output_path": str(written), "campaign_id": payload["campaign_meta"]["campaign_id"]}


def _campaign_run_command(root: Path, selected: str) -> list[str] | None:
    manifest_path = _resolve(root, selected)
    campaign = load_campaign(manifest_path)
    strict = curses.wrapper(
        _choice_dialog,
        "Choose campaign execution mode",
        ("Strict final experiment", "Normal / compatibility"),
        "Strict final experiment",
    )
    if strict is None:
        return None

    command = [
        sys.executable,
        str(root / "run_campaign.py"),
        "--campaign",
        str(manifest_path),
        "--display",
        "compact",
        "--no-update-field-translation",
    ]
    if strict == "Strict final experiment":
        command.append("--experiment-mode")

    output_value = campaign.get("output_directory") or str(Path("outcomes") / str(campaign["campaign_meta"]["campaign_id"]))
    state_path = campaign_state_path(_resolve(root, str(output_value)))
    if state_path.is_file():
        try:
            state = json.loads(state_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            state = {}
        status = str(state.get("status") or "unknown")
        if status in {"running", "interrupted"}:
            command.append("--resume")
        elif status == "needs_attention":
            action = curses.wrapper(
                _choice_dialog,
                "Campaign needs attention",
                ("Resume remaining matrices", "Retry failed/attention matrices", "Cancel"),
                "Retry failed/attention matrices",
            )
            if action in (None, "Cancel"):
                return None
            command.append("--resume")
            if action == "Retry failed/attention matrices":
                command.append("--retry-failed")
        elif status == "completed":
            action = curses.wrapper(
                _choice_dialog,
                "Campaign already completed",
                ("Review completed checkpoint", "Cancel"),
                "Review completed checkpoint",
            )
            if action in (None, "Cancel"):
                return None
            command.append("--resume")
    return command


def run_campaign_tool_action(action: str, repo_root: Path | None = None) -> int:
    root = (repo_root or Path.cwd()).expanduser().resolve()
    if action == "campaign_build":
        launch_campaign_builder(root)
        return 0
    if action == "campaign_run":
        selected = curses.wrapper(_json_browser, root, title="Choose comparison campaign", initial_dir="campaigns")
        if selected is None:
            return 0
        command = _campaign_run_command(root, selected)
        if command is None:
            return 0
        return subprocess.run(command, cwd=root, check=False).returncode
    raise ValueError(f"Unknown comparison campaign action: {action}")
