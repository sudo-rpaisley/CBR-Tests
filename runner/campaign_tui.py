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
from runner.tui import _display_path


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


def _discover_batch_manifests(root: Path, directory: Path | None = None) -> list[tuple[Path, dict[str, Any]]]:
    """Return valid saved batch manifests without descending into generated per-job plan folders."""

    root = root.expanduser().resolve()
    base = (directory or (root / "plans")).expanduser().resolve()
    if not base.is_dir():
        return []

    manifests: list[tuple[Path, dict[str, Any]]] = []
    for candidate in sorted(base.rglob("*.json")):
        try:
            relative = candidate.relative_to(base)
        except ValueError:
            continue
        if any(part.endswith("_batch_plans") for part in relative.parts[:-1]):
            continue
        try:
            batch = validate_batch_manifest(candidate)
        except (OSError, ValueError, json.JSONDecodeError):
            continue
        manifests.append((candidate.resolve(), batch))
    return manifests


def _batch_picker(stdscr, root: Path, *, initial_dir: str = "plans") -> list[str] | None:
    """Select one or more already-built batch matrices to append to a campaign queue."""

    root = root.expanduser().resolve()
    search_root = (root / initial_dir).resolve()
    selected = 0
    checked: set[Path] = set()
    manifests = _discover_batch_manifests(root, search_root)
    message = ""

    while True:
        stdscr.erase()
        height, _ = stdscr.getmaxyx()
        selected = min(selected, max(0, len(manifests) - 1))
        _safe_addstr(stdscr, 0, 0, "Choose saved batch matrices", curses.A_BOLD)
        _safe_addstr(
            stdscr,
            1,
            0,
            "Space toggle   Enter queue selected   a select all   c clear   r refresh   q cancel",
        )
        _safe_addstr(
            stdscr,
            2,
            0,
            f"Found {len(manifests)} batch(es) under {_display_path(search_root, root)}; selected {len(checked)}",
        )
        if message:
            _safe_addstr(stdscr, 3, 0, message, curses.A_BOLD)

        start_row = 5
        visible = max(1, height - start_row - 1)
        scroll = min(max(0, selected - visible + 1), max(0, len(manifests) - visible))
        for row, (manifest_path, batch) in enumerate(manifests[scroll : scroll + visible], start=start_row):
            index = scroll + row - start_row
            meta = batch["batch_meta"]
            name = str(meta.get("name") or meta["batch_id"])
            marker = ">" if index == selected else " "
            tick = "x" if manifest_path in checked else " "
            attr = curses.A_REVERSE if index == selected else curses.A_NORMAL
            label = (
                f"{marker} [{tick}] {name} — {len(batch['jobs'])} jobs — "
                f"{_display_path(manifest_path, root)}"
            )
            _safe_addstr(stdscr, row, 0, label, attr)

        if not manifests:
            _safe_addstr(
                stdscr,
                start_row,
                0,
                "No valid saved batch manifests were found under plans/. Build the batches first, then return here.",
            )

        key = stdscr.getch()
        message = ""
        if key in (ord("q"), ord("Q"), 27):
            return None
        if key in (curses.KEY_UP, ord("k")):
            selected = max(0, selected - 1)
            continue
        if key in (curses.KEY_DOWN, ord("j")):
            selected = min(max(0, len(manifests) - 1), selected + 1)
            continue
        if key == ord(" ") and manifests:
            manifest_path = manifests[selected][0]
            if manifest_path in checked:
                checked.remove(manifest_path)
            else:
                checked.add(manifest_path)
            continue
        if key in (ord("a"), ord("A")):
            chosen = _batch_picker(stdscr, root, initial_dir="plans")
            if chosen is None:
                continue
            state["batches"].extend(chosen)
            selected = len(state["batches"]) - 1
            message = f"Queued {len(chosen)} batch matrix{'es' if len(chosen) != 1 else ''}."
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
    print(f"Batch matrices queued: {payload['campaign_meta']['matrix_count']}")
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
