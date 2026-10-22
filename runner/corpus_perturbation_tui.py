"""Single-folder selector for recursive CTGAN leave-one-out corpus experiments."""
from __future__ import annotations

import curses
from datetime import datetime, timezone
from pathlib import Path
import subprocess
import sys

from runner.friendly_tui import _safe_addstr
from runner.tui import _display_path, _edit_text


def _directories(path: Path) -> list[Path]:
    """List directories only; never show PCAP file paths in the root selector."""
    try:
        return sorted(
            (entry for entry in path.iterdir() if entry.is_dir() and not entry.is_symlink()),
            key=lambda p: (p.name.casefold(), p.name),
        )
    except PermissionError:
        return []


def _select_directory(stdscr, root: Path) -> Path | None:
    """Choose any folder using the usual navigation plus d=current directory."""
    curses.curs_set(0)
    current = root / "datasets" if (root / "datasets").is_dir() else root
    current = current.resolve()
    selected = 0
    message = "Enter opens folder  d selects CURRENT folder  Backspace up  R repo  H home  e path  q cancel"
    while True:
        stdscr.erase()
        height, width = stdscr.getmaxyx()
        entries = _directories(current)
        selected = min(selected, max(0, len(entries) - 1))
        _safe_addstr(stdscr, 0, 0, "Recursive leave-one-out GAN campaign", curses.A_BOLD)
        _safe_addstr(stdscr, 1, 0, message)
        _safe_addstr(stdscr, 2, 0, f"Current folder: {_display_path(current, root)}")
        _safe_addstr(stdscr, 3, 0, "All descendants are scanned. Each folder with >=2 PCAPs is a separate group.")
        visible = max(1, height - 6)
        start = min(max(0, selected - visible + 1), max(0, len(entries) - visible))
        for row, entry in enumerate(entries[start : start + visible], start=5):
            index = start + row - 5
            attr = curses.A_REVERSE if selected == index else curses.A_NORMAL
            _safe_addstr(stdscr, row, 0, f"{'>' if selected == index else ' '} {entry.name}/", attr)
        _safe_addstr(stdscr, height - 1, 0, "d = use highlighted CURRENT directory, including all subfolders")
        key = stdscr.getch()
        if key in (ord("q"), ord("Q"), 27):
            return None
        if key in (ord("d"), ord("D")):
            return current.resolve()
        if key == ord("R"):
            current = root.resolve()
            selected = 0
            continue
        if key == ord("H"):
            current = Path.home().resolve()
            selected = 0
            continue
        if key == ord("M"):
            if Path("/media").is_dir():
                current = Path("/media").resolve()
                selected = 0
            else:
                message = "/media is unavailable."
            continue
        if key == ord("e"):
            typed = _edit_text(
                stdscr, min(height - 2, 4), 0, str(current), max(8, width - 1)
            ).strip()
            if typed:
                path = Path(typed).expanduser()
                if not path.is_absolute():
                    path = current / path
                if path.is_dir():
                    current = path.resolve()
                    selected = 0
                else:
                    message = "Choose an existing directory."
            continue
        if key in (curses.KEY_BACKSPACE, 127, 8):
            if current.parent != current:
                current = current.parent.resolve()
                selected = 0
            continue
        if key in (curses.KEY_UP, ord("k")):
            selected = max(0, selected - 1)
            continue
        if key in (curses.KEY_DOWN, ord("j")):
            selected = min(len(entries) - 1, selected + 1)
            continue
        if key in (curses.KEY_PPAGE,):
            selected = max(0, selected - visible)
            continue
        if key in (curses.KEY_NPAGE,):
            selected = min(len(entries) - 1, selected + visible)
            continue
        if key in (10, 13) and entries:
            current = entries[selected].resolve()
            selected = 0


def run_corpus_tool_action(root: Path) -> int:
    """Select one folder, preview all leave-one-out cases, then confirm once."""
    root = root.expanduser().resolve()
    chosen = curses.wrapper(_select_directory, root)
    if chosen is None:
        print("\nRecursive GAN campaign cancelled.")
        return 0
    from runner.corpus_perturbation import discover_groups

    groups, skipped = discover_groups(chosen)
    cases = sum(len(g["captures"]) for g in groups)
    print(f"\nDataset root: {chosen}")
    print(f"Eligible PCAP groups: {len(groups)}")
    print(f"Held-out training/evaluation cases: {cases}")
    print(f"Singleton directories skipped: {len(skipped)}")
    print(f"Default comparisons: {cases * 11} (0, 10, …, 100%, one seed)")
    expected = 11 * sum(
        p.stat().st_size for group in groups for p in group["captures"]
    )
    print(f"Estimated generated capture storage: {expected / (1024**3):,.2f} GiB "
          "(excludes models, metrics and overhead)")
    print("Each held-out file gets a new CTGAN trained on every other PCAP in its own directory.")
    print("Full raw outcome JSON, complete nested metric measurements and provenance are archived.")
    if not cases:
        print("No eligible groups (a folder needs at least two PCAP/PCAPNG files).")
        return 1
    try:
        confirmation = input("Type RUN to begin the campaign (anything else cancels): ").strip()
    except (EOFError, KeyboardInterrupt):
        return 0
    if confirmation != "RUN":
        print("Campaign cancelled; no files generated.")
        return 0
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output = root / "outcomes" / "gan-corpus" / run_id
    command = [
        sys.executable, str(root / "run_perturbation_corpus.py"),
        "--dataset-root", str(chosen),
        "--output-dir", str(output),
    ]
    print(f"\nArchive: {output}")
    print("For interrupted cases, repeat with --resume and this output directory.\n")
    return subprocess.run(command, cwd=root, check=False).returncode
