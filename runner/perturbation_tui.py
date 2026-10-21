"""Guided file-picker setup for GAN perturbations in the unified Toolbox.

The field browser is the same reusable curses browser used for normal dataset
selection, with file-type filters appropriate to each GAN experiment input.
"""
from __future__ import annotations

import curses
from pathlib import Path
import subprocess
import sys

from runner.friendly_tui import _friendly_dataset_browser, _safe_addstr
from runner.tui import _display_path, _edit_text
from runner.perturbation_experiment import parse_integer_set


FILE_FIELDS = (
    ("base", "Dataset 1: base/reference PCAP", frozenset({".pcap", ".pcapng"}), "datasets"),
    ("comparison", "Dataset 2: comparison PCAP (perturb)", frozenset({".pcap", ".pcapng"}), "datasets"),
    ("plan", "Existing PCAP test plan", frozenset({".json"}), "plans"),
    ("model", "Trained CTGAN model", frozenset({".pkl"}), "models"),
    ("generator_script", "GAN generator script", frozenset({".py"}), ""),
)
TEXT_FIELDS = (
    ("output_dir", "New output directory"),
    ("percentages", "Trailing replacement percentages"),
    ("seeds", "Random seeds"),
)
ALL_FIELDS = (*FILE_FIELDS, *TEXT_FIELDS, ("prepare_only", "Prepare captures only"))


def initial_settings() -> dict[str, str | bool]:
    """Require both capture roles, a fixed plan, model and generator."""
    return {
        "base": "",
        "comparison": "",
        "plan": "",
        "model": "",
        "generator_script": "traffic_gan.py",
        "output_dir": "outcomes/gan-perturbation",
        "percentages": "0,10,20,30,40,50,60,70,80,90,100",
        "seeds": "42",
        "prepare_only": False,
    }


def _resolve_file(value: str, root: Path) -> Path:
    """Resolve paths relative to the repository as all CBR CLI tools do."""
    candidate = Path(value).expanduser()
    return (candidate if candidate.is_absolute() else root / candidate).resolve()


def validate_settings(values: dict, root: Path) -> list[str]:
    """Preflight the chosen files and prevent accidental output replacement."""
    issues: list[str] = []
    for key, title, suffixes, _ in FILE_FIELDS:
        value = str(values.get(key, "")).strip()
        if not value:
            issues.append(f"Select {title.lower()}.")
            continue
        path = _resolve_file(value, root)
        if not path.is_file():
            issues.append(f"{title} does not exist: {path}")
        elif path.suffix.lower() not in suffixes:
            issues.append(f"{title} must be {', '.join(sorted(suffixes))}.")
    if values.get("base") and values.get("comparison") and (
        _resolve_file(str(values["base"]), root) == _resolve_file(str(values["comparison"]), root)
    ):
        issues.append("Dataset 1 and Dataset 2 must be different captures.")
    output = str(values.get("output_dir", "")).strip()
    if not output:
        issues.append("Choose a new output directory.")
    else:
        path = _resolve_file(output, root)
        if path.exists() and (not path.is_dir() or any(path.iterdir())):
            issues.append("Output directory must be new or empty; existing results will not be overwritten.")
    for key, label, high in (("percentages", "Percentages", 100), ("seeds", "Seeds", 2**31 - 1)):
        try:
            numbers = parse_integer_set(str(values.get(key, "")), low=0, high=high)
        except ValueError as exc:
            issues.append(f"{label}: {exc}")
        else:
            if key == "percentages" and 0 not in numbers:
                issues.append("Include 0% for the unmodified baseline.")
    return issues


def build_command(values: dict, root: Path) -> list[str]:
    """Construct the existing CLI invocation from selected setup values."""
    command = [
        sys.executable, str(root / "run_perturbation.py"),
        "--base", str(_resolve_file(str(values["base"]), root)),
        "--comparison", str(_resolve_file(str(values["comparison"]), root)),
        "--plan", str(_resolve_file(str(values["plan"]), root)),
        "--model", str(_resolve_file(str(values["model"]), root)),
        "--generator-script", str(_resolve_file(str(values["generator_script"]), root)),
        "--output-dir", str(_resolve_file(str(values["output_dir"]), root)),
        "--percentages", str(values["percentages"]),
        "--seeds", str(values["seeds"]),
    ]
    if values.get("prepare_only"):
        command.append("--prepare-only")
    return command


def _setup_curses(stdscr, root: Path) -> dict | None:
    """Display the GAN setup menu; Enter opens a picker on file fields."""
    curses.curs_set(0)
    values = initial_settings()
    selected = 0
    message = "Select Dataset 1 and Dataset 2, then plan/model/script. Enter browses files."
    while True:
        stdscr.erase()
        height, width = stdscr.getmaxyx()
        _safe_addstr(stdscr, 0, 0, "CBR Tests - GAN trailing-suffix experiment", curses.A_BOLD)
        _safe_addstr(stdscr, 1, 0, "Up/Down move   Enter select/edit   e type path   Space toggle   r review/run   q/Esc cancel")
        _safe_addstr(stdscr, 2, 0, "Compares Dataset 1 vs Dataset 2 (0%), then Dataset 1 vs Dataset 2 at 10-100% GAN replacement.")
        _safe_addstr(stdscr, 3, 0, "Only Dataset 2 is perturbed. GAN training must use independent captures.")
        visible = max(1, height - 8)
        start = min(max(0, selected - visible + 1), max(0, len(ALL_FIELDS) - visible))
        for row, item in enumerate(ALL_FIELDS[start : start + visible], start=5):
            index = start + row - 5
            key, title = item[:2]
            value = values[key]
            shown = "Yes" if key == "prepare_only" and value else "No" if key == "prepare_only" else str(value or "(not selected)")
            marker = ">" if index == selected else " "
            attr = curses.A_REVERSE if index == selected else curses.A_NORMAL
            _safe_addstr(stdscr, row, 0, f"{marker} {title}: {shown}", attr)
        _safe_addstr(stdscr, height - 2, 0, message[:max(1, width - 1)])
        _safe_addstr(stdscr, height - 1, 0, "File browsers: Up/Down, Enter, Backspace, R repo, H home, M /media, e path, q back")

        key = stdscr.getch()
        if key in (ord("q"), ord("Q"), 27):
            return None
        if key in (curses.KEY_UP, ord("k")):
            selected = max(0, selected - 1)
            continue
        if key in (curses.KEY_DOWN, ord("j")):
            selected = min(len(ALL_FIELDS) - 1, selected + 1)
            continue
        if key == curses.KEY_PPAGE:
            selected = max(0, selected - visible)
            continue
        if key == curses.KEY_NPAGE:
            selected = min(len(ALL_FIELDS) - 1, selected + visible)
            continue
        if key in (ord("r"), ord("R")):
            problems = validate_settings(values, root)
            if problems:
                message = problems[0]
                continue
            return values.copy()

        field = ALL_FIELDS[selected]
        name = field[0]
        if name == "prepare_only":
            if key in (10, 13, 32):
                values[name] = not bool(values[name])
                message = "Prepare-only: generates validated captures without running CBR metrics."
            continue

        if key not in (10, 13, ord("e")):
            continue
        if len(field) == 4 and key != ord("e"):
            _, title, suffixes, directory = field
            initial_dir = root / directory if directory else root
            picked = _friendly_dataset_browser(
                stdscr,
                root,
                str(values[name]),
                title=f"Choose {title.lower()}",
                allowed_suffixes=suffixes,
                start_dir=initial_dir,
            )
            if picked is not None:
                values[name] = picked
                message = f"Selected {title.lower()}: {Path(picked).name}"
            continue

        typed = _edit_text(
            stdscr, max(0, height - 3), 0, str(values[name]),
            max(8, width - 1),
        ).strip()
        if typed:
            values[name] = typed
            message = f"Updated {field[1]}."
        else:
            message = f"{field[1]} unchanged."


def run_perturbation_tool_action(repo_root: Path) -> int:
    """Select files with the standard browser then run the existing CLI."""
    root = repo_root.expanduser().resolve()
    selected = curses.wrapper(_setup_curses, root)
    if selected is None:
        print("\nGAN perturbation setup cancelled.")
        return 0
    command = build_command(selected, root)
    print("\nGAN trailing-suffix perturbation experiment")
    print(f"Dataset 1 (fixed base/reference): {selected['base']}")
    print(f"Dataset 2 (comparison to perturb): {selected['comparison']}")
    print(f"Fixed test plan: {selected['plan']}")
    print(f"Model: {selected['model']}")
    print(f"Generator: {selected['generator_script']}")
    print(f"Percentages: {selected['percentages']}   Seeds: {selected['seeds']}")
    print("Dataset 1 remains fixed; Dataset 2 is perturbed only in generated copies.\n")
    return subprocess.run(command, cwd=root, check=False).returncode
