"""Guided CTGAN training and perturbation setup, using CBR-Tests file selectors."""
from __future__ import annotations

import curses
from pathlib import Path
import subprocess
import sys

from runner.friendly_tui import _choice_dialog, _friendly_dataset_browser, _safe_addstr
from runner.tui import _edit_text
from runner.tui_batch import _multi_file_browser
from runner.perturbation_experiment import parse_integer_set


CAPTURE_SUFFIXES = frozenset({".pcap", ".pcapng"})
MODEL_MODES = ("Train new CTGAN from PCAPs", "Use existing CTGAN checkpoint")
TRAIN_MODE = MODEL_MODES[0]
EXISTING_MODE = MODEL_MODES[1]
FILE_FIELDS = (
    ("base", "Dataset 1: fixed reference", CAPTURE_SUFFIXES, "datasets"),
    ("comparison", "Dataset 2: comparison to perturb", CAPTURE_SUFFIXES, "datasets"),
    ("plan", "Existing PCAP test plan", frozenset({".json"}), "plans"),
    ("model", "Existing CTGAN model", frozenset({".pkl"}), "models"),
    ("generator_script", "GAN generator script", frozenset({".py"}), ""),
)
TRAINING_INPUT = ("training_pcaps", "Training PCAPs (one or more)")
MODEL_SOURCE = ("model_source", "CTGAN model source")
DEVICE_FIELD = ("training_device", "Training device")
TEXT_FIELDS = (
    ("output_dir", "New output directory"),
    ("percentages", "Trailing replacement percentages"),
    ("seeds", "Random seeds"),
)
TRAINING_TEXT_FIELDS = (
    ("training_epochs", "Training epochs"),
    ("training_batch_size", "Training batch size"),
)


def initial_settings() -> dict:
    """Default to training from separate captures; existing models remain available."""
    return {
        "base": "",
        "comparison": "",
        "plan": "",
        "model_source": TRAIN_MODE,
        "training_pcaps": [],
        "model": "",
        "generator_script": "traffic_gan.py",
        "training_epochs": "100",
        "training_batch_size": "100",
        "training_device": "cpu",
        "output_dir": "outcomes/gan-perturbation",
        "percentages": "0,10,20,30,40,50,60,70,80,90,100",
        "seeds": "42",
        "prepare_only": False,
    }


def _resolve_file(value: str, root: Path) -> Path:
    candidate = Path(value).expanduser()
    return (candidate if candidate.is_absolute() else root / candidate).resolve()


def visible_fields(values: dict) -> tuple:
    """Show only the fields applicable to the selected training mode."""
    fields = [*FILE_FIELDS[:3], MODEL_SOURCE]
    if values.get("model_source") == TRAIN_MODE:
        fields.extend((TRAINING_INPUT, *TRAINING_TEXT_FIELDS, DEVICE_FIELD))
    else:
        fields.append(FILE_FIELDS[3])
    fields.extend((FILE_FIELDS[4], *TEXT_FIELDS, ("prepare_only", "Prepare captures only")))
    return tuple(fields)


def validate_settings(values: dict, root: Path) -> list[str]:
    """Preflight paths, capture roles, training options, and safe outputs."""
    issues: list[str] = []
    mode = values.get("model_source", EXISTING_MODE)
    if mode not in MODEL_MODES:
        issues.append("Choose how to obtain the CTGAN model.")
    required_files = (*FILE_FIELDS[:3], FILE_FIELDS[4])
    if mode == EXISTING_MODE:
        required_files += (FILE_FIELDS[3],)
    for key, title, suffixes, _ in required_files:
        value = str(values.get(key, "")).strip()
        if not value:
            issues.append(f"Select {title.lower()}.")
            continue
        path = _resolve_file(value, root)
        if not path.is_file():
            issues.append(f"{title} does not exist: {path}")
        elif path.suffix.lower() not in suffixes:
            issues.append(f"{title} must be {', '.join(sorted(suffixes))}.")

    base = _resolve_file(str(values["base"]), root) if values.get("base") else None
    comparison = _resolve_file(str(values["comparison"]), root) if values.get("comparison") else None
    if base and comparison and base == comparison:
        issues.append("Dataset 1 and Dataset 2 must be different captures.")
    if mode == TRAIN_MODE:
        selected = values.get("training_pcaps") or []
        if not selected:
            issues.append("Select at least one training PCAP/PCAPNG.")
        else:
            paths = [_resolve_file(str(item), root) for item in selected]
            if len(set(paths)) != len(paths):
                issues.append("Do not select the same training capture twice.")
            for path in paths:
                if not path.is_file() or path.suffix.lower() not in CAPTURE_SUFFIXES:
                    issues.append(f"Invalid training capture: {path}")
                if path in (base, comparison):
                    issues.append("Training captures must be independent of Dataset 1 and Dataset 2.")
        try:
            epochs = int(values.get("training_epochs", ""))
            batch_size = int(values.get("training_batch_size", ""))
            if epochs < 1 or batch_size < 10 or batch_size % 10:
                issues.append("Epochs must be positive; batch size must be a multiple of ten (>=10).")
        except (TypeError, ValueError):
            issues.append("Training epochs and batch size must be integers.")
        if values.get("training_device") not in {"cpu", "cuda"}:
            issues.append("Choose a valid training device (cpu/cuda).")
    output = str(values.get("output_dir", "")).strip()
    if not output:
        issues.append("Choose a new output directory.")
    else:
        path = _resolve_file(output, root)
        if path.exists() and (not path.is_dir() or any(path.iterdir())):
            issues.append("Output directory must be new or empty; previous results will not be overwritten.")
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
    """Pass selected training PCAPs or an existing checkpoint to the same runner."""
    command = [
        sys.executable, str(root / "run_perturbation.py"),
        "--base", str(_resolve_file(str(values["base"]), root)),
        "--comparison", str(_resolve_file(str(values["comparison"]), root)),
        "--plan", str(_resolve_file(str(values["plan"]), root)),
        "--generator-script", str(_resolve_file(str(values["generator_script"]), root)),
        "--output-dir", str(_resolve_file(str(values["output_dir"]), root)),
        "--percentages", str(values["percentages"]),
        "--seeds", str(values["seeds"]),
    ]
    if values.get("model_source") == TRAIN_MODE:
        for capture in values.get("training_pcaps", []):
            command.extend(("--train-pcap", str(_resolve_file(capture, root))))
        command.extend((
            "--training-epochs", str(values["training_epochs"]),
            "--training-batch-size", str(values["training_batch_size"]),
            "--training-device", str(values["training_device"]),
        ))
    else:
        command.extend(("--model", str(_resolve_file(str(values["model"]), root))))
    if values.get("prepare_only"):
        command.append("--prepare-only")
    return command


def _setup_curses(stdscr, root: Path) -> dict | None:
    """A compact setup screen with a multi-PCAP training file selector."""
    curses.curs_set(0)
    values = initial_settings()
    selected = 0
    message = "Choose Dataset 1, Dataset 2, then training captures; Enter opens file selectors."
    while True:
        stdscr.erase()
        height, width = stdscr.getmaxyx()
        fields = visible_fields(values)
        selected = min(selected, len(fields) - 1)
        _safe_addstr(stdscr, 0, 0, "CBR Tests - CTGAN training and perturbation", curses.A_BOLD)
        _safe_addstr(stdscr, 1, 0, "Up/Down move  Enter choose/edit  e type path  Space toggle  r run  q/Esc cancel")
        _safe_addstr(stdscr, 2, 0, "Dataset 1 is fixed; only Dataset 2 is perturbed. Training PCAPs must be separate.")
        _safe_addstr(stdscr, 3, 0, "Train a model here or select a previously trained checkpoint. No Git uploads of captures.")
        visible = max(1, height - 8)
        start = min(max(0, selected - visible + 1), max(0, len(fields) - visible))
        for row, field in enumerate(fields[start : start + visible], start=5):
            index = start + row - 5
            key, label = field[:2]
            value = values[key]
            if key == "prepare_only":
                shown = "Yes" if value else "No"
            elif key == "training_pcaps":
                shown = f"{len(value)} selected: " + ", ".join(Path(p).name for p in value[-2:])
            else:
                shown = str(value or "(not selected)")
            marker = ">" if index == selected else " "
            attr = curses.A_REVERSE if index == selected else curses.A_NORMAL
            _safe_addstr(stdscr, row, 0, f"{marker} {label}: {shown}", attr)
        _safe_addstr(stdscr, height - 2, 0, message)
        _safe_addstr(stdscr, height - 1, 0, "Browser: Space select; d done; R repo; H home; M /media; e path; q back")
        key = stdscr.getch()
        if key in (ord("q"), ord("Q"), 27):
            return None
        if key in (curses.KEY_UP, ord("k")):
            selected = max(0, selected - 1)
            continue
        if key in (curses.KEY_DOWN, ord("j")):
            selected = min(len(fields) - 1, selected + 1)
            continue
        if key == curses.KEY_PPAGE:
            selected = max(0, selected - visible)
            continue
        if key == curses.KEY_NPAGE:
            selected = min(len(fields) - 1, selected + visible)
            continue
        if key in (ord("r"), ord("R")):
            problems = validate_settings(values, root)
            if problems:
                message = problems[0]
                continue
            return values.copy()
        field = fields[selected]
        name = field[0]
        if name == "prepare_only":
            if key in (10, 13, 32):
                values[name] = not bool(values[name])
            continue
        if name == "model_source" and key in (10, 13, 32):
            chosen = _choice_dialog(stdscr, "CTGAN model source", MODEL_MODES, values[name])
            if chosen is not None:
                values[name] = chosen
                message = f"Model source: {chosen}"
            continue
        if name == "training_device" and key in (10, 13, 32):
            chosen = _choice_dialog(stdscr, "Training device", ("cpu", "cuda"), values[name])
            if chosen:
                values[name] = chosen
            continue
        if name == "training_pcaps" and key in (10, 13, 32):
            chosen = _multi_file_browser(
                stdscr, root, list(values[name]),
                title="Select independent training PCAP/PCAPNG captures",
                allow_empty=False, allowed_suffixes=CAPTURE_SUFFIXES,
            )
            if chosen is not None:
                values[name] = chosen
                message = f"Selected {len(chosen)} training capture(s)."
            continue
        if key not in (10, 13, ord("e")):
            continue
        if len(field) == 4 and key != ord("e"):
            _, label, suffixes, directory = field
            picked = _friendly_dataset_browser(
                stdscr, root, str(values[name]),
                title=f"Choose {label.lower()}",
                allowed_suffixes=suffixes,
                start_dir=(root / directory if directory else root),
            )
            if picked is not None:
                values[name] = picked
                message = f"Selected {label.lower()}: {Path(picked).name}"
            continue
        typed = _edit_text(
            stdscr, max(0, height - 3), 0, str(values[name]), max(8, width - 1)
        ).strip()
        if typed:
            values[name] = typed
            message = f"Updated {field[1]}."
        else:
            message = f"{field[1]} unchanged."


def run_perturbation_tool_action(repo_root: Path) -> int:
    """Launch the guided picker and the existing perturbation CLI."""
    root = repo_root.expanduser().resolve()
    selected = curses.wrapper(_setup_curses, root)
    if selected is None:
        print("\nGAN perturbation setup cancelled.")
        return 0
    command = build_command(selected, root)
    print("\nGAN trailing-suffix experiment")
    print(f"Dataset 1 (fixed reference): {selected['base']}")
    print(f"Dataset 2 (comparison): {selected['comparison']}")
    if selected["model_source"] == TRAIN_MODE:
        print(f"Training from {len(selected['training_pcaps'])} independent PCAPs.")
        print(f"Device: {selected['training_device']}; epochs: {selected['training_epochs']}")
    else:
        print(f"Using existing checkpoint: {selected['model']}")
    print("Dataset 1 remains unchanged. Only copies of Dataset 2 will be perturbed.\n")
    return subprocess.run(command, cwd=root, check=False).returncode
