"""Guided terminal setup for the GAN perturbation entry in the Toolbox."""
from __future__ import annotations

from pathlib import Path
import subprocess
import sys


def _ask(prompt: str, default: str = "") -> str:
    suffix = f" [{default}]" if default else ""
    answer = input(f"{prompt}{suffix}: ").strip()
    return answer or default


def run_perturbation_tool_action(repo_root: Path) -> int:
    """Offer a simple setup dialogue after exiting the curses Toolbox menu."""
    print("\nGAN trailing-suffix perturbation experiment")
    print("This uses the separately installed traffic_gan.py and an already trained CTGAN model.")
    print("The evaluation capture must be separate from the GAN's training data.")
    print("Use a new empty output directory; no existing outcomes will be overwritten.\n")
    try:
        source = _ask("Evaluation PCAP/PCAPNG (blank cancels)")
        if not source:
            return 0
        plan = _ask("Existing PCAP test-plan JSON (blank cancels)")
        if not plan:
            return 0
        model = _ask("Trained CTGAN checkpoint (blank cancels)")
        if not model:
            return 0
        generator = _ask("Path to companion traffic_gan.py (blank cancels)")
        if not generator:
            return 0
        output = _ask("New empty output directory", "outcomes/gan-perturbation")
        percentages = _ask("Trailing replacement percentages", "0,10,20,30,40,50,60,70,80,90,100")
        seeds = _ask("Random seeds", "42")
        mode = _ask("Prepare only (generate PCAPs; do not run metrics)? y/N", "N")
        command = [
            sys.executable, str(repo_root / "run_perturbation.py"),
            "--input", source, "--plan", plan, "--model", model,
            "--generator-script", generator, "--output-dir", output,
            "--percentages", percentages, "--seeds", seeds,
        ]
        if mode.lower() in ("y", "yes"):
            command.append("--prepare-only")
        print("\nThe original capture will not be modified. Starting the experiment.\n")
        return subprocess.run(command, cwd=repo_root, check=False).returncode
    except (EOFError, KeyboardInterrupt):
        print("\nPerturbation setup cancelled.")
        return 0
