"""Attach to a running or finished GAN corpus campaign without restarting it."""
from __future__ import annotations

import argparse
from pathlib import Path

from runner.corpus_monitor_tui import launch_corpus_monitor


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Live campaign/group/held-out/job progress with sample-based ETAs"
    )
    parser.add_argument("output_dir", type=Path, help="Existing outputs/gan-corpus/RUN folder")
    args = parser.parse_args(argv)
    if not (args.output_dir / "campaign.json").is_file() and not args.output_dir.is_dir():
        parser.error(f"Campaign directory not found: {args.output_dir}")
    launch_corpus_monitor(args.output_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
