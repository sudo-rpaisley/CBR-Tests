from __future__ import annotations

import argparse
from pathlib import Path

from runner.fixed_plan_matrix import build_fixed_plan_matrix


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Map PCAP/PCAPNG candidate/reference datasets onto one existing CBR-Tests plan."
    )
    parser.add_argument("--name", required=True, help="Readable matrix name")
    parser.add_argument("--plan", required=True, help="Existing PCAP plan JSON")
    parser.add_argument("--dataset", action="append", required=True, help="Candidate PCAP/PCAPNG; repeat as needed")
    parser.add_argument("--reference", action="append", default=[], help="Reference PCAP/PCAPNG; repeat as needed")
    parser.add_argument("--output", required=True, help="Batch manifest JSON to write")
    parser.add_argument("--force", action="store_true", help="Replace existing binding plans/manifest")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    written = build_fixed_plan_matrix(
        name=args.name,
        plan_path=Path(args.plan),
        candidate_paths=[Path(value) for value in args.dataset],
        reference_paths=[Path(value) for value in args.reference],
        output_path=Path(args.output),
        overwrite=bool(args.force),
        repo_root=Path.cwd(),
    )
    print(f"Mapped PCAP matrix written: {written}")
    print(f"Run with: python run_batch.py --batch {written}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
