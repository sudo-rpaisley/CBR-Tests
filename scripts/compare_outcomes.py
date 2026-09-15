#!/usr/bin/env python3
"""Compare two CBR-Tests outcome JSON files."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from cbr_tests.outcome_comparison import compare_outcomes, load_outcome, render_markdown


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Compare pre-overhaul and post-overhaul CBR-Tests outcome JSON while "
            "ignoring volatile run metadata by default."
        )
    )
    parser.add_argument("before", type=Path, help="Earlier/baseline outcome JSON")
    parser.add_argument("after", type=Path, help="New/rerun outcome JSON")
    parser.add_argument(
        "--markdown",
        type=Path,
        help="Optional path for a Markdown comparison report",
    )
    parser.add_argument(
        "--json",
        dest="json_output",
        type=Path,
        help="Optional path for the machine-readable comparison JSON",
    )
    parser.add_argument(
        "--abs-tol",
        type=float,
        default=1e-12,
        help="Absolute tolerance for numeric equality (default: 1e-12)",
    )
    parser.add_argument(
        "--rel-tol",
        type=float,
        default=1e-9,
        help="Relative tolerance for numeric equality (default: 1e-9)",
    )
    parser.add_argument(
        "--include-volatile",
        action="store_true",
        help="Also compare run IDs, timestamps, output paths and runtime metadata",
    )
    parser.add_argument(
        "--fail-on-high-impact-change",
        action="store_true",
        help="Exit with status 2 when a status/metric/provenance change is detected",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    before = load_outcome(args.before)
    after = load_outcome(args.after)
    report = compare_outcomes(
        before,
        after,
        abs_tol=args.abs_tol,
        rel_tol=args.rel_tol,
        include_volatile=args.include_volatile,
    )
    markdown = render_markdown(report)

    if args.markdown:
        args.markdown.parent.mkdir(parents=True, exist_ok=True)
        args.markdown.write_text(markdown, encoding="utf-8")
    else:
        print(markdown, end="")

    if args.json_output:
        args.json_output.parent.mkdir(parents=True, exist_ok=True)
        args.json_output.write_text(
            json.dumps(report, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )

    if args.fail_on_high_impact_change and report["summary"]["high_impact"]:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
