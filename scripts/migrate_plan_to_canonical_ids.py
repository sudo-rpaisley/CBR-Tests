#!/usr/bin/env python3
"""Rewrite a saved CBR-Tests plan to canonical metric-conformance IDs."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from cbr_tests.plan_migration import legacy_intrinsic_metric_ids, migrate_plan_to_canonical_ids


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Migrate legacy intrinsic metric IDs in an existing plan to the "
            "canonical metric-conformance IDs without changing its scientific inputs."
        )
    )
    parser.add_argument("plan", type=Path, help="Existing plan JSON to migrate.")
    parser.add_argument(
        "--output",
        type=Path,
        help="Destination plan. Defaults to <stem>_canonical.json beside the input.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Allow replacement of an existing output file. The input is never overwritten implicitly.",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Report legacy IDs and exit without writing a plan.",
    )
    return parser


def main() -> int:
    args = _parser().parse_args()
    source = args.plan.expanduser().resolve()
    if not source.is_file():
        raise SystemExit(f"Plan does not exist or is not a file: {source}")

    plan = json.loads(source.read_text(encoding="utf-8"))
    legacy = legacy_intrinsic_metric_ids(plan)
    if args.check:
        if legacy:
            print(f"Legacy intrinsic metric IDs ({len(legacy)}):")
            for metric_id in legacy:
                print(f"  - {metric_id}")
            return 2
        print("No legacy intrinsic metric IDs found.")
        return 0

    migrated, changes = migrate_plan_to_canonical_ids(plan)
    if not changes:
        print("Plan already uses canonical intrinsic metric IDs; no output written.")
        return 0

    destination = (
        args.output.expanduser().resolve()
        if args.output
        else source.with_name(source.stem + "_canonical.json")
    )
    if destination == source and not args.force:
        raise SystemExit("Refusing to overwrite the source plan without --force.")
    if destination.exists() and not args.force:
        raise SystemExit(f"Output already exists; use --force to replace it: {destination}")

    destination.write_text(json.dumps(migrated, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"Wrote canonical plan: {destination}")
    print(f"Migrated {len(changes)} metric ID(s):")
    for change in changes:
        print(f"  {change['from_metric_id']} -> {change['to_metric_id']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
