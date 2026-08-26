from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
import sys

from runner.metric_catalog import available_metric_ids, build_metric_catalog
from runner.plan_builder import build_plan, write_plan
from runner.schema import validate_plan_schema


def _slug(value: str) -> str:
    value = re.sub(r"[^a-zA-Z0-9]+", "-", value.strip()).strip("-").lower()
    return value or "generated"


def _split_metric_args(values: list[str] | None) -> list[str] | None:
    if not values:
        return None
    output: list[str] = []
    for value in values:
        output.extend(part.strip() for part in value.split(",") if part.strip())
    return output


def _prompt(value: str | None, label: str, default: str | None = None, *, required: bool = False) -> str | None:
    if value:
        return value
    if not sys.stdin.isatty():
        if default is not None:
            return default
        if required:
            raise ValueError(f"{label} is required in non-interactive mode.")
        return None
    suffix = f" [{default}]" if default else ""
    while True:
        answer = input(f"{label}{suffix}: ").strip()
        if answer:
            return answer
        if default is not None:
            return default
        if not required:
            return None


def _confirm_overwrite(output_path: Path) -> bool:
    """Ask an interactive user whether an existing plan may be replaced."""

    answer = input(
        f"\nPlan already exists: {output_path}\n"
        "Overwrite the existing plan? [y/N] "
    ).strip().lower()
    return answer in {"y", "yes"}


def _print_report(report: dict) -> None:
    print("\nPlan preflight summary")
    print(f"  Available tests:       {report['available_metric_count']}")
    print(f"  Tests considered:      {report['candidate_metric_count']}")
    print(f"  Runnable / in plan:    {report['runnable_metric_count']}")
    print(f"  Excluded as unrunnable:{report['excluded_metric_count']:>4}")
    for status, count in sorted(report["configuration_status_counts"].items()):
        print(f"  {status.replace('_', ' ').title():<22} {count}")

    excluded = [
        (metric_id, details)
        for metric_id, details in report["metrics"].items()
        if not details["included"]
    ]
    if excluded:
        print("\nTests excluded from the plan")
        for metric_id, details in excluded:
            reason = details.get("reason", details["status"])
            print(f"  - {metric_id}: {details['status']} ({reason})")
            missing = details.get("missing_fields", [])
            if missing:
                print("      missing: " + ", ".join(missing))


def _list_tests() -> None:
    for entry in build_metric_catalog():
        path = " / ".join(entry["taxonomy_path"])
        reason = entry.get("manual_configuration_reason") or ""
        suffix = f" | manual: {reason}" if reason else ""
        print(f"{entry['metric_id']} | {path}{suffix}")


def _check_plan(path: Path) -> int:
    with open(path, "r", encoding="utf-8") as handle:
        plan = json.load(handle)
    validate_plan_schema(plan)
    available = set(available_metric_ids())
    configured = {metric["metric_id"] for metric in plan.get("metrics", [])}
    unknown = sorted(configured - available)
    absent = sorted(available - configured)
    print("Plan schema: valid")
    print(f"Configured tests: {len(configured)}")
    if unknown:
        print("Unknown tests: " + ", ".join(unknown))
    if absent:
        print(f"Registry tests not in this plan: {len(absent)}")
    return 1 if unknown else 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create validated CBR-Tests plans containing only tests runnable on a dataset."
    )
    parser.add_argument(
        "--plan-id",
        help=(
            "Deprecated compatibility option. Plan IDs are derived automatically from --name; "
            "if supplied, this value must match the derived ID."
        ),
    )
    parser.add_argument("--name", help="Human-readable plan name; the plan ID is derived from this name")
    parser.add_argument("--description", help="Plan description")
    parser.add_argument("--dataset", help="Dataset to inspect; interactive mode opens a file browser when omitted")
    parser.add_argument(
        "--field-translation",
        help="Optional field translation JSON. If omitted, an existing dataset sidecar is used automatically.",
    )
    parser.add_argument(
        "--output",
        help="Destination JSON path; defaults to plans/<derived-plan-id>_plan.json",
    )
    parser.add_argument("--include", action="append", help="Only consider these metric IDs (repeat or comma-separate)")
    parser.add_argument("--exclude", action="append", help="Do not consider these metric IDs (repeat or comma-separate)")
    parser.add_argument("--force", action="store_true", help="Replace an existing output plan without prompting")
    parser.add_argument("--list-tests", action="store_true", help="List all tests discoverable by plan creation")
    parser.add_argument("--check", metavar="PLAN", help="Validate a plan and compare it with the live registry")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.list_tests:
        _list_tests()
        return 0
    if args.check:
        return _check_plan(Path(args.check).expanduser().resolve())

    name = _prompt(args.name, "Plan name", required=True)
    assert name is not None
    plan_id = _slug(name)
    if args.plan_id and args.plan_id != plan_id:
        raise ValueError(
            f"Plan ID is derived from the plan name. '{name}' becomes '{plan_id}', "
            f"but --plan-id was '{args.plan_id}'. Remove --plan-id or make it match."
        )

    dataset_value = args.dataset
    if not dataset_value:
        if not sys.stdin.isatty() or not sys.stdout.isatty():
            raise ValueError("Dataset path is required in non-interactive mode.")
        import curses
        from runner.tui import _browse_file

        repo_root = Path.cwd()
        initial = "datasets" if (repo_root / "datasets").is_dir() else ""
        dataset_value = curses.wrapper(
            lambda stdscr: _browse_file(stdscr, repo_root, initial)
        )
        if dataset_value is None:
            print("Dataset selection cancelled. Plan not created.")
            return 0

    dataset_path = Path(dataset_value)
    field_translation_path = Path(args.field_translation) if args.field_translation else None
    description = args.description or "Automatically generated CBR-Tests plan."
    output_path = Path(args.output) if args.output else Path("plans") / f"{plan_id}_plan.json"

    if sys.stdout.isatty():
        print(f"\nPlan ID:     {plan_id}")
        print(f"Dataset:     {dataset_path}")
        print(f"Output path: {output_path}")
        if output_path.exists() and not args.force:
            print("Output status: existing plan (overwrite confirmation will be requested before saving)")

    plan, report = build_plan(
        plan_id=plan_id,
        name=name,
        description=description,
        dataset_path=dataset_path,
        field_translation_path=field_translation_path,
        include_metric_ids=_split_metric_args(args.include),
        exclude_metric_ids=_split_metric_args(args.exclude),
    )
    _print_report(report)

    interactive = sys.stdin.isatty()
    if interactive:
        answer = input("\nSave this runnable-only plan? [Y/n] ").strip().lower()
        if answer not in {"", "y", "yes"}:
            print("Plan not saved.")
            return 0

    overwrite = args.force
    if output_path.exists() and not overwrite and interactive:
        overwrite = _confirm_overwrite(output_path)
        if not overwrite:
            print("Plan not saved; existing plan was left unchanged.")
            return 0

    written_path = write_plan(output_path, plan, overwrite=overwrite)
    print(f"\nPlan written: {written_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
