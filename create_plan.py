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
    value = re.sub(r"[^a-zA-Z0-9._-]+", "-", value.strip()).strip("-._")
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


def _print_report(report: dict) -> None:
    print("\nPlan configuration summary")
    print(f"  Available tests: {report['available_metric_count']}")
    print(f"  Selected tests:  {report['selected_metric_count']}")
    print(f"  Enabled tests:   {report['enabled_metric_count']}")
    for status, count in sorted(report["configuration_status_counts"].items()):
        print(f"  {status.replace('_', ' ').title():<20} {count}")

    attention = [
        (metric_id, details)
        for metric_id, details in report["metrics"].items()
        if details["status"] != "ready"
    ]
    if attention:
        print("\nTests requiring attention")
        for metric_id, details in attention:
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
    missing = sorted(available - configured)
    print("Plan schema: valid")
    print(f"Configured tests: {len(configured)}")
    if unknown:
        print("Unknown tests: " + ", ".join(unknown))
    if missing:
        print("Available but absent: " + ", ".join(missing))
    return 1 if unknown else 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create validated CBR-Tests plans from the live metric registry."
    )
    parser.add_argument("--plan-id", help="Stable plan identifier")
    parser.add_argument("--name", help="Human-readable plan name")
    parser.add_argument("--description", help="Plan description")
    parser.add_argument("--dataset", help="Optional dataset used to preflight fields and applicability")
    parser.add_argument("--output", help="Destination JSON path (defaults to plans/<plan-id>_plan.json)")
    parser.add_argument("--include", action="append", help="Only include these metric IDs (repeat or comma-separate)")
    parser.add_argument("--exclude", action="append", help="Exclude these metric IDs (repeat or comma-separate)")
    parser.add_argument(
        "--enable-unready",
        action="store_true",
        help="Enable tests even when configuration/mapping is incomplete (not recommended)",
    )
    parser.add_argument("--force", action="store_true", help="Replace an existing output plan")
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

    plan_id = _prompt(args.plan_id, "Plan ID", required=True)
    assert plan_id is not None
    default_name = plan_id.replace("-", " ").replace("_", " ").title()
    name = _prompt(args.name, "Plan name", default_name, required=True)
    assert name is not None
    dataset_value = _prompt(args.dataset, "Dataset path (optional)", None)
    dataset_path = Path(dataset_value) if dataset_value else None
    description = args.description or "Automatically generated CBR-Tests plan."
    default_output = str(Path("plans") / f"{_slug(plan_id)}_plan.json")
    output_value = _prompt(args.output, "Output path", default_output, required=True)
    assert output_value is not None

    plan, report = build_plan(
        plan_id=plan_id,
        name=name,
        description=description,
        dataset_path=dataset_path,
        include_metric_ids=_split_metric_args(args.include),
        exclude_metric_ids=_split_metric_args(args.exclude),
        enable_unready=args.enable_unready,
    )
    _print_report(report)

    if sys.stdin.isatty():
        answer = input("\nSave this plan? [Y/n] ").strip().lower()
        if answer not in {"", "y", "yes"}:
            print("Plan not saved.")
            return 0

    output_path = write_plan(Path(output_value), plan, overwrite=args.force)
    print(f"\nPlan written: {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
