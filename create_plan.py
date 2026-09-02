from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import sys
import tempfile
import textwrap

from runner.metric_catalog import available_metric_ids, build_metric_catalog
from runner.plan_builder import build_plan, write_plan
from runner.schema import validate_plan_schema
from runner.taxonomy import build_plan_taxonomy


BATCH_SCHEMA_VERSION = 1
PCAP_SUFFIXES = {".pcap", ".pcapng"}


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


def _parse_expected_ports(value: str | None) -> list[int]:
    if not value:
        return []
    ports = []
    for part in value.split(","):
        text = part.strip()
        if not text:
            continue
        try:
            port = int(text)
        except ValueError as exc:
            raise ValueError(f"Expected service ports must be integers: {text}") from exc
        if port < 0 or port > 65535:
            raise ValueError(f"Expected service port is outside 0-65535: {port}")
        ports.append(port)
    return sorted(set(ports))


def _browse_dataset_file() -> str | None:
    import curses
    from runner.tui import _browse_file

    repo_root = Path.cwd()
    initial = "datasets" if (repo_root / "datasets").is_dir() else ""
    return curses.wrapper(lambda stdscr: _browse_file(stdscr, repo_root, initial))


def _browse_dataset_files() -> list[str]:
    """Interactively collect one or more datasets using the existing file browser."""

    selected: list[str] = []
    while True:
        value = _browse_dataset_file()
        if value is None:
            if not selected:
                return []
            print("Dataset selection cancelled; keeping the datasets already selected.")
            return selected
        if value not in selected:
            selected.append(value)
            print(f"Selected dataset {len(selected)}: {value}")
        else:
            print(f"Dataset already selected: {value}")

        answer = input("Add another dataset to this plan batch? [y/N] ").strip().lower()
        if answer not in {"y", "yes"}:
            return selected


def _browse_reference_files() -> list[str]:
    """Interactively collect one or more independent reference datasets."""

    selected: list[str] = []
    while True:
        value = _browse_dataset_file()
        if value is None:
            if not selected:
                return []
            print("Reference selection cancelled; keeping the references already selected.")
            return selected
        if value not in selected:
            selected.append(value)
            print(f"Selected reference {len(selected)}: {value}")
        else:
            print(f"Reference already selected: {value}")

        answer = input("Add another reference dataset? [y/N] ").strip().lower()
        if answer not in {"y", "yes"}:
            return selected


def _prompt(
    value: str | None,
    label: str,
    default: str | None = None,
    *,
    required: bool = False,
) -> str | None:
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


def _print_wrapped(prefix: str, text: str, *, width: int = 108) -> None:
    available = max(30, width - len(prefix))
    lines = textwrap.wrap(str(text), width=available) or [""]
    print(prefix + lines[0])
    continuation = " " * len(prefix)
    for line in lines[1:]:
        print(continuation + line)


def _compact_metric_ids(metric_ids: list[str], *, limit: int = 8) -> str:
    metric_ids = sorted(metric_ids)
    if len(metric_ids) <= limit:
        return ", ".join(metric_ids)
    return ", ".join(metric_ids[:limit]) + f", +{len(metric_ids) - limit} more"


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
            advice = details.get("advice", {})
            if advice.get("title"):
                print("      needed: " + str(advice["title"]))

    actions = report.get("unlock_actions", [])
    if actions:
        print("\nHow to unlock more tests")
        for action in actions:
            count = int(action.get("metric_count", 0))
            suffix = "" if action.get("actionable", True) else " [requires different input/support]"
            print(f"  - {action['title']} ({count} test{'s' if count != 1 else ''}){suffix}")
            _print_wrapped("      ", str(action.get("advice", "")))
            missing = action.get("missing_fields", [])
            if missing:
                _print_wrapped("      Missing fields: ", ", ".join(missing))
            if action.get("example"):
                _print_wrapped("      Example: ", str(action["example"]))
            metric_ids = action.get("metric_ids", [])
            if metric_ids:
                _print_wrapped("      Affects: ", _compact_metric_ids(metric_ids))


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


def _deduplicate_dataset_values(values: list[str]) -> list[Path]:
    datasets: list[Path] = []
    seen: set[Path] = set()
    for value in values:
        path = Path(value).expanduser().resolve()
        if path in seen:
            continue
        seen.add(path)
        datasets.append(path)
    return datasets


def _portable_path(path: Path, repo_root: Path) -> str:
    path = path.expanduser().resolve()
    try:
        return str(path.relative_to(repo_root))
    except ValueError:
        return str(path)


def _job_slug(dataset_path: Path, index: int, used: set[str]) -> str:
    base = _slug(dataset_path.stem)
    candidate = base
    if candidate in used:
        candidate = f"{base}-{index:02d}"
    used.add(candidate)
    return candidate


def _filter_plan_to_metric_ids(plan: dict, metric_ids: set[str]) -> None:
    plan["metrics"] = [
        metric for metric in plan.get("metrics", [])
        if metric.get("metric_id") in metric_ids
    ]
    if not plan["metrics"]:
        raise ValueError("The common metric set is empty after batch filtering.")
    plan["plan_taxonomy"] = build_plan_taxonomy(plan["metrics"])
    plan_creation = plan.setdefault("plan_creation", {})
    plan_creation["batch_common_metric_count"] = len(plan["metrics"])
    plan_creation["batch_metric_policy"] = "common_across_all_datasets"
    validate_plan_schema(plan)


def _write_json_atomic(path: Path, payload: dict, *, overwrite: bool = False) -> Path:
    path = path.expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and not overwrite:
        raise FileExistsError(f"File already exists: {path}. Use --force to replace it.")

    fd, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent, text=True
    )
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, path)
    except Exception:
        temporary_path.unlink(missing_ok=True)
        raise
    return path


def _build_single_plan(
    *,
    plan_id: str,
    name: str,
    description: str,
    dataset_path: Path,
    field_translation_path: Path | None,
    include_metric_ids: list[str] | None,
    exclude_metric_ids: list[str] | None,
    reference_dataset_path: Path | None,
    service_port_configuration: dict | None,
) -> tuple[dict, dict]:
    return build_plan(
        plan_id=plan_id,
        name=name,
        description=description,
        dataset_path=dataset_path,
        field_translation_path=field_translation_path,
        include_metric_ids=include_metric_ids,
        exclude_metric_ids=exclude_metric_ids,
        reference_dataset_path=reference_dataset_path,
        service_port_configuration=service_port_configuration,
    )


def _create_batch(
    *,
    plan_id: str,
    name: str,
    description: str,
    dataset_paths: list[Path],
    field_translation_path: Path | None,
    include_metric_ids: list[str] | None,
    exclude_metric_ids: list[str] | None,
    reference_dataset_paths: list[Path],
    service_port_configuration: dict | None,
    output_path: Path,
    force: bool,
    per_dataset_metrics: bool,
    interactive: bool,
) -> Path:
    repo_root = Path.cwd().resolve()
    batch_plan_dir = output_path.parent / f"{plan_id}_batch_plans"
    used_slugs: set[str] = set()
    generated: list[dict] = []

    combinations: list[tuple[Path, Path | None]] = []
    if reference_dataset_paths:
        for dataset_path in dataset_paths:
            for reference_path in reference_dataset_paths:
                if dataset_path.resolve() == reference_path.resolve():
                    print(f"Skipping self-comparison: {dataset_path}")
                    continue
                combinations.append((dataset_path, reference_path))
    else:
        combinations = [(dataset_path, None) for dataset_path in dataset_paths]

    if not combinations:
        raise ValueError("No runnable candidate/reference combinations remain after excluding self-comparisons.")

    print(
        f"\nBuilding batch plan for {len(dataset_paths)} candidate dataset(s), "
        f"{len(reference_dataset_paths)} reference dataset(s), and {len(combinations)} job(s)"
    )
    for index, (dataset_path, reference_path) in enumerate(combinations, start=1):
        base_slug = _job_slug(dataset_path, index, used_slugs)
        if reference_path is not None:
            reference_slug = _slug(reference_path.stem)
            slug = f"{base_slug}-vs-{reference_slug}"
            if slug in used_slugs:
                slug = f"{slug}-{index:02d}"
            used_slugs.add(slug)
        else:
            slug = base_slug
        child_plan_id = f"{plan_id}-{slug}"
        child_name = f"{name} — {dataset_path.name}"
        if reference_path is not None:
            child_name += f" vs {reference_path.name}"
        print(f"\n[{index}/{len(combinations)}] Preflighting {dataset_path}")
        if reference_path is not None:
            print(f"    Reference: {reference_path}")
        plan, report = _build_single_plan(
            plan_id=child_plan_id,
            name=child_name,
            description=description,
            dataset_path=dataset_path,
            field_translation_path=field_translation_path,
            include_metric_ids=include_metric_ids,
            exclude_metric_ids=exclude_metric_ids,
            reference_dataset_path=reference_path,
            service_port_configuration=service_port_configuration,
        )
        _print_report(report)
        generated.append(
            {
                "dataset_path": dataset_path,
                "reference_dataset_path": reference_path,
                "slug": slug,
                "plan": plan,
                "report": report,
                "plan_path": batch_plan_dir / f"{index:02d}_{slug}_plan.json",
            }
        )

    common_metric_ids: set[str] | None = None
    if not per_dataset_metrics:
        metric_sets = [
            {metric["metric_id"] for metric in item["plan"].get("metrics", [])}
            for item in generated
        ]
        common_metric_ids = set.intersection(*metric_sets) if metric_sets else set()
        if not common_metric_ids:
            raise ValueError(
                "No metric is runnable across every selected candidate/reference job. "
                "Resolve field mappings, choose more compatible datasets, or use --per-dataset-metrics."
            )
        print(
            f"\nBatch common metric set: {len(common_metric_ids)} tests runnable across all "
            f"{len(generated)} jobs."
        )
        for item in generated:
            _filter_plan_to_metric_ids(item["plan"], common_metric_ids)
    else:
        print("\nBatch metric policy: each candidate/reference job keeps its own runnable metric set.")

    if interactive:
        answer = input("\nSave this dataset batch and its generated plans? [Y/n] ").strip().lower()
        if answer not in {"", "y", "yes"}:
            raise KeyboardInterrupt("Batch save cancelled by user.")

    existing_paths = [item["plan_path"] for item in generated if item["plan_path"].exists()]
    if output_path.exists():
        existing_paths.append(output_path)
    overwrite = force
    if existing_paths and not overwrite and interactive:
        print("\nThe following batch files already exist:")
        for path in existing_paths:
            print(f"  - {path}")
        answer = input("Overwrite the existing batch files? [y/N] ").strip().lower()
        overwrite = answer in {"y", "yes"}
        if not overwrite:
            raise KeyboardInterrupt("Batch save cancelled; existing files were left unchanged.")

    if existing_paths and not overwrite:
        raise FileExistsError(
            "One or more batch output files already exist. Use --force to replace them."
        )

    jobs = []
    for index, item in enumerate(generated, start=1):
        reference_path = item["reference_dataset_path"]
        item["plan"].setdefault("plan_creation", {})["batch"] = {
            "batch_id": plan_id,
            "batch_name": name,
            "job_index": index,
            "job_count": len(generated),
            "metric_policy": (
                "per_dataset" if per_dataset_metrics else "common_across_all_datasets"
            ),
        }
        write_plan(item["plan_path"], item["plan"], overwrite=overwrite)
        jobs.append(
            {
                "job_id": f"{plan_id}-{index:02d}-{item['slug']}",
                "dataset_path": str(item["dataset_path"].resolve()),
                "reference_dataset_path": (
                    str(reference_path.resolve()) if reference_path is not None else None
                ),
                "plan_path": _portable_path(item["plan_path"], repo_root),
                "runnable_metric_count": len(item["plan"].get("metrics", [])),
                "metric_ids": [metric["metric_id"] for metric in item["plan"].get("metrics", [])],
            }
        )

    batch_payload = {
        "schema_version": BATCH_SCHEMA_VERSION,
        "batch_meta": {
            "batch_id": plan_id,
            "name": name,
            "description": description,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "execution_mode": "sequential",
            "dataset_count": len(dataset_paths),
            "reference_dataset_count": len(reference_dataset_paths),
            "job_count": len(jobs),
            "comparison_mode": "candidate_reference_matrix" if reference_dataset_paths else "dataset_batch",
            "metric_policy": (
                "per_dataset" if per_dataset_metrics else "common_across_all_datasets"
            ),
        },
        "output_directory": str(Path("outcomes") / plan_id),
        "common_metric_ids": sorted(common_metric_ids) if common_metric_ids is not None else None,
        "reference_datasets": [str(path.resolve()) for path in reference_dataset_paths],
        "reference_dataset": (
            str(reference_dataset_paths[0].resolve()) if len(reference_dataset_paths) == 1 else None
        ),
        "jobs": jobs,
    }
    written = _write_json_atomic(output_path, batch_payload, overwrite=overwrite)
    print(f"\nBatch manifest written: {written}")
    print(f"Generated per-job plans: {batch_plan_dir.resolve()}")
    print(f"Run the batch with: python run_batch.py --batch {written}")
    return written


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Create validated CBR-Tests plans containing only tests runnable on a dataset, "
            "or create a sequential batch from multiple datasets."
        )
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
    parser.add_argument(
        "--dataset",
        action="append",
        help=(
            "Dataset to inspect. Repeat --dataset to create a sequential multi-dataset batch. "
            "Interactive mode can select several datasets when omitted."
        ),
    )
    parser.add_argument(
        "--field-translation",
        help="Optional field translation JSON. If omitted, each dataset's existing sidecar is used automatically.",
    )
    parser.add_argument(
        "--reference-dataset",
        action="append",
        help=(
            "Optional independent reference dataset. Repeat --reference-dataset to compare every selected "
            "candidate against several references. Interactive mode uses the file browser instead of typed paths."
        ),
    )
    parser.add_argument(
        "--single-service",
        help="Explicitly assert that the entire PCAP represents this one application service.",
    )
    parser.add_argument(
        "--expected-service-ports",
        help="Comma-separated expected ports for --single-service, e.g. 53 or 80,8080.",
    )
    parser.add_argument(
        "--output",
        help=(
            "Destination JSON path. For one dataset this is the plan JSON. For multiple datasets "
            "this is the batch manifest."
        ),
    )
    parser.add_argument(
        "--per-dataset-metrics",
        action="store_true",
        help=(
            "For a multi-dataset batch, keep each dataset's full runnable metric set instead of "
            "restricting every generated plan to the metric IDs runnable across all datasets."
        ),
    )
    parser.add_argument("--include", action="append", help="Only consider these metric IDs (repeat or comma-separate)")
    parser.add_argument("--exclude", action="append", help="Do not consider these metric IDs (repeat or comma-separate)")
    parser.add_argument("--force", action="store_true", help="Replace existing plan/batch outputs without prompting")
    parser.add_argument("--list-tests", action="store_true", help="List all tests discoverable by plan creation")
    parser.add_argument("--check", metavar="PLAN", help="Validate a plan and compare it with the live registry")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if getattr(args, "list_tests", False):
        _list_tests()
        return 0
    if getattr(args, "check", None):
        return _check_plan(Path(args.check).expanduser().resolve())

    name = _prompt(getattr(args, "name", None), "Plan name", required=True)
    assert name is not None
    plan_id = _slug(name)
    supplied_plan_id = getattr(args, "plan_id", None)
    if supplied_plan_id and supplied_plan_id != plan_id:
        raise ValueError(
            f"Plan ID is derived from the plan name. '{name}' becomes '{plan_id}', "
            f"but --plan-id was '{supplied_plan_id}'. Remove --plan-id or make it match."
        )

    dataset_arg = getattr(args, "dataset", None)
    if isinstance(dataset_arg, (str, Path)):
        dataset_values = [str(dataset_arg)]
    else:
        dataset_values = list(dataset_arg or [])
    if not dataset_values:
        if not sys.stdin.isatty() or not sys.stdout.isatty():
            raise ValueError("At least one --dataset path is required in non-interactive mode.")
        dataset_values = _browse_dataset_files()
        if not dataset_values:
            print("Dataset selection cancelled. Plan not created.")
            return 0

    dataset_paths = _deduplicate_dataset_values(dataset_values)
    if not dataset_paths:
        raise ValueError("At least one dataset must be selected.")

    all_pcap = all(path.suffix.lower() in PCAP_SUFFIXES for path in dataset_paths)
    any_pcap = any(path.suffix.lower() in PCAP_SUFFIXES for path in dataset_paths)
    single_service_arg = getattr(args, "single_service", None)
    reference_arg = getattr(args, "reference_dataset", None)
    if any_pcap and not all_pcap and (single_service_arg or reference_arg):
        raise ValueError(
            "PCAP-specific reference/service configuration cannot be shared across a mixed PCAP/tabular batch."
        )

    if isinstance(reference_arg, (str, Path)):
        reference_values = [str(reference_arg)]
    else:
        reference_values = list(reference_arg or [])

    if not reference_values and sys.stdin.isatty() and sys.stdout.isatty():
        answer = input(
            "\nAdd one or more independent reference datasets for comparison metrics? [y/N] "
        ).strip().lower()
        if answer in {"y", "yes"}:
            reference_values = _browse_reference_files()
            if not reference_values:
                print("No references selected; reference-comparison metrics remain excluded.")

    reference_dataset_paths = _deduplicate_dataset_values(reference_values)
    if all_pcap and any(path.suffix.lower() not in PCAP_SUFFIXES for path in reference_dataset_paths):
        raise ValueError("Raw-PCAP candidates can only use PCAP/PCAPNG reference datasets.")
    if not all_pcap and any(path.suffix.lower() in PCAP_SUFFIXES for path in reference_dataset_paths):
        raise ValueError("Tabular candidates cannot use raw-PCAP reference datasets.")

    single_service = single_service_arg
    expected_ports = _parse_expected_ports(getattr(args, "expected_service_ports", None))
    if bool(single_service) != bool(expected_ports):
        raise ValueError("--single-service and --expected-service-ports must be supplied together.")
    if all_pcap and not single_service and sys.stdin.isatty() and sys.stdout.isatty():
        answer = input(
            "\nAre all selected captures independently known to represent the same one application service? [y/N] "
        ).strip().lower()
        if answer in {"y", "yes"}:
            single_service = _prompt(None, "Service name", required=True)
            expected_ports = _parse_expected_ports(
                _prompt(None, "Expected service port(s), comma-separated", required=True)
            )
            if not expected_ports:
                raise ValueError("At least one expected service port is required.")
    service_port_configuration = None
    if single_service and expected_ports:
        service_port_configuration = {
            "service_name": single_service,
            "expected_ports": expected_ports,
            "population_mode": "all_rows",
        }

    field_translation_value = getattr(args, "field_translation", None)
    field_translation_path = Path(field_translation_value) if field_translation_value else None
    description = getattr(args, "description", None) or "Automatically generated CBR-Tests plan."
    include_metric_ids = _split_metric_args(getattr(args, "include", None))
    exclude_metric_ids = _split_metric_args(getattr(args, "exclude", None))
    interactive = sys.stdin.isatty()
    per_dataset_metrics = bool(getattr(args, "per_dataset_metrics", False))
    force = bool(getattr(args, "force", False))
    output_value = getattr(args, "output", None)

    if len(dataset_paths) > 1 or len(reference_dataset_paths) > 1:
        output_path = Path(output_value) if output_value else Path("plans") / f"{plan_id}_batch.json"
        print(f"\nBatch ID:     {plan_id}")
        print(f"Datasets:     {len(dataset_paths)}")
        for index, path in enumerate(dataset_paths, start=1):
            print(f"  {index:>2}. {path}")
        if reference_dataset_paths:
            print(f"References:   {len(reference_dataset_paths)}")
            for index, path in enumerate(reference_dataset_paths, start=1):
                print(f"  R{index:>2}. {path}")
        print(
            f"Metric policy:{' per-dataset' if per_dataset_metrics else ' common across all datasets'}"
        )
        print(f"Output path:  {output_path}")
        try:
            _create_batch(
                plan_id=plan_id,
                name=name,
                description=description,
                dataset_paths=dataset_paths,
                field_translation_path=field_translation_path,
                include_metric_ids=include_metric_ids,
                exclude_metric_ids=exclude_metric_ids,
                reference_dataset_paths=reference_dataset_paths,
                service_port_configuration=service_port_configuration,
                output_path=output_path,
                force=force,
                per_dataset_metrics=per_dataset_metrics,
                interactive=interactive,
            )
        except KeyboardInterrupt as exc:
            print(str(exc) or "Batch creation cancelled.")
            return 0
        return 0

    dataset_path = dataset_paths[0]
    reference_dataset_path = reference_dataset_paths[0] if reference_dataset_paths else None
    output_path = Path(output_value) if output_value else Path("plans") / f"{plan_id}_plan.json"

    if sys.stdout.isatty():
        print(f"\nPlan ID:     {plan_id}")
        print(f"Dataset:     {dataset_path}")
        if reference_dataset_path:
            print(f"Reference:   {reference_dataset_path}")
        if service_port_configuration:
            print(
                f"Service:     {service_port_configuration['service_name']} on "
                f"{service_port_configuration['expected_ports']} (explicit single-service capture)"
            )
        print(f"Output path: {output_path}")
        if output_path.exists() and not force:
            print("Output status: existing plan (overwrite confirmation will be requested before saving)")

    plan, report = _build_single_plan(
        plan_id=plan_id,
        name=name,
        description=description,
        dataset_path=dataset_path,
        field_translation_path=field_translation_path,
        include_metric_ids=include_metric_ids,
        exclude_metric_ids=exclude_metric_ids,
        reference_dataset_path=reference_dataset_path,
        service_port_configuration=service_port_configuration,
    )
    _print_report(report)

    if interactive:
        answer = input("\nSave this runnable-only plan? [Y/n] ").strip().lower()
        if answer not in {"", "y", "yes"}:
            print("Plan not saved.")
            return 0

    overwrite = force
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
