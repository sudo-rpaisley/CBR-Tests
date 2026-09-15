#!/usr/bin/env python3
"""Archive a baseline outcome, rerun a plan, and compare the result."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from cbr_tests.plan_migration import (
    COMPATIBILITY_ONLY_METRIC_PROFILES,
    compatibility_only_metric_ids,
    legacy_intrinsic_metric_ids,
)
from cbr_tests.rerun_workflow import run_and_compare


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run one representative dataset through the normal CBR-Tests runner, "
            "archive its pre-overhaul baseline, and generate pre/post comparison records."
        )
    )
    parser.add_argument("--baseline", required=True, type=Path, help="Authoritative pre-overhaul outcome JSON")
    parser.add_argument("--plan", required=True, type=Path, help="Plan or case JSON passed to run_plan.py")
    parser.add_argument("--dataset", required=True, type=Path, help="Dataset to rerun")
    parser.add_argument("--record-dir", required=True, type=Path, help="Directory for the complete rerun evidence record")
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=Path(__file__).resolve().parents[1],
        help="CBR-Tests repository root (defaults to the repository containing this script)",
    )
    parser.add_argument("--case-id", default="ad_hoc_case", help="Case ID for a plan JSON rerun")
    parser.add_argument(
        "--display",
        choices=("compact", "full", "quiet", "interactive"),
        default="compact",
        help="Display mode passed to run_plan.py",
    )
    parser.add_argument("--workers", type=int, help="Optional worker-count override")
    parser.add_argument("--force", action="store_true", help="Replace generated files already present in the record directory")
    parser.add_argument(
        "--allow-legacy-metric-ids",
        action="store_true",
        help=(
            "Allow a representative rerun to use pre-overhaul/compatibility metric IDs. "
            "This is intended only for deliberate compatibility experiments."
        ),
    )
    parser.add_argument(
        "--no-update-field-translation",
        action="store_true",
        help="Pass --no-update-field-translation to run_plan.py",
    )
    parser.add_argument(
        "--yes-field-translation-sidecar",
        action="store_true",
        help="Allow run_plan.py to create/update a field-translation sidecar without prompting",
    )
    parser.add_argument(
        "--no-dataset-summary",
        action="store_true",
        help="Suppress dataset-side summary creation during the rerun",
    )
    parser.add_argument(
        "--refresh-dataset-summary",
        action="store_true",
        help="Force refresh of the dataset-side summary during the rerun",
    )
    return parser


def _resolved_plan_payload(run_definition: Path) -> tuple[Path, dict]:
    """Resolve either a direct plan or a case's referenced plan."""
    source = run_definition.expanduser().resolve()
    payload = json.loads(source.read_text(encoding="utf-8"))
    if isinstance(payload, dict) and isinstance(payload.get("metrics"), list):
        return source, payload

    test_plan = payload.get("test_plan") if isinstance(payload, dict) else None
    if isinstance(test_plan, dict) and test_plan.get("path"):
        plan_path = Path(str(test_plan["path"])).expanduser()
        if not plan_path.is_absolute():
            plan_path = source.parent / plan_path
        plan_path = plan_path.resolve()
        plan_payload = json.loads(plan_path.read_text(encoding="utf-8"))
        return plan_path, plan_payload

    return source, payload


def _reject_legacy_representative_plan(run_definition: Path) -> None:
    plan_path, plan = _resolved_plan_payload(run_definition)
    legacy_intrinsic = legacy_intrinsic_metric_ids(plan)
    compatibility_only = compatibility_only_metric_ids(plan)
    if not legacy_intrinsic and not compatibility_only:
        return

    lines = [
        "Representative reruns require the canonical metric-conformance contract.",
    ]
    if legacy_intrinsic:
        lines.append(f"Legacy intrinsic metric IDs ({len(legacy_intrinsic)}):")
        lines.extend(f"  - {metric_id}" for metric_id in legacy_intrinsic)
        lines.extend(
            [
                "These can be renamed safely with:",
                f"  python scripts/migrate_plan_to_canonical_ids.py {plan_path}",
            ]
        )
    if compatibility_only:
        lines.append(f"Compatibility-only profile IDs ({len(compatibility_only)}):")
        for metric_id in compatibility_only:
            lines.append(
                f"  - {metric_id}: {COMPATIBILITY_ONLY_METRIC_PROFILES[metric_id]}"
            )
        lines.extend(
            [
                "Because those profiles do not have one-to-one canonical replacements,",
                "regenerate this plan with the current dataset-aware builder before the representative rerun.",
            ]
        )
    lines.append(
        "Use --allow-legacy-metric-ids only for a deliberate compatibility experiment, not a final representative rerun."
    )
    raise SystemExit("\n".join(lines))


def main() -> int:
    args = build_parser().parse_args()
    if not args.allow_legacy_metric_ids:
        _reject_legacy_representative_plan(args.plan)

    extra_args: list[str] = []
    if args.no_update_field_translation:
        extra_args.append("--no-update-field-translation")
    if args.yes_field_translation_sidecar:
        extra_args.append("--yes-field-translation-sidecar")
    if args.no_dataset_summary:
        extra_args.append("--no-dataset-summary")
    if args.refresh_dataset_summary:
        extra_args.append("--refresh-dataset-summary")

    manifest = run_and_compare(
        repo_root=args.repo_root,
        baseline_path=args.baseline,
        plan_path=args.plan,
        dataset_path=args.dataset,
        record_dir=args.record_dir,
        case_id=args.case_id,
        display=args.display,
        workers=args.workers,
        extra_args=extra_args,
        force=args.force,
    )

    print(json.dumps(manifest["comparison_summary"], indent=2))
    print(f"Rerun record: {args.record_dir.resolve()}")
    return int(manifest["process_return_code"] != 0)


if __name__ == "__main__":
    raise SystemExit(main())
