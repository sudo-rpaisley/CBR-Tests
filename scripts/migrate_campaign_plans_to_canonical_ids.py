#!/usr/bin/env python3
"""Prepare every plan referenced by a comparison campaign for final experiments.

The command performs a two-phase migration.  It first analyses every unique plan
referenced by every queued batch, applies only the repository's explicitly safe
one-to-one metric-ID migrations in memory, and validates the resulting plan
against the strict final-experiment contract.  Nothing is written if any plan
would still require scientific regeneration or another contract change.

Use the default dry-run first.  ``--apply`` creates a timestamped backup of every
plan that will change and then replaces the referenced plan files in place.  The
batch manifests therefore continue to point at exactly the same plan paths.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import tempfile
from typing import Any

from cbr_tests.plan_migration import (
    compatibility_only_metric_ids,
    legacy_intrinsic_metric_ids,
    migrate_plan_to_canonical_ids,
)
from runner.campaign import load_campaign, resolve_repo_path, validate_batch_manifest
from runner.experiment_contract import ExperimentContractError, validate_final_experiment_plan


@dataclass
class PlanAssessment:
    path: Path
    legacy_ids: list[str]
    compatibility_ids: list[str]
    migrated: dict[str, Any]
    changes: list[dict[str, Any]]
    contract_error: str | None

    @property
    def changed(self) -> bool:
        return bool(self.changes)

    @property
    def ready_after_safe_migration(self) -> bool:
        return self.contract_error is None


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Analyse or safely migrate every unique plan referenced by a comparison "
            "campaign to canonical metric IDs."
        )
    )
    parser.add_argument("campaign", type=Path, help="Comparison campaign manifest JSON")
    parser.add_argument(
        "--apply",
        action="store_true",
        help=(
            "Apply safe migrations in place after validating the whole campaign. "
            "Without this flag the command is a dry run."
        ),
    )
    parser.add_argument(
        "--backup-dir",
        type=Path,
        help=(
            "Backup directory used with --apply. Defaults to "
            "migration_backups/<campaign-id>-<UTC timestamp>."
        ),
    )
    return parser


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Plan JSON must contain an object: {path}")
    return payload


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path = path.expanduser().resolve()
    fd, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent, text=True
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, ensure_ascii=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def _campaign_plan_paths(repo_root: Path, campaign_path: Path) -> tuple[dict[str, Any], list[Path]]:
    campaign = load_campaign(campaign_path)
    seen: set[Path] = set()
    paths: list[Path] = []
    for matrix in campaign["matrices"]:
        batch_path = resolve_repo_path(repo_root, str(matrix["batch_path"]))
        batch = validate_batch_manifest(batch_path)
        for job in batch["jobs"]:
            plan_path = resolve_repo_path(repo_root, str(job["plan_path"]))
            if plan_path not in seen:
                seen.add(plan_path)
                paths.append(plan_path)
    return campaign, paths


def assess_plan(path: Path) -> PlanAssessment:
    plan = _read_json(path)
    legacy_ids = legacy_intrinsic_metric_ids(plan)
    compatibility_ids = compatibility_only_metric_ids(plan)
    migrated, changes = migrate_plan_to_canonical_ids(plan)
    contract_error: str | None = None
    try:
        validate_final_experiment_plan(migrated)
    except ExperimentContractError as exc:
        contract_error = str(exc)
    return PlanAssessment(
        path=path,
        legacy_ids=legacy_ids,
        compatibility_ids=compatibility_ids,
        migrated=migrated,
        changes=changes,
        contract_error=contract_error,
    )


def _portable_backup_path(repo_root: Path, plan_path: Path) -> Path:
    try:
        return plan_path.resolve().relative_to(repo_root.resolve())
    except ValueError:
        digest = hashlib.sha256(str(plan_path.resolve()).encode("utf-8")).hexdigest()[:12]
        return Path("external") / f"{digest}_{plan_path.name}"


def _print_assessment_summary(assessments: list[PlanAssessment], *, apply: bool) -> None:
    changed = [item for item in assessments if item.changed]
    blockers = [item for item in assessments if not item.ready_after_safe_migration]
    legacy_occurrences = sum(len(item.legacy_ids) for item in assessments)
    compatibility_plans = sum(1 for item in assessments if item.compatibility_ids)

    print("Campaign final-experiment plan audit")
    print("=" * 72)
    print(f"Unique plans referenced: {len(assessments)}")
    print(f"Plans needing safe ID migration: {len(changed)}")
    print(f"Legacy metric-ID occurrences: {legacy_occurrences}")
    print(f"Plans containing compatibility-only profiles: {compatibility_plans}")
    print(f"Plans still blocked after safe migration: {len(blockers)}")
    print(f"Mode: {'APPLY' if apply else 'DRY RUN'}")

    if changed:
        print("\nSafe one-to-one migrations required:")
        for item in changed:
            pairs = ", ".join(
                f"{change['from_metric_id']} -> {change['to_metric_id']}"
                for change in item.changes
            )
            print(f"  - {item.path}: {pairs}")

    if blockers:
        print("\nBLOCKERS — these plans require regeneration or another contract correction:")
        for item in blockers:
            print(f"  - {item.path}")
            if item.compatibility_ids:
                print(f"      compatibility-only metrics: {', '.join(item.compatibility_ids)}")
            if item.contract_error:
                print(f"      {item.contract_error}")


def main() -> int:
    args = _parser().parse_args()
    repo_root = Path(__file__).resolve().parents[1]
    campaign_path = args.campaign.expanduser()
    if not campaign_path.is_absolute():
        campaign_path = (Path.cwd() / campaign_path).resolve()
    else:
        campaign_path = campaign_path.resolve()
    if not campaign_path.is_file():
        raise SystemExit(f"Campaign does not exist or is not a file: {campaign_path}")

    campaign, plan_paths = _campaign_plan_paths(repo_root, campaign_path)
    assessments: list[PlanAssessment] = []
    for plan_path in plan_paths:
        if not plan_path.is_file():
            raise SystemExit(f"Referenced plan does not exist: {plan_path}")
        assessments.append(assess_plan(plan_path))

    _print_assessment_summary(assessments, apply=bool(args.apply))

    blockers = [item for item in assessments if not item.ready_after_safe_migration]
    if blockers:
        print("\nNo files were changed. Resolve the blockers above, then run the audit again.")
        return 2

    changed = [item for item in assessments if item.changed]
    if not args.apply:
        if changed:
            print("\nDry run only; no files changed.")
            print(f"Apply with: python {Path(__file__).relative_to(repo_root)} {campaign_path} --apply")
        else:
            print("\nAll referenced plans already satisfy the strict final-experiment contract.")
        return 0

    if not changed:
        print("\nNothing to migrate; all referenced plans already satisfy the strict contract.")
        return 0

    campaign_id = str(campaign["campaign_meta"]["campaign_id"])
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup_root = (
        args.backup_dir.expanduser().resolve()
        if args.backup_dir
        else (repo_root / "migration_backups" / f"{campaign_id}-{stamp}").resolve()
    )

    print(f"\nBackup directory: {backup_root}")
    for item in changed:
        backup_path = backup_root / _portable_backup_path(repo_root, item.path)
        backup_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(item.path, backup_path)

    for item in changed:
        _atomic_write_json(item.path, item.migrated)

    # Re-read and validate every plan after writing, including plans that required no migration.
    post_errors: list[tuple[Path, str]] = []
    for path in plan_paths:
        try:
            validate_final_experiment_plan(_read_json(path))
        except (ExperimentContractError, ValueError, OSError, json.JSONDecodeError) as exc:
            post_errors.append((path, str(exc)))

    if post_errors:
        print("\nERROR: post-migration validation failed. Backups were preserved:")
        for path, error in post_errors:
            print(f"  - {path}: {error}")
        return 3

    print(f"\nMigrated {len(changed)} plan(s) safely in place.")
    print(f"Backups preserved under: {backup_root}")
    print(f"Validated {len(plan_paths)} unique plan(s) against the strict final-experiment contract.")
    print("The campaign can now be run again in Strict final experiment mode.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
