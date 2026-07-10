from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from runner.field_translation import (
    available_translated_fields,
    detect_standard_pcap_field_translation_for_dataset,
    load_field_translation,
    merge_field_translations,
    metrics_missing_optional_fields,
    metrics_missing_required_fields,
    read_tabular_dataset_columns,
)
from runner.field_translation_reports import (
    build_field_translation_report,
    format_field_translation_markdown_report,
    format_field_translation_report,
    write_field_translation_report,
    write_text_report,
)
from runner.field_translation_sidecar import default_field_translation_path, ensure_field_translation_file


@dataclass
class FieldTranslationContext:
    """Prepared field translation state for a run."""

    metrics: list[dict]
    dataset_columns: list[str]
    translation_path: Path | None
    detected_translation: dict[str, str]
    explicit_translation: dict[str, str]
    field_translation: dict[str, str]
    skipped_metrics: dict[str, list[str]]
    missing_optional_fields: dict[str, list[str]]
    field_translation_report: dict[str, Any] | None
    human_report: str | None
    sidecar_status: str


def prepare_field_translation_context(
    *,
    args,
    dataset_path: Path,
    plan: dict,
    metrics: list[dict],
    all_enabled_metrics: list[dict],
    translation_path: Path | None,
    run_state,
) -> FieldTranslationContext:
    """Load, create/update, validate, and report field translations for a run."""
    dataset_columns = read_tabular_dataset_columns(dataset_path)
    detected_translation = detect_standard_pcap_field_translation_for_dataset(dataset_path)
    explicit_translation_path = translation_path is not None
    sidecar_status = "explicit" if explicit_translation_path else "none"

    if translation_path is None:
        sidecar_path = default_field_translation_path(dataset_path)
        if sidecar_path.exists():
            translation_path = sidecar_path
            sidecar_status = "existing"
        elif _confirm_sidecar_update("created", sidecar_path, args):
            translation_path = ensure_field_translation_file(
                dataset_path=dataset_path,
                plan=plan,
                detected_dataset_to_test=detected_translation,
            )
            sidecar_status = "created" if translation_path is not None else "none"

    explicit_translation = load_field_translation(translation_path)
    field_translation = merge_field_translations(detected_translation, explicit_translation)

    if not explicit_translation_path and translation_path is not None:
        if _confirm_sidecar_update("updated", translation_path, args):
            before_payload = translation_path.read_text(encoding="utf-8") if translation_path.exists() else None
            ensure_field_translation_file(
                dataset_path=dataset_path,
                plan=plan,
                detected_dataset_to_test=detected_translation,
            )
            after_payload = translation_path.read_text(encoding="utf-8") if translation_path.exists() else None
            if sidecar_status != "created":
                sidecar_status = "updated" if before_payload != after_payload else "unchanged"
        elif sidecar_status == "none":
            sidecar_status = "suppressed"

    skipped_metrics: dict[str, list[str]] = {}
    missing_optional_fields: dict[str, list[str]] = {}
    if dataset_columns:
        available_fields = available_translated_fields(dataset_columns, field_translation)
        skipped_metrics = metrics_missing_required_fields(metrics, available_fields)
        missing_optional_fields = metrics_missing_optional_fields(metrics, available_fields)
        if skipped_metrics:
            _print_skipped_metric_warning(skipped_metrics)
            for metric_id, missing_fields in skipped_metrics.items():
                run_state.mark_skipped(metric_id, missing_fields)
            metrics = [m for m in metrics if m["metric_id"] not in skipped_metrics]
            if not metrics and not args.field_translation_dry_run:
                raise ValueError("No enabled metrics can run because required field mappings are missing.")
    else:
        available_fields = set()

    field_translation_report = None
    human_report = None
    if dataset_columns:
        field_translation_report = build_field_translation_report(
            dataset_path=dataset_path,
            translation_path=translation_path,
            plan=plan,
            metrics=all_enabled_metrics,
            available_fields=available_fields,
            skipped_metrics=skipped_metrics,
            dataset_columns=dataset_columns,
            detected_translation=detected_translation,
            explicit_translation=explicit_translation,
            field_translation=field_translation,
            sidecar_status=sidecar_status,
            missing_optional_fields=missing_optional_fields,
        )
        human_report = format_field_translation_report(field_translation_report, use_color=_should_use_color())
        _write_requested_reports(args, field_translation_report, human_report)

    return FieldTranslationContext(
        metrics=metrics,
        dataset_columns=dataset_columns,
        translation_path=translation_path,
        detected_translation=detected_translation,
        explicit_translation=explicit_translation,
        field_translation=field_translation,
        skipped_metrics=skipped_metrics,
        missing_optional_fields=missing_optional_fields,
        field_translation_report=field_translation_report,
        human_report=human_report,
        sidecar_status=sidecar_status,
    )


def skipped_metric_records(skipped_metrics: dict[str, list[str]]) -> list[dict[str, Any]]:
    """Return outcome-ready skipped metric records for missing field mappings."""
    return [
        {
            "metric_id": metric_id,
            "status": "skipped",
            "reason": "missing_field_mappings",
            "missing_fields": fields,
        }
        for metric_id, fields in sorted(skipped_metrics.items())
    ]


def print_field_translation_dry_run_summary(context: FieldTranslationContext) -> None:
    """Print the dry-run field translation summary and completion status."""
    if context.translation_path is not None:
        print(f"Field translation file: {context.translation_path}")
    else:
        print("Field translation file: n/a")
    if context.human_report:
        print(context.human_report)
    if context.skipped_metrics:
        print("Dry run complete: some metrics would be skipped due to missing field mappings.")
    else:
        print("Dry run complete: all enabled metrics have required field mappings.")


def _confirm_sidecar_update(action: str, path: Path, args) -> bool:
    if args.no_update_field_translation:
        return False
    if args.yes_field_translation_sidecar or args.field_translation_dry_run:
        return True
    if not sys.stdin.isatty():
        print(f"WARNING: Field translation sidecar {action} skipped in non-interactive mode: {path}")
        print("         Re-run with --yes-field-translation-sidecar to allow this update.")
        return False
    answer = input(f"Field translation sidecar {path} needs to be {action}. Continue? [y/N] ").strip().lower()
    return answer in {"y", "yes"}


def _print_skipped_metric_warning(skipped_metrics: dict[str, list[str]]) -> None:
    yellow = "\033[33m" if _should_use_color() else ""
    reset = "\033[0m" if _should_use_color() else ""
    print(f"{yellow}WARNING: Skipping metrics with missing required field mappings:{reset}")
    for metric_id, missing_fields in skipped_metrics.items():
        print(f"  {yellow}[SKIPPED]{reset} {metric_id}: {', '.join(missing_fields)}")


def _write_requested_reports(args, field_translation_report: dict[str, Any], human_report: str) -> None:
    if args.field_translation_report:
        write_field_translation_report(Path(args.field_translation_report).expanduser().resolve(), field_translation_report)
    if args.field_translation_text_report:
        write_text_report(Path(args.field_translation_text_report).expanduser().resolve(), human_report)
    if args.field_translation_markdown_report:
        write_text_report(
            Path(args.field_translation_markdown_report).expanduser().resolve(),
            format_field_translation_markdown_report(field_translation_report),
        )


def _should_use_color() -> bool:
    return sys.stdout.isatty() and os.environ.get("TERM", "").lower() not in {"", "dumb"}
