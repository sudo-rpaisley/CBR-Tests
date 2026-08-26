import os
import sys
import time
from pathlib import Path

from runner.contract import enforce_skip_policy, validate_loaded_dataset_applicability
from runner.dataset_loading import is_tabular_dataset, load_shared_tabular_dataset
from runner.dispatch import build_metric_handlers
from runner.execution import auto_worker_count, render_live_taxonomy, run_metrics_parallel
from runner.field_translation import (
    available_translated_fields,
    build_field_translation_report,
    default_field_translation_path,
    detect_standard_pcap_field_translation_for_dataset,
    ensure_field_translation_file,
    format_field_translation_markdown_report,
    format_field_translation_report,
    load_field_translation,
    merge_field_translations,
    metrics_missing_optional_fields,
    metrics_missing_required_fields,
    read_tabular_dataset_columns,
    write_field_translation_report,
    write_text_report,
)
from runner.parallel_progress import build_parallel_progress_callback
from runner.parallel_results import collect_parallel_metric_results
from runner.progress import print_live_status
from runner.provenance import build_provenance_manifest
from runner.pcap_adapter import PCAP_PACKET_BACKED_METRICS, build_pcap_packet_dataframe, is_packet_capture
from runner.run_context import prepare_run_context
from runner.run_display import print_phase_status, print_title_box
from runner.run_plan_helpers import (
    build_base_header_lines,
    build_outcome,
    detect_ip_fields,
    parse_run_plan_args,
    update_live_header,
    write_outcome,
)
from runner.run_plan_serial import run_serial_metrics
from runner.tabular import load_tabular_dataset
from runner.taxonomy import print_taxonomy_summary
from runner.tui import launch_tui, show_post_run_menu

DEFAULT_METRIC_PREDICTIONS = {
    "column_quality_profile": 2.0,
    "pearson_correlation_profile": 3.0,
    "valid_port_range_profile": 25.0,
    "service_port_consistency_profile": 80.0,
    "tcp_flag_consistency_profile": 20.0,
    "handshake_plausibility_profile": 70.0,
    "flow_duration_consistency_profile": 20.0,
    "packet_byte_consistency_profile": 20.0,
    "reserved_ip_address_profile": 105.0,
}


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


def _run_result(*, dry_run: bool, status: str | None, output_path: Path, metrics_total: int, skipped_count: int) -> dict:
    return {
        "dry_run": dry_run,
        "status": status,
        "output_path": str(output_path),
        "metrics_total": metrics_total,
        "skipped_count": skipped_count,
    }


def run_once(args):
    """Execute one configured run and return a small summary for the TUI/session layer."""
    context = prepare_run_context(args, DEFAULT_METRIC_PREDICTIONS)
    shutdown_requested = context.shutdown_requested
    control_state = context.control_state
    live_render_enabled = context.live_render_enabled
    display_mode = context.display_mode
    display_max_lines = context.display_max_lines
    default_metric_predictions = context.default_metric_predictions
    plan = context.plan
    dataset_path = context.dataset_path
    output_path = context.output_path
    case_id = context.case_id
    translation_path = context.translation_path
    metrics = context.metrics
    all_enabled_metrics = context.all_enabled_metrics
    run_state = context.run_state
    run_started_at = context.run_started_at
    run_start_perf = context.run_start_perf

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

    skipped_metrics = {}
    missing_optional_fields = {}
    if dataset_columns:
        available_fields = available_translated_fields(dataset_columns, field_translation)
        skipped_metrics = metrics_missing_required_fields(metrics, available_fields)
        missing_optional_fields = metrics_missing_optional_fields(metrics, available_fields)
        if skipped_metrics:
            use_color = sys.stdout.isatty() and os.environ.get("TERM", "").lower() not in {"", "dumb"}
            yellow = "\033[33m" if use_color else ""
            reset = "\033[0m" if use_color else ""
            print(f"{yellow}WARNING: Metrics have missing required field mappings:{reset}")
            for metric_id, missing_fields in skipped_metrics.items():
                print(f"  {yellow}[SKIPPED]{reset} {metric_id}: {', '.join(missing_fields)}")
                run_state.mark_skipped(metric_id, missing_fields)
            enforce_skip_policy(plan, skipped_metrics, dry_run=args.field_translation_dry_run)
            metrics = [m for m in metrics if m["metric_id"] not in skipped_metrics]
            if not metrics and not args.field_translation_dry_run:
                raise ValueError("No enabled metrics can run because required field mappings are missing.")

    field_translation_report = None
    if dataset_columns:
        field_translation_report = build_field_translation_report(
            dataset_path=dataset_path,
            translation_path=translation_path,
            plan=plan,
            metrics=all_enabled_metrics,
            available_fields=available_fields if dataset_columns else set(),
            skipped_metrics=skipped_metrics,
            dataset_columns=dataset_columns,
            detected_translation=detected_translation,
            explicit_translation=explicit_translation,
            field_translation=field_translation,
            sidecar_status=sidecar_status,
            missing_optional_fields=missing_optional_fields,
        )
        human_report = format_field_translation_report(
            field_translation_report,
            use_color=sys.stdout.isatty() and os.environ.get("TERM", "").lower() not in {"", "dumb"},
        )
        if args.field_translation_report:
            write_field_translation_report(Path(args.field_translation_report).expanduser().resolve(), field_translation_report)
        if args.field_translation_text_report:
            write_text_report(Path(args.field_translation_text_report).expanduser().resolve(), human_report)
        if args.field_translation_markdown_report:
            write_text_report(
                Path(args.field_translation_markdown_report).expanduser().resolve(),
                format_field_translation_markdown_report(field_translation_report),
            )

    if args.field_translation_dry_run:
        if translation_path is not None:
            print(f"Field translation file: {translation_path}")
        else:
            print("Field translation file: n/a")
        if field_translation_report:
            print(human_report)
        if skipped_metrics:
            print("Dry run complete: some metrics would be skipped due to missing field mappings.")
        else:
            print("Dry run complete: all enabled metrics have required field mappings.")
        result = _run_result(
            dry_run=True,
            status="needs_attention" if skipped_metrics else "ready",
            output_path=output_path,
            metrics_total=len(all_enabled_metrics),
            skipped_count=len(skipped_metrics),
        )
        result["missing_fields"] = sorted({field for fields in skipped_metrics.values() for field in fields})
        result["dataset_columns"] = sorted(dataset_columns)
        result["field_translation_path"] = str(translation_path or default_field_translation_path(dataset_path))
        return result

    provenance = build_provenance_manifest(
        plan=plan,
        dataset_path=dataset_path,
        case_file=context.case_file,
        plan_source_path=context.plan_source_path,
        field_translation=field_translation,
        translation_path=translation_path,
        taxonomy_path=context.taxonomy_path,
        cli_arguments=vars(args),
    )

    base_header_lines = build_base_header_lines(plan, case_id, dataset_path, output_path, include_dataset_size=True)
    print_title_box(base_header_lines)
    update_live_header(base_header_lines, ["Status: Initializing run context"])
    print_phase_status("Startup", "Initializing run context")
    print_live_status(
        render_live_taxonomy(
            metrics,
            "startup",
            {},
            {},
            default_metric_predictions,
            max(20.0, float(len(metrics))),
            elapsed=0.0,
            completed=False,
            display_mode=display_mode,
            max_lines=display_max_lines,
            run_state=run_state,
        ),
        "",
        None,
    )
    shared_tabular_df = None
    if is_tabular_dataset(dataset_path):
        shared_tabular_df = load_shared_tabular_dataset(
            dataset_path=dataset_path,
            plan=plan,
            case_id=case_id,
            output_path=output_path,
            metrics=metrics,
            field_translation=field_translation,
            default_metric_predictions=default_metric_predictions,
            display_mode=display_mode,
            display_max_lines=display_max_lines,
            run_state=run_state,
        )
        validate_loaded_dataset_applicability(plan, shared_tabular_df)
    elif is_packet_capture(dataset_path) and any(
        metric["metric_id"] in PCAP_PACKET_BACKED_METRICS for metric in metrics
    ):
        print_phase_status("PCAP", "Building canonical packet view")
        shared_tabular_df = build_pcap_packet_dataframe(dataset_path)

    def _load_dataset_for_metric(path: Path):
        return load_tabular_dataset(path, field_translation=field_translation)

    metric_handlers = build_metric_handlers(shared_tabular_df, _load_dataset_for_metric, field_translation)

    execution_policy = plan.get("execution_policy", {})
    fail_fast = execution_policy.get("fail_fast", True)

    total_metrics = len(metrics)
    completed_statuses: dict[str, str] = {}
    completed_durations: dict[str, float] = {}
    workers = args.workers if args.workers is not None else auto_worker_count(total_metrics)
    workers = max(1, int(workers))
    if shared_tabular_df is not None and workers > 4:
        workers = 4
    mode = "parallel" if workers > 1 else "serial"
    if shared_tabular_df is not None:
        source_field, destination_field = detect_ip_fields(shared_tabular_df)
        update_live_header([
            f"Run Title: {plan['plan_meta']['name']} ({plan['plan_meta']['plan_id']})",
            f"Case ID: {case_id}",
            f"Run ID: {provenance['run_id']}",
            f"{'Packets' if is_packet_capture(dataset_path) else 'Rows'}: {len(shared_tabular_df):,} | Columns: {shared_tabular_df.shape[1]} | Metrics: {total_metrics}",
            f"Execution: {mode} | Workers: {workers}",
            f"Source Field: {source_field}",
            f"Destination Field: {destination_field}",
            f"Source Path: {dataset_path}",
            f"Destination Output: {output_path}",
        ])
    skipped_metric_records = [
        {"metric_id": mid, "status": "skipped", "reason": "missing_field_mappings", "missing_fields": fields}
        for mid, fields in skipped_metrics.items()
    ]

    if workers > 1:
        parallel_progress = build_parallel_progress_callback(
            plan=plan,
            case_id=case_id,
            dataset_path=dataset_path,
            output_path=output_path,
            metrics=metrics,
            shared_tabular_df=shared_tabular_df,
            mode=mode,
            completed_statuses=completed_statuses,
            completed_durations=completed_durations,
            default_metric_predictions=default_metric_predictions,
            run_start_perf=run_start_perf,
            display_mode=display_mode,
            display_max_lines=display_max_lines,
            run_state=run_state,
        )
        parallel_out = run_metrics_parallel(
            dataset_path,
            metrics,
            metric_handlers,
            workers,
            progress_callback=parallel_progress,
            control_state=control_state,
            fail_fast=fail_fast,
        )
        overall_status, test_results, metric_results, column_validations = collect_parallel_metric_results(
            parallel_out=parallel_out,
            metrics=metrics,
            run_started_at=run_started_at,
            fail_fast=fail_fast,
            completed_statuses=completed_statuses,
            completed_durations=completed_durations,
        )
        outcome = build_outcome(
            overall_status,
            case_id,
            plan["plan_meta"]["plan_id"],
            metrics,
            dataset_path,
            metric_results,
            test_results,
            run_started_at,
            run_start_perf,
            column_validations,
            skipped_metrics=skipped_metric_records,
            all_metrics=all_enabled_metrics,
            provenance=provenance,
        )
        write_outcome(output_path, outcome)
        print_phase_status("Completed")
        return _run_result(
            dry_run=False,
            status=outcome.get("status"),
            output_path=output_path,
            metrics_total=len(all_enabled_metrics),
            skipped_count=len(skipped_metric_records),
        )

    early_returned, outcome = run_serial_metrics(
        dataset_path=dataset_path,
        output_path=output_path,
        plan=plan,
        case_id=case_id,
        metrics=metrics,
        metric_handlers=metric_handlers,
        shutdown_requested=shutdown_requested,
        control_state=control_state,
        default_metric_predictions=default_metric_predictions,
        live_render_enabled=live_render_enabled,
        fail_fast=fail_fast,
        run_started_at=run_started_at,
        run_start_perf=run_start_perf,
        completed_statuses=completed_statuses,
        completed_durations=completed_durations,
        skipped_metrics=skipped_metric_records,
        all_metrics=all_enabled_metrics,
        display_mode=display_mode,
        display_max_lines=display_max_lines,
        run_state=run_state,
        provenance=provenance,
    )
    if early_returned:
        return _run_result(
            dry_run=False,
            status=outcome.get("status") if outcome else "cancelled",
            output_path=output_path,
            metrics_total=len(all_enabled_metrics),
            skipped_count=len(skipped_metric_records),
        )

    write_outcome(output_path, outcome)
    print_phase_status("Completed")

    if sys.stdout.isatty():
        print()
    if not live_render_enabled:
        print("Results by taxonomy:")
        print_taxonomy_summary(outcome["result_taxonomy"])
    return _run_result(
        dry_run=False,
        status=outcome.get("status"),
        output_path=output_path,
        metrics_total=len(all_enabled_metrics),
        skipped_count=len(skipped_metric_records),
    )


def main():
    args = parse_run_plan_args()
    while True:
        result = run_once(args)
        if not getattr(args, "tui_session", False):
            return result

        action = show_post_run_menu(result, args)
        if action == "quit":
            return result
        if action == "run":
            args.field_translation_dry_run = False
            continue

        args = launch_tui(args)
        args.tui_session = True


if __name__ == "__main__":
    main()
