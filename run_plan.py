import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from runner.taxonomy import print_taxonomy_summary
from runner.run_plan_serial import run_serial_metrics
from runner.dispatch import build_metric_handlers
from runner.io import load_case_or_plan
from runner.execution import auto_worker_count, run_metric_with_heartbeat, run_metrics_parallel, render_live_taxonomy
from runner.progress import render_overall_progress_line, print_live_status, set_live_header, set_live_output_enabled
from runner.order import load_taxonomy_order, order_metrics_by_taxonomy
from runner.tabular import load_tabular_dataset
from runner.telemetry import RunState
from runner.field_translation import (
    available_translated_fields,
    default_field_translation_path,
    detect_standard_pcap_field_translation_for_dataset,
    ensure_field_translation_file,
    load_field_translation,
    merge_field_translations,
    metrics_missing_required_fields,
    read_tabular_dataset_columns,
    build_field_translation_report,
    format_field_translation_markdown_report,
    format_field_translation_report,
    metrics_missing_optional_fields,
    write_field_translation_report,
    write_text_report,
)
from runner.run_plan_helpers import (
    build_base_header_lines,
    build_outcome,
    build_title_box_lines,
    configure_signal_handlers,
    detect_ip_fields,
    parse_run_plan_args,
    update_live_header,
    write_outcome,
)

DISPLAY_FALLBACK_NOTICE = "Interactive display requested; using compact live view until the Textual TUI is available."

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


def dispatch_metric_with_handlers(dataset_path: Path, metric: dict, metric_handlers: dict) -> tuple[bool, dict]:
    metric_id = metric["metric_id"]
    handler = metric_handlers.get(metric_id)
    if handler is None:
        raise ValueError(f"Unsupported metric_id: {metric_id}")
    return handler(dataset_path, metric)


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


def main():
    args = parse_run_plan_args()

    shutdown_requested = {"requested": False, "confirm_before": 0.0}
    control_state = {"pause_requested": False, "cancel_requested": False}
    live_render_enabled = (
        args.display != "quiet"
        and sys.stdout.isatty()
        and os.environ.get("TERM", "").lower() not in {"", "dumb"}
    )
    default_metric_predictions = dict(DEFAULT_METRIC_PREDICTIONS)

    configure_signal_handlers(control_state, shutdown_requested)
    display_mode = "compact" if args.display == "interactive" else args.display
    display_max_lines = 24 if display_mode == "compact" else None
    set_live_output_enabled(args.display != "quiet")
    if args.display == "interactive":
        print(DISPLAY_FALLBACK_NOTICE)

    case_file = Path(args.case).resolve()
    plan, dataset_path, output_path, case_id, translation_path = load_case_or_plan(
        case_file, args.dataset, args.output, args.case_id, args.field_translation
    )
    if not dataset_path.exists():
        raise FileNotFoundError(f"Dataset does not exist: {dataset_path}")

    metrics = [m for m in plan.get("metrics", []) if m.get("enabled", True)]
    all_enabled_metrics = list(metrics)
    if args.taxonomy_file:
        taxonomy_ranks = load_taxonomy_order(Path(args.taxonomy_file).expanduser().resolve())
        metrics = order_metrics_by_taxonomy(metrics, taxonomy_ranks, strict=args.taxonomy_strict)
        all_enabled_metrics = list(metrics)
    if not metrics:
        raise ValueError("The plan does not contain any enabled metrics.")

    run_state = RunState.from_plan(
        case_id=case_id,
        plan=plan,
        metrics=all_enabled_metrics,
        dataset_path=dataset_path,
        output_path=output_path,
        started_at=datetime.now(timezone.utc),
    )

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
            print(f"{yellow}WARNING: Skipping metrics with missing required field mappings:{reset}")
            for metric_id, missing_fields in skipped_metrics.items():
                print(f"  {yellow}[SKIPPED]{reset} {metric_id}: {', '.join(missing_fields)}")
                run_state.mark_skipped(metric_id, missing_fields)
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
        return

    def _print_title_box(lines: list[str]):
        for line in build_title_box_lines(lines):
            print(line)

    def _base_header_lines(include_dataset_size: bool = False) -> list[str]:
        return build_base_header_lines(plan, case_id, dataset_path, output_path, include_dataset_size)

    def _print_startup_banner():
        base_lines = _base_header_lines(include_dataset_size=True)
        _print_title_box(base_lines)
        update_live_header(base_lines, ["Status: Initializing run context"])

    def _print_phase_status(phase: str, detail: str = ""):
        timestamp = datetime.now(timezone.utc).strftime("%H:%M:%S")
        suffix = f" | {detail}" if detail else ""
        print(f"[{timestamp}] {phase}{suffix}")

    def _load_with_progress(path: Path):
        started = time.perf_counter()

        def _chunk_progress(chunk_idx: int, total_rows: int):
            elapsed = time.perf_counter() - started
            overall_header = render_overall_progress_line(0, len(metrics), 0.0, 0.0)
            compact_overall_header = overall_header.replace("Overall  ", "", 1)
            compact_overall_header = re.sub(r"\s+\(\d+/\d+\)", "", compact_overall_header, count=1)
            base_lines = _base_header_lines()
            update_live_header(base_lines, [
                f"Status: Loading dataset",
                f"Elapsed: {elapsed:0.1f}s | Chunk: {chunk_idx} | Rows Loaded: {total_rows:,}",
                f"Overall Progress: 0/{len(metrics)} metrics completed",
                compact_overall_header,
            ])
            print_live_status(
                render_live_taxonomy(
                    metrics,
                    "dataset_loading",
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

        return load_tabular_dataset(path, progress_callback=_chunk_progress, field_translation=field_translation)

    _print_startup_banner()
    _print_phase_status("Startup", "Initializing run context")
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
    if dataset_path.suffix.lower() in {".csv", ".tsv", ".xlsx", ".xls"}:
        _print_phase_status("Dataset", "Loading tabular dataset")
        shared_tabular_df = _load_with_progress(dataset_path)
        source_field, destination_field = detect_ip_fields(shared_tabular_df)
        update_live_header([
            f"Run Title: {plan['plan_meta']['name']} ({plan['plan_meta']['plan_id']})",
            f"Case ID: {case_id}",
            f"Rows: {len(shared_tabular_df):,} | Columns: {shared_tabular_df.shape[1]}",
            f"Source Field: {source_field}",
            f"Destination Field: {destination_field}",
            f"Source Path: {dataset_path}",
            f"Destination Output: {output_path}",
        ], [
            "Status: Dataset loaded",
            f"Overall Progress: 0/{len(metrics)} metrics completed",
        ])
    def _load_dataset_for_metric(path: Path):
        return load_tabular_dataset(path, field_translation=field_translation)

    metric_handlers = build_metric_handlers(shared_tabular_df, _load_dataset_for_metric, field_translation)

    execution_policy = plan.get("execution_policy", {})
    fail_fast = execution_policy.get("fail_fast", True)

    run_started_at = datetime.now(timezone.utc)
    run_start_perf = time.perf_counter()

    overall_status = "success"
    test_results = {}
    metric_results = []
    column_validations = {}

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
            f"Rows: {len(shared_tabular_df):,} | Columns: {shared_tabular_df.shape[1]} | Metrics: {total_metrics}",
            f"Execution: {mode} | Workers: {workers}",
            f"Source Field: {source_field}",
            f"Destination Field: {destination_field}",
            f"Source Path: {dataset_path}",
            f"Destination Output: {output_path}",
        ])
    if workers > 1:
        running_started_at: dict[str, float] = {}
        def _parallel_progress(event, completed, total, pending, metric_id, ok, running_ids, elapsed_seconds):
            active_running = set(running_ids or [])
            if event == "stopping":
                for m in metrics:
                    mid = m["metric_id"]
                    if mid not in completed_statuses:
                        completed_statuses[mid] = "stopping"
            for m in metrics:
                mid = m["metric_id"]
                if mid in completed_statuses:
                    continue
                if mid in active_running:
                    completed_statuses[mid] = "running"
                    running_started_at.setdefault(mid, time.perf_counter())
                    run_state.mark_running(mid)
                elif completed_statuses.get(mid) == "running":
                    completed_statuses[mid] = "pending"
                    running_started_at.pop(mid, None)
            if event == "completed" and metric_id:
                completed_statuses[metric_id] = "success" if ok else "failed"
                running_started_at.pop(metric_id, None)
                if elapsed_seconds is not None:
                    completed_durations[metric_id] = float(elapsed_seconds)
                run_state.mark_completed(
                    metric_id,
                    "success" if ok else "failed",
                    elapsed_seconds=float(elapsed_seconds) if elapsed_seconds is not None else None,
                )
            running_elapsed = {
                mid: (time.perf_counter() - started_at)
                for mid, started_at in running_started_at.items()
            }
            overall_header = render_overall_progress_line(max(1, completed), total, time.perf_counter() - run_start_perf, None)
            compact_overall_header = overall_header.replace("Overall  ", "", 1)
            compact_overall_header = re.sub(r"\s+\(\d+/\d+\)", "", compact_overall_header, count=1)
            update_live_header([
                f"Run Title: {plan['plan_meta']['name']} ({plan['plan_meta']['plan_id']})",
                f"Case ID: {case_id}",
                f"Rows: {len(shared_tabular_df):,} | Columns: {shared_tabular_df.shape[1]}" if shared_tabular_df is not None else f"Metrics: {total}",
                f"Source Path: {dataset_path}",
                f"Destination Output: {output_path}",
            ], [
                f"Status: {'Stopping' if event == 'stopping' else f'Running ({mode})'}",
                f"Overall Progress: {completed}/{total} metrics completed",
                compact_overall_header,
            ])
            print_live_status(
                render_live_taxonomy(
                    metrics,
                    metric_id if metric_id else "parallel_batch",
                    completed_statuses,
                    completed_durations,
                    default_metric_predictions,
                    max(20.0, float(total)),
                    elapsed=(time.perf_counter() - run_start_perf),
                    completed=False,
                    running_elapsed=running_elapsed,
                    display_mode=display_mode,
                    max_lines=display_max_lines,
                    run_state=run_state,
                ),
                "",
                None,
            )

        parallel_out = run_metrics_parallel(
            dataset_path,
            metrics,
            metric_handlers,
            workers,
            progress_callback=_parallel_progress,
            control_state=control_state,
        )
        for idx0, success, metric_payload in parallel_out:
            metric = metrics[idx0]
            metric_record = {
                "metric_id": metric["metric_id"],
                "status": "success" if success else "failed",
                "started_at": run_started_at.isoformat(),
                "finished_at": datetime.now(timezone.utc).isoformat(),
                "elapsed_seconds": metric_payload.get("elapsed_seconds", 0.0),
            }
            if success:
                test_results.update(metric_payload.get("test_results", {}))
                if "column_validation" in metric_payload:
                    column_validations[metric["metric_id"]] = metric_payload["column_validation"]
            else:
                metric_record["error"] = metric_payload.get("error", "Unknown error")
                overall_status = "failed" if overall_status == "success" else overall_status
                if fail_fast:
                    metric_results.append(metric_record)
                    break
            metric_results.append(metric_record)
            completed_statuses[metric["metric_id"]] = metric_record["status"]
            completed_durations[metric["metric_id"]] = metric_record["elapsed_seconds"]
        # finalize immediately for parallel path
        outcome = build_outcome(
            overall_status, case_id, plan["plan_meta"]["plan_id"], metrics, dataset_path,
            metric_results, test_results, run_started_at, run_start_perf, column_validations,
            skipped_metrics=[{"metric_id": mid, "status": "skipped", "reason": "missing_field_mappings", "missing_fields": fields} for mid, fields in skipped_metrics.items()],
            all_metrics=all_enabled_metrics,
        )
        write_outcome(output_path, outcome)
        _print_phase_status("Completed")
        return
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
        skipped_metrics=[{"metric_id": mid, "status": "skipped", "reason": "missing_field_mappings", "missing_fields": fields} for mid, fields in skipped_metrics.items()],
        all_metrics=all_enabled_metrics,
        display_mode=display_mode,
        display_max_lines=display_max_lines,
        run_state=run_state,
    )
    if early_returned:
        return

    write_outcome(output_path, outcome)
    _print_phase_status("Completed")

    if sys.stdout.isatty():
        print()
    if not live_render_enabled:
        print("Results by taxonomy:")
        print_taxonomy_summary(outcome["result_taxonomy"])
if __name__ == "__main__":
    main()
