import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from runner.taxonomy import print_taxonomy_summary
from runner.run_plan_serial import run_serial_metrics
from runner.dispatch import build_metric_handlers
from runner.io import load_case_or_plan
from runner.execution import auto_worker_count, run_metrics_parallel
from runner.live_rendering import render_live_taxonomy
from runner.progress import render_overall_progress_line, print_live_status
from runner.order import load_taxonomy_order, order_metrics_by_taxonomy
from runner.dataset_loading import is_tabular_dataset, load_shared_tabular_dataset
from runner.tabular import load_tabular_dataset
from runner.telemetry import RunState
from runner.field_translation_workflow import (
    prepare_field_translation_context,
    print_field_translation_dry_run_summary,
    skipped_metric_records,
)
from runner.run_display import compact_overall_progress_line, configure_display, print_phase_status, print_title_box
from runner.run_plan_helpers import (
    build_base_header_lines,
    build_outcome,
    configure_signal_handlers,
    detect_ip_fields,
    parse_run_plan_args,
    update_live_header,
    write_outcome,
)


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


def main():
    args = parse_run_plan_args()

    shutdown_requested = {"requested": False, "confirm_before": 0.0}
    control_state = {"pause_requested": False, "cancel_requested": False}
    live_render_enabled, display_mode, display_max_lines = configure_display(args)
    default_metric_predictions = dict(DEFAULT_METRIC_PREDICTIONS)

    configure_signal_handlers(control_state, shutdown_requested)

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

    field_translation_context = prepare_field_translation_context(
        args=args,
        dataset_path=dataset_path,
        plan=plan,
        metrics=metrics,
        all_enabled_metrics=all_enabled_metrics,
        translation_path=translation_path,
        run_state=run_state,
    )
    metrics = field_translation_context.metrics
    field_translation = field_translation_context.field_translation
    skipped_metrics = field_translation_context.skipped_metrics

    if args.field_translation_dry_run:
        print_field_translation_dry_run_summary(field_translation_context)
        return

    def _base_header_lines(include_dataset_size: bool = False) -> list[str]:
        return build_base_header_lines(plan, case_id, dataset_path, output_path, include_dataset_size)

    def _print_startup_banner():
        base_lines = _base_header_lines(include_dataset_size=True)
        print_title_box(base_lines)
        update_live_header(base_lines, ["Status: Initializing run context"])

    _print_startup_banner()
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
            compact_overall_header = compact_overall_progress_line(overall_header)
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
            skipped_metrics=skipped_metric_records(skipped_metrics),
            all_metrics=all_enabled_metrics,
        )
        write_outcome(output_path, outcome)
        print_phase_status("Completed")
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
        skipped_metrics=skipped_metric_records(skipped_metrics),
        all_metrics=all_enabled_metrics,
        display_mode=display_mode,
        display_max_lines=display_max_lines,
        run_state=run_state,
    )
    if early_returned:
        return

    write_outcome(output_path, outcome)
    print_phase_status("Completed")

    if sys.stdout.isatty():
        print()
    if not live_render_enabled:
        print("Results by taxonomy:")
        print_taxonomy_summary(outcome["result_taxonomy"])
if __name__ == "__main__":
    main()
