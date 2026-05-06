import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from runner.taxonomy import print_taxonomy_summary
from runner.run_plan_serial import run_serial_metrics
from runner.dispatch import build_metric_handlers
from runner.io import load_case_or_plan
from runner.execution import auto_worker_count, run_metric_with_heartbeat, run_metrics_parallel, render_live_taxonomy
from runner.progress import render_overall_progress_line, print_live_status, set_live_header
from runner.order import load_taxonomy_order, order_metrics_by_taxonomy
from runner.tabular import load_tabular_dataset
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
    live_render_enabled = sys.stdout.isatty() and os.environ.get("TERM", "").lower() not in {"", "dumb"}
    default_metric_predictions = dict(DEFAULT_METRIC_PREDICTIONS)

    configure_signal_handlers(control_state, shutdown_requested)

    case_file = Path(args.case).resolve()
    plan, dataset_path, output_path, case_id = load_case_or_plan(case_file, args.dataset, args.output, args.case_id)


    metrics = [m for m in plan.get("metrics", []) if m.get("enabled", True)]
    if args.taxonomy_file:
        taxonomy_ranks = load_taxonomy_order(Path(args.taxonomy_file).expanduser().resolve())
        metrics = order_metrics_by_taxonomy(metrics, taxonomy_ranks, strict=args.taxonomy_strict)
    if not metrics:
        raise ValueError("The plan does not contain any enabled metrics.")

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
            base_lines = _base_header_lines()
            update_live_header(base_lines, [
                f"Status: Loading dataset",
                f"Elapsed: {elapsed:0.1f}s | Chunk: {chunk_idx} | Rows Loaded: {total_rows:,}",
                f"Overall Progress: 0/{len(metrics)} metrics completed",
                overall_header,
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
                ),
                "",
                None,
            )

        return load_tabular_dataset(path, progress_callback=_chunk_progress)

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
        ),
        "",
        None,
    )
    shared_tabular_df = None
    if dataset_path.suffix.lower() in {".csv", ".tsv", ".xlsx", ".xls"}:
        _print_phase_status("Dataset", "Loading tabular dataset")
        shared_tabular_df = _load_with_progress(dataset_path)
        source_field, destination_field = detect_ip_fields(shared_tabular_df)
        _print_title_box([
            "Dataset Summary",
            f"Rows: {len(shared_tabular_df):,}",
            f"Columns: {shared_tabular_df.shape[1]}",
            f"Source Field: {source_field}",
            f"Destination Field: {destination_field}",
        ])
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
    metric_handlers = build_metric_handlers(shared_tabular_df, load_tabular_dataset)

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
                elif completed_statuses.get(mid) == "running":
                    completed_statuses[mid] = "pending"
                    running_started_at.pop(mid, None)
            if event == "completed" and metric_id:
                completed_statuses[metric_id] = "success" if ok else "failed"
                running_started_at.pop(metric_id, None)
                if elapsed_seconds is not None:
                    completed_durations[metric_id] = float(elapsed_seconds)
            running_elapsed = {
                mid: (time.perf_counter() - started_at)
                for mid, started_at in running_started_at.items()
            }
            overall_header = render_overall_progress_line(max(1, completed), total, time.perf_counter() - run_start_perf, None)
            update_live_header([
                f"Run Title: {plan['plan_meta']['name']} ({plan['plan_meta']['plan_id']})",
                f"Case ID: {case_id}",
                f"Rows: {len(shared_tabular_df):,} | Columns: {shared_tabular_df.shape[1]}" if shared_tabular_df is not None else f"Metrics: {total}",
                f"Source Path: {dataset_path}",
                f"Destination Output: {output_path}",
            ], [
                f"Status: {'Stopping' if event == 'stopping' else f'Running ({mode})'}",
                f"Overall Progress: {completed}/{total} metrics completed",
                overall_header,
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
            metric_results, test_results, run_started_at, run_start_perf, column_validations
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
