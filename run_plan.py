import json
import argparse
import os
import signal
import sys
import time
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from pathlib import Path

from runner.schema import validate_plan_schema
from runner.taxonomy import build_plan_taxonomy, build_result_taxonomy, build_test_results_taxonomy, print_taxonomy_summary
from runner.dispatch import build_metric_handlers
from runner.io import load_case_or_plan
from runner.execution import auto_worker_count, run_metric_with_heartbeat, run_metrics_parallel, render_live_taxonomy
from runner.progress import render_overall_progress_line, print_live_status, set_live_header
from runner.order import load_taxonomy_order, order_metrics_by_taxonomy
from runner.tabular import load_tabular_dataset

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
    parser = argparse.ArgumentParser(
        description="Run a test plan from a case JSON."
    )
    parser.add_argument("--case", required=True, help="Path to case JSON or plan JSON file")
    parser.add_argument("--dataset", help="Dataset path (required when --case points to a plan JSON)")
    parser.add_argument("--output", help="Output path (required when --case points to a plan JSON)")
    parser.add_argument("--case-id", default="ad_hoc_case", help="Case ID used when running a plan JSON directly")
    parser.add_argument("--taxonomy-file", help="Optional taxonomy JSON used to order metrics")
    parser.add_argument("--taxonomy-strict", action="store_true", help="Fail if enabled plan metrics are missing from taxonomy order")
    parser.add_argument("--workers", type=int, default=None, help="Optional worker count override. Use 1 to force serial execution.")
    args = parser.parse_args()

    shutdown_requested = {"requested": False, "confirm_before": 0.0}
    control_state = {"pause_requested": False, "cancel_requested": False}
    live_render_enabled = sys.stdout.isatty() and os.environ.get("TERM", "").lower() not in {"", "dumb"}
    default_metric_predictions = dict(DEFAULT_METRIC_PREDICTIONS)

    def _handle_sigint(_signum, _frame):
        control_state["cancel_requested"] = True
        shutdown_requested["requested"] = True
        shutdown_requested["confirm_before"] = time.time()
        print("\nStop requested. Cancelling current task and pending tasks...")

    def _handle_sigusr1(_signum, _frame):
        control_state["pause_requested"] = True
        print("\nPause requested (SIGUSR1). Send SIGUSR2 to resume.")

    def _handle_sigusr2(_signum, _frame):
        control_state["pause_requested"] = False
        print("\nResume requested (SIGUSR2).")

    signal.signal(signal.SIGINT, _handle_sigint)
    if hasattr(signal, "SIGUSR1"):
        signal.signal(signal.SIGUSR1, _handle_sigusr1)
    if hasattr(signal, "SIGUSR2"):
        signal.signal(signal.SIGUSR2, _handle_sigusr2)

    case_file = Path(args.case).resolve()
    plan, dataset_path, output_path, case_id = load_case_or_plan(case_file, args.dataset, args.output, args.case_id)


    metrics = [m for m in plan.get("metrics", []) if m.get("enabled", True)]
    if args.taxonomy_file:
        taxonomy_ranks = load_taxonomy_order(Path(args.taxonomy_file).expanduser().resolve())
        metrics = order_metrics_by_taxonomy(metrics, taxonomy_ranks, strict=args.taxonomy_strict)
    if not metrics:
        raise ValueError("The plan does not contain any enabled metrics.")

    def _print_title_box(lines: list[str]):
        width = 108
        print("=" * width)

    def _build_title_box_lines(lines: list[str], status_lines: list[str] | None = None) -> list[str]:
        width = 108
        framed = ["=" * width]
        for line in lines:
            framed.append(f"| {line[:width-4].ljust(width-4)} |")
        if status_lines:
            framed.append(f"| {'-' * (width-4)} |")
            for s in status_lines:
                framed.append(f"| {s[:width-4].ljust(width-4)} |")
        framed.append("=" * width)
        return framed
        for line in lines:
            print(f"| {line[:width-4].ljust(width-4)} |")
        print("=" * width)

    def _print_startup_banner():
        dataset_name = dataset_path.name
        dataset_size = dataset_path.stat().st_size if dataset_path.exists() else 0
        dataset_size_mb = round(dataset_size / (1024 * 1024), 2)
        _print_title_box([
            f"Run Title: {plan['plan_meta']['name']} ({plan['plan_meta']['plan_id']})",
            f"Case ID: {case_id}",
            f"Source Dataset: {dataset_name} ({dataset_size_mb} MB)",
            f"Source Path: {dataset_path}",
            f"Destination Output: {output_path}",
        ])
        base_lines = [
            f"Run Title: {plan['plan_meta']['name']} ({plan['plan_meta']['plan_id']})",
            f"Case ID: {case_id}",
            f"Source Dataset: {dataset_name} ({dataset_size_mb} MB)",
            f"Source Path: {dataset_path}",
            f"Destination Output: {output_path}",
        ]
        set_live_header(_build_title_box_lines(base_lines, ["Status: Initializing run context"]))

    def _print_phase_status(phase: str, detail: str = ""):
        timestamp = datetime.now(timezone.utc).strftime("%H:%M:%S")
        suffix = f" | {detail}" if detail else ""
        print(f"[{timestamp}] {phase}{suffix}")

    def _load_with_progress(path: Path):
        started = time.perf_counter()

        def _chunk_progress(chunk_idx: int, total_rows: int):
            elapsed = time.perf_counter() - started
            base_lines = [
                f"Run Title: {plan['plan_meta']['name']} ({plan['plan_meta']['plan_id']})",
                f"Case ID: {case_id}",
                f"Source Path: {dataset_path}",
                f"Destination Output: {output_path}",
            ]
            set_live_header(_build_title_box_lines(base_lines, [
                f"Status: Loading dataset",
                f"Elapsed: {elapsed:0.1f}s | Chunk: {chunk_idx} | Rows Loaded: {total_rows:,}",
                f"Overall Progress: 0/{len(metrics)} metrics completed",
            ]))
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
                render_overall_progress_line(0, len(metrics), 0.0, 0.0),
                f"Preparing dataset load... {elapsed:0.1f}s | chunk={chunk_idx} | rows_loaded={total_rows:,}",
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
        render_overall_progress_line(0, len(metrics), 0.0, 0.0),
        "Preparing dataset load...",
    )
    shared_tabular_df = None
    if dataset_path.suffix.lower() in {".csv", ".tsv", ".xlsx", ".xls"}:
        _print_phase_status("Dataset", "Loading tabular dataset")
        shared_tabular_df = _load_with_progress(dataset_path)
        source_candidates = ["Source IP", "Src IP", "source_ip", "src_ip"]
        destination_candidates = ["Destination IP", "Dst IP", "destination_ip", "dst_ip"]
        source_field = next((c for c in source_candidates if c in shared_tabular_df.columns), "n/a")
        destination_field = next((c for c in destination_candidates if c in shared_tabular_df.columns), "n/a")
        _print_title_box([
            "Dataset Summary",
            f"Rows: {len(shared_tabular_df):,}",
            f"Columns: {shared_tabular_df.shape[1]}",
            f"Source Field: {source_field}",
            f"Destination Field: {destination_field}",
        ])
        set_live_header(_build_title_box_lines([
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
        ]))
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
        source_candidates = ["Source IP", "Src IP", "source_ip", "src_ip"]
        destination_candidates = ["Destination IP", "Dst IP", "destination_ip", "dst_ip"]
        source_field = next((c for c in source_candidates if c in shared_tabular_df.columns), "n/a")
        destination_field = next((c for c in destination_candidates if c in shared_tabular_df.columns), "n/a")
        set_live_header(_build_title_box_lines([
            f"Run Title: {plan['plan_meta']['name']} ({plan['plan_meta']['plan_id']})",
            f"Case ID: {case_id}",
            f"Rows: {len(shared_tabular_df):,} | Columns: {shared_tabular_df.shape[1]} | Metrics: {total_metrics}",
            f"Execution: {mode} | Workers: {workers}",
            f"Source Field: {source_field}",
            f"Destination Field: {destination_field}",
            f"Source Path: {dataset_path}",
            f"Destination Output: {output_path}",
        ]))
    if workers > 1:
        running_started_at: dict[str, float] = {}
        def _parallel_progress(event, completed, total, pending, metric_id, ok, running_ids, elapsed_seconds):
            active_running = set(running_ids or [])
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
            set_live_header(_build_title_box_lines([
                f"Run Title: {plan['plan_meta']['name']} ({plan['plan_meta']['plan_id']})",
                f"Case ID: {case_id}",
                f"Rows: {len(shared_tabular_df):,} | Columns: {shared_tabular_df.shape[1]}" if shared_tabular_df is not None else f"Metrics: {total}",
                f"Source Path: {dataset_path}",
                f"Destination Output: {output_path}",
            ], [
                f"Status: Running ({mode})",
                f"Overall Progress: {completed}/{total} metrics completed",
                f"Pending Metrics: {pending}",
            ]))
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
                render_overall_progress_line(max(1, completed), total, time.perf_counter() - run_start_perf, None),
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
        outcome = {
            "status": overall_status,
            "case_id": case_id,
            "plan_id": plan["plan_meta"]["plan_id"],
            "metric_ids": [m["metric_id"] for m in metrics],
            "dataset_path": str(dataset_path),
            "plan_taxonomy": build_plan_taxonomy(metrics),
            "metric_results": metric_results,
            "test_results": test_results,
            "test_results_taxonomy": build_test_results_taxonomy(metrics, test_results),
            "result_taxonomy": build_result_taxonomy(metrics, metric_results, test_results),
            "run_started_at": run_started_at.isoformat(),
            "run_finished_at": datetime.now(timezone.utc).isoformat(),
            "run_elapsed_seconds": round(time.perf_counter() - run_start_perf, 6)
        }
        if column_validations:
            outcome["column_validations"] = column_validations
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(outcome, f, indent=2)
        _print_phase_status("Completed")
        return
    for idx, metric in enumerate(metrics, start=1):
        while control_state.get("pause_requested") and not control_state.get("cancel_requested"):
            set_live_header(_build_title_box_lines([
                f"Run Title: {plan['plan_meta']['name']} ({plan['plan_meta']['plan_id']})",
                f"Case ID: {case_id}",
                f"Source Path: {dataset_path}",
                f"Destination Output: {output_path}",
            ], [
                "Status: Paused",
                f"Overall Progress: {idx-1}/{total_metrics} metrics completed",
                "Send SIGUSR2 to resume or Ctrl-C to cancel",
            ]))
            time.sleep(0.2)
        metric_started_at = datetime.now(timezone.utc)
        metric_start_perf = time.perf_counter()
        try:
            success, metric_payload = run_metric_with_heartbeat(
                dataset_path, metric, metrics, completed_statuses, completed_durations, idx, total_metrics, shutdown_requested, run_start_perf, metric_handlers, default_metric_predictions
            )
        except KeyboardInterrupt:
            overall_status = "cancelled"
            metric_results.append({
                "metric_id": metric["metric_id"],
                "status": "cancelled",
                "started_at": metric_started_at.isoformat(),
                "finished_at": datetime.now(timezone.utc).isoformat(),
                "elapsed_seconds": round(time.perf_counter() - metric_start_perf, 6),
                "error": "Cancelled by user"
            })
            completed_statuses[metric["metric_id"]] = "cancelled"
            completed_durations[metric["metric_id"]] = round(time.perf_counter() - metric_start_perf, 6)
            break
        metric_elapsed_seconds = round(time.perf_counter() - metric_start_perf, 6)
        metric_finished_at = datetime.now(timezone.utc)

        metric_record = {
            "metric_id": metric["metric_id"],
            "status": "success" if success else "failed",
            "started_at": metric_started_at.isoformat(),
            "finished_at": metric_finished_at.isoformat(),
            "elapsed_seconds": metric_elapsed_seconds
        }

        if success:
            test_results.update(metric_payload.get("test_results", {}))
            if "column_validation" in metric_payload:
                column_validations[metric["metric_id"]] = metric_payload["column_validation"]
        else:
            metric_record["error"] = metric_payload.get("error", "Unknown error")
            if "column_validation" in metric_payload:
                column_validations[metric["metric_id"]] = metric_payload["column_validation"]

            if overall_status == "success":
                overall_status = "failed"

            if fail_fast:
                metric_results.append(metric_record)
                outcome = {
                    "status": "failed",
                    "case_id": case_id,
                    "plan_id": plan["plan_meta"]["plan_id"],
                    "metric_ids": [m["metric_id"] for m in metrics],
                    "dataset_path": str(dataset_path),
                    "plan_taxonomy": build_plan_taxonomy(metrics),
                    "metric_results": metric_results,
                    "test_results": test_results,
                    "test_results_taxonomy": build_test_results_taxonomy(metrics, test_results),
                    "result_taxonomy": build_result_taxonomy(metrics, metric_results, test_results),
                    "run_started_at": run_started_at.isoformat(),
                    "run_finished_at": datetime.now(timezone.utc).isoformat(),
                    "run_elapsed_seconds": round(time.perf_counter() - run_start_perf, 6)
                }
                if column_validations:
                    outcome["column_validations"] = column_validations
                with open(output_path, "w", encoding="utf-8") as f:
                    json.dump(outcome, f, indent=2)
                if sys.stdout.isatty():
                    print()
                if not live_render_enabled:
                    print("Results by taxonomy:")
                    print_taxonomy_summary(outcome["result_taxonomy"])
                return

        metric_results.append(metric_record)
        completed_statuses[metric["metric_id"]] = metric_record["status"]
        completed_durations[metric["metric_id"]] = metric_elapsed_seconds

        if shutdown_requested["requested"]:
            overall_status = "cancelled"
            break

    outcome = {
        "status": overall_status,
        "case_id": case_id,
        "plan_id": plan["plan_meta"]["plan_id"],
        "metric_ids": [m["metric_id"] for m in metrics],
        "dataset_path": str(dataset_path),
        "plan_taxonomy": build_plan_taxonomy(metrics),
        "metric_results": metric_results,
        "test_results": test_results,
        "test_results_taxonomy": build_test_results_taxonomy(metrics, test_results),
        "result_taxonomy": build_result_taxonomy(metrics, metric_results, test_results),
        "run_started_at": run_started_at.isoformat(),
        "run_finished_at": datetime.now(timezone.utc).isoformat(),
        "run_elapsed_seconds": round(time.perf_counter() - run_start_perf, 6)
    }

    if column_validations:
        outcome["column_validations"] = column_validations

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(outcome, f, indent=2)
    _print_phase_status("Completed")

    if sys.stdout.isatty():
        print()
    if not live_render_enabled:
        print("Results by taxonomy:")
        print_taxonomy_summary(outcome["result_taxonomy"])


if __name__ == "__main__":
    main()
