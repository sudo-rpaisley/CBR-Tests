import json
import argparse
import os
import signal
import sys
import time
import threading
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from pathlib import Path

import pandas as pd

from runner.schema import validate_plan_schema
from runner.taxonomy import build_plan_taxonomy, build_result_taxonomy, build_test_results_taxonomy, print_taxonomy_summary
from runner.dispatch import build_metric_handlers
from runner.io import load_case_or_plan
from runner.execution import auto_worker_count, run_metric_with_heartbeat, run_metrics_parallel
from runner.order import load_taxonomy_order, order_metrics_by_taxonomy

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



def load_tabular_dataset(dataset_path: Path) -> pd.DataFrame:
    suffix = dataset_path.suffix.lower()

    if suffix == ".csv":
        df = pd.read_csv(dataset_path, skipinitialspace=True, low_memory=False)
    elif suffix == ".tsv":
        df = pd.read_csv(dataset_path, sep="\t", skipinitialspace=True, low_memory=False)
    elif suffix in [".xlsx", ".xls"]:
        df = pd.read_excel(dataset_path)
    else:
        raise ValueError(f"Unsupported tabular dataset format: {suffix}")

    df.columns = df.columns.str.strip()
    return df


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
    live_render_enabled = sys.stdout.isatty() and os.environ.get("TERM", "").lower() not in {"", "dumb"}
    default_metric_predictions = dict(DEFAULT_METRIC_PREDICTIONS)

    def _handle_sigint(_signum, _frame):
        shutdown_requested["requested"] = True
        shutdown_requested["confirm_before"] = time.time()
        print("\nStop requested. Cancelling current task and pending tasks...")
        raise KeyboardInterrupt

    signal.signal(signal.SIGINT, _handle_sigint)

    case_file = Path(args.case).resolve()
    plan, dataset_path, output_path, case_id = load_case_or_plan(case_file, args.dataset, args.output, args.case_id)


    metrics = [m for m in plan.get("metrics", []) if m.get("enabled", True)]
    if args.taxonomy_file:
        taxonomy_ranks = load_taxonomy_order(Path(args.taxonomy_file).expanduser().resolve())
        metrics = order_metrics_by_taxonomy(metrics, taxonomy_ranks, strict=args.taxonomy_strict)

    def _print_startup_banner():
        dataset_name = dataset_path.name
        dataset_size = dataset_path.stat().st_size if dataset_path.exists() else 0
        dataset_size_mb = round(dataset_size / (1024 * 1024), 2)
        print("=" * 88)
        print(f"Starting run: case_id={case_id} | plan_id={plan['plan_meta']['plan_id']}")
        print(f"Dataset: {dataset_name} | Size: {dataset_size_mb} MB | Path: {dataset_path}")
        print("=" * 88)

    def _load_with_progress(path: Path):
        stop = {"done": False}

        def _spinner():
            frames = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]
            idx = 0
            start = time.perf_counter()
            while not stop["done"]:
                elapsed = time.perf_counter() - start
                print(f"\r{frames[idx % len(frames)]} Loading dataset... {elapsed:0.1f}s", end="", flush=True)
                idx += 1
                time.sleep(0.1)

        spinner_thread = threading.Thread(target=_spinner, daemon=True)
        spinner_thread.start()
        try:
            df = load_tabular_dataset(path)
            return df
        finally:
            stop["done"] = True
            spinner_thread.join(timeout=0.3)
            print("\r✓ Dataset loaded.                              ")

    _print_startup_banner()
    shared_tabular_df = None
    if dataset_path.suffix.lower() in {".csv", ".tsv", ".xlsx", ".xls"}:
        shared_tabular_df = _load_with_progress(dataset_path)
        print(
            f"Dataset details: rows={len(shared_tabular_df):,} | columns={shared_tabular_df.shape[1]} | "
            f"feature_sample={list(shared_tabular_df.columns[:8])}"
        )
    metric_handlers = build_metric_handlers(shared_tabular_df, load_tabular_dataset)

    if not metrics:
        raise ValueError("The plan does not contain any enabled metrics.")

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
    if workers > 1:
        parallel_out = run_metrics_parallel(dataset_path, metrics, metric_handlers, workers)
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
        print(f"Done. Wrote {output_path}")
        return
    for idx, metric in enumerate(metrics, start=1):
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

    if sys.stdout.isatty():
        print()
    if not live_render_enabled:
        print("Results by taxonomy:")
        print_taxonomy_summary(outcome["result_taxonomy"])
    print(f"Done. Wrote {output_path}")


if __name__ == "__main__":
    main()
