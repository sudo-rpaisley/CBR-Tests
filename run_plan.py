import json
import argparse
import os
import signal
import sys
import time
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from pathlib import Path

import pandas as pd

from runner.schema import validate_plan_schema
from runner.taxonomy import build_plan_taxonomy, build_result_taxonomy, print_taxonomy_summary
from runner.dispatch import build_metric_handlers
from runner.progress import render_metric_activity_bar, render_overall_progress_line, print_live_status



def resolve_path(base_dir: Path, path_str: str) -> Path:
    path = Path(path_str)
    if path.is_absolute():
        return path
    return (base_dir / path).resolve()


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


def _render_live_taxonomy(
    metrics: list[dict],
    current_metric_id: str,
    completed_statuses: dict[str, str],
    completed_durations: dict[str, float],
    predicted_metric_total: float,
    elapsed: float | None = None,
    completed: bool = False
) -> str:
    lines: list[str] = []
    printed_nodes: set[tuple[str, ...]] = set()
    predicted_metric_total = max(1.0, predicted_metric_total)
    if elapsed is not None:
        predicted_metric_total = max(predicted_metric_total, elapsed)
    for metric in metrics:
        path = metric.get("taxonomy_path", [])
        for depth in range(len(path)):
            node_tuple = tuple(path[: depth + 1])
            if node_tuple in printed_nodes:
                continue
            printed_nodes.add(node_tuple)
            lines.append(f"{'  ' * depth}↳ {path[depth]}")
        metric_id = metric.get("metric_id", "unknown_metric")
        metric_prediction = completed_durations.get(metric_id, DEFAULT_METRIC_PREDICTIONS.get(metric_id, predicted_metric_total))
        if metric_id in completed_statuses:
            run_time = completed_durations.get(metric_id)
            if run_time is not None:
                suffix = f" [{completed_statuses[metric_id]} | run time {run_time:.1f}s]"
            else:
                suffix = f" [{completed_statuses[metric_id]}]"
        elif metric_id == current_metric_id:
            if completed:
                suffix = f" [success] | done in {elapsed:.1f}s"
            elif elapsed is not None:
                suffix = (
                    f" [running | {elapsed:.1f}/{predicted_metric_total:.0f}s ] "
                    f"[{render_metric_activity_bar(elapsed, expected_seconds=predicted_metric_total)}]"
                )
            else:
                suffix = " [running]"
        else:
            suffix = f" [pending | 0.0/{metric_prediction:.0f}s]"
        lines.append(f"{'  ' * len(path)}↳ {metric_id}{suffix}")
    return "\n".join(lines)


def _run_metric_with_heartbeat(
    dataset_path: Path,
    metric: dict,
    metrics: list[dict],
    completed_statuses: dict[str, str],
    completed_durations: dict[str, float],
    current: int,
    total: int,
    shutdown_requested: dict,
    shared_df: pd.DataFrame | None = None,
    run_start_perf: float | None = None,
    metric_handlers: dict | None = None
) -> tuple[bool, dict]:
    metric_id = metric.get("metric_id", "unknown_metric")
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(dispatch_metric_with_handlers, dataset_path, metric, metric_handlers or {})
        heartbeat_start = time.perf_counter()
        smoothed_total = 20.0
        while True:
            try:
                result = future.result(timeout=1.0)
                elapsed = time.perf_counter() - heartbeat_start
                instant_total = max(elapsed + 1.0, elapsed * 1.2, 20.0)
                smoothed_total = max(elapsed, 0.7 * smoothed_total + 0.3 * instant_total)
                task_line = _render_live_taxonomy(metrics, metric_id, completed_statuses, completed_durations, smoothed_total, elapsed, completed=True)
                run_elapsed = (time.perf_counter() - run_start_perf) if run_start_perf is not None else None
                overall_line = render_overall_progress_line(current, total, run_elapsed, elapsed)
                print_live_status(task_line, overall_line, None)
                if sys.stdout.isatty():
                    print("\n", end="")
                return result
            except TimeoutError:
                elapsed = time.perf_counter() - heartbeat_start
                instant_total = max(elapsed + 1.0, elapsed * 1.2, 20.0)
                smoothed_total = max(elapsed, 0.7 * smoothed_total + 0.3 * instant_total)
                task_line = _render_live_taxonomy(metrics, metric_id, completed_statuses, completed_durations, smoothed_total, elapsed)
                run_elapsed = (time.perf_counter() - run_start_perf) if run_start_perf is not None else None
                overall_line = render_overall_progress_line(max(0, current - 1), total, run_elapsed, elapsed)
                warning_line = None
                if shutdown_requested.get("requested"):
                    remaining = int(max(0, shutdown_requested.get("confirm_before", 0.0) - time.time()))
                    if remaining > 0:
                        warning_line = (
                            f"Stop requested. Press Ctrl+C again within {remaining}s to force quit. "
                            "Waiting for current metric to finish..."
                        )
                print_live_status(task_line, overall_line, warning_line)




def _append_timing_history(history_path: Path, run_entry: dict) -> None:
    history_path.parent.mkdir(parents=True, exist_ok=True)
    with open(history_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(run_entry) + "\n")


def main():
    parser = argparse.ArgumentParser(
        description="Run a test plan from a case JSON."
    )
    parser.add_argument("--case", required=True, help="Path to case JSON or plan JSON file")
    parser.add_argument("--dataset", help="Dataset path (required when --case points to a plan JSON)")
    parser.add_argument("--output", help="Output path (required when --case points to a plan JSON)")
    parser.add_argument("--case-id", default="ad_hoc_case", help="Case ID used when running a plan JSON directly")
    parser.add_argument("--timing-history", help="Optional JSONL file to append run/metric timing history")
    args = parser.parse_args()

    shutdown_requested = {"requested": False, "confirm_before": 0.0}
    live_render_enabled = sys.stdout.isatty() and os.environ.get("TERM", "").lower() not in {"", "dumb"}

    def _handle_sigint(_signum, _frame):
        shutdown_requested["requested"] = True
        shutdown_requested["confirm_before"] = time.time()
        print("\nStop requested. Cancelling current task and pending tasks...")
        raise KeyboardInterrupt

    signal.signal(signal.SIGINT, _handle_sigint)

    case_file = Path(args.case).resolve()
    case_dir = case_file.parent

    with open(case_file, "r", encoding="utf-8") as f:
        case = json.load(f)

    if "test_plan" in case and "dataset" in case and "output" in case:
        plan_path = resolve_path(case_dir, case["test_plan"]["path"])
        dataset_path = resolve_path(case_dir, case["dataset"]["path"])
        output_path = resolve_path(case_dir, case["output"]["path"])
        case_id = case.get("case_id", "unknown_case")
        with open(plan_path, "r", encoding="utf-8") as f:
            plan = json.load(f)
    elif "metrics" in case and "plan_meta" in case:
        if not args.dataset or not args.output:
            raise ValueError(
                "When --case points to a plan JSON, you must also provide --dataset and --output."
            )
        plan = case
        plan_path = case_file
        dataset_path = Path(args.dataset).expanduser().resolve()
        output_path = Path(args.output).expanduser().resolve()
        case_id = args.case_id
    else:
        raise ValueError("Invalid input JSON: provide a case JSON, or a plan JSON with --dataset and --output.")


    validate_plan_schema(plan)
    if args.timing_history:
        timing_history_path = Path(args.timing_history).expanduser().resolve()
    else:
        timing_history_path = output_path.parent / "timing_history.jsonl"

    metrics = [m for m in plan.get("metrics", []) if m.get("enabled", True)]

    shared_tabular_df = None
    if dataset_path.suffix.lower() in {".csv", ".tsv", ".xlsx", ".xls"}:
        shared_tabular_df = load_tabular_dataset(dataset_path)
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
    for idx, metric in enumerate(metrics, start=1):
        metric_started_at = datetime.now(timezone.utc)
        metric_start_perf = time.perf_counter()
        try:
            success, metric_payload = _run_metric_with_heartbeat(
                dataset_path, metric, metrics, completed_statuses, completed_durations, idx, total_metrics, shutdown_requested, shared_tabular_df, run_start_perf, metric_handlers
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
                    "result_taxonomy": build_result_taxonomy(metrics, metric_results, test_results),
                    "run_started_at": run_started_at.isoformat(),
                    "run_finished_at": datetime.now(timezone.utc).isoformat(),
                    "run_elapsed_seconds": round(time.perf_counter() - run_start_perf, 6)
                }
                if column_validations:
                    outcome["column_validations"] = column_validations
                with open(output_path, "w", encoding="utf-8") as f:
                    json.dump(outcome, f, indent=2)
                _append_timing_history(timing_history_path, {
                    "run_started_at": outcome["run_started_at"],
                    "run_finished_at": outcome["run_finished_at"],
                    "run_elapsed_seconds": outcome["run_elapsed_seconds"],
                    "case_id": outcome["case_id"],
                    "plan_id": outcome["plan_id"],
                    "status": outcome["status"],
                    "metric_results": outcome["metric_results"]
                })
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
        "result_taxonomy": build_result_taxonomy(metrics, metric_results, test_results),
        "run_started_at": run_started_at.isoformat(),
        "run_finished_at": datetime.now(timezone.utc).isoformat(),
        "run_elapsed_seconds": round(time.perf_counter() - run_start_perf, 6)
    }

    if column_validations:
        outcome["column_validations"] = column_validations

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(outcome, f, indent=2)

    _append_timing_history(timing_history_path, {
        "run_started_at": outcome["run_started_at"],
        "run_finished_at": outcome["run_finished_at"],
        "run_elapsed_seconds": outcome["run_elapsed_seconds"],
        "case_id": outcome["case_id"],
        "plan_id": outcome["plan_id"],
        "status": outcome["status"],
        "metric_results": outcome["metric_results"]
    })
    if sys.stdout.isatty():
        print()
    if not live_render_enabled:
        print("Results by taxonomy:")
        print_taxonomy_summary(outcome["result_taxonomy"])
    print(f"Done. Wrote {output_path}")
    print(f"Timing history appended to {timing_history_path}")


if __name__ == "__main__":
    main()
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
