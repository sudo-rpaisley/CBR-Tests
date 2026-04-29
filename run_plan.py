import json
import argparse
import signal
import sys
import time
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from pathlib import Path

import pandas as pd

from tests.pearson_profile import validate_candidate_fields, compute_pearson_profile
from tests.column_quality_profile import compute_column_quality_profile
from tests.timestamp_coherence_profile import run_timestamp_coherence_metric
from tests.metrics.dataset_heuristics.protocol_and_network_realism.address_validity.valid_ip_address_profile import run_protocol_validity_metric
from tests.metrics.dataset_heuristics.protocol_and_network_realism.address_validity.reserved_ip_address_profile import run_reserved_ip_address_metric
from tests.metrics.dataset_heuristics.protocol_and_network_realism.port_validity.valid_port_range_profile import run_valid_port_range_metric
from tests.metrics.dataset_heuristics.protocol_and_network_realism.port_validity.service_port_consistency_profile import run_service_port_consistency_metric
from tests.metrics.dataset_heuristics.protocol_and_network_realism.flow_semantics.packet_byte_consistency_profile import run_packet_byte_consistency_metric
from tests.metrics.dataset_heuristics.protocol_and_network_realism.flow_semantics.flow_duration_consistency_profile import run_flow_duration_consistency_metric
from tests.metrics.dataset_heuristics.protocol_and_network_realism.flow_semantics.handshake_plausibility_profile import run_handshake_plausibility_metric
from tests.metrics.dataset_heuristics.protocol_and_network_realism.flow_semantics.tcp_flag_consistency_profile import run_tcp_flag_consistency_metric
from tests.metrics.dataset_heuristics.protocol_and_network_realism.slice_metadata_integrity.slice_identifier_consistency_profile import run_slice_identifier_consistency_metric
from tests.metrics.dataset_heuristics.protocol_and_network_realism.slice_metadata_integrity.valid_slice_identifier_profile import run_valid_slice_identifier_metric


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


def run_pearson_metric(dataset_path: Path, metric: dict, shared_df: pd.DataFrame | None = None) -> tuple[bool, dict]:
    df = shared_df.copy() if shared_df is not None else load_tabular_dataset(dataset_path)

    candidate_fields = metric["input_requirements"]["candidate_fields"]
    minimum_runnable_fields = metric["input_requirements"]["minimum_runnable_fields"]

    column_validation, runnable_fields, df = validate_candidate_fields(df, candidate_fields)

    if len(runnable_fields) < minimum_runnable_fields:
        return False, {
            "column_validation": column_validation,
            "error": "Not enough usable numeric columns to compute Pearson correlation."
        }

    pearson_profile = compute_pearson_profile(df, runnable_fields)

    return True, {
        "column_validation": column_validation,
        "test_results": {
            "pearson_correlation_profile": pearson_profile
        }
    }


def run_column_quality_metric(dataset_path: Path, metric: dict, shared_df: pd.DataFrame | None = None) -> tuple[bool, dict]:
    df = shared_df.copy() if shared_df is not None else load_tabular_dataset(dataset_path)
    candidate_fields = metric["input_requirements"]["candidate_fields"]
    quality_profile = compute_column_quality_profile(df, candidate_fields)

    return True, {
        "test_results": {
            "column_quality_profile": quality_profile
        }
    }


def dispatch_metric(dataset_path: Path, metric: dict, shared_df: pd.DataFrame | None = None) -> tuple[bool, dict]:
    metric_id = metric["metric_id"]

    metric_handlers = {
        "pearson_correlation_profile": lambda dp, m: run_pearson_metric(dp, m, shared_df),
        "column_quality_profile": lambda dp, m: run_column_quality_metric(dp, m, shared_df),
        "timestamp_coherence_profile": run_timestamp_coherence_metric,
        "protocol_validity_profile": run_protocol_validity_metric,
        "reserved_ip_address_profile": run_reserved_ip_address_metric,
        "valid_port_range_profile": run_valid_port_range_metric,
        "service_port_consistency_profile": run_service_port_consistency_metric,
        "tcp_flag_consistency_profile": run_tcp_flag_consistency_metric,
        "handshake_plausibility_profile": run_handshake_plausibility_metric,
        "flow_duration_consistency_profile": run_flow_duration_consistency_metric,
        "packet_byte_consistency_profile": run_packet_byte_consistency_metric,
        "valid_slice_identifier_profile": run_valid_slice_identifier_metric,
        "slice_identifier_consistency_profile": run_slice_identifier_consistency_metric,
    }

    handler = metric_handlers.get(metric_id)
    if handler is None:
        raise ValueError(f"Unsupported metric_id: {metric_id}")
    return handler(dataset_path, metric)






def _render_overall_progress_line(current: int, total: int, run_elapsed: float | None = None, in_metric_elapsed: float | None = None) -> str:
    total = max(total, 1)
    width = 30
    filled = int(width * current / total)
    bar = "#" * filled + "-" * (width - filled)
    pct = int((current / total) * 100)
    suffix = ""
    if run_elapsed is not None:
        metric_fraction = 0.0
        if in_metric_elapsed is not None:
            metric_fraction = min(in_metric_elapsed / 60.0, 0.99)
        progress_fraction = min(((current - 1) + metric_fraction) / total, 0.999)
        if progress_fraction > 0:
            predicted_total = run_elapsed / progress_fraction
            suffix = f" | {int(run_elapsed)}/{int(predicted_total)}s"
    return f"Overall  [{bar}] {pct:3d}% ({current}/{total}){suffix}"


def _render_live_taxonomy(metrics: list[dict], current_metric_id: str, completed_statuses: dict[str, str], elapsed: float | None = None, completed: bool = False) -> str:
    lines: list[str] = []
    printed_nodes: set[tuple[str, ...]] = set()
    for metric in metrics:
        path = metric.get("taxonomy_path", [])
        for depth in range(len(path)):
            node_tuple = tuple(path[: depth + 1])
            if node_tuple in printed_nodes:
                continue
            printed_nodes.add(node_tuple)
            lines.append(f"{'  ' * depth}↳ {path[depth]}")
        metric_id = metric.get("metric_id", "unknown_metric")
        if metric_id in completed_statuses:
            suffix = f" [{completed_statuses[metric_id]}]"
        elif metric_id == current_metric_id:
            if completed:
                suffix = f" [success] | done in {elapsed:.1f}s"
            elif elapsed is not None:
                suffix = f" [running] {elapsed:.1f}s [{_render_metric_activity_bar(elapsed)}]"
            else:
                suffix = " [running]"
        else:
            suffix = " [pending]"
        lines.append(f"{'  ' * len(path)}↳ {metric_id}{suffix}")
    return "\n".join(lines)


def _print_live_status(task_line: str, overall_line: str, warning_line: str | None = None) -> None:
    if not sys.stdout.isatty():
        if warning_line is not None:
            print(f"{overall_line} | {warning_line}")
        return

    block_lines = task_line.splitlines() + [overall_line]
    if warning_line is not None:
        block_lines.append(warning_line)
    for idx, line in enumerate(block_lines):
        prefix = "\r" if idx == 0 else "\n"
        print(f"{prefix}\x1b[2K{line}", end="")
    if len(block_lines) > 1:
        print(f"\x1b[{len(block_lines)-1}A", end="", flush=True)
    else:
        print("", end="", flush=True)


def _print_taxonomy_summary(result_taxonomy: dict, indent: int = 0) -> None:
    for key, value in result_taxonomy.items():
        if key == "_metrics":
            for metric in value:
                status = metric.get("status", "unknown")
                print(f"{'  ' * indent}↳ {metric.get('metric_id')} [{status}]")
            continue
        if isinstance(value, dict) and set(value.keys()) == {"_metrics"}:
            for metric in value["_metrics"]:
                status = metric.get("status", "unknown")
                print(f"{'  ' * indent}↳ {metric.get('metric_id')} [{status}]")
            continue
        print(f"{'  ' * indent}↳ {key}")
        _print_taxonomy_summary(value, indent + 1)


def _run_metric_with_heartbeat(
    dataset_path: Path,
    metric: dict,
    metrics: list[dict],
    completed_statuses: dict[str, str],
    current: int,
    total: int,
    shutdown_requested: dict,
    shared_df: pd.DataFrame | None = None,
    run_start_perf: float | None = None
) -> tuple[bool, dict]:
    metric_id = metric.get("metric_id", "unknown_metric")
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(dispatch_metric, dataset_path, metric, shared_df)
        heartbeat_start = time.perf_counter()
        while True:
            try:
                result = future.result(timeout=1.0)
                elapsed = time.perf_counter() - heartbeat_start
                task_line = _render_live_taxonomy(metrics, metric_id, completed_statuses, elapsed, completed=True)
                run_elapsed = (time.perf_counter() - run_start_perf) if run_start_perf is not None else None
                overall_line = _render_overall_progress_line(current, total, run_elapsed, elapsed)
                _print_live_status(task_line, overall_line, None)
                if sys.stdout.isatty():
                    print("\n", end="")
                return result
            except TimeoutError:
                elapsed = time.perf_counter() - heartbeat_start
                task_line = _render_live_taxonomy(metrics, metric_id, completed_statuses, elapsed)
                run_elapsed = (time.perf_counter() - run_start_perf) if run_start_perf is not None else None
                overall_line = _render_overall_progress_line(current, total, run_elapsed, elapsed)
                warning_line = None
                if shutdown_requested.get("requested"):
                    remaining = int(max(0, shutdown_requested.get("confirm_before", 0.0) - time.time()))
                    if remaining > 0:
                        warning_line = (
                            f"Stop requested. Press Ctrl+C again within {remaining}s to force quit. "
                            "Waiting for current metric to finish..."
                        )
                _print_live_status(task_line, overall_line, warning_line)




def _append_timing_history(history_path: Path, run_entry: dict) -> None:
    history_path.parent.mkdir(parents=True, exist_ok=True)
    with open(history_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(run_entry) + "\n")


def _ensure_taxonomy_path(root: dict, taxonomy_path: list[str]) -> dict:
    node = root
    for segment in taxonomy_path:
        node = node.setdefault(segment, {})
    return node


def _build_plan_taxonomy(metrics: list[dict]) -> dict:
    taxonomy: dict = {}
    for metric in metrics:
        node = _ensure_taxonomy_path(taxonomy, metric.get("taxonomy_path", []))
        node.setdefault("_metrics", []).append({
            "metric_id": metric.get("metric_id"),
            "label": metric.get("label"),
            "enabled": metric.get("enabled", True),
        })
    return taxonomy


def _build_result_taxonomy(metrics: list[dict], metric_results: list[dict], test_results: dict) -> dict:
    by_metric_id = {record["metric_id"]: record for record in metric_results}
    taxonomy: dict = {}
    for metric in metrics:
        metric_id = metric.get("metric_id")
        node = _ensure_taxonomy_path(taxonomy, metric.get("taxonomy_path", []))
        node.setdefault("_metrics", []).append({
            "metric_id": metric_id,
            "status": by_metric_id.get(metric_id, {}).get("status", "skipped"),
            "result": test_results.get(metric_id),
            "error": by_metric_id.get(metric_id, {}).get("error")
        })
    return taxonomy


def _render_metric_activity_bar(elapsed: float, expected_seconds: float = 60.0, width: int = 12) -> str:
    if width < 3:
        width = 3
    if expected_seconds <= 0:
        expected_seconds = 60.0
    progress = min(elapsed / expected_seconds, 0.99)
    filled = max(1, int(progress * width))
    return "#" * filled + "-" * (width - filled)


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

    if args.timing_history:
        timing_history_path = Path(args.timing_history).expanduser().resolve()
    else:
        timing_history_path = output_path.parent / "timing_history.jsonl"

    metrics = [m for m in plan.get("metrics", []) if m.get("enabled", True)]

    shared_tabular_df = None
    if dataset_path.suffix.lower() in {".csv", ".tsv", ".xlsx", ".xls"}:
        shared_tabular_df = load_tabular_dataset(dataset_path)
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
    for idx, metric in enumerate(metrics, start=1):
        metric_started_at = datetime.now(timezone.utc)
        metric_start_perf = time.perf_counter()
        try:
            success, metric_payload = _run_metric_with_heartbeat(
                dataset_path, metric, metrics, completed_statuses, idx, total_metrics, shutdown_requested, shared_tabular_df, run_start_perf
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
                    "plan_taxonomy": _build_plan_taxonomy(metrics),
                    "metric_results": metric_results,
                    "test_results": test_results,
                    "result_taxonomy": _build_result_taxonomy(metrics, metric_results, test_results),
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
                print("Results by taxonomy:")
                _print_taxonomy_summary(outcome["result_taxonomy"])
                return

        metric_results.append(metric_record)
        completed_statuses[metric["metric_id"]] = metric_record["status"]

        if shutdown_requested["requested"]:
            overall_status = "cancelled"
            break

    outcome = {
        "status": overall_status,
        "case_id": case_id,
        "plan_id": plan["plan_meta"]["plan_id"],
        "metric_ids": [m["metric_id"] for m in metrics],
        "dataset_path": str(dataset_path),
        "plan_taxonomy": _build_plan_taxonomy(metrics),
        "metric_results": metric_results,
        "test_results": test_results,
        "result_taxonomy": _build_result_taxonomy(metrics, metric_results, test_results),
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
    print("Results by taxonomy:")
    _print_taxonomy_summary(outcome["result_taxonomy"])
    print(f"Done. Wrote {output_path}")
    print(f"Timing history appended to {timing_history_path}")


if __name__ == "__main__":
    main()
