import json
import argparse
import signal
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


def run_pearson_metric(dataset_path: Path, metric: dict) -> tuple[bool, dict]:
    df = load_tabular_dataset(dataset_path)

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


def run_column_quality_metric(dataset_path: Path, metric: dict) -> tuple[bool, dict]:
    df = load_tabular_dataset(dataset_path)
    candidate_fields = metric["input_requirements"]["candidate_fields"]
    quality_profile = compute_column_quality_profile(df, candidate_fields)

    return True, {
        "test_results": {
            "column_quality_profile": quality_profile
        }
    }


def dispatch_metric(dataset_path: Path, metric: dict) -> tuple[bool, dict]:
    metric_id = metric["metric_id"]

    if metric_id == "pearson_correlation_profile":
        return run_pearson_metric(dataset_path, metric)

    if metric_id == "column_quality_profile":
        return run_column_quality_metric(dataset_path, metric)

    if metric_id == "timestamp_coherence_profile":
        return run_timestamp_coherence_metric(dataset_path, metric)

    if metric_id == "protocol_validity_profile":
        return run_protocol_validity_metric(dataset_path, metric)

    if metric_id == "reserved_ip_address_profile":
        return run_reserved_ip_address_metric(dataset_path, metric)

    if metric_id == "valid_port_range_profile":
        return run_valid_port_range_metric(dataset_path, metric)

    if metric_id == "service_port_consistency_profile":
        return run_service_port_consistency_metric(dataset_path, metric)

    if metric_id == "tcp_flag_consistency_profile":
        return run_tcp_flag_consistency_metric(dataset_path, metric)

    if metric_id == "handshake_plausibility_profile":
        return run_handshake_plausibility_metric(dataset_path, metric)

    if metric_id == "flow_duration_consistency_profile":
        return run_flow_duration_consistency_metric(dataset_path, metric)

    if metric_id == "packet_byte_consistency_profile":
        return run_packet_byte_consistency_metric(dataset_path, metric)

    if metric_id == "valid_slice_identifier_profile":
        return run_valid_slice_identifier_metric(dataset_path, metric)

    if metric_id == "slice_identifier_consistency_profile":
        return run_slice_identifier_consistency_metric(dataset_path, metric)

    raise ValueError(f"Unsupported metric_id: {metric_id}")




def _render_progress_line(current: int, total: int, metric_id: str, elapsed: float | None = None) -> str:
    total = max(total, 1)
    width = 30
    filled = int(width * current / total)
    bar = "#" * filled + "-" * (width - filled)
    pct = int((current / total) * 100)
    suffix = f" | running {elapsed:.1f}s" if elapsed is not None else ""
    return f"Progress [{bar}] {pct:3d}% ({current}/{total}) - {metric_id}{suffix}"


def _run_metric_with_heartbeat(dataset_path: Path, metric: dict, current: int, total: int) -> tuple[bool, dict]:
    metric_id = metric.get("metric_id", "unknown_metric")
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(dispatch_metric, dataset_path, metric)
        heartbeat_start = time.perf_counter()
        while True:
            try:
                result = future.result(timeout=1.0)
                line = _render_progress_line(current, total, metric_id)
                print(f"\r\x1b[2K{line}", end="", flush=True)
                print()
                return result
            except TimeoutError:
                elapsed = time.perf_counter() - heartbeat_start
                line = _render_progress_line(current, total, metric_id, elapsed)
                print(f"\r\x1b[2K{line}", end="", flush=True)




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

    def _handle_sigint(_signum, _frame):
        now = time.time()
        if shutdown_requested["requested"] and now <= shutdown_requested["confirm_before"]:
            print("\nForced stop confirmed. Exiting immediately.")
            raise KeyboardInterrupt
        shutdown_requested["requested"] = True
        shutdown_requested["confirm_before"] = now + 5.0
        print("\nStop requested. Press Ctrl+C again within 5 seconds to force quit, or wait for current metric to finish.")

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
    for idx, metric in enumerate(metrics, start=1):
        metric_started_at = datetime.now(timezone.utc)
        metric_start_perf = time.perf_counter()
        success, metric_payload = _run_metric_with_heartbeat(dataset_path, metric, idx, total_metrics)
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
                    "metric_results": metric_results,
                    "test_results": test_results,
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
                return

        metric_results.append(metric_record)

        if shutdown_requested["requested"]:
            overall_status = "cancelled"
            break

    outcome = {
        "status": overall_status,
        "case_id": case_id,
        "plan_id": plan["plan_meta"]["plan_id"],
        "metric_ids": [m["metric_id"] for m in metrics],
        "dataset_path": str(dataset_path),
        "metric_results": metric_results,
        "test_results": test_results,
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
    print(f"Done. Wrote {output_path}")
    print(f"Timing history appended to {timing_history_path}")


if __name__ == "__main__":
    main()
