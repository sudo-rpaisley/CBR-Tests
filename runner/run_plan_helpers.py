import argparse
import json
import signal
import time
from datetime import datetime, timezone
from pathlib import Path

from runner.taxonomy import build_plan_taxonomy, build_result_taxonomy, build_test_results_taxonomy

SOURCE_FIELD_CANDIDATES = ("Source IP", "Src IP", "source_ip", "src_ip")
DESTINATION_FIELD_CANDIDATES = ("Destination IP", "Dst IP", "destination_ip", "dst_ip")


def build_outcome(
    status: str,
    case_id: str,
    plan_id: str,
    metrics: list[dict],
    dataset_path: Path,
    metric_results: list[dict],
    test_results: dict,
    run_started_at: datetime,
    run_start_perf: float,
    column_validations: dict,
) -> dict:
    outcome = {
        "status": status,
        "case_id": case_id,
        "plan_id": plan_id,
        "metric_ids": [m["metric_id"] for m in metrics],
        "dataset_path": str(dataset_path),
        "plan_taxonomy": build_plan_taxonomy(metrics),
        "metric_results": metric_results,
        "test_results": test_results,
        "test_results_taxonomy": build_test_results_taxonomy(metrics, test_results),
        "result_taxonomy": build_result_taxonomy(metrics, metric_results, test_results),
        "run_started_at": run_started_at.isoformat(),
        "run_finished_at": datetime.now(timezone.utc).isoformat(),
        "run_elapsed_seconds": round(time.perf_counter() - run_start_perf, 6),
    }
    if column_validations:
        outcome["column_validations"] = column_validations
    return outcome


def write_outcome(output_path: Path, outcome: dict) -> None:
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(outcome, f, indent=2)


def detect_ip_fields(tabular_df) -> tuple[str, str]:
    source_field = next((c for c in SOURCE_FIELD_CANDIDATES if c in tabular_df.columns), "n/a")
    destination_field = next((c for c in DESTINATION_FIELD_CANDIDATES if c in tabular_df.columns), "n/a")
    return source_field, destination_field


def build_title_box_lines(lines: list[str], status_lines: list[str] | None = None, width: int = 108) -> list[str]:
    framed = ["=" * width]
    for line in lines:
        framed.append(f"| {line[:width-4].ljust(width-4)} |")
    if status_lines:
        framed.append(f"| {'-' * (width-4)} |")
        for status_line in status_lines:
            framed.append(f"| {status_line[:width-4].ljust(width-4)} |")
    framed.append("=" * width)
    return framed


def build_base_header_lines(
    plan: dict,
    case_id: str,
    dataset_path: Path,
    output_path: Path,
    include_dataset_size: bool = False,
) -> list[str]:
    lines = [
        f"Run Title: {plan['plan_meta']['name']} ({plan['plan_meta']['plan_id']})",
        f"Case ID: {case_id}",
    ]
    if include_dataset_size:
        dataset_name = dataset_path.name
        dataset_size = dataset_path.stat().st_size if dataset_path.exists() else 0
        dataset_size_mb = round(dataset_size / (1024 * 1024), 2)
        lines.append(f"Source Dataset: {dataset_name} ({dataset_size_mb} MB)")
    lines.extend([
        f"Source Path: {dataset_path}",
        f"Destination Output: {output_path}",
    ])
    return lines


def configure_signal_handlers(control_state: dict, shutdown_requested: dict) -> None:
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


def parse_run_plan_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a test plan from a case JSON.")
    parser.add_argument("--case", required=True, help="Path to case JSON or plan JSON file")
    parser.add_argument("--dataset", help="Dataset path (required when --case points to a plan JSON)")
    parser.add_argument("--output", help="Output path (required when --case points to a plan JSON)")
    parser.add_argument("--case-id", default="ad_hoc_case", help="Case ID used when running a plan JSON directly")
    parser.add_argument("--taxonomy-file", help="Optional taxonomy JSON used to order metrics")
    parser.add_argument(
        "--taxonomy-strict",
        action="store_true",
        help="Fail if enabled plan metrics are missing from taxonomy order",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help="Optional worker count override. Use 1 to force serial execution.",
    )
    return parser.parse_args()
