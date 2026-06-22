import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from runner.execution import run_metric_with_heartbeat
from runner.run_plan_helpers import build_outcome, write_outcome, update_live_header
from runner.taxonomy import print_taxonomy_summary


def run_serial_metrics(
    *,
    dataset_path: Path,
    output_path: Path,
    plan: dict,
    case_id: str,
    metrics: list[dict],
    metric_handlers: dict,
    shutdown_requested: dict,
    control_state: dict,
    default_metric_predictions: dict,
    live_render_enabled: bool,
    fail_fast: bool,
    run_started_at: datetime,
    run_start_perf: float,
    completed_statuses: dict,
    completed_durations: dict,
):
    overall_status = "success"
    test_results = {}
    metric_results = []
    column_validations = {}
    total_metrics = len(metrics)

    for idx, metric in enumerate(metrics, start=1):
        while control_state.get("pause_requested") and not control_state.get("cancel_requested"):
            update_live_header([
                f"Run Title: {plan['plan_meta']['name']} ({plan['plan_meta']['plan_id']})",
                f"Case ID: {case_id}",
                f"Source Path: {dataset_path}",
                f"Destination Output: {output_path}",
            ], [
                "Status: Paused",
                f"Overall Progress: {idx-1}/{total_metrics} metrics completed",
                "Send SIGUSR2 to resume or Ctrl-C to cancel",
            ])
            time.sleep(0.2)
        metric_started_at = datetime.now(timezone.utc)
        metric_start_perf = time.perf_counter()
        try:
            success, metric_payload = run_metric_with_heartbeat(
                dataset_path, metric, metrics, completed_statuses, completed_durations, idx, total_metrics,
                shutdown_requested, run_start_perf, metric_handlers, default_metric_predictions
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
                outcome = build_outcome(
                    "failed", case_id, plan["plan_meta"]["plan_id"], metrics, dataset_path,
                    metric_results, test_results, run_started_at, run_start_perf, column_validations
                )
                write_outcome(output_path, outcome)
                if sys.stdout.isatty():
                    print()
                if not live_render_enabled:
                    print("Results by taxonomy:")
                    print_taxonomy_summary(outcome["result_taxonomy"])
                return True, outcome

        metric_results.append(metric_record)
        completed_statuses[metric["metric_id"]] = metric_record["status"]
        completed_durations[metric["metric_id"]] = metric_elapsed_seconds

        if shutdown_requested["requested"]:
            overall_status = "cancelled"
            break

    outcome = build_outcome(
        overall_status, case_id, plan["plan_meta"]["plan_id"], metrics, dataset_path,
        metric_results, test_results, run_started_at, run_start_perf, column_validations
    )
    return False, outcome
