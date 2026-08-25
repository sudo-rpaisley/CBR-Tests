import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from runner.execution import run_metric_with_heartbeat
from runner.live_rendering import render_live_taxonomy
from runner.metric_diagnostics import display_status, extract_diagnostic, extract_result_status
from runner.progress import print_live_status, render_overall_progress_line
from runner.run_plan_helpers import build_outcome, update_live_header, write_outcome
from runner.taxonomy import print_taxonomy_summary
from runner.telemetry import RunState


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
    skipped_metrics: list[dict] | None = None,
    all_metrics: list[dict] | None = None,
    display_mode: str = "full",
    display_max_lines: int | None = None,
    run_state: RunState | None = None,
    provenance: dict | None = None,
):
    overall_status = "success"
    test_results = {}
    metric_results = []
    column_validations = {}
    total_metrics = len(metrics)

    for idx, metric in enumerate(metrics, start=1):
        metric_id = metric["metric_id"]
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
        if run_state is not None:
            run_state.mark_running(metric_id, started_at=metric_started_at)

        try:
            success, metric_payload = run_metric_with_heartbeat(
                dataset_path,
                metric,
                metrics,
                completed_statuses,
                completed_durations,
                idx,
                total_metrics,
                shutdown_requested,
                run_start_perf,
                metric_handlers,
                default_metric_predictions,
                display_mode=display_mode,
                max_lines=display_max_lines,
                run_state=run_state,
            )
        except KeyboardInterrupt:
            overall_status = "cancelled"
            elapsed = round(time.perf_counter() - metric_start_perf, 6)
            metric_results.append({
                "metric_id": metric_id,
                "status": "cancelled",
                "started_at": metric_started_at.isoformat(),
                "finished_at": datetime.now(timezone.utc).isoformat(),
                "elapsed_seconds": elapsed,
                "error": "Cancelled by user",
            })
            completed_statuses[metric_id] = "cancelled"
            completed_durations[metric_id] = elapsed
            if run_state is not None:
                run_state.mark_completed(
                    metric_id,
                    "cancelled",
                    elapsed_seconds=elapsed,
                    error="Cancelled by user",
                )
            break

        metric_payload = metric_payload if isinstance(metric_payload, dict) else {"value": metric_payload}
        metric_elapsed_seconds = round(time.perf_counter() - metric_start_perf, 6)
        metric_finished_at = datetime.now(timezone.utc)
        execution_status = "success" if success else "failed"
        result_status = extract_result_status(metric_id, metric_payload) if success else None
        diagnostic = extract_diagnostic(metric_id, success, metric_payload)
        display = display_status(metric_id, success, metric_payload)

        metric_record = {
            "metric_id": metric_id,
            "status": execution_status,
            "started_at": metric_started_at.isoformat(),
            "finished_at": metric_finished_at.isoformat(),
            "elapsed_seconds": metric_elapsed_seconds,
        }
        if result_status:
            metric_record["result_status"] = result_status
        if diagnostic:
            metric_record["diagnostic"] = diagnostic

        if success:
            test_results.update(metric_payload.get("test_results", {}))
            if "column_validation" in metric_payload:
                column_validations[metric_id] = metric_payload["column_validation"]
        else:
            metric_record["error"] = metric_payload.get("error", "Unknown error")
            if metric_payload.get("reason"):
                metric_record["reason"] = metric_payload["reason"]
            if metric_payload.get("reason_code"):
                metric_record["reason_code"] = metric_payload["reason_code"]
            if "column_validation" in metric_payload:
                column_validations[metric_id] = metric_payload["column_validation"]
            if overall_status == "success":
                overall_status = "failed"

        metric_results.append(metric_record)
        completed_statuses[metric_id] = display
        completed_durations[metric_id] = metric_elapsed_seconds
        if run_state is not None:
            run_state.mark_completed(
                metric_id,
                execution_status,
                elapsed_seconds=metric_elapsed_seconds,
                error=metric_record.get("error"),
                result_status=result_status,
                diagnostic=diagnostic,
                finished_at=metric_finished_at,
            )

        if live_render_enabled:
            print_live_status(
                render_live_taxonomy(
                    metrics,
                    metric_id,
                    completed_statuses,
                    completed_durations,
                    default_metric_predictions,
                    max(20.0, float(total_metrics)),
                    elapsed=metric_elapsed_seconds,
                    completed=True,
                    display_mode=display_mode,
                    max_lines=display_max_lines,
                    run_state=run_state,
                ),
                render_overall_progress_line(
                    idx,
                    total_metrics,
                    time.perf_counter() - run_start_perf,
                    None,
                ),
                None,
            )

        if not success and fail_fast:
            outcome = build_outcome(
                "failed",
                case_id,
                plan["plan_meta"]["plan_id"],
                metrics,
                dataset_path,
                metric_results,
                test_results,
                run_started_at,
                run_start_perf,
                column_validations,
                skipped_metrics=skipped_metrics,
                all_metrics=all_metrics,
                provenance=provenance,
            )
            write_outcome(output_path, outcome)
            if sys.stdout.isatty():
                print()
            if not live_render_enabled:
                print("Results by taxonomy:")
                print_taxonomy_summary(outcome["result_taxonomy"])
            return True, outcome

        if shutdown_requested["requested"]:
            overall_status = "cancelled"
            break

    outcome = build_outcome(
        overall_status,
        case_id,
        plan["plan_meta"]["plan_id"],
        metrics,
        dataset_path,
        metric_results,
        test_results,
        run_started_at,
        run_start_perf,
        column_validations,
        skipped_metrics=skipped_metrics,
        all_metrics=all_metrics,
        provenance=provenance,
    )
    return False, outcome
