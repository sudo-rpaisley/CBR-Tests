from __future__ import annotations

from datetime import datetime, timezone


def collect_parallel_metric_results(
    *,
    parallel_out,
    metrics: list[dict],
    run_started_at: datetime,
    fail_fast: bool,
    completed_statuses: dict[str, str],
    completed_durations: dict[str, float],
) -> tuple[str, dict, list[dict], dict]:
    """Collect run outputs from completed parallel metric payloads."""
    overall_status = "success"
    test_results = {}
    metric_results = []
    column_validations = {}

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

    return overall_status, test_results, metric_results, column_validations
