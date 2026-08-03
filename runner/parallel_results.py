from __future__ import annotations

from datetime import datetime


TERMINAL_FAILURE_STATUSES = {"failed", "cancelled", "not_run_fail_fast", "not_run_cancelled"}


def _normalise_status(success: bool, payload: dict) -> str:
    explicit_status = payload.get("status")
    if explicit_status:
        return str(explicit_status)
    return "success" if success else "failed"


def collect_parallel_metric_results(
    *,
    parallel_out,
    metrics: list[dict],
    run_started_at: datetime,
    fail_fast: bool,
    completed_statuses: dict[str, str],
    completed_durations: dict[str, float],
) -> tuple[str, dict, list[dict], dict]:
    """Collect parallel outputs without discarding already-completed work.

    ``fail_fast`` is retained for call-site compatibility. The executor is
    responsible for stopping submission and marking metrics that were not run.
    """
    del fail_fast

    overall_status = "success"
    test_results: dict = {}
    metric_results: list[dict] = []
    column_validations: dict = {}

    for idx0, success, metric_payload in sorted(parallel_out, key=lambda item: item[0]):
        metric = metrics[idx0]
        payload = metric_payload if isinstance(metric_payload, dict) else {"value": metric_payload}
        status = _normalise_status(success, payload)
        metric_record = {
            "metric_id": metric["metric_id"],
            "status": status,
            "started_at": payload.get("started_at", run_started_at.isoformat()),
            "finished_at": payload.get("finished_at", run_started_at.isoformat()),
            "elapsed_seconds": float(payload.get("elapsed_seconds", 0.0)),
        }

        if status == "success":
            test_results.update(payload.get("test_results", {}))
            if "column_validation" in payload:
                column_validations[metric["metric_id"]] = payload["column_validation"]
        else:
            if payload.get("error"):
                metric_record["error"] = payload["error"]
            if payload.get("reason"):
                metric_record["reason"] = payload["reason"]

            if status in {"cancelled", "not_run_cancelled"}:
                overall_status = "cancelled"
            elif overall_status != "cancelled" and status in TERMINAL_FAILURE_STATUSES:
                overall_status = "failed"

        metric_results.append(metric_record)
        completed_statuses[metric["metric_id"]] = status
        completed_durations[metric["metric_id"]] = metric_record["elapsed_seconds"]

    return overall_status, test_results, metric_results, column_validations
