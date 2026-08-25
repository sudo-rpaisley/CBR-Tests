from __future__ import annotations

from typing import Any

DOMAIN_STATUSES = {"pass", "warn", "fail", "not_applicable"}


def extract_metric_result(metric_id: str, payload: dict | None) -> dict | None:
    if not isinstance(payload, dict):
        return None
    test_results = payload.get("test_results")
    if not isinstance(test_results, dict):
        return None
    result = test_results.get(metric_id)
    return result if isinstance(result, dict) else None


def extract_result_status(metric_id: str, payload: dict | None) -> str | None:
    result = extract_metric_result(metric_id, payload)
    if not result:
        return None
    status = result.get("status")
    if status is None:
        return None
    status_text = str(status).strip().lower()
    return status_text if status_text in DOMAIN_STATUSES else None


def _infer_execution_reason_code(payload: dict, error: str) -> str:
    explicit = payload.get("reason_code") or payload.get("reason")
    if explicit:
        return str(explicit)
    if payload.get("missing_fields"):
        return "missing_required_fields"
    lowered = error.lower()
    if "load dataset" in lowered or "failed to load" in lowered:
        return "dataset_load_error"
    if "required" in lowered or "provided" in lowered or "unsupported" in lowered:
        return "invalid_metric_configuration"
    return "execution_error"


def extract_diagnostic(metric_id: str, success: bool, payload: dict | None) -> dict[str, Any] | None:
    payload = payload if isinstance(payload, dict) else {}

    if not success:
        error = str(payload.get("error") or payload.get("reason") or "Metric execution failed for an unknown reason.")
        diagnostic: dict[str, Any] = {
            "reason_code": _infer_execution_reason_code(payload, error),
            "summary": error,
        }
        if payload.get("missing_fields"):
            diagnostic["details"] = {"missing_fields": list(payload["missing_fields"])}
            diagnostic["suggestion"] = "Check the dataset field-translation mapping and required plan fields."
        elif diagnostic["reason_code"] == "invalid_metric_configuration":
            diagnostic["suggestion"] = "Check the metric parameters in the selected test plan."
        return diagnostic

    result = extract_metric_result(metric_id, payload)
    if not result:
        return None
    diagnostic = result.get("diagnostic")
    if isinstance(diagnostic, dict):
        normalized = dict(diagnostic)
        normalized.setdefault("reason_code", f"metric_{result.get('status', 'result')}")
        normalized.setdefault("summary", f"Metric returned {result.get('status', 'a result')}.")
        return normalized

    result_status = extract_result_status(metric_id, payload)
    if result_status in {"warn", "fail", "not_applicable"}:
        return {
            "reason_code": f"metric_{result_status}",
            "summary": f"Metric completed with result status {result_status}.",
        }
    return None


def display_status(metric_id: str, success: bool, payload: dict | None) -> str:
    if not success:
        return "error"
    return extract_result_status(metric_id, payload) or "success"
