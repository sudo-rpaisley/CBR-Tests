"""Derive scientific display semantics from one stored metric result."""

from __future__ import annotations


VERDICT_VALUES = {"pass", "warn", "fail", "not_applicable"}


def _result_summary(payload: object) -> dict:
    if not isinstance(payload, dict):
        return {}
    summary = payload.get("summary")
    return summary if isinstance(summary, dict) else payload


def metric_result_semantics(payload: object) -> dict:
    """Separate applicability/verdict semantics from execution success.

    Execution status belongs to ``metric_results`` and only says whether the
    handler ran. This function inspects the scientific result payload and reports
    whether the metric was actually runnable/applicable and whether it emitted a
    decision-policy verdict.
    """
    summary = _result_summary(payload)
    outer = payload if isinstance(payload, dict) else {}

    runnable = summary.get("runnable")
    if not isinstance(runnable, bool):
        runnable = outer.get("runnable")
    if not isinstance(runnable, bool):
        runnable = None

    verdict = None
    for source in (summary, outer):
        for key in ("verdict", "status", "domain_status"):
            value = source.get(key)
            if isinstance(value, str) and value.strip().lower() in VERDICT_VALUES:
                verdict = value.strip().lower()
                break
        if verdict is not None:
            break

    if verdict == "not_applicable":
        runnable = False

    if runnable is True:
        applicability = "runnable"
    elif runnable is False:
        applicability = "not_runnable"
    else:
        applicability = "not_reported"

    interpretation_direction = summary.get("interpretation_direction")
    contextual = (
        isinstance(interpretation_direction, str)
        and interpretation_direction.strip().lower() == "contextual"
    )

    return {
        "runnable": runnable,
        "applicability": applicability,
        "verdict": verdict,
        "contextual": contextual,
    }


def scientific_status_fragments(payload: object) -> list[str]:
    """Return concise TUI fragments describing applicability and verdict."""
    semantics = metric_result_semantics(payload)
    parts: list[str] = []
    if semantics["runnable"] is True:
        parts.append("runnable=yes")
    elif semantics["runnable"] is False:
        parts.append("runnable=no")
    if semantics["verdict"] is not None:
        parts.append(f"verdict={semantics['verdict']}")
    if semantics["contextual"]:
        parts.append("interpretation=contextual")
    return parts
