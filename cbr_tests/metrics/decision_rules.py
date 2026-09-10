"""Decision-policy helpers kept separate from scientific metric equations."""

from __future__ import annotations


def resolve_ratio_decision_rule(
    parameters: dict | None,
    *,
    default_pass: float,
    default_warn: float,
) -> dict:
    """Resolve PASS/WARN cutoffs and record their provenance.

    The thresholds classify a measured ratio; they do not define the ratio itself.
    Defaults are framework policy unless an experiment explicitly overrides them.
    """
    parameters = parameters or {}
    pass_threshold = float(parameters.get("pass_threshold", default_pass))
    warn_threshold = float(parameters.get("warn_threshold", default_warn))
    if not 0 <= warn_threshold <= pass_threshold <= 1:
        raise ValueError(
            "Ratio decision thresholds must satisfy 0 <= warn_threshold <= pass_threshold <= 1."
        )

    configured = "pass_threshold" in parameters or "warn_threshold" in parameters
    provenance = str(
        parameters.get(
            "threshold_provenance",
            "scenario-configured" if configured else "framework-default",
        )
    ).strip() or ("scenario-configured" if configured else "framework-default")

    return {
        "pass_threshold": pass_threshold,
        "warn_threshold": warn_threshold,
        "provenance": provenance,
        "source": "metric.calculation.parameters" if configured else "framework default",
        "scientific_role": "decision_policy_not_metric_definition",
    }


def classify_ratio(value: float | None, decision_rule: dict) -> str:
    """Apply a resolved decision policy to a ratio without redefining the metric."""
    if value is None:
        return "not_applicable"
    if value >= decision_rule["pass_threshold"]:
        return "pass"
    if value >= decision_rule["warn_threshold"]:
        return "warn"
    return "fail"
