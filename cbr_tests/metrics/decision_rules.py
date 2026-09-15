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


def resolve_maximum_ratio_failure_rule(
    parameters: dict | None,
    *,
    parameter_name: str,
    default: float,
) -> dict:
    """Resolve a one-sided failure cutoff and record its provenance.

    This is a decision rule applied after measurement. It is deliberately
    separate from the metric equation and must not be presented as a universal
    realism constant unless its provenance explicitly supports that claim.
    """
    parameters = parameters or {}
    threshold = float(parameters.get(parameter_name, default))
    if not 0 <= threshold <= 1:
        raise ValueError(f"{parameter_name} must be between 0 and 1.")

    configured = parameter_name in parameters
    provenance_key = f"{parameter_name}_provenance"
    provenance = str(
        parameters.get(
            provenance_key,
            "scenario-configured" if configured else "framework-default",
        )
    ).strip() or ("scenario-configured" if configured else "framework-default")

    return {
        "parameter": parameter_name,
        "failure_threshold": threshold,
        "comparison": "observed_ratio > failure_threshold",
        "provenance": provenance,
        "source": "metric.calculation.parameters" if configured else "framework default",
        "scientific_role": "decision_policy_not_metric_definition",
    }


def describe_measurement_parameter(
    parameters: dict | None,
    *,
    parameter_name: str,
    default,
    value=None,
) -> dict:
    """Record provenance for a parameter that changes the measured quantity.

    Unlike PASS/WARN/FAIL cutoffs, measurement tolerances can alter which
    observations enter a metric numerator. They are therefore part of the
    operationalisation and must be reported explicitly.
    """
    parameters = parameters or {}
    configured = parameter_name in parameters
    provenance_key = f"{parameter_name}_provenance"
    provenance = str(
        parameters.get(
            provenance_key,
            "scenario-configured" if configured else "framework-default",
        )
    ).strip() or ("scenario-configured" if configured else "framework-default")
    resolved_value = parameters.get(parameter_name, default) if value is None else value

    return {
        "parameter": parameter_name,
        "value": resolved_value,
        "provenance": provenance,
        "source": "metric.calculation.parameters" if configured else "framework default",
        "scientific_role": "measurement_operationalisation_parameter",
    }
