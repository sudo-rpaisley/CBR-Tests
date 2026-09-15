from __future__ import annotations

from cbr_tests.plan_migration import (
    COMPATIBILITY_ONLY_METRIC_PROFILES,
    compatibility_only_metric_ids,
    legacy_intrinsic_metric_ids,
)
from runner.schema import validate_plan_schema


FINAL_EXPERIMENT_CONTRACT = "cbr-tests-final-experiment-v1"


class ExperimentContractError(ValueError):
    """Raised when a plan is runnable historically but unsafe for final experiments."""


def validate_final_experiment_plan(plan: dict) -> dict:
    """Validate the stricter contract used for authoritative experiment runs.

    Normal ``run_plan.py`` execution intentionally remains backwards compatible
    so historical outcomes can be reproduced.  Final experiments need a tighter
    boundary: canonical metric identities only, no compatibility-only profiles,
    a full-population execution policy, and no field-mapping skips.

    The returned dictionary is suitable for provenance and records the contract
    that was enforced.  Scientific metric definitions and thresholds are not
    changed by this function.
    """
    validate_plan_schema(plan)

    legacy_ids = legacy_intrinsic_metric_ids(plan)
    if legacy_ids:
        joined = ", ".join(legacy_ids)
        raise ExperimentContractError(
            "Final experiment plans must use canonical metric IDs. "
            f"Legacy metric IDs present: {joined}. Regenerate the plan with the current "
            "plan builder (preferred), or use scripts/migrate_plan_to_canonical_ids.py "
            "for safe one-to-one ID migrations before the experiment."
        )

    compatibility_ids = compatibility_only_metric_ids(plan)
    if compatibility_ids:
        details = "; ".join(
            f"{metric_id}: {COMPATIBILITY_ONLY_METRIC_PROFILES[metric_id]}"
            for metric_id in compatibility_ids
        )
        raise ExperimentContractError(
            "Final experiment plans cannot contain compatibility-only metric profiles. "
            "These constructs changed scientifically and require dataset-aware plan "
            f"regeneration rather than renaming. {details}"
        )

    execution_policy = plan.get("execution_policy", {})
    sample_mode = str(execution_policy.get("sample_mode", "full"))
    if sample_mode != "full":
        raise ExperimentContractError(
            f"Final experiments require execution_policy.sample_mode='full'; got {sample_mode!r}."
        )

    if execution_policy.get("allow_skips", False):
        raise ExperimentContractError(
            "Final experiments require execution_policy.allow_skips=false so a missing "
            "field mapping cannot silently change the metric set."
        )

    enabled_metrics = [
        metric
        for metric in plan.get("metrics", [])
        if isinstance(metric, dict) and metric.get("enabled", True)
    ]
    if not enabled_metrics:
        raise ExperimentContractError("Final experiment plan contains no enabled metrics.")

    return {
        "contract": FINAL_EXPERIMENT_CONTRACT,
        "canonical_metric_ids": True,
        "compatibility_only_profiles": False,
        "sample_mode": "full",
        "allow_skips": False,
        "enabled_metric_count": len(enabled_metrics),
    }
