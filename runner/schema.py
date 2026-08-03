from __future__ import annotations

_ALLOWED_BOOLEAN_POLICY_FIELDS = {"fail_fast", "allow_skips"}


def _require_non_empty_string(value, path: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{path} must be a non-empty string.")
    return value


def _validate_string_list(value, path: str, *, allow_empty: bool = True) -> None:
    if not isinstance(value, list):
        raise ValueError(f"{path} must be a list.")
    if not allow_empty and not value:
        raise ValueError(f"{path} must not be empty.")
    if any(not isinstance(item, str) or not item.strip() for item in value):
        raise ValueError(f"{path} must contain only non-empty strings.")


def validate_plan_schema(plan: dict) -> None:
    if not isinstance(plan, dict):
        raise ValueError("Plan must be a JSON object.")

    plan_meta = plan.get("plan_meta")
    if not isinstance(plan_meta, dict):
        raise ValueError("Plan must include plan_meta object.")
    _require_non_empty_string(plan_meta.get("plan_id"), "plan_meta.plan_id")
    if "name" in plan_meta:
        _require_non_empty_string(plan_meta["name"], "plan_meta.name")
    if "version" in plan_meta:
        _require_non_empty_string(plan_meta["version"], "plan_meta.version")

    execution_policy = plan.get("execution_policy", {})
    if not isinstance(execution_policy, dict):
        raise ValueError("execution_policy must be an object.")
    for field in _ALLOWED_BOOLEAN_POLICY_FIELDS:
        if field in execution_policy and not isinstance(execution_policy[field], bool):
            raise ValueError(f"execution_policy.{field} must be a boolean.")
    if "sample_mode" in execution_policy:
        _require_non_empty_string(execution_policy["sample_mode"], "execution_policy.sample_mode")

    metrics = plan.get("metrics")
    if not isinstance(metrics, list) or not metrics:
        raise ValueError("Plan must include a non-empty metrics list.")

    seen_metric_ids: set[str] = set()
    for index, metric in enumerate(metrics):
        path = f"metrics[{index}]"
        if not isinstance(metric, dict):
            raise ValueError(f"{path} must be an object.")

        metric_id = _require_non_empty_string(metric.get("metric_id"), f"{path}.metric_id")
        if metric_id in seen_metric_ids:
            raise ValueError(f"Duplicate metric_id: {metric_id}.")
        seen_metric_ids.add(metric_id)

        if "enabled" in metric and not isinstance(metric["enabled"], bool):
            raise ValueError(f"Metric {metric_id} enabled must be a boolean.")

        taxonomy_path = metric.get("taxonomy_path")
        _validate_string_list(
            taxonomy_path,
            f"Metric {metric_id} taxonomy_path",
            allow_empty=False,
        )

        input_requirements = metric.get("input_requirements", {})
        if not isinstance(input_requirements, dict):
            raise ValueError(f"Metric {metric_id} input_requirements must be an object.")

        field_requirements = metric.get("field_requirements")
        if field_requirements is not None:
            if not isinstance(field_requirements, dict):
                raise ValueError(f"Metric {metric_id} field_requirements must be an object.")
            unknown = set(field_requirements) - {"required", "optional"}
            if unknown:
                raise ValueError(
                    f"Metric {metric_id} field_requirements contains unknown keys: "
                    + ", ".join(sorted(unknown))
                    + "."
                )
            for key in ("required", "optional"):
                if key in field_requirements:
                    _validate_string_list(
                        field_requirements[key],
                        f"Metric {metric_id} field_requirements.{key}",
                    )
            required = set(field_requirements.get("required", []))
            optional = set(field_requirements.get("optional", []))
            overlap = sorted(required & optional)
            if overlap:
                raise ValueError(
                    f"Metric {metric_id} fields cannot be both required and optional: "
                    + ", ".join(overlap)
                    + "."
                )

        calculation = metric.get("calculation")
        if calculation is not None:
            if not isinstance(calculation, dict):
                raise ValueError(f"Metric {metric_id} calculation must be an object.")
            _require_non_empty_string(calculation.get("method"), f"Metric {metric_id} calculation.method")
            if "parameters" in calculation and not isinstance(calculation["parameters"], dict):
                raise ValueError(f"Metric {metric_id} calculation.parameters must be an object.")

        retention = metric.get("retention")
        if retention is not None and not isinstance(retention, dict):
            raise ValueError(f"Metric {metric_id} retention must be an object.")
