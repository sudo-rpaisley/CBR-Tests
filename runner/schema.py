from __future__ import annotations


def validate_plan_schema(plan: dict) -> None:
    if not isinstance(plan, dict):
        raise ValueError('Plan must be a JSON object.')
    if 'plan_meta' not in plan or not isinstance(plan['plan_meta'], dict):
        raise ValueError('Plan must include plan_meta object.')
    if 'plan_id' not in plan['plan_meta']:
        raise ValueError('plan_meta must include plan_id.')
    metrics = plan.get('metrics')
    if not isinstance(metrics, list) or not metrics:
        raise ValueError('Plan must include a non-empty metrics list.')
    for i, metric in enumerate(metrics):
        if not isinstance(metric, dict):
            raise ValueError(f'Metric at index {i} must be an object.')
        if 'metric_id' not in metric:
            raise ValueError(f'Metric at index {i} missing metric_id.')
        if 'taxonomy_path' in metric and not isinstance(metric['taxonomy_path'], list):
            raise ValueError(f"Metric {metric['metric_id']} taxonomy_path must be a list.")
        if 'input_requirements' in metric and not isinstance(metric['input_requirements'], dict):
            raise ValueError(f"Metric {metric['metric_id']} input_requirements must be an object.")
        if 'calculation' in metric:
            if not isinstance(metric['calculation'], dict):
                raise ValueError(f"Metric {metric['metric_id']} calculation must be an object.")
            if 'method' not in metric['calculation']:
                raise ValueError(f"Metric {metric['metric_id']} calculation.method is required.")
        if not metric.get('taxonomy_path'):
            raise ValueError(f"Metric {metric['metric_id']} must include a non-empty taxonomy_path.")
