from __future__ import annotations

def ensure_taxonomy_path(root: dict, taxonomy_path: list[str]) -> dict:
    node = root
    for segment in taxonomy_path:
        node = node.setdefault(segment, {})
    return node


def build_plan_taxonomy(metrics: list[dict]) -> dict:
    taxonomy: dict = {}
    for metric in metrics:
        node = ensure_taxonomy_path(taxonomy, metric.get('taxonomy_path', []))
        node.setdefault('_metrics', []).append({
            'metric_id': metric.get('metric_id'),
            'label': metric.get('label'),
            'enabled': metric.get('enabled', True),
        })
    return taxonomy


def build_result_taxonomy(metrics: list[dict], metric_results: list[dict], test_results: dict) -> dict:
    by_metric_id = {record['metric_id']: record for record in metric_results}
    taxonomy: dict = {}
    for metric in metrics:
        metric_id = metric.get('metric_id')
        node = ensure_taxonomy_path(taxonomy, metric.get('taxonomy_path', []))
        node.setdefault('_metrics', []).append({
            'metric_id': metric_id,
            'status': by_metric_id.get(metric_id, {}).get('status', 'skipped'),
            'result': test_results.get(metric_id),
            'error': by_metric_id.get(metric_id, {}).get('error')
        })
    return taxonomy


def build_test_results_taxonomy(metrics: list[dict], test_results: dict) -> dict:
    taxonomy: dict = {}
    for metric in metrics:
        metric_id = metric.get('metric_id')
        node = ensure_taxonomy_path(taxonomy, metric.get('taxonomy_path', []))
        node.setdefault('_results', []).append({
            'metric_id': metric_id,
            'result': test_results.get(metric_id)
        })
    return taxonomy


def print_taxonomy_summary(result_taxonomy: dict, indent: int = 0) -> None:
    for key, value in result_taxonomy.items():
        if key == '_metrics':
            for metric in value:
                status = metric.get('status', 'unknown')
                print(f"{'  ' * indent}↳ {metric.get('metric_id')} [{status}]")
            continue
        if isinstance(value, dict) and set(value.keys()) == {'_metrics'}:
            for metric in value['_metrics']:
                status = metric.get('status', 'unknown')
                print(f"{'  ' * indent}↳ {metric.get('metric_id')} [{status}]")
            continue
        print(f"{'  ' * indent}↳ {key}")
        print_taxonomy_summary(value, indent + 1)
