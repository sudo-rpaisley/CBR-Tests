from __future__ import annotations
import json
from pathlib import Path


def _walk_taxonomy(node: dict, ranks: dict[str, int], counter: list[int]) -> None:
    for key, value in node.items():
        if key == '_metrics' and isinstance(value, list):
            for metric_id in value:
                if metric_id not in ranks:
                    ranks[metric_id] = counter[0]
                    counter[0] += 1
            continue
        if isinstance(value, dict):
            _walk_taxonomy(value, ranks, counter)


def load_taxonomy_order(taxonomy_file: Path) -> dict[str, int]:
    with open(taxonomy_file, 'r', encoding='utf-8') as f:
        taxonomy = json.load(f)
    ranks: dict[str, int] = {}
    _walk_taxonomy(taxonomy, ranks, [0])
    return ranks


def order_metrics_by_taxonomy(metrics: list[dict], ranks: dict[str, int], strict: bool = False) -> list[dict]:
    indexed = list(enumerate(metrics))
    missing = [m['metric_id'] for m in metrics if m.get('metric_id') not in ranks]
    if strict and missing:
        raise ValueError(f"Metrics missing from taxonomy order: {missing}")
    indexed.sort(key=lambda t: (ranks.get(t[1].get('metric_id'), 10**9), t[0]))
    return [m for _, m in indexed]
