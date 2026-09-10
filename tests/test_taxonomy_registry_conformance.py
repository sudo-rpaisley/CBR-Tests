import json
from pathlib import Path

from runner.dispatch import METRIC_REGISTRY


TAXONOMY_PATH = Path(__file__).resolve().parents[1] / "taxonomy" / "master_taxonomy.json"


def _collect_metric_ids(node):
    metric_ids = set()
    if isinstance(node, dict):
        metrics = node.get("_metrics")
        if isinstance(metrics, list):
            for entry in metrics:
                if isinstance(entry, str):
                    metric_ids.add(entry)
                elif isinstance(entry, dict):
                    metric_id = entry.get("metric_id")
                    if isinstance(metric_id, str):
                        metric_ids.add(metric_id)
        for value in node.values():
            metric_ids.update(_collect_metric_ids(value))
    elif isinstance(node, list):
        for value in node:
            metric_ids.update(_collect_metric_ids(value))
    return metric_ids


def test_every_metric_advertised_by_master_taxonomy_has_a_runtime_handler():
    taxonomy = json.loads(TAXONOMY_PATH.read_text(encoding="utf-8"))
    taxonomy_ids = _collect_metric_ids(taxonomy)

    assert taxonomy_ids, "master_taxonomy.json did not expose any metric IDs"

    missing_handlers = sorted(taxonomy_ids - set(METRIC_REGISTRY))
    assert missing_handlers == [], (
        "master_taxonomy.json advertises metric IDs that the runtime dispatcher "
        f"cannot execute: {missing_handlers}"
    )
