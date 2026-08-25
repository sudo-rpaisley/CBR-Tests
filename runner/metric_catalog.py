from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path
from typing import Iterable

from runner.dispatch import build_metric_handlers
from runner.field_translation import collect_required_test_fields_for_metric


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_TAXONOMY_PATH = REPOSITORY_ROOT / "taxonomy" / "master_taxonomy.json"
DEFAULT_PLANS_DIR = REPOSITORY_ROOT / "plans"

PCAP_ONLY_METRICS = {
    "protocol_validity_profile",
    "timestamp_coherence_profile",
}

MANUAL_CONFIGURATION_REASONS = {
    "service_port_consistency_profile": "service_definition_required",
    "valid_slice_identifier_profile": "allowed_slice_ids_required",
    "slice_identifier_consistency_profile": "slice_consistency_rules_required",
    "attack_window_alignment_score": "attack_window_configuration_required",
    "pre_post_attack_label_bleed_ratio": "attack_window_configuration_required",
    "train_test_duplicate_overlap_ratio": "split_configuration_required",
    "train_test_identifier_contamination_ratio": "split_configuration_required",
    "benchmark_model_accuracy": "benchmark_model_configuration_required",
    "benchmark_model_precision": "benchmark_model_configuration_required",
    "benchmark_model_recall": "benchmark_model_configuration_required",
    "benchmark_model_f1_score": "benchmark_model_configuration_required",
}


def available_metric_ids() -> list[str]:
    """Return metric IDs accepted by the runtime dispatcher.

    The plan builder deliberately asks the dispatcher for its handlers instead of
    keeping a second metric-ID list. Adding a runnable metric therefore makes it
    discoverable by plan creation automatically.
    """

    handlers = build_metric_handlers(None, lambda _path: None, {})
    return sorted(handlers)


def _walk_taxonomy(node: dict, path: tuple[str, ...], output: dict[str, list[str]]) -> None:
    for key, value in node.items():
        if key == "_metrics":
            if not isinstance(value, list):
                continue
            for entry in value:
                if isinstance(entry, str):
                    output.setdefault(entry, list(path))
                elif isinstance(entry, dict):
                    metric_id = entry.get("metric_id")
                    if isinstance(metric_id, str) and metric_id:
                        output.setdefault(metric_id, list(path))
            continue
        if isinstance(value, dict):
            _walk_taxonomy(value, path + (key,), output)


def load_taxonomy_paths(path: Path = DEFAULT_TAXONOMY_PATH) -> dict[str, list[str]]:
    with open(path, "r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"Taxonomy must be a JSON object: {path}")
    output: dict[str, list[str]] = {}
    _walk_taxonomy(payload, (), output)
    return output


def load_metric_templates(plans_dir: Path = DEFAULT_PLANS_DIR) -> dict[str, list[dict]]:
    """Collect existing plan metric definitions as configuration templates."""

    templates: dict[str, list[dict]] = {}
    if not plans_dir.exists():
        return templates
    for plan_path in sorted(plans_dir.glob("*.json")):
        try:
            with open(plan_path, "r", encoding="utf-8") as handle:
                payload = json.load(handle)
        except (OSError, json.JSONDecodeError):
            continue
        for metric in payload.get("metrics", []) if isinstance(payload, dict) else []:
            if not isinstance(metric, dict):
                continue
            metric_id = metric.get("metric_id")
            if isinstance(metric_id, str) and metric_id:
                templates.setdefault(metric_id, []).append(deepcopy(metric))
    return templates


def required_fields(metric: dict) -> list[str]:
    requirements = metric.get("field_requirements")
    if isinstance(requirements, dict) and "required" in requirements:
        required = requirements.get("required", [])
        if isinstance(required, list):
            return sorted({field for field in required if isinstance(field, str) and field.strip()})
    return collect_required_test_fields_for_metric(metric)


def choose_metric_template(candidates: Iterable[dict], available_fields: set[str] | None = None) -> dict | None:
    candidates = list(candidates)
    if not candidates:
        return None

    def score(metric: dict) -> tuple[int, int, int, str]:
        fields = required_fields(metric)
        if available_fields is None:
            matched = 0
            missing = len(fields)
        else:
            matched = sum(field in available_fields for field in fields)
            missing = sum(field not in available_fields for field in fields)
        return (matched, -missing, -len(fields), json.dumps(metric, sort_keys=True))

    return deepcopy(max(candidates, key=score))


def metric_manual_configuration_reason(metric_id: str) -> str | None:
    if metric_id.endswith("_from_reference"):
        return "reference_dataset_required"
    return MANUAL_CONFIGURATION_REASONS.get(metric_id)


def humanize_metric_id(metric_id: str) -> str:
    return metric_id.replace("_", " ").strip().title()


def _blank_reference_paths(value):
    if isinstance(value, dict):
        for key, child in list(value.items()):
            if "reference" in key.lower() and "path" in key.lower():
                value[key] = ""
            else:
                _blank_reference_paths(child)
    elif isinstance(value, list):
        for child in value:
            _blank_reference_paths(child)


def sanitize_manual_template(metric: dict, reason: str) -> dict:
    """Remove dataset-specific values that would be unsafe as universal defaults."""

    metric = deepcopy(metric)
    inputs = metric.get("input_requirements")
    params = metric.get("calculation", {}).get("parameters")

    if reason == "reference_dataset_required":
        _blank_reference_paths(metric)
    elif reason == "service_definition_required" and isinstance(params, dict):
        params["service_name"] = ""
        params["expected_ports"] = []
    elif reason == "allowed_slice_ids_required" and isinstance(params, dict):
        params["allowed_slice_ids"] = []
    elif reason == "slice_consistency_rules_required" and isinstance(params, dict):
        params["rules"] = []
    elif reason == "attack_window_configuration_required" and isinstance(params, dict):
        for key in list(params):
            if "attack" in key or "window" in key:
                value = params[key]
                params[key] = [] if isinstance(value, list) else None
    elif reason == "split_configuration_required" and isinstance(inputs, dict):
        for key in list(inputs):
            if "train" in key or "test" in key or "split" in key:
                value = inputs[key]
                inputs[key] = [] if isinstance(value, list) else ""

    return metric


def build_metric_catalog(
    *,
    metric_ids: Iterable[str] | None = None,
    taxonomy_path: Path = DEFAULT_TAXONOMY_PATH,
    plans_dir: Path = DEFAULT_PLANS_DIR,
    available_fields: set[str] | None = None,
) -> list[dict]:
    """Build catalogue entries for every runnable metric."""

    ids = sorted(set(metric_ids if metric_ids is not None else available_metric_ids()))
    taxonomy_paths = load_taxonomy_paths(taxonomy_path)
    templates = load_metric_templates(plans_dir)
    catalogue: list[dict] = []

    for metric_id in ids:
        template = choose_metric_template(templates.get(metric_id, []), available_fields)
        manual_reason = metric_manual_configuration_reason(metric_id)
        if template is not None and manual_reason:
            template = sanitize_manual_template(template, manual_reason)
        catalogue.append(
            {
                "metric_id": metric_id,
                "label": (template or {}).get("label") or humanize_metric_id(metric_id),
                "taxonomy_path": taxonomy_paths.get(metric_id, ["uncategorized", metric_id]),
                "template": template,
                "manual_configuration_reason": manual_reason,
                "registered_in_taxonomy": metric_id in taxonomy_paths,
            }
        )

    return catalogue
