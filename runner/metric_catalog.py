from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path
from typing import Iterable

from runner.dispatch import build_metric_handlers
from runner.field_translation import collect_required_test_fields_for_metric
from runner.pcap_adapter import PCAP_DIRECT_METRICS


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_TAXONOMY_PATH = REPOSITORY_ROOT / "taxonomy" / "master_taxonomy.json"
DEFAULT_PLANS_DIR = REPOSITORY_ROOT / "plans"

PCAP_ONLY_METRICS = PCAP_DIRECT_METRICS

# Historical metric IDs remain executable for reproducibility, but new plans and
# the canonical taxonomy use the scientifically corrected diagnostic names.
LEGACY_METRIC_ID_ALIASES = {
    "inter_arrival_time_distribution_divergence": "inter_arrival_internal_drift_ks",
    "burstiness_coefficient_deviation": "burstiness_internal_drift",
    "hourly_activity_distribution_divergence": "day_to_day_hourly_activity_divergence",
    "diurnal_pattern_similarity_score": "day_to_day_diurnal_similarity",
    "periodicity_preservation_score": "lagged_periodicity_similarity",
    "kolmogorov_smirnov_feature_divergence": "feature_ks_internal_drift",
    "wasserstein_feature_distance": "feature_wasserstein_internal_drift",
    "energy_distance": "feature_energy_internal_drift",
    "maximum_mean_discrepancy": "feature_mmd2_internal_drift",
    "pearson_correlation_profile": "pearson_dependency_profile",
    "spearman_correlation_matrix_deviation": "spearman_dependency_profile",
    "distance_correlation_matrix_deviation": "distance_correlation_dependency_profile",
}

MANUAL_CONFIGURATION_REASONS = {
    "reserved_address_misuse_ratio": "address_policy_required",
    "service_port_consistency_profile": "service_definition_required",
    "valid_slice_identifier_profile": "allowed_slice_ids_required",
    "slice_identifier_consistency_profile": "slice_consistency_rules_required",
    "per_slice_sample_coverage_ratio": "expected_slice_ids_required",
    "per_slice_class_coverage_ratio": "expected_classes_required",
    "cross_slice_identifier_leakage_ratio": "slice_exclusivity_policy_required",
    "per_slice_label_entropy_score": "expected_classes_required",
    "class_imbalance_score": "expected_classes_required",
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
    """Return non-legacy runtime metric IDs exposed to plan creation.

    Legacy IDs are deliberately omitted here even though the runtime dispatcher
    still accepts them, preventing old construct names from re-entering newly
    generated plans. Taxonomy registration is checked separately when the
    catalogue is assembled because supporting diagnostics are also executable.
    """

    handlers = build_metric_handlers(None, lambda _path: None, {})
    return sorted(
        metric_id
        for metric_id in handlers
        if metric_id not in LEGACY_METRIC_ID_ALIASES
    )


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
    _walk_taxonomy(output=output, node=payload, path=())
    return output


def load_metric_templates(plans_dir: Path = DEFAULT_PLANS_DIR) -> dict[str, list[dict]]:
    """Collect plan templates and migrate legacy intrinsic IDs in memory.

    Saved plans are not rewritten. Their configuration is copied to the
    canonical replacement ID for new plan generation so historical artefacts
    remain reproducible while the new taxonomy stays clean.
    """

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
            if not isinstance(metric_id, str) or not metric_id:
                continue
            templates.setdefault(metric_id, []).append(deepcopy(metric))
            canonical_id = LEGACY_METRIC_ID_ALIASES.get(metric_id)
            if canonical_id:
                migrated = deepcopy(metric)
                migrated["metric_id"] = canonical_id
                migrated["label"] = humanize_metric_id(canonical_id)
                templates.setdefault(canonical_id, []).append(migrated)
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
    """Remove dataset-specific or non-canonical values from reusable templates."""

    metric = deepcopy(metric)
    inputs = metric.get("input_requirements")
    params = metric.get("calculation", {}).get("parameters")

    if reason == "reference_dataset_required":
        _blank_reference_paths(metric)
    elif reason == "address_policy_required" and isinstance(params, dict):
        params["misuse_categories"] = []
    elif reason == "service_definition_required" and isinstance(params, dict):
        params["service_name"] = ""
        params["expected_ports"] = []
    elif reason == "allowed_slice_ids_required" and isinstance(params, dict):
        params["allowed_slice_ids"] = []
        # Historical plans may use count_invalid to reproduce old outcomes, but
        # the canonical paper equation excludes missing identifiers from N_checked.
        params["missing_policy"] = "exclude_missing"
    elif reason == "slice_consistency_rules_required" and isinstance(params, dict):
        params["rules"] = []
        # Keep newly generated plans on the canonical denominator even when the
        # source template came from a historical compatibility plan.
        params["missing_policy"] = "exclude_missing"
    elif reason == "expected_slice_ids_required" and isinstance(inputs, dict):
        inputs["expected_slice_ids"] = []
    elif reason == "expected_classes_required" and isinstance(inputs, dict):
        inputs["expected_classes"] = []
    elif reason == "slice_exclusivity_policy_required":
        if isinstance(inputs, dict):
            inputs["identifier_fields"] = []
        if isinstance(params, dict):
            params["expect_slice_exclusive"] = False
    elif reason == "attack_window_configuration_required":
        if isinstance(inputs, dict):
            for key in list(inputs):
                if "attack" in key.lower() or "window" in key.lower():
                    value = inputs[key]
                    inputs[key] = [] if isinstance(value, list) else None
        if isinstance(params, dict):
            for key in list(params):
                if "attack" in key.lower() or "window" in key.lower():
                    value = params[key]
                    params[key] = [] if isinstance(value, list) else None
    elif reason == "split_configuration_required" and isinstance(inputs, dict):
        for key in list(inputs):
            if "train" in key.lower() or "test" in key.lower() or "split" in key.lower():
                value = inputs[key]
                inputs[key] = [] if isinstance(value, list) else ""
        inputs["entity_disjoint_expected"] = False

    return metric


def build_metric_catalog(
    *,
    metric_ids: Iterable[str] | None = None,
    taxonomy_path: Path = DEFAULT_TAXONOMY_PATH,
    plans_dir: Path = DEFAULT_PLANS_DIR,
    available_fields: set[str] | None = None,
) -> list[dict]:
    """Build catalogue entries for every runnable canonical metric."""

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