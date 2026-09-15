"""Migrate saved plans from legacy intrinsic metric IDs to the canonical overhaul IDs."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone

from runner.schema import validate_plan_schema


LEGACY_INTRINSIC_METRIC_MIGRATIONS = {
    "inter_arrival_time_distribution_divergence": {
        "metric_id": "inter_arrival_internal_drift_ks",
        "label": "Inter-Arrival Internal Drift (KS)",
        "taxonomy_path": [
            "dataset_heuristics",
            "temporal_metrics",
            "temporal_structure_diagnostics",
            "traffic_dynamics",
            "inter_arrival_internal_drift_ks",
        ],
    },
    "burstiness_coefficient_deviation": {
        "metric_id": "burstiness_internal_drift",
        "label": "Burstiness Internal Drift",
        "taxonomy_path": [
            "dataset_heuristics",
            "temporal_metrics",
            "temporal_structure_diagnostics",
            "traffic_dynamics",
            "burstiness_internal_drift",
        ],
    },
    "hourly_activity_distribution_divergence": {
        "metric_id": "day_to_day_hourly_activity_divergence",
        "label": "Day-to-Day Hourly Activity Divergence",
        "taxonomy_path": [
            "dataset_heuristics",
            "temporal_metrics",
            "temporal_structure_diagnostics",
            "cyclic_structure",
            "day_to_day_hourly_activity_divergence",
        ],
    },
    "diurnal_pattern_similarity_score": {
        "metric_id": "day_to_day_diurnal_similarity",
        "label": "Day-to-Day Diurnal Similarity",
        "taxonomy_path": [
            "dataset_heuristics",
            "temporal_metrics",
            "temporal_structure_diagnostics",
            "cyclic_structure",
            "day_to_day_diurnal_similarity",
        ],
    },
    "periodicity_preservation_score": {
        "metric_id": "lagged_periodicity_similarity",
        "label": "Lagged Periodicity Similarity",
        "taxonomy_path": [
            "dataset_heuristics",
            "temporal_metrics",
            "temporal_structure_diagnostics",
            "cyclic_structure",
            "lagged_periodicity_similarity",
        ],
    },
    "kolmogorov_smirnov_feature_divergence": {
        "metric_id": "feature_ks_internal_drift",
        "label": "Feature KS Internal Drift",
        "taxonomy_path": [
            "dataset_heuristics",
            "statistical_structure_diagnostics",
            "internal_distribution_drift",
            "feature_ks_internal_drift",
        ],
    },
    "wasserstein_feature_distance": {
        "metric_id": "feature_wasserstein_internal_drift",
        "label": "Feature Wasserstein Internal Drift",
        "taxonomy_path": [
            "dataset_heuristics",
            "statistical_structure_diagnostics",
            "internal_distribution_drift",
            "feature_wasserstein_internal_drift",
        ],
    },
    "energy_distance": {
        "metric_id": "feature_energy_internal_drift",
        "label": "Feature Energy Internal Drift",
        "taxonomy_path": [
            "dataset_heuristics",
            "statistical_structure_diagnostics",
            "internal_distribution_drift",
            "feature_energy_internal_drift",
        ],
    },
    "maximum_mean_discrepancy": {
        "metric_id": "feature_mmd2_internal_drift",
        "label": "Feature MMD² Internal Drift",
        "taxonomy_path": [
            "dataset_heuristics",
            "statistical_structure_diagnostics",
            "internal_distribution_drift",
            "feature_mmd2_internal_drift",
        ],
    },
    "pearson_correlation_profile": {
        "metric_id": "pearson_dependency_profile",
        "label": "Pearson Dependency Profile",
        "taxonomy_path": [
            "dataset_heuristics",
            "statistical_structure_diagnostics",
            "dependency_profiles",
            "pearson_dependency_profile",
        ],
    },
    "spearman_correlation_matrix_deviation": {
        "metric_id": "spearman_dependency_profile",
        "label": "Spearman Dependency Profile",
        "taxonomy_path": [
            "dataset_heuristics",
            "statistical_structure_diagnostics",
            "dependency_profiles",
            "spearman_dependency_profile",
        ],
    },
    "distance_correlation_matrix_deviation": {
        "metric_id": "distance_correlation_dependency_profile",
        "label": "Distance-Correlation Dependency Profile",
        "taxonomy_path": [
            "dataset_heuristics",
            "statistical_structure_diagnostics",
            "dependency_profiles",
            "distance_correlation_dependency_profile",
        ],
    },
}

# Compatibility handlers that are intentionally still executable, but whose
# scientific constructs changed enough that a representative plan must be
# regenerated rather than renamed in place.
COMPATIBILITY_ONLY_METRIC_PROFILES = {
    "protocol_validity_profile": (
        "The canonical Valid IP Address Ratio is now a dedicated metric; the old "
        "protocol profile mixes several packet-validity concepts."
    ),
    "reserved_ip_address_profile": (
        "Reserved-address misuse now requires an explicit address-use policy and "
        "cannot be inferred from the legacy profile."
    ),
}


def legacy_intrinsic_metric_ids(plan: dict) -> list[str]:
    """Return legacy intrinsic metric IDs present in a plan, in plan order."""
    return [
        str(metric.get("metric_id"))
        for metric in plan.get("metrics", [])
        if isinstance(metric, dict)
        and metric.get("metric_id") in LEGACY_INTRINSIC_METRIC_MIGRATIONS
    ]


def compatibility_only_metric_ids(plan: dict) -> list[str]:
    """Return profile IDs that require plan regeneration for final experiments."""
    return [
        str(metric.get("metric_id"))
        for metric in plan.get("metrics", [])
        if isinstance(metric, dict)
        and metric.get("metric_id") in COMPATIBILITY_ONLY_METRIC_PROFILES
    ]


def migrate_plan_to_canonical_ids(plan: dict) -> tuple[dict, list[dict]]:
    """Return a schema-valid copy of a plan with legacy intrinsic IDs migrated.

    Scientific inputs and calculation parameters are preserved. Only the metric
    identity, human label and taxonomy path are refreshed to the canonical
    overhaul contract. Compatibility-only profiles are intentionally left alone
    because they require plan regeneration rather than a one-to-one rename.
    """
    validate_plan_schema(plan)
    migrated = deepcopy(plan)
    changes: list[dict] = []

    for metric in migrated.get("metrics", []):
        old_id = metric.get("metric_id")
        replacement = LEGACY_INTRINSIC_METRIC_MIGRATIONS.get(old_id)
        if replacement is None:
            continue
        metric["metric_id"] = replacement["metric_id"]
        metric["label"] = replacement["label"]
        metric["taxonomy_path"] = list(replacement["taxonomy_path"])
        changes.append(
            {
                "from_metric_id": old_id,
                "to_metric_id": replacement["metric_id"],
            }
        )

    if changes:
        meta = migrated.setdefault("plan_meta", {})
        meta["metric_id_contract"] = "metric-conformance-canonical-v1"
        meta["metric_id_migration"] = {
            "migrated_at": datetime.now(timezone.utc).isoformat(),
            "legacy_metric_count": len(changes),
            "changes": changes,
        }

    validate_plan_schema(migrated)
    return migrated, changes
