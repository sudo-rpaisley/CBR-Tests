# Migrating saved plans to canonical metric IDs

The metric-conformance overhaul renamed twelve intrinsic temporal/statistical metrics so their IDs describe what they actually measure. The old IDs remain executable only for historical-plan and outcome compatibility.

## Why migrate

A legacy plan can still execute successfully, but its metric names may imply external realism/fidelity when the implementation is actually an intrinsic dataset diagnostic. Representative post-overhaul experiments should therefore use the canonical IDs.

The TUI flags outcomes produced with legacy intrinsic IDs as `id_contract=legacy_compatibility`, and `scripts/rerun_and_compare.py` rejects such plans by default.

## Check a saved plan

```bash
python scripts/migrate_plan_to_canonical_ids.py /path/to/plan.json --check
```

Exit status `2` means legacy intrinsic IDs were found. The command lists each one.

## Create a canonical copy

```bash
python scripts/migrate_plan_to_canonical_ids.py /path/to/plan.json
```

This writes `<original stem>_canonical.json` beside the source. It does not overwrite the original plan.

To choose the destination explicitly:

```bash
python scripts/migrate_plan_to_canonical_ids.py /path/to/plan.json \
  --output /path/to/plan_canonical.json
```

The migration preserves input requirements, field mappings, sample limits and other calculation parameters. It updates only the obsolete metric ID, its human-readable label and taxonomy path, and records migration metadata under `plan_meta`.

## Legacy-to-canonical mapping

| Legacy ID | Canonical ID |
|---|---|
| `inter_arrival_time_distribution_divergence` | `inter_arrival_internal_drift_ks` |
| `burstiness_coefficient_deviation` | `burstiness_internal_drift` |
| `hourly_activity_distribution_divergence` | `day_to_day_hourly_activity_divergence` |
| `diurnal_pattern_similarity_score` | `day_to_day_diurnal_similarity` |
| `periodicity_preservation_score` | `lagged_periodicity_similarity` |
| `kolmogorov_smirnov_feature_divergence` | `feature_ks_internal_drift` |
| `wasserstein_feature_distance` | `feature_wasserstein_internal_drift` |
| `energy_distance` | `feature_energy_internal_drift` |
| `maximum_mean_discrepancy` | `feature_mmd2_internal_drift` |
| `pearson_correlation_profile` | `pearson_dependency_profile` |
| `spearman_correlation_matrix_deviation` | `spearman_dependency_profile` |
| `distance_correlation_matrix_deviation` | `distance_correlation_dependency_profile` |

## Representative reruns

After migration, use the canonical plan with the evidence-preserving rerun wrapper:

```bash
python scripts/rerun_and_compare.py \
  --baseline /path/to/pre_overhaul_outcome.json \
  --plan /path/to/plan_canonical.json \
  --dataset /path/to/dataset.pcapng \
  --record-dir outcomes/overhaul-reruns/example
```

`--allow-legacy-metric-ids` exists only for deliberate compatibility experiments. It should not be used for the representative reruns that will support the final paper results.
