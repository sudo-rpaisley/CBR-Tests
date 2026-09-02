from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import tempfile
from typing import Iterable

from runner.field_translation import (
    available_translated_fields,
    default_field_translation_path,
    detect_standard_pcap_field_translation_for_dataset,
    field_resolver,
    load_field_translation,
    merge_field_translations,
    read_tabular_dataset_columns,
)
from runner.metric_catalog import build_metric_catalog, required_fields
from runner.pcap_adapter import (
    PCAP_CONTEXT_CONFIGURATION_REASONS,
    PCAP_DIRECT_METRICS,
    PCAP_PACKET_COLUMNS,
    PCAP_PACKET_METRICS,
    PCAP_REFERENCE_METRICS,
    PCAP_REFERENCE_UNSUPPORTED_REASONS,
    PCAP_SELF_DERIVED_METRICS,
    pcap_metric_template,
    pcap_reference_metric_template,
    pcap_service_port_template,
)
from runner.preflight_advice import advice_for_exclusion, build_unlock_actions
from runner.schema import validate_plan_schema
from runner.taxonomy import build_plan_taxonomy


TABULAR_SUFFIXES = {".csv", ".tsv", ".xlsx", ".xls"}
PCAP_SUFFIXES = {".pcap", ".pcapng"}
SUPPORTED_SUFFIXES = TABULAR_SUFFIXES | PCAP_SUFFIXES


def dataset_format(dataset_path: Path | None) -> str | None:
    if dataset_path is None:
        return None
    suffix = dataset_path.suffix.lower()
    return suffix[1:] if suffix.startswith(".") else suffix


def _canonical_fields(columns: list[str], translation: dict[str, str]) -> set[str]:
    return {translation.get(column, column) for column in columns}


def _sample_numeric_fields(
    dataset_path: Path,
    columns: list[str],
    translation: dict[str, str],
    *,
    sample_rows: int = 250,
) -> set[str]:
    """Identify numeric-compatible canonical fields from a small deterministic prefix sample."""

    if not columns:
        return set()
    import pandas as pd

    suffix = dataset_path.suffix.lower()
    try:
        if suffix in {".csv", ".tsv"}:
            frame = pd.read_csv(
                dataset_path,
                sep="\t" if suffix == ".tsv" else ",",
                skipinitialspace=True,
                low_memory=False,
                nrows=sample_rows,
            )
        else:
            frame = pd.read_excel(dataset_path, nrows=sample_rows)
    except Exception:
        return set()

    frame.columns = [str(column).strip() for column in frame.columns]
    numeric: set[str] = set()
    for raw_field in frame.columns:
        series = frame[raw_field]
        non_null = series.dropna()
        if len(non_null) < 2:
            continue
        converted = pd.to_numeric(non_null, errors="coerce")
        if float(converted.notna().mean()) >= 0.8:
            numeric.add(translation.get(raw_field, raw_field))
    return numeric


def _reference_field_map(candidate: dict, reference: dict, fields: list[str]) -> dict[str, str]:
    candidate_resolver = field_resolver(candidate.get("field_translation", {}), candidate.get("columns", []))
    reference_resolver = field_resolver(reference.get("field_translation", {}), reference.get("columns", []))
    mapping: dict[str, str] = {}
    for canonical in fields:
        candidate_field = candidate_resolver.get(canonical, canonical)
        reference_field = reference_resolver.get(canonical, canonical)
        if candidate_field != reference_field:
            mapping[reference_field] = candidate_field
    return mapping


def tabular_reference_metric_template(
    metric_id: str,
    label: str,
    candidate: dict,
    reference: dict,
) -> dict | None:
    """Build a reference-comparison template from fields shared by two tabular datasets."""

    common_fields = set(candidate.get("canonical_fields", set())) & set(reference.get("canonical_fields", set()))
    common_numeric = sorted(
        set(candidate.get("numeric_fields", set())) & set(reference.get("numeric_fields", set()))
    )
    reference_path = str(reference["path"])
    requirements: dict = {"reference_dataset_path": reference_path}
    parameters: dict = {}

    feature_metrics = {
        "feature_wise_wasserstein_distance_from_reference",
        "feature_wise_ks_statistic_from_reference",
        "feature_wise_energy_distance_from_reference",
        "flow_statistic_deviation_from_reference",
    }
    matrix_metrics = {
        "feature_set_mmd_score_from_reference",
        "pearson_matrix_deviation_from_reference",
        "spearman_matrix_deviation_from_reference",
        "distance_correlation_matrix_deviation_from_reference",
    }
    temporal_metrics = {
        "inter_arrival_distribution_divergence_from_reference",
        "burstiness_deviation_from_reference",
        "hourly_activity_divergence_from_reference",
    }

    required_fields: list[str] = []
    if metric_id in feature_metrics:
        if not common_numeric:
            return None
        requirements["candidate_fields"] = common_numeric
        required_fields = list(common_numeric)
        parameters["max_sample_size"] = 1000
    elif metric_id in matrix_metrics:
        if len(common_numeric) < 2:
            return None
        requirements["candidate_fields"] = common_numeric
        required_fields = list(common_numeric)
        parameters["max_sample_size"] = 500 if metric_id == "feature_set_mmd_score_from_reference" else 1000
    elif metric_id in temporal_metrics:
        timestamp_field = "timestamp" if "timestamp" in common_fields else "Timestamp" if "Timestamp" in common_fields else None
        if timestamp_field is None:
            return None
        requirements["timestamp_field"] = timestamp_field
        required_fields = [timestamp_field]
    elif metric_id == "protocol_mix_divergence_from_reference":
        if "Protocol" not in common_fields:
            return None
        requirements["protocol_field"] = "Protocol"
        required_fields = ["Protocol"]
    elif metric_id == "port_use_divergence_from_reference":
        port_fields = [field for field in ("Source Port", "Destination Port") if field in common_fields]
        if not port_fields:
            return None
        requirements["port_fields"] = port_fields
        required_fields = list(port_fields)
    elif metric_id == "slice_proportion_deviation_from_reference":
        if "slice" not in common_fields:
            return None
        requirements["slice_field"] = "slice"
        required_fields = ["slice"]
    elif metric_id == "per_slice_class_divergence_from_reference":
        if not {"slice", "label"}.issubset(common_fields):
            return None
        requirements.update({"slice_field": "slice", "label_field": "label"})
        required_fields = ["slice", "label"]
    elif metric_id == "per_slice_feature_distribution_deviation_from_reference":
        if "slice" not in common_fields or not common_numeric:
            return None
        requirements.update({"slice_field": "slice", "candidate_fields": common_numeric})
        required_fields = ["slice", *common_numeric]
    else:
        return None

    mapping_fields = [field for field in required_fields if field in common_fields]
    template = {
        "metric_id": metric_id,
        "label": label,
        "input_requirements": requirements,
        "field_requirements": {"required": required_fields},
        "calculation": {
            "method": f"tabular_{metric_id}",
            "parameters": parameters,
        },
    }
    reference_map = _reference_field_map(candidate, reference, mapping_fields)
    if reference_map:
        template["reference_field_map"] = reference_map
    return template


def inspect_dataset(
    dataset_path: Path | None,
    *,
    field_translation_path: Path | None = None,
) -> dict:
    """Inspect a dataset enough to decide which metrics are structurally runnable."""

    if dataset_path is None:
        raise ValueError(
            "A dataset is required for automatic plan creation so the builder can "
            "exclude tests that cannot run."
        )

    dataset_path = Path(dataset_path).expanduser().resolve()
    if not dataset_path.exists() or not dataset_path.is_file():
        raise FileNotFoundError(f"Dataset does not exist or is not a file: {dataset_path}")
    suffix = dataset_path.suffix.lower()
    if suffix not in SUPPORTED_SUFFIXES:
        raise ValueError(
            f"Unsupported dataset format '{suffix or '<none>'}'. "
            f"Supported formats: {', '.join(sorted(SUPPORTED_SUFFIXES))}."
        )

    columns = read_tabular_dataset_columns(dataset_path) if suffix in TABULAR_SUFFIXES else []
    automatic_translation = (
        detect_standard_pcap_field_translation_for_dataset(dataset_path)
        if suffix in TABULAR_SUFFIXES
        else {}
    )

    resolved_translation_path: Path | None = None
    explicit_translation: dict[str, str] = {}
    if suffix in TABULAR_SUFFIXES:
        if field_translation_path is not None:
            resolved_translation_path = Path(field_translation_path).expanduser().resolve()
            if not resolved_translation_path.exists():
                raise FileNotFoundError(
                    f"Field translation file does not exist: {resolved_translation_path}"
                )
        else:
            sidecar = default_field_translation_path(dataset_path)
            if sidecar.exists():
                resolved_translation_path = sidecar

        if resolved_translation_path is not None:
            explicit_translation = load_field_translation(resolved_translation_path)

    translation = merge_field_translations(automatic_translation, explicit_translation)
    fields = (
        available_translated_fields(columns, translation)
        if columns
        else set(PCAP_PACKET_COLUMNS) if suffix in PCAP_SUFFIXES else set()
    )
    canonical_fields = (
        _canonical_fields(columns, translation)
        if columns
        else set(PCAP_PACKET_COLUMNS) if suffix in PCAP_SUFFIXES else set()
    )
    numeric_fields = (
        _sample_numeric_fields(dataset_path, columns, translation)
        if suffix in TABULAR_SUFFIXES
        else {"Timestamp", "Source Port", "Destination Port", "Protocol", "IP Version", "Packet Length", "TCP Flags", "Inter Arrival Time"}
        if suffix in PCAP_SUFFIXES
        else set()
    )
    return {
        "path": dataset_path,
        "format": dataset_format(dataset_path),
        "columns": columns,
        "available_fields": fields,
        "canonical_fields": canonical_fields,
        "numeric_fields": numeric_fields,
        "field_translation": translation,
        "field_translation_path": resolved_translation_path,
    }


def _configuration_state(
    metric_spec: dict,
    dataset: dict,
    reference_dataset: dict | None = None,
) -> tuple[str, str | None, list[str]]:
    metric_id = metric_spec["metric_id"]
    template = metric_spec.get("template")
    manual_reason = metric_spec.get("manual_configuration_reason")
    fmt = dataset.get("format")
    is_pcap = fmt in {"pcap", "pcapng"}

    if is_pcap and metric_id in PCAP_REFERENCE_METRICS:
        if reference_dataset is None:
            return "needs_configuration", "reference_dataset_required", []
        if reference_dataset.get("format") not in {"pcap", "pcapng"}:
            return "needs_configuration", "pcap_reference_representation_mismatch", []
        if template is None:
            return "needs_configuration", "pcap_reference_template_missing", []
        required = required_fields(template)
        available = dataset.get("available_fields", set())
        missing = [field for field in required if field not in available]
        if missing:
            return "needs_mapping", "pcap_adapter_fields_missing", missing
        reference_available = reference_dataset.get("available_fields", set())
        reference_missing = [field for field in required if field not in reference_available]
        if reference_missing:
            return "needs_mapping", "reference_pcap_adapter_fields_missing", reference_missing
        return "ready", None, []

    if is_pcap and metric_id in PCAP_REFERENCE_UNSUPPORTED_REASONS:
        if reference_dataset is None:
            return "needs_configuration", "reference_dataset_required", []
        return "needs_configuration", PCAP_REFERENCE_UNSUPPORTED_REASONS[metric_id], []

    if is_pcap and metric_id == "service_port_consistency_profile":
        if template is None:
            return "needs_configuration", "service_definition_required", []
        params = template.get("calculation", {}).get("parameters", {})
        if not params.get("service_name") or not params.get("expected_ports") or params.get("population_mode") != "all_rows":
            return "needs_configuration", "pcap_service_population_evidence_required", []
        required = required_fields(template)
        available = dataset.get("available_fields", set())
        missing = [field for field in required if field not in available]
        if missing:
            return "needs_mapping", "pcap_adapter_fields_missing", missing
        return "ready", None, []

    if not is_pcap and metric_id.endswith("_from_reference"):
        if reference_dataset is None:
            return "needs_configuration", "reference_dataset_required", []
        if reference_dataset.get("format") not in {"csv", "tsv", "xlsx", "xls"}:
            return "needs_configuration", "reference_representation_mismatch", []
        if template is None:
            return "needs_configuration", "reference_shared_fields_missing", []
        required = required_fields(template)
        available = dataset.get("available_fields", set())
        missing = [field for field in required if field not in available]
        if missing:
            return "needs_mapping", "required_fields_not_resolved", missing
        reference_available = reference_dataset.get("available_fields", set())
        reference_missing = [field for field in required if field not in reference_available]
        if reference_missing:
            return "needs_mapping", "reference_required_fields_not_resolved", reference_missing
        return "ready", None, []

    if manual_reason:
        return "needs_configuration", manual_reason, []
    if not metric_spec.get("registered_in_taxonomy", False):
        return "needs_configuration", "missing_master_taxonomy_entry", []

    if is_pcap:
        if metric_id in PCAP_DIRECT_METRICS:
            return "ready", None, []
        context_reason = PCAP_CONTEXT_CONFIGURATION_REASONS.get(metric_id)
        if context_reason:
            return "needs_configuration", context_reason, []
        if metric_id in PCAP_SELF_DERIVED_METRICS:
            return "not_applicable", "self_derived_pcap_invariant_not_independent", []
        if metric_id in PCAP_PACKET_METRICS:
            if template is None:
                return "needs_configuration", "pcap_adapter_template_missing", []
            required = required_fields(template)
            available = dataset.get("available_fields", set())
            missing = [field for field in required if field not in available]
            if missing:
                return "needs_mapping", "pcap_adapter_fields_missing", missing
            return "ready", None, []
        return "not_applicable", "pcap_adapter_not_available", []

    if metric_id in PCAP_DIRECT_METRICS:
        return "not_applicable", "packet_capture_metric_on_tabular_dataset", []
    if template is None:
        return "needs_configuration", "no_metric_template_available", []
    required = required_fields(template)
    available = dataset.get("available_fields", set())
    missing = [field for field in required if field not in available]
    if missing:
        return "needs_mapping", "required_fields_not_resolved", missing
    return "ready", None, []

def _metric_from_spec(metric_spec: dict) -> dict:
    """Create a plan metric from a spec already proven ready by preflight."""

    template = metric_spec.get("template")
    if template is None:
        metric = {
            "metric_id": metric_spec["metric_id"],
            "label": metric_spec["label"],
            "taxonomy_path": metric_spec["taxonomy_path"],
        }
    else:
        metric = deepcopy(template)
        metric["metric_id"] = metric_spec["metric_id"]
        metric["label"] = metric.get("label") or metric_spec["label"]
        metric["taxonomy_path"] = metric_spec["taxonomy_path"]

    metric["enabled"] = True
    metric["configuration"] = {"status": "ready"}
    return metric


def build_plan(
    *,
    plan_id: str,
    name: str,
    description: str = "Automatically generated CBR-Tests plan.",
    dataset_path: Path,
    field_translation_path: Path | None = None,
    include_metric_ids: Iterable[str] | None = None,
    exclude_metric_ids: Iterable[str] | None = None,
    reference_dataset_path: Path | None = None,
    service_port_configuration: dict | None = None,
) -> tuple[dict, dict]:
    """Build a plan containing only metrics that can run on the supplied dataset.

    Every discoverable metric is considered unless include/exclude filters narrow
    the candidate set. Metrics that need missing fields, dataset-specific
    configuration, a reference dataset, or a different input format are reported
    but are never written into the generated plan.
    """

    dataset = inspect_dataset(dataset_path, field_translation_path=field_translation_path)
    reference_dataset = None
    if reference_dataset_path is not None:
        reference_path = Path(reference_dataset_path).expanduser().resolve()
        if reference_path == dataset["path"]:
            raise ValueError("Reference dataset must be independent; candidate and reference paths are identical.")
        reference_dataset = inspect_dataset(reference_path)

    catalogue = build_metric_catalog(available_fields=dataset["available_fields"] or None)
    available_ids = {entry["metric_id"] for entry in catalogue}

    include = set(include_metric_ids or available_ids)
    exclude = set(exclude_metric_ids or [])
    unknown = sorted((include | exclude) - available_ids)
    if unknown:
        raise ValueError("Unknown metric IDs: " + ", ".join(unknown))
    selected = include - exclude
    if not selected:
        raise ValueError("Plan selection is empty after applying include/exclude filters.")

    metrics: list[dict] = []
    status_counts: dict[str, int] = {}
    status_by_metric: dict[str, dict] = {}

    for spec in catalogue:
        metric_id = spec["metric_id"]
        if metric_id not in selected:
            continue

        if dataset["format"] in {"pcap", "pcapng"}:
            if metric_id in PCAP_PACKET_METRICS:
                spec = dict(spec)
                spec["template"] = pcap_metric_template(metric_id)
            elif metric_id in PCAP_REFERENCE_METRICS and reference_dataset is not None:
                spec = dict(spec)
                spec["template"] = pcap_reference_metric_template(metric_id, reference_dataset["path"])
            elif metric_id == "service_port_consistency_profile":
                # Never inherit a service definition from another discovered plan.
                # Raw-PCAP service identity must be asserted for this capture.
                spec = dict(spec)
                spec["template"] = None
                if service_port_configuration:
                    spec["template"] = pcap_service_port_template(
                        service_port_configuration.get("service_name", ""),
                        service_port_configuration.get("expected_ports", []),
                    )
        elif reference_dataset is not None and metric_id.endswith("_from_reference"):
            spec = dict(spec)
            spec["template"] = tabular_reference_metric_template(
                metric_id,
                spec["label"],
                dataset,
                reference_dataset,
            )

        state, reason, missing = _configuration_state(spec, dataset, reference_dataset)
        included = state == "ready"
        status_counts[state] = status_counts.get(state, 0) + 1
        details = {
            "status": state,
            "included": included,
            **({"reason": reason} if reason else {}),
            **({"missing_fields": missing} if missing else {}),
        }
        if not included:
            details["advice"] = advice_for_exclusion(
                reason,
                status=state,
                dataset_format=dataset["format"],
                missing_fields=missing,
            )
        status_by_metric[metric_id] = details

        if not included:
            continue
        metrics.append(_metric_from_spec(spec))

    if not metrics:
        raise ValueError(
            "No runnable metrics were found for this dataset. Resolve field mappings "
            "or required metric configuration before creating a plan."
        )

    fmt = dataset["format"]
    plan = {
        "plan_meta": {
            "plan_id": plan_id,
            "name": name,
            "version": "1.0.0",
            "description": description,
        },
        "applicability": {
            "dataset_formats": [fmt],
        },
        "execution_policy": {
            "fail_fast": False,
            "allow_skips": False,
            "sample_mode": "full",
        },
        "metrics": metrics,
        "plan_taxonomy": build_plan_taxonomy(metrics),
        "plan_creation": {
            "generator": "CBR-Tests plan_builder",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "selection_mode": "all_available" if include_metric_ids is None and not exclude else "filtered",
            "available_metric_count": len(available_ids),
            "candidate_metric_count": len(selected),
            "runnable_metric_count": len(metrics),
            "excluded_unrunnable_metric_count": len(selected) - len(metrics),
            "configuration_status_counts": status_counts,
            "dataset": str(dataset["path"]),
            "dataset_format": fmt,
            "dataset_column_count": len(dataset["columns"]),
            "field_translation": (
                str(dataset["field_translation_path"])
                if dataset["field_translation_path"] is not None
                else None
            ),
        },
    }
    plan["plan_creation"]["reference_dataset"] = (
        str(reference_dataset["path"]) if reference_dataset is not None else None
    )
    plan["plan_creation"]["reference_dataset_format"] = (
        reference_dataset["format"] if reference_dataset is not None else None
    )
    plan["plan_creation"]["reference_field_translation"] = (
        str(reference_dataset["field_translation_path"])
        if reference_dataset is not None and reference_dataset.get("field_translation_path") is not None
        else None
    )
    if service_port_configuration:
        plan["plan_creation"]["service_port_configuration"] = {
            "service_name": str(service_port_configuration.get("service_name", "")),
            "expected_ports": sorted({int(port) for port in service_port_configuration.get("expected_ports", [])}),
            "population_mode": "all_rows",
            "assumption": "candidate capture independently known to represent one service",
        }
    validate_plan_schema(plan)

    report = {
        "available_metric_count": len(available_ids),
        "candidate_metric_count": len(selected),
        "runnable_metric_count": len(metrics),
        "excluded_metric_count": len(selected) - len(metrics),
        "configuration_status_counts": status_counts,
        "metrics": status_by_metric,
        "unlock_actions": build_unlock_actions(status_by_metric, dataset_format=fmt),
        "dataset": str(dataset["path"]),
        "dataset_format": fmt,
        "dataset_columns": dataset["columns"],
        "detected_field_translation": dataset["field_translation"],
        "field_translation_path": (
            str(dataset["field_translation_path"])
            if dataset["field_translation_path"] is not None
            else None
        ),
    }
    report["reference_dataset"] = str(reference_dataset["path"]) if reference_dataset is not None else None
    report["reference_dataset_format"] = reference_dataset["format"] if reference_dataset is not None else None
    report["service_port_configuration"] = service_port_configuration
    return plan, report


def write_plan(path: Path, plan: dict, *, overwrite: bool = False) -> Path:
    """Validate and atomically write a generated plan."""

    validate_plan_schema(plan)
    path = Path(path).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and not overwrite:
        raise FileExistsError(f"Plan already exists: {path}. Use --force to replace it.")

    fd, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent, text=True
    )
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(plan, handle, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, path)
    except Exception:
        temporary_path.unlink(missing_ok=True)
        raise
    return path
