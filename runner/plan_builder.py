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
    load_field_translation,
    merge_field_translations,
    read_tabular_dataset_columns,
)
from runner.metric_catalog import build_metric_catalog, required_fields
from runner.pcap_adapter import (
    PCAP_DIRECT_METRICS,
    PCAP_PACKET_COLUMNS,
    PCAP_PACKET_METRICS,
    PCAP_SELF_DERIVED_METRICS,
    pcap_metric_template,
)
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
    return {
        "path": dataset_path,
        "format": dataset_format(dataset_path),
        "columns": columns,
        "available_fields": fields,
        "field_translation": translation,
        "field_translation_path": resolved_translation_path,
    }


def _configuration_state(metric_spec: dict, dataset: dict) -> tuple[str, str | None, list[str]]:
    metric_id = metric_spec["metric_id"]
    template = metric_spec.get("template")
    manual_reason = metric_spec.get("manual_configuration_reason")
    fmt = dataset.get("format")

    if manual_reason:
        return "needs_configuration", manual_reason, []
    if not metric_spec.get("registered_in_taxonomy", False):
        return "needs_configuration", "missing_master_taxonomy_entry", []

    is_pcap = fmt in {"pcap", "pcapng"}
    if is_pcap:
        if metric_id in PCAP_DIRECT_METRICS:
            return "ready", None, []
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
) -> tuple[dict, dict]:
    """Build a plan containing only metrics that can run on the supplied dataset.

    Every discoverable metric is considered unless include/exclude filters narrow
    the candidate set. Metrics that need missing fields, dataset-specific
    configuration, a reference dataset, or a different input format are reported
    but are never written into the generated plan.
    """

    dataset = inspect_dataset(dataset_path, field_translation_path=field_translation_path)
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

        if dataset["format"] in {"pcap", "pcapng"} and metric_id in PCAP_PACKET_METRICS:
            spec = dict(spec)
            spec["template"] = pcap_metric_template(metric_id)

        state, reason, missing = _configuration_state(spec, dataset)
        included = state == "ready"
        status_counts[state] = status_counts.get(state, 0) + 1
        status_by_metric[metric_id] = {
            "status": state,
            "included": included,
            **({"reason": reason} if reason else {}),
            **({"missing_fields": missing} if missing else {}),
        }

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
    validate_plan_schema(plan)

    report = {
        "available_metric_count": len(available_ids),
        "candidate_metric_count": len(selected),
        "runnable_metric_count": len(metrics),
        "excluded_metric_count": len(selected) - len(metrics),
        "configuration_status_counts": status_counts,
        "metrics": status_by_metric,
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
