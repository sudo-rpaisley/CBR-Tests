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
    detect_standard_pcap_field_translation_for_dataset,
    read_tabular_dataset_columns,
)
from runner.metric_catalog import (
    PCAP_ONLY_METRICS,
    build_metric_catalog,
    required_fields,
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


def inspect_dataset(dataset_path: Path | None) -> dict:
    if dataset_path is None:
        return {
            "path": None,
            "format": None,
            "columns": [],
            "available_fields": set(),
            "field_translation": {},
        }

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
    translation = (
        detect_standard_pcap_field_translation_for_dataset(dataset_path)
        if suffix in TABULAR_SUFFIXES
        else {}
    )
    fields = available_translated_fields(columns, translation) if columns else set()
    return {
        "path": dataset_path,
        "format": dataset_format(dataset_path),
        "columns": columns,
        "available_fields": fields,
        "field_translation": translation,
    }


def _configuration_state(metric_spec: dict, dataset: dict) -> tuple[str, str | None, list[str]]:
    metric_id = metric_spec["metric_id"]
    template = metric_spec.get("template")
    manual_reason = metric_spec.get("manual_configuration_reason")
    dataset_path = dataset.get("path")
    fmt = dataset.get("format")

    if manual_reason:
        return "needs_configuration", manual_reason, []
    if not metric_spec.get("registered_in_taxonomy", False):
        return "needs_configuration", "missing_master_taxonomy_entry", []
    if dataset_path is None:
        return "needs_dataset", "dataset_not_supplied", []

    is_pcap = fmt in {"pcap", "pcapng"}
    if is_pcap:
        if metric_id in PCAP_ONLY_METRICS:
            return "ready", None, []
        return "not_applicable", "tabular_metric_on_packet_capture", []

    if metric_id in PCAP_ONLY_METRICS:
        return "not_applicable", "packet_capture_metric_on_tabular_dataset", []
    if template is None:
        return "needs_configuration", "no_metric_template_available", []

    required = required_fields(template)
    available = dataset.get("available_fields", set())
    missing = [field for field in required if field not in available]
    if missing:
        return "needs_mapping", "required_fields_not_resolved", missing
    return "ready", None, []


def _metric_from_spec(metric_spec: dict, *, enabled: bool, state: str, reason: str | None, missing: list[str]) -> dict:
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

    metric["enabled"] = enabled
    metric["configuration"] = {"status": state}
    if reason:
        metric["configuration"]["reason"] = reason
    if missing:
        metric["configuration"]["missing_fields"] = missing
    return metric


def build_plan(
    *,
    plan_id: str,
    name: str,
    description: str = "Automatically generated CBR-Tests plan.",
    dataset_path: Path | None = None,
    include_metric_ids: Iterable[str] | None = None,
    exclude_metric_ids: Iterable[str] | None = None,
    enable_unready: bool = False,
) -> tuple[dict, dict]:
    dataset = inspect_dataset(dataset_path)
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
        state, reason, missing = _configuration_state(spec, dataset)
        enabled = state == "ready" or (enable_unready and state != "not_applicable")
        metric = _metric_from_spec(spec, enabled=enabled, state=state, reason=reason, missing=missing)
        metrics.append(metric)
        status_counts[state] = status_counts.get(state, 0) + 1
        status_by_metric[metric_id] = {
            "status": state,
            "enabled": enabled,
            **({"reason": reason} if reason else {}),
            **({"missing_fields": missing} if missing else {}),
        }

    fmt = dataset.get("format")
    applicability_formats = [fmt] if fmt else ["csv", "tsv", "xlsx", "xls", "pcap", "pcapng"]
    plan = {
        "plan_meta": {
            "plan_id": plan_id,
            "name": name,
            "version": "1.0.0",
            "description": description,
        },
        "applicability": {
            "dataset_formats": applicability_formats,
        },
        "execution_policy": {
            "fail_fast": False,
            "allow_skips": True,
            "sample_mode": "full",
        },
        "metrics": metrics,
        "plan_taxonomy": build_plan_taxonomy(metrics),
        "plan_creation": {
            "generator": "CBR-Tests plan_builder",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "selection_mode": "all_available" if include_metric_ids is None and not exclude else "filtered",
            "available_metric_count": len(available_ids),
            "selected_metric_count": len(metrics),
            "configuration_status_counts": status_counts,
            "dataset": str(dataset["path"]) if dataset["path"] is not None else None,
            "dataset_format": fmt,
            "dataset_column_count": len(dataset["columns"]),
        },
    }
    validate_plan_schema(plan)

    report = {
        "available_metric_count": len(available_ids),
        "selected_metric_count": len(metrics),
        "enabled_metric_count": sum(metric.get("enabled", True) for metric in metrics),
        "configuration_status_counts": status_counts,
        "metrics": status_by_metric,
        "dataset": str(dataset["path"]) if dataset["path"] is not None else None,
        "dataset_format": fmt,
        "dataset_columns": dataset["columns"],
        "detected_field_translation": dataset["field_translation"],
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
