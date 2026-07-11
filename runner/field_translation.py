from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from runner.field_translation_reports import format_column_section, format_metric_section

PCAP_FIELD_CANDIDATES: dict[str, tuple[str, ...]] = {
    "timestamp": ("frame.time_epoch", "frame.time", "timestamp", "time"),
    "Source IP": ("ip.src", "ipv6.src"),
    "Destination IP": ("ip.dst", "ipv6.dst"),
    "Source Port": ("tcp.srcport", "udp.srcport", "sctp.srcport"),
    "Destination Port": ("tcp.dstport", "udp.dstport", "sctp.dstport"),
    "Protocol": ("ip.proto", "_ws.col.Protocol", "frame.protocols"),
    "Packet Length": ("frame.len", "ip.len"),
    "tcp_flags": ("tcp.flags",),
    "syn_flag_count": ("tcp.flags.syn",),
    "ack_flag_count": ("tcp.flags.ack",),
    "fin_flag_count": ("tcp.flags.fin",),
    "rst_flag_count": ("tcp.flags.reset", "tcp.flags.rst"),
}


def detect_standard_pcap_field_translation(columns) -> dict[str, str]:
    """Detect Wireshark/tshark-style packet columns and map them to test fields.

    Returns a dataset-column -> canonical-test-field mapping. When multiple PCAP
    columns could satisfy the same test field, the first present candidate wins
    to avoid creating duplicate canonical columns.
    """
    column_set = {str(column).strip() for column in columns}
    mapping: dict[str, str] = {}
    for test_field, candidates in PCAP_FIELD_CANDIDATES.items():
        source = next((candidate for candidate in candidates if candidate in column_set), None)
        if source is not None and source != test_field:
            mapping[source] = test_field
    return mapping


def merge_field_translations(automatic: dict[str, str], explicit: dict[str, str]) -> dict[str, str]:
    """Merge automatic and explicit translations, with explicit mappings winning."""
    if not automatic:
        return dict(explicit)
    if not explicit:
        return dict(automatic)

    explicit_sources = set(explicit)
    explicit_targets = set(explicit.values())
    merged = {
        source: target
        for source, target in automatic.items()
        if source not in explicit_sources and target not in explicit_targets
    }
    merged.update(explicit)
    _validate_unique_targets(merged)
    return merged


class FieldTranslationError(ValueError):
    """Raised when a dataset field translation file is invalid or unsafe."""


def load_field_translation(path: Path | None) -> dict[str, str]:
    """Load a dataset-to-test field translation mapping from JSON.

    Preferred file shape::

        {"fields": {"Dataset Column": "canonical_test_column"}}

    The mapping direction is always dataset column name -> field name used by
    test plans/metrics. For readability, files may also use
    ``dataset_to_test_fields`` with the same direction, or ``test_to_dataset_fields``
    in the opposite direction.
    """
    if path is None:
        return {}
    with open(path, "r", encoding="utf-8") as f:
        payload: Any = json.load(f)
    if not isinstance(payload, dict):
        raise FieldTranslationError("Field translation file must be a JSON object.")

    return load_field_translation_from_payload(payload)




def _validate_unique_targets(mapping: dict[str, str]) -> None:
    targets: dict[str, str] = {}
    duplicates: dict[str, list[str]] = {}
    for source, target in mapping.items():
        if target in targets:
            duplicates.setdefault(target, [targets[target]]).append(source)
        targets[target] = source
    if duplicates:
        duplicate_text = ", ".join(f"{target}: {sources}" for target, sources in sorted(duplicates.items()))
        raise FieldTranslationError(f"Multiple dataset fields map to the same test field: {duplicate_text}")


def _validate_no_column_collisions(columns, rename_map: dict[str, str]) -> None:
    original_columns = set(columns)
    renamed_sources = set(rename_map)
    collisions = sorted(
        target
        for target in rename_map.values()
        if target in original_columns and target not in renamed_sources
    )
    if collisions:
        raise FieldTranslationError(
            "Field translation would overwrite existing dataset columns: " + ", ".join(collisions)
        )


def default_field_translation_path(dataset_path: Path) -> Path:
    """Return the sidecar translation path for a dataset."""
    return dataset_path.with_name(f"{dataset_path.stem}.field_translation.json")


def collect_required_test_fields(plan: dict) -> list[str]:
    """Collect canonical field names referenced by enabled metric input requirements."""
    fields: set[str] = set()
    for metric in plan.get("metrics", []):
        if not metric.get("enabled", True):
            continue
        requirements = metric.get("input_requirements", {})
        if isinstance(requirements, dict):
            _collect_required_fields_from_requirements(requirements, fields)
    return sorted(fields)


def ensure_field_translation_file(
    *,
    dataset_path: Path,
    plan: dict,
    detected_dataset_to_test: dict[str, str] | None = None,
) -> Path | None:
    """Create or update the dataset sidecar translation template.

    The sidecar is written next to the dataset as ``<dataset stem>.field_translation.json``.
    It uses ``test_to_dataset_fields`` so every canonical field required by the
    plan is visible to users, even when the dataset column is not known yet.
    """
    field_usage = collect_field_requirements(plan)
    template_fields = sorted(field_usage)
    if not template_fields or not dataset_path.exists():
        return None

    translation_path = default_field_translation_path(dataset_path)
    detected_test_to_dataset = _invert_translation(detected_dataset_to_test or {})
    existing_payload = _load_existing_translation_payload(translation_path)
    existing_test_to_dataset = _payload_to_test_to_dataset_fields(existing_payload)

    if existing_payload:
        missing_template_fields = [field for field in template_fields if field not in existing_test_to_dataset]
        if not missing_template_fields:
            return translation_path
        updated_test_to_dataset = dict(existing_test_to_dataset)
        for field in missing_template_fields:
            updated_test_to_dataset[field] = detected_test_to_dataset.get(field, "")
    else:
        updated_test_to_dataset = {field: detected_test_to_dataset.get(field, "") for field in template_fields}

    payload = {
        "schema_version": SIDECAR_SCHEMA_VERSION,
        "description": "Dataset field translation template. Fill empty values with dataset column names.",
        "dataset": str(dataset_path),
        "plan_id": plan.get("plan_meta", {}).get("plan_id"),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "test_to_dataset_fields": {field: updated_test_to_dataset.get(field, "") for field in sorted(updated_test_to_dataset)},
        "field_metadata": {
            field: {
                "required_by": field_usage.get(field, {}).get("required_by", []),
                "optional_for": field_usage.get(field, {}).get("optional_for", []),
                "mapping_source": "auto_detected" if updated_test_to_dataset.get(field) else "template",
            }
            for field in sorted(updated_test_to_dataset)
        },
    }

    translation_path.parent.mkdir(parents=True, exist_ok=True)
    with open(translation_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
        f.write("\n")
    return translation_path


def detect_standard_pcap_field_translation_for_dataset(dataset_path: Path) -> dict[str, str]:
    """Read tabular headers, when possible, and detect standard PCAP heading mappings."""
    suffix = dataset_path.suffix.lower()
    if suffix not in {".csv", ".tsv", ".xlsx", ".xls"} or not dataset_path.exists():
        return {}

    try:
        import pandas as pd

        if suffix in {".csv", ".tsv"}:
            sep = "," if suffix == ".csv" else "\t"
            columns = pd.read_csv(dataset_path, sep=sep, skipinitialspace=True, nrows=0).columns
        else:
            columns = pd.read_excel(dataset_path, nrows=0).columns
    except Exception:
        return {}

    stripped_columns = [str(column).strip() for column in columns]
    pcap_translation = detect_standard_pcap_field_translation(stripped_columns)
    alias_translation = detect_known_field_translation(stripped_columns)
    return merge_field_translations(alias_translation, pcap_translation)


def _collect_required_fields_from_requirements(requirements: dict, fields: set[str]) -> None:
    ignored_field_keys = {"reference_dataset_path"}
    for key, value in requirements.items():
        if key in ignored_field_keys:
            continue
        if key == "field_map" and isinstance(value, dict):
            for field_name in value.values():
                if isinstance(field_name, str):
                    fields.add(field_name)
            continue
        if key.endswith("_field") and isinstance(value, str):
            fields.add(value)
            continue
        if key.endswith("_fields") or key in {"candidate_fields", "port_fields", "subset_fields", "identifier_fields"}:
            if isinstance(value, list):
                for field_name in value:
                    if isinstance(field_name, str):
                        fields.add(field_name)


def _load_existing_translation_payload(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            payload: Any = json.load(f)
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _payload_to_test_to_dataset_fields(payload: dict[str, Any]) -> dict[str, str]:
    if not payload:
        return {}
    validate_field_translation_payload(payload)
    return {
        str(test_field).strip(): str(dataset_field).strip()
        for test_field, dataset_field in payload["test_to_dataset_fields"].items()
        if str(test_field).strip()
    }


def validate_field_translation_payload(payload: dict[str, Any]) -> None:
    """Validate the standard field translation sidecar payload shape."""
    if not isinstance(payload, dict):
        raise FieldTranslationError("Field translation payload must be a JSON object.")
    schema_version = payload.get("schema_version")
    if schema_version is not None and schema_version != SIDECAR_SCHEMA_VERSION:
        raise FieldTranslationError(f"Unsupported field translation schema_version: {schema_version}")
    mapping = payload.get("test_to_dataset_fields")
    if mapping is None:
        raise FieldTranslationError("Field translation must include test_to_dataset_fields.")
    if not isinstance(mapping, dict):
        raise FieldTranslationError("test_to_dataset_fields must be an object.")
    for test_field, dataset_field in mapping.items():
        if not isinstance(test_field, str) or not isinstance(dataset_field, str):
            raise FieldTranslationError("test_to_dataset_fields entries must map strings to strings.")
        if not test_field.strip():
            raise FieldTranslationError("test_to_dataset_fields cannot include blank canonical field names.")
    metadata = payload.get("field_metadata")
    if metadata is not None:
        if not isinstance(metadata, dict):
            raise FieldTranslationError("field_metadata must be an object when provided.")
        unknown_metadata = set(metadata) - set(mapping)
        if unknown_metadata:
            raise FieldTranslationError(
                "field_metadata contains fields not listed in test_to_dataset_fields: "
                + ", ".join(sorted(unknown_metadata))
            )


def load_field_translation_from_payload(payload: dict[str, Any]) -> dict[str, str]:
    """Load the standard test-to-dataset mapping from an already parsed payload."""
    validate_field_translation_payload(payload)
    raw_mapping = payload["test_to_dataset_fields"]

    mapping: dict[str, str] = {}
    for raw_test_field, raw_dataset_field in raw_mapping.items():
        if not isinstance(raw_test_field, str) or not isinstance(raw_dataset_field, str):
            raise FieldTranslationError("Field translation entries must map strings to strings.")
        test_field = raw_test_field.strip()
        dataset_field = raw_dataset_field.strip()
        if not test_field or not dataset_field or test_field == dataset_field:
            continue
        if dataset_field in mapping:
            raise FieldTranslationError(f"Dataset field is mapped more than once: {dataset_field}")
        mapping[dataset_field] = test_field
    _validate_unique_targets(mapping)
    return mapping


def _invert_translation(dataset_to_test: dict[str, str]) -> dict[str, str]:
    return {test_field: dataset_field for dataset_field, test_field in dataset_to_test.items()}


def collect_required_test_fields_for_metric(metric: dict) -> list[str]:
    """Collect canonical field names referenced by one metric's input requirements."""
    fields: set[str] = set()
    requirements = metric.get("input_requirements", {})
    if isinstance(requirements, dict):
        _collect_required_fields_from_requirements(requirements, fields)
    return sorted(fields)


def read_tabular_dataset_columns(dataset_path: Path) -> list[str]:
    """Read only dataset headers for supported tabular formats."""
    suffix = dataset_path.suffix.lower()
    if suffix not in {".csv", ".tsv", ".xlsx", ".xls"} or not dataset_path.exists():
        return []

    import pandas as pd

    if suffix in {".csv", ".tsv"}:
        sep = "," if suffix == ".csv" else "\t"
        columns = pd.read_csv(dataset_path, sep=sep, skipinitialspace=True, nrows=0).columns
    else:
        columns = pd.read_excel(dataset_path, nrows=0).columns
    return [str(column).strip() for column in columns]


def available_translated_fields(columns, field_translation: dict[str, str]) -> set[str]:
    """Return canonical fields available before metrics run."""
    column_set = {str(column).strip() for column in columns}
    available = set(column_set)
    for source, target in field_translation.items():
        if source in column_set:
            available.add(target)
    return available


def metrics_missing_required_fields(metrics: list[dict], available_fields: set[str]) -> dict[str, list[str]]:
    """Return required fields unavailable for each metric."""
    missing_by_metric: dict[str, list[str]] = {}
    for metric in metrics:
        required = collect_field_requirements_for_metric(metric)["required"]
        missing = [field for field in required if field not in available_fields]
        if missing:
            missing_by_metric[metric["metric_id"]] = missing
    return missing_by_metric

SIDECAR_SCHEMA_VERSION = 1

COMMON_FIELD_CANDIDATES: dict[str, tuple[str, ...]] = {
    "Source IP": ("Source IP", "Src IP", "source_ip", "src_ip", "ip.src", "ipv6.src"),
    "Destination IP": ("Destination IP", "Dst IP", "destination_ip", "dst_ip", "ip.dst", "ipv6.dst"),
    "Source Port": ("Source Port", "Src Port", "source_port", "src_port", "tcp.srcport", "udp.srcport"),
    "Destination Port": ("Destination Port", "Dst Port", "destination_port", "dst_port", "tcp.dstport", "udp.dstport"),
    "Protocol": ("Protocol", "protocol", "ip.proto", "_ws.col.Protocol", "frame.protocols"),
    "timestamp": ("timestamp", "Timestamp", "time", "frame.time_epoch", "frame.time"),
    "label": ("label", "Label", "class", "Class"),
    "slice": ("slice", "Slice", "slice_id", "Slice ID"),
}


def detect_known_field_translation(columns, required_fields: list[str] | None = None) -> dict[str, str]:
    """Detect common dataset aliases and map them to canonical test fields."""
    column_set = {str(column).strip() for column in columns}
    wanted = set(required_fields or COMMON_FIELD_CANDIDATES.keys())
    mapping: dict[str, str] = {}
    for test_field in wanted:
        candidates = COMMON_FIELD_CANDIDATES.get(test_field, (test_field,))
        source = next((candidate for candidate in candidates if candidate in column_set), None)
        if source is not None and source != test_field:
            mapping[source] = test_field
    return mapping


def collect_field_requirements_for_metric(metric: dict) -> dict[str, list[str]]:
    """Return required and optional canonical fields for one metric."""
    explicit = metric.get("field_requirements")
    required: set[str] = set()
    optional: set[str] = set()
    if isinstance(explicit, list):
        required.update(field for field in explicit if isinstance(field, str))
    elif isinstance(explicit, dict):
        req = explicit.get("required", [])
        opt = explicit.get("optional", [])
        if isinstance(req, list):
            required.update(field for field in req if isinstance(field, str))
        if isinstance(opt, list):
            optional.update(field for field in opt if isinstance(field, str))

    if not required:
        required.update(collect_required_test_fields_for_metric(metric))
    return {"required": sorted(required), "optional": sorted(optional - required)}


def collect_field_requirements(plan: dict) -> dict[str, dict[str, list[str]]]:
    """Collect required/optional field usage across enabled plan metrics."""
    usage: dict[str, dict[str, list[str]]] = {}
    for metric in plan.get("metrics", []):
        if not metric.get("enabled", True):
            continue
        metric_id = metric["metric_id"]
        requirements = collect_field_requirements_for_metric(metric)
        for field in requirements["required"]:
            usage.setdefault(field, {"required_by": [], "optional_for": []})["required_by"].append(metric_id)
        for field in requirements["optional"]:
            usage.setdefault(field, {"required_by": [], "optional_for": []})["optional_for"].append(metric_id)
    return {field: {k: sorted(v) for k, v in details.items()} for field, details in sorted(usage.items())}


def field_resolver(field_translation: dict[str, str], dataset_columns=None) -> dict[str, str]:
    """Build canonical field -> dataset field resolver mapping."""
    resolver = _invert_translation(field_translation)
    if dataset_columns is not None:
        for column in dataset_columns:
            name = str(column).strip()
            resolver.setdefault(name, name)
    return resolver


def translate_metric_fields(metric: dict, field_translation: dict[str, str], dataset_columns=None) -> dict:
    """Return a metric copy with canonical input fields resolved to dataset columns."""
    resolver = field_resolver(field_translation, dataset_columns)
    translated = dict(metric)
    requirements = metric.get("input_requirements")
    if isinstance(requirements, dict):
        translated["input_requirements"] = _translate_requirement_value(requirements, resolver)
    return translated


def metrics_missing_optional_fields(metrics: list[dict], available_fields: set[str]) -> dict[str, list[str]]:
    """Return optional fields unavailable for each metric."""
    missing_by_metric: dict[str, list[str]] = {}
    for metric in metrics:
        optional = collect_field_requirements_for_metric(metric)["optional"]
        missing = [field for field in optional if field not in available_fields]
        if missing:
            missing_by_metric[metric["metric_id"]] = missing
    return missing_by_metric


def _normalise_field_name(value: str) -> str:
    return "".join(ch.lower() for ch in value if ch.isalnum())


def suggest_field_mappings(fields: list[str], columns: list[str]) -> dict[str, list[str]]:
    """Suggest dataset columns for unmapped canonical fields using loose name matching."""
    suggestions: dict[str, list[str]] = {}
    normalised_columns = {column: _normalise_field_name(column) for column in columns}
    for field in fields:
        field_key = _normalise_field_name(field)
        matches = [
            column
            for column, column_key in normalised_columns.items()
            if field_key and (field_key in column_key or column_key in field_key)
        ]
        if matches:
            suggestions[field] = sorted(matches)[:5]
    return suggestions


def field_mapping_details(
    *,
    detected_translation: dict[str, str] | None,
    explicit_translation: dict[str, str] | None,
    field_translation: dict[str, str] | None = None,
) -> dict[str, dict[str, str]]:
    """Return canonical-field mapping details for reports."""
    details: dict[str, dict[str, str]] = {}
    for source, target in (detected_translation or {}).items():
        details[target] = {"dataset_field": source, "mapping_source": "auto_detected"}
    for source, target in (explicit_translation or {}).items():
        details[target] = {"dataset_field": source, "mapping_source": "explicit"}
    for source, target in (field_translation or {}).items():
        details.setdefault(target, {"dataset_field": source, "mapping_source": "merged"})
    return dict(sorted(details.items()))


def build_field_translation_report(
    *,
    dataset_path: Path,
    translation_path: Path | None,
    plan: dict,
    metrics: list[dict],
    available_fields: set[str],
    skipped_metrics: dict[str, list[str]],
    dataset_columns: list[str] | None = None,
    detected_translation: dict[str, str] | None = None,
    explicit_translation: dict[str, str] | None = None,
    field_translation: dict[str, str] | None = None,
    sidecar_status: str | None = None,
    missing_optional_fields: dict[str, list[str]] | None = None,
) -> dict:
    """Build a machine-readable field translation validation report."""
    usage = collect_field_requirements(plan)
    dataset_column_set = set(dataset_columns or [])
    mapped_sources = set((field_translation or {}).keys())
    directly_used_columns = dataset_column_set & set(usage)
    unused_dataset_columns = sorted(dataset_column_set - mapped_sources - directly_used_columns)
    missing_fields = sorted({field for fields in skipped_metrics.values() for field in fields})
    optional_missing = missing_optional_fields or {}
    optional_missing_fields = sorted({field for fields in optional_missing.values() for field in fields})
    return {
        "schema_version": SIDECAR_SCHEMA_VERSION,
        "dataset": str(dataset_path),
        "translation_file": str(translation_path) if translation_path else None,
        "sidecar_status": sidecar_status or "unknown",
        "dataset_columns": sorted(dataset_columns or []),
        "unused_dataset_columns": unused_dataset_columns,
        "available_fields": sorted(available_fields),
        "detected_mappings": dict(sorted((detected_translation or {}).items())),
        "explicit_mappings": dict(sorted((explicit_translation or {}).items())),
        "field_mappings": field_mapping_details(
            detected_translation=detected_translation,
            explicit_translation=explicit_translation,
            field_translation=field_translation,
        ),
        "field_usage": usage,
        "missing_optional_fields": optional_missing,
        "mapping_suggestions": suggest_field_mappings(missing_fields + optional_missing_fields, dataset_columns or []),
        "metrics": {
            metric["metric_id"]: {
                "status": "skipped" if metric["metric_id"] in skipped_metrics else "runnable",
                "missing_fields": skipped_metrics.get(metric["metric_id"], []),
                "missing_optional_fields": optional_missing.get(metric["metric_id"], []),
            }
            for metric in metrics
        },
        "skipped_metrics": [
            {"metric_id": metric_id, "reason": "missing_field_mappings", "missing_fields": fields}
            for metric_id, fields in sorted(skipped_metrics.items())
        ],
    }


def write_field_translation_report(path: Path, report: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)
        f.write("\n")


def format_field_translation_report(report: dict, use_color: bool = False) -> str:
    """Format a field translation report for humans."""
    lines = [
        "Field translation report",
        f"Dataset: {report.get('dataset')}",
        f"Translation file: {report.get('translation_file') or 'n/a'}",
        f"Sidecar status: {report.get('sidecar_status', 'unknown')}",
        "",
        *format_metric_section(report, use_color=use_color),
    ]
    skipped = report.get("skipped_metrics", [])
    lines.extend(["", f"Skipped metric count: {len(skipped)}"])
    detected = report.get("detected_mappings", {})
    explicit = report.get("explicit_mappings", {})
    if detected:
        lines.append("Detected mappings:")
        for source, target in sorted(detected.items()):
            lines.append(f"  {target} <- {source}")
    if explicit:
        lines.append("Explicit mappings:")
        for source, target in sorted(explicit.items()):
            lines.append(f"  {target} <- {source}")
    suggestions = report.get("mapping_suggestions", {})
    if suggestions:
        lines.append("Mapping suggestions:")
        for field, columns in sorted(suggestions.items()):
            lines.append(f"  {field}: {', '.join(columns)}")
    unused = report.get("unused_dataset_columns", [])
    if unused:
        lines.extend(["", *format_column_section("Unused dataset columns", unused)])
    return "\n".join(lines)


def format_field_translation_markdown_report(report: dict) -> str:
    """Format a field translation report as Markdown."""
    lines = [
        "# Field translation report",
        "",
        f"- Dataset: `{report.get('dataset')}`",
        f"- Translation file: `{report.get('translation_file') or 'n/a'}`",
        f"- Sidecar status: `{report.get('sidecar_status', 'unknown')}`",
        "",
        "## Metrics",
        "",
        "| Metric | Status | Missing required | Missing optional |",
        "|---|---|---|---|",
    ]
    for metric_id, details in sorted(report.get("metrics", {}).items()):
        lines.append(
            f"| `{metric_id}` | {details.get('status', 'unknown')} | "
            f"{', '.join(details.get('missing_fields', [])) or '-'} | "
            f"{', '.join(details.get('missing_optional_fields', [])) or '-'} |"
        )
    if report.get("mapping_suggestions"):
        lines.extend(["", "## Mapping suggestions", ""] )
        for field, columns in sorted(report["mapping_suggestions"].items()):
            lines.append(f"- `{field}`: {', '.join(f'`{column}`' for column in columns)}")
    return "\n".join(lines)


def write_text_report(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text + "\n", encoding="utf-8")


def _translate_requirement_value(value, resolver: dict[str, str]):
    if isinstance(value, dict):
        return {key: _translate_requirement_value(child, resolver) for key, child in value.items()}
    if isinstance(value, list):
        return [_translate_requirement_value(child, resolver) for child in value]
    if isinstance(value, str):
        return resolver.get(value, value)
    return value
