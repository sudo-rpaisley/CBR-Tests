from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from runner.field_translation_schema import (
    FieldTranslationError,
    SIDECAR_SCHEMA_VERSION,
    load_field_translation_from_payload,
    validate_field_translation_payload,
    validate_unique_targets as _validate_unique_targets,
)
from runner.field_translation_sidecar import default_field_translation_path, ensure_field_translation_file

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


def load_field_translation(path: Path | None) -> dict[str, str]:
    """Load a dataset-to-test field translation mapping from a standard sidecar JSON file."""
    if path is None:
        return {}
    with open(path, "r", encoding="utf-8") as f:
        payload: Any = json.load(f)
    if not isinstance(payload, dict):
        raise FieldTranslationError("Field translation file must be a JSON object.")
    return load_field_translation_from_payload(payload)


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


def _invert_translation(dataset_to_test: dict[str, str]) -> dict[str, str]:
    return {test_field: dataset_field for dataset_field, test_field in dataset_to_test.items()}


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


class FieldResolver:
    """Resolve canonical metric field names to dataset column names."""

    def __init__(self, field_translation: dict[str, str], dataset_columns=None):
        self.fields = _invert_translation(field_translation)
        if dataset_columns is not None:
            for column in dataset_columns:
                name = str(column).strip()
                self.fields.setdefault(name, name)

    def resolve(self, field_name: str) -> str:
        """Return the dataset column for a canonical field name when mapped."""
        return self.fields.get(field_name, field_name)

    def translate_value(self, value):
        """Translate strings inside nested metric input requirement values."""
        if isinstance(value, dict):
            return {key: self.translate_value(child) for key, child in value.items()}
        if isinstance(value, list):
            return [self.translate_value(child) for child in value]
        if isinstance(value, str):
            return self.resolve(value)
        return value


def field_resolver(field_translation: dict[str, str], dataset_columns=None) -> dict[str, str]:
    """Build canonical field -> dataset field resolver mapping."""
    return FieldResolver(field_translation, dataset_columns).fields


def translate_metric_fields(metric: dict, field_translation: dict[str, str], dataset_columns=None) -> dict:
    """Return a metric copy with canonical input fields resolved to dataset columns."""
    resolver = FieldResolver(field_translation, dataset_columns)
    translated = dict(metric)
    requirements = metric.get("input_requirements")
    if isinstance(requirements, dict):
        translated["input_requirements"] = resolver.translate_value(requirements)
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


from runner.field_translation_reports import (
    build_field_translation_report,
    field_mapping_details,
    format_field_translation_markdown_report,
    format_field_translation_report,
    suggest_field_mappings,
    write_field_translation_report,
    write_text_report,
)
