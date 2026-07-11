from __future__ import annotations

import json
from pathlib import Path

SIDECAR_SCHEMA_VERSION = 1

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
    from runner.field_translation import collect_field_requirements

    usage = collect_field_requirements(plan)
    mapped_sources = set((field_translation or {}).keys())
    missing_fields = sorted({field for fields in skipped_metrics.values() for field in fields})
    optional_missing = missing_optional_fields or {}
    optional_missing_fields = sorted({field for fields in optional_missing.values() for field in fields})
    return {
        "schema_version": SIDECAR_SCHEMA_VERSION,
        "dataset": str(dataset_path),
        "translation_file": str(translation_path) if translation_path else None,
        "sidecar_status": sidecar_status or "unknown",
        "dataset_columns": sorted(dataset_columns or []),
        "unused_dataset_columns": sorted(set(dataset_columns or []) - mapped_sources),
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


def format_column_section(title: str, items: list[str], *, indent: int = 2, max_width: int = 100) -> list[str]:
    """Format a long list as readable fixed-width columns."""
    if not items:
        return []

    prefix = " " * indent
    available_width = max(20, max_width - indent)
    cell_width = min(max(max(len(item) for item in items) + 2, 18), available_width)
    column_count = max(1, available_width // cell_width)
    row_count = (len(items) + column_count - 1) // column_count
    lines = [f"{title} ({len(items)}):"]

    for row in range(row_count):
        cells = []
        for column in range(column_count):
            index = column * row_count + row
            if index < len(items):
                cells.append(items[index].ljust(cell_width))
        lines.append(prefix + "".join(cells).rstrip())
    return lines


def format_field_translation_report(report: dict, use_color: bool = False) -> str:
    """Format a field translation report for humans."""
    yellow = "\033[33m" if use_color else ""
    green = "\033[32m" if use_color else ""
    reset = "\033[0m" if use_color else ""
    lines = [
        "Field translation report",
        f"Dataset: {report.get('dataset')}",
        f"Translation file: {report.get('translation_file') or 'n/a'}",
        f"Sidecar status: {report.get('sidecar_status', 'unknown')}",
        "",
        "Metrics:",
    ]
    for metric_id, details in sorted(report.get("metrics", {}).items()):
        status = details.get("status", "unknown")
        missing = details.get("missing_fields", [])
        optional = details.get("missing_optional_fields", [])
        if status == "skipped":
            lines.append(f"  {yellow}[SKIPPED]{reset} {metric_id}: missing {', '.join(missing)}")
        elif optional:
            lines.append(f"  {green}[RUNNABLE]{reset} {metric_id}: optional missing {', '.join(optional)}")
        else:
            lines.append(f"  {green}[RUNNABLE]{reset} {metric_id}")
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


