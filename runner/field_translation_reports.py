from __future__ import annotations

import json
import re
import shutil
import unicodedata
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


ANSI_ESCAPE_RE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
MAX_COLUMN_CELL_WIDTH = 40


def _display_width(value: str) -> int:
    """Return terminal display width for a string, ignoring ANSI escapes."""
    width = 0
    for character in ANSI_ESCAPE_RE.sub("", value):
        if unicodedata.combining(character):
            continue
        width += 2 if unicodedata.east_asian_width(character) in {"F", "W"} else 1
    return width


def _display_ljust(value: str, width: int) -> str:
    """Left-pad based on display width rather than Python character count."""
    return value + " " * max(0, width - _display_width(value))


def _split_display_width(value: str, width: int) -> list[str]:
    """Split a string into display-width-limited chunks."""
    if width <= 0 or not value:
        return [value]

    chunks: list[str] = []
    current: list[str] = []
    current_width = 0
    for character in value:
        if unicodedata.combining(character):
            character_width = 0
        elif unicodedata.east_asian_width(character) in {"F", "W"}:
            character_width = 2
        else:
            character_width = 1
        if current and current_width + character_width > width:
            chunks.append("".join(current))
            current = []
            current_width = 0
        current.append(character)
        current_width += character_width
    if current:
        chunks.append("".join(current))
    return chunks or [""]


def _wrap_display_width(value: str, width: int) -> list[str]:
    """Wrap a value on spaces when possible, falling back to display-width chunks."""
    if _display_width(value) <= width:
        return [value]

    lines: list[str] = []
    current = ""
    for word in value.split(" "):
        if not word:
            continue
        candidate = word if not current else f"{current} {word}"
        if _display_width(candidate) <= width:
            current = candidate
            continue
        if current:
            lines.append(current)
        if _display_width(word) > width:
            word_chunks = _split_display_width(word, width)
            lines.extend(word_chunks[:-1])
            current = word_chunks[-1]
        else:
            current = word
    if current:
        lines.append(current)
    return lines or [value]


def format_column_grid(items: list[str], *, indent: int = 2, max_width: int | None = None) -> list[str]:
    """Format values as a display-width-aware grid without a section title."""
    if not items:
        return []

    if max_width is None:
        max_width = shutil.get_terminal_size(fallback=(100, 24)).columns

    prefix = " " * indent
    available_width = max(20, max_width - indent)
    wrap_width = min(MAX_COLUMN_CELL_WIDTH, available_width)
    wrapped_items = [_wrap_display_width(item, wrap_width) for item in items]
    widest_segment = max(_display_width(segment) for item in wrapped_items for segment in item)
    cell_width = min(max(widest_segment + 2, 18), available_width)
    column_count = max(1, available_width // cell_width)
    row_count = (len(items) + column_count - 1) // column_count
    lines: list[str] = []

    for row in range(row_count):
        row_cells = []
        for column in range(column_count):
            index = column * row_count + row
            if index < len(wrapped_items):
                row_cells.append(wrapped_items[index])
        row_height = max(len(cell) for cell in row_cells)
        for cell_line in range(row_height):
            cells = []
            for cell in row_cells:
                segment = cell[cell_line] if cell_line < len(cell) else ""
                cells.append(_display_ljust(segment, cell_width))
            lines.append(prefix + "".join(cells).rstrip())
    return lines


def format_column_section(title: str, items: list[str], *, indent: int = 2, max_width: int | None = None) -> list[str]:
    """Format a long list as a readable fixed-width column section.

    By default, use the current terminal width so the report displays as many
    columns as will fit on the user's display. Long names are wrapped inside
    cells, and width calculations use terminal display width for better Unicode
    alignment. A ``max_width`` can still be provided by tests or callers that
    need deterministic wrapping.
    """
    if not items:
        return []
    return [f"{title} ({len(items)}):", *format_column_grid(items, indent=indent, max_width=max_width)]


def _metric_detail_entry(metric_id: str, details: dict) -> str:
    """Format a metric name with any available status details."""
    details_text = []
    missing = details.get("missing_fields", [])
    optional = details.get("missing_optional_fields", [])
    error = details.get("error")
    if missing:
        details_text.append(f"missing {', '.join(missing)}")
    if optional:
        details_text.append(f"optional missing {', '.join(optional)}")
    if error:
        details_text.append(str(error))
    if details_text:
        return f"{metric_id}: {'; '.join(details_text)}"
    return metric_id


def _status_title(status: str) -> str:
    return status.replace("_", " ").strip().title() or "Unknown"


def format_metric_section(report: dict, use_color: bool = False, *, max_width: int | None = None) -> list[str]:
    """Format metric statuses as non-empty category sections."""
    runnable: list[str] = []
    runnable_with_optional_missing: list[str] = []
    skipped: list[str] = []
    other_statuses: dict[str, list[str]] = {}

    for metric_id, details in sorted(report.get("metrics", {}).items()):
        status = details.get("status", "unknown")
        optional = details.get("missing_optional_fields", [])
        if status == "runnable" and optional:
            runnable_with_optional_missing.append(_metric_detail_entry(metric_id, details))
        elif status == "runnable":
            runnable.append(metric_id)
        elif status == "skipped":
            skipped.append(_metric_detail_entry(metric_id, details))
        else:
            other_statuses.setdefault(status, []).append(_metric_detail_entry(metric_id, details))

    lines = ["Metrics:"]
    if runnable:
        lines.extend(format_column_section("Runnable metrics", runnable, max_width=max_width))
    if runnable_with_optional_missing:
        lines.extend(
            format_column_section(
                "Runnable metrics with missing optional fields",
                runnable_with_optional_missing,
                max_width=max_width,
            )
        )
    if skipped:
        lines.extend(format_column_section("Skipped metrics", skipped, max_width=max_width))
    for status, items in sorted(other_statuses.items()):
        lines.extend(format_column_section(f"{_status_title(status)} metrics", items, max_width=max_width))
    return lines


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


