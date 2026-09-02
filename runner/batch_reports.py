from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any


REFERENCE_SUFFIX = "_from_reference"


def _load_json(path: Path) -> dict | None:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _is_scalar(value: Any) -> bool:
    return value is None or isinstance(value, (str, int, float, bool))


def _metric_record_map(outcome: dict) -> dict[str, dict]:
    records: dict[str, dict] = {}
    for record in outcome.get("metric_results", []):
        if not isinstance(record, dict):
            continue
        metric_id = record.get("metric_id")
        if isinstance(metric_id, str) and metric_id:
            records[metric_id] = record
    return records


def extract_primary_metric_value(metric_id: str, test_result: Any) -> tuple[Any, Any, dict]:
    """Return a stable scalar comparison value, optional max value and summary.

    Reference-comparison implementations expose either ``summary[metric_id]``
    or ``summary['mean_' + metric_id]`` as their batch-comparable scalar.  The
    latter is used by feature-wise metrics that also retain per-field detail.
    """

    if _is_scalar(test_result):
        return test_result, None, {}
    if not isinstance(test_result, dict):
        return None, None, {}

    raw_summary = test_result.get("summary", {})
    summary = raw_summary if isinstance(raw_summary, dict) else {}
    candidates = (
        metric_id,
        f"mean_{metric_id}",
    )
    value = None
    for key in candidates:
        candidate = summary.get(key)
        if _is_scalar(candidate) and candidate is not None:
            value = candidate
            break
    if value is None:
        candidate = test_result.get(metric_id)
        if _is_scalar(candidate):
            value = candidate

    max_value = summary.get(f"max_{metric_id}")
    if not _is_scalar(max_value):
        max_value = None
    return value, max_value, summary


def _unique_labels(paths: list[str]) -> dict[str, str]:
    """Create compact labels while disambiguating duplicate file names."""

    path_objects = [Path(path) for path in paths]
    counts: dict[str, int] = {}
    for path in path_objects:
        counts[path.name] = counts.get(path.name, 0) + 1

    labels: dict[str, str] = {}
    used: set[str] = set()
    for path in path_objects:
        resolved = str(path)
        label = path.name
        if counts[path.name] > 1:
            label = f"{path.parent.name}/{path.name}" if path.parent.name else resolved
        if label in used:
            label = resolved
        used.add(label)
        labels[resolved] = label
    return labels


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    return path


def _format_markdown_value(value: Any) -> str:
    if value is None or value == "":
        return "—"
    if isinstance(value, float):
        return f"{value:.6g}"
    return str(value)


def _markdown_table(row_header: str, row_labels: list[str], column_labels: list[str], cells: dict[tuple[str, str], str]) -> list[str]:
    lines = [
        "| " + row_header + " | " + " | ".join(column_labels) + " |",
        "|---|" + "---:|" * len(column_labels),
    ]
    for row in row_labels:
        values = [cells.get((row, column), "—") for column in column_labels]
        lines.append("| " + row + " | " + " | ".join(values) + " |")
    return lines


def write_comparison_reports(
    *,
    output_dir: Path,
    timestamp: str,
    batch_meta: dict,
    results: list[dict],
) -> dict[str, Any]:
    """Write human-readable and analysis-friendly candidate/reference reports.

    Only jobs with an explicit reference dataset participate.  Existing JSON
    outcomes remain authoritative; these files are denormalised views intended
    for comparison, spreadsheets and statistical analysis.
    """

    comparison_jobs = [
        result for result in results
        if result.get("reference_dataset_path")
    ]
    if not comparison_jobs:
        return {}

    candidate_paths = list(dict.fromkeys(str(item["dataset_path"]) for item in comparison_jobs))
    reference_paths = list(dict.fromkeys(str(item["reference_dataset_path"]) for item in comparison_jobs))
    candidate_labels = _unique_labels(candidate_paths)
    reference_labels = _unique_labels(reference_paths)

    loaded: list[dict] = []
    metric_ids: set[str] = set()
    for job in comparison_jobs:
        output_path = Path(str(job["output_path"]))
        outcome = _load_json(output_path)
        metric_records = _metric_record_map(outcome or {})
        test_results = (outcome or {}).get("test_results", {})
        if not isinstance(test_results, dict):
            test_results = {}
        job_metric_ids = {
            metric_id
            for metric_id in (outcome or {}).get("metric_ids", [])
            if isinstance(metric_id, str) and metric_id.endswith(REFERENCE_SUFFIX)
        }
        job_metric_ids.update(
            metric_id for metric_id in test_results
            if isinstance(metric_id, str) and metric_id.endswith(REFERENCE_SUFFIX)
        )
        job_metric_ids.update(
            metric_id for metric_id in metric_records
            if metric_id.endswith(REFERENCE_SUFFIX)
        )
        metric_ids.update(job_metric_ids)
        loaded.append(
            {
                "job": job,
                "outcome": outcome,
                "metric_records": metric_records,
                "test_results": test_results,
                "metric_ids": job_metric_ids,
            }
        )

    sorted_metric_ids = sorted(metric_ids)
    long_rows: list[dict] = []
    overview_rows: list[dict] = []
    pair_values: dict[tuple[str, str, str], dict] = {}

    for item in loaded:
        job = item["job"]
        candidate_path = str(job["dataset_path"])
        reference_path = str(job["reference_dataset_path"])
        candidate = candidate_labels[candidate_path]
        reference = reference_labels[reference_path]
        outcome = item["outcome"] or {}
        overview = {
            "candidate": candidate,
            "reference": reference,
            "candidate_path": candidate_path,
            "reference_path": reference_path,
            "job_id": job.get("job_id"),
            "outcome_status": job.get("outcome_status"),
            "output_path": job.get("output_path"),
        }

        for metric_id in sorted_metric_ids:
            record = item["metric_records"].get(metric_id, {})
            test_result = item["test_results"].get(metric_id)
            value, max_value, summary = extract_primary_metric_value(metric_id, test_result)
            execution_status = record.get("status")
            result_status = record.get("result_status")
            overview[metric_id] = value
            overview[f"{metric_id}__result_status"] = result_status
            pair_values[(candidate, reference, metric_id)] = {
                "value": value,
                "execution_status": execution_status,
                "result_status": result_status,
            }
            long_rows.append(
                {
                    "candidate": candidate,
                    "reference": reference,
                    "candidate_path": candidate_path,
                    "reference_path": reference_path,
                    "job_id": job.get("job_id"),
                    "outcome_status": job.get("outcome_status"),
                    "metric_id": metric_id,
                    "execution_status": execution_status,
                    "result_status": result_status,
                    "primary_value": value,
                    "max_value": max_value,
                    "summary_json": json.dumps(summary, sort_keys=True, allow_nan=False) if summary else "",
                    "outcome_path": job.get("output_path"),
                }
            )
        overview_rows.append(overview)

    overview_fields = [
        "candidate",
        "reference",
        "candidate_path",
        "reference_path",
        "job_id",
        "outcome_status",
        "output_path",
    ]
    for metric_id in sorted_metric_ids:
        overview_fields.extend([metric_id, f"{metric_id}__result_status"])

    overview_path = output_dir / f"comparison_overview_{timestamp}.csv"
    long_path = output_dir / f"comparison_long_{timestamp}.csv"
    _write_csv(overview_path, overview_fields, overview_rows)
    _write_csv(
        long_path,
        [
            "candidate",
            "reference",
            "candidate_path",
            "reference_path",
            "job_id",
            "outcome_status",
            "metric_id",
            "execution_status",
            "result_status",
            "primary_value",
            "max_value",
            "summary_json",
            "outcome_path",
        ],
        long_rows,
    )

    matrices_dir = output_dir / f"comparison_matrices_{timestamp}"
    matrices_dir.mkdir(parents=True, exist_ok=True)
    matrix_paths: dict[str, str] = {}
    row_labels = [candidate_labels[path] for path in candidate_paths]
    column_labels = [reference_labels[path] for path in reference_paths]

    status_rows = []
    for candidate_path in candidate_paths:
        row = {"candidate": candidate_labels[candidate_path]}
        for reference_path in reference_paths:
            matching = next(
                (
                    item["job"] for item in loaded
                    if str(item["job"]["dataset_path"]) == candidate_path
                    and str(item["job"]["reference_dataset_path"]) == reference_path
                ),
                None,
            )
            row[reference_labels[reference_path]] = matching.get("outcome_status") if matching else ""
        status_rows.append(row)
    status_matrix_path = matrices_dir / "overall_status.csv"
    _write_csv(status_matrix_path, ["candidate", *column_labels], status_rows)

    for metric_id in sorted_metric_ids:
        rows = []
        for candidate in row_labels:
            row = {"candidate": candidate}
            for reference in column_labels:
                row[reference] = pair_values.get((candidate, reference, metric_id), {}).get("value")
            rows.append(row)
        matrix_path = matrices_dir / f"{metric_id}.csv"
        _write_csv(matrix_path, ["candidate", *column_labels], rows)
        matrix_paths[metric_id] = str(matrix_path)

    markdown_path = output_dir / f"comparison_report_{timestamp}.md"
    markdown_lines = [
        f"# Comparison report — {batch_meta.get('name') or batch_meta.get('batch_id', 'batch')}",
        "",
        f"- Candidates: {len(candidate_paths)}",
        f"- References: {len(reference_paths)}",
        f"- Comparison jobs represented: {len(comparison_jobs)}",
        f"- Reference metrics: {len(sorted_metric_ids)}",
        "",
        "The JSON outcomes remain authoritative. CSV and Markdown files are denormalised comparison views.",
        "For the current reference-distance/divergence metrics, lower values generally indicate a closer match; consult the metric documentation for exact interpretation.",
        "",
        "## Overall run status",
        "",
    ]
    status_cells: dict[tuple[str, str], str] = {}
    for item in loaded:
        candidate = candidate_labels[str(item["job"]["dataset_path"])]
        reference = reference_labels[str(item["job"]["reference_dataset_path"])]
        status_cells[(candidate, reference)] = str(item["job"].get("outcome_status") or "unknown")
    markdown_lines.extend(_markdown_table("Candidate", row_labels, column_labels, status_cells))

    for metric_id in sorted_metric_ids:
        markdown_lines.extend(["", f"## `{metric_id}`", ""])
        cells: dict[tuple[str, str], str] = {}
        for candidate in row_labels:
            for reference in column_labels:
                detail = pair_values.get((candidate, reference, metric_id), {})
                value = _format_markdown_value(detail.get("value"))
                result_status = detail.get("result_status")
                execution_status = detail.get("execution_status")
                if result_status:
                    value = f"{value} ({result_status})"
                elif execution_status and execution_status != "success":
                    value = f"{value} ({execution_status})"
                cells[(candidate, reference)] = value
        markdown_lines.extend(_markdown_table("Candidate", row_labels, column_labels, cells))

    markdown_path.write_text("\n".join(markdown_lines) + "\n", encoding="utf-8")

    return {
        "comparison_overview_csv": str(overview_path),
        "comparison_long_csv": str(long_path),
        "comparison_markdown": str(markdown_path),
        "comparison_matrices_directory": str(matrices_dir),
        "overall_status_matrix_csv": str(status_matrix_path),
        "metric_matrix_csvs": matrix_paths,
        "reference_metric_ids": sorted_metric_ids,
    }
