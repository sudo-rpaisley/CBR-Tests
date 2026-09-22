from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Iterable


REFERENCE_SUFFIX = "_from_reference"
DOMAIN_STATUSES = ("pass", "warn", "fail", "not_applicable")


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


def _markdown_table(
    row_header: str,
    row_labels: list[str],
    column_labels: list[str],
    cells: dict[tuple[str, str], str],
) -> list[str]:
    lines = [
        "| " + row_header + " | " + " | ".join(column_labels) + " |",
        "|---|" + "---:|" * len(column_labels),
    ]
    for row in row_labels:
        values = [cells.get((row, column), "—") for column in column_labels]
        lines.append("| " + row + " | " + " | ".join(values) + " |")
    return lines


def _record_counts(records: Iterable[dict]) -> dict[str, int]:
    counts = {
        "pass": 0,
        "warn": 0,
        "fail": 0,
        "not_applicable": 0,
        "informational": 0,
        "execution_issues": 0,
    }
    for record in records:
        execution_status = str(record.get("status") or "unknown")
        result_status = record.get("result_status")
        if execution_status != "success":
            counts["execution_issues"] += 1
        if result_status in DOMAIN_STATUSES:
            counts[str(result_status)] += 1
        elif execution_status == "success":
            counts["informational"] += 1
    return counts


def _scope_for_metric(metric_id: str) -> str:
    return "reference" if metric_id.endswith(REFERENCE_SUFFIX) else "intrinsic"


def _walk_taxonomy(node: dict, path: tuple[str, ...], output: dict[str, list[str]]) -> None:
    for key, value in node.items():
        if key == "_metrics":
            if not isinstance(value, list):
                continue
            for entry in value:
                metric_id = entry if isinstance(entry, str) else entry.get("metric_id") if isinstance(entry, dict) else None
                if isinstance(metric_id, str) and metric_id:
                    output.setdefault(metric_id, list(path))
            continue
        if isinstance(value, dict):
            _walk_taxonomy(value, path + (key,), output)


def _load_taxonomy_paths() -> dict[str, list[str]]:
    taxonomy_path = Path(__file__).resolve().parents[1] / "taxonomy" / "master_taxonomy.json"
    try:
        payload = json.loads(taxonomy_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(payload, dict):
        return {}
    output: dict[str, list[str]] = {}
    _walk_taxonomy(payload, (), output)
    return output


def _taxonomy_dimension(metric_id: str, taxonomy_paths: dict[str, list[str]]) -> str:
    path = taxonomy_paths.get(metric_id, [])
    if not path:
        return "uncategorized"
    if path[0] in {"dataset_heuristics", "reference_model_comparison"} and len(path) > 1:
        return path[1]
    return path[0]


def _representative_intrinsic_records(items: list[dict]) -> tuple[dict[str, dict], int]:
    """Deduplicate intrinsic metrics repeated across candidate/reference jobs.

    An all-v-all matrix re-runs intrinsic metrics for every reference pairing.
    Candidate summaries count each intrinsic metric once.  If the execution or
    domain verdict changes between repeated runs, the metric is flagged as an
    inconsistency rather than silently counted multiple times.
    """

    representatives: dict[str, dict] = {}
    signatures: dict[str, set[tuple[str, str]]] = {}
    for item in items:
        for metric_id, record in item["metric_records"].items():
            if metric_id.endswith(REFERENCE_SUFFIX):
                continue
            signature = (
                str(record.get("status") or "unknown"),
                str(record.get("result_status") or ""),
            )
            signatures.setdefault(metric_id, set()).add(signature)
            current = representatives.get(metric_id)
            if current is None or (
                current.get("status") != "success" and record.get("status") == "success"
            ):
                representatives[metric_id] = record
    inconsistent = sum(1 for values in signatures.values() if len(values) > 1)
    return representatives, inconsistent


def _compact_domain_counts(row: dict, prefix: str) -> str:
    return "/".join(
        str(row.get(f"{prefix}_{name}", 0))
        for name in ("pass", "warn", "fail", "not_applicable", "informational")
    )


def _attention_reason(record: dict) -> tuple[str, str]:
    diagnostic = record.get("diagnostic")
    diagnostic = diagnostic if isinstance(diagnostic, dict) else {}
    reason_code = diagnostic.get("reason_code") or record.get("reason_code") or record.get("reason") or ""
    summary = diagnostic.get("summary") or record.get("error") or record.get("reason") or ""
    return str(reason_code or ""), str(summary or "")


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
    for comparison, spreadsheets and statistical analysis.  Candidate-level
    intrinsic counts are deduplicated because all-v-all matrices repeat those
    metrics for each reference pairing.
    """

    comparison_jobs = [result for result in results if result.get("reference_dataset_path")]
    if not comparison_jobs:
        return {}

    candidate_paths = list(dict.fromkeys(str(item["dataset_path"]) for item in comparison_jobs))
    reference_paths = list(dict.fromkeys(str(item["reference_dataset_path"]) for item in comparison_jobs))
    candidate_labels = _unique_labels(candidate_paths)
    reference_labels = _unique_labels(reference_paths)
    taxonomy_paths = _load_taxonomy_paths()

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
            metric_id
            for metric_id in test_results
            if isinstance(metric_id, str) and metric_id.endswith(REFERENCE_SUFFIX)
        )
        job_metric_ids.update(
            metric_id for metric_id in metric_records if metric_id.endswith(REFERENCE_SUFFIX)
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

    output_dir.mkdir(parents=True, exist_ok=True)
    overview_path = output_dir / "overview.csv"
    long_path = output_dir / "long.csv"
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

    job_rows: list[dict] = []
    attention_rows: list[dict] = []
    attention_seen: set[tuple[str, str, str, str, str, str]] = set()
    for item in loaded:
        job = item["job"]
        candidate_path = str(job["dataset_path"])
        reference_path = str(job["reference_dataset_path"])
        candidate = candidate_labels[candidate_path]
        reference = reference_labels[reference_path]
        records = list(item["metric_records"].values())
        intrinsic_records = [
            record for metric_id, record in item["metric_records"].items()
            if not metric_id.endswith(REFERENCE_SUFFIX)
        ]
        reference_records = [
            record for metric_id, record in item["metric_records"].items()
            if metric_id.endswith(REFERENCE_SUFFIX)
        ]
        all_counts = _record_counts(records)
        intrinsic_counts = _record_counts(intrinsic_records)
        reference_counts = _record_counts(reference_records)
        row = {
            "candidate": candidate,
            "reference": reference,
            "job_id": job.get("job_id"),
            "outcome_status": job.get("outcome_status"),
            "metric_records": len(records),
            "pass": all_counts["pass"],
            "warn": all_counts["warn"],
            "fail": all_counts["fail"],
            "not_applicable": all_counts["not_applicable"],
            "informational": all_counts["informational"],
            "execution_issues": all_counts["execution_issues"],
            "intrinsic_pass": intrinsic_counts["pass"],
            "intrinsic_warn": intrinsic_counts["warn"],
            "intrinsic_fail": intrinsic_counts["fail"],
            "reference_pass": reference_counts["pass"],
            "reference_warn": reference_counts["warn"],
            "reference_fail": reference_counts["fail"],
            "outcome_path": job.get("output_path"),
        }
        job_rows.append(row)

        if str(job.get("outcome_status") or "") not in {"", "success"}:
            key = (candidate, reference, "", "job", str(job.get("outcome_status")), "")
            if key not in attention_seen:
                attention_seen.add(key)
                attention_rows.append(
                    {
                        "candidate": candidate,
                        "reference": reference,
                        "scope": "job",
                        "metric_id": "",
                        "execution_status": job.get("outcome_status"),
                        "result_status": "",
                        "reason_code": "job_outcome_attention",
                        "summary": "Batch job outcome requires attention.",
                        "outcome_path": job.get("output_path"),
                    }
                )

        for metric_id, record in item["metric_records"].items():
            execution_status = str(record.get("status") or "unknown")
            result_status = str(record.get("result_status") or "")
            if execution_status == "success" and result_status not in {"warn", "fail"}:
                continue
            scope = _scope_for_metric(metric_id)
            reason_code, summary = _attention_reason(record)
            dedup_reference = reference if scope == "reference" else ""
            key = (candidate, dedup_reference, metric_id, execution_status, result_status, reason_code)
            if key in attention_seen:
                continue
            attention_seen.add(key)
            attention_rows.append(
                {
                    "candidate": candidate,
                    "reference": reference if scope == "reference" else "",
                    "scope": scope,
                    "metric_id": metric_id,
                    "execution_status": execution_status,
                    "result_status": result_status,
                    "reason_code": reason_code,
                    "summary": summary,
                    "outcome_path": job.get("output_path"),
                }
            )

    job_summary_path = output_dir / "job_summary.csv"
    _write_csv(
        job_summary_path,
        [
            "candidate", "reference", "job_id", "outcome_status", "metric_records",
            "pass", "warn", "fail", "not_applicable", "informational", "execution_issues",
            "intrinsic_pass", "intrinsic_warn", "intrinsic_fail",
            "reference_pass", "reference_warn", "reference_fail", "outcome_path",
        ],
        job_rows,
    )

    candidate_rows: list[dict] = []
    taxonomy_rows: list[dict] = []
    for candidate_path in candidate_paths:
        candidate = candidate_labels[candidate_path]
        items = [item for item in loaded if str(item["job"]["dataset_path"]) == candidate_path]
        intrinsic_map, inconsistent_intrinsic = _representative_intrinsic_records(items)
        intrinsic_counts = _record_counts(intrinsic_map.values())
        reference_records = [
            record
            for item in items
            for metric_id, record in item["metric_records"].items()
            if metric_id.endswith(REFERENCE_SUFFIX)
        ]
        reference_counts = _record_counts(reference_records)
        successful_jobs = sum(1 for item in items if item["job"].get("outcome_status") == "success")
        candidate_rows.append(
            {
                "candidate": candidate,
                "candidate_path": candidate_path,
                "comparison_jobs": len(items),
                "successful_jobs": successful_jobs,
                "jobs_needing_attention": len(items) - successful_jobs,
                "intrinsic_metrics": len(intrinsic_map),
                "intrinsic_pass": intrinsic_counts["pass"],
                "intrinsic_warn": intrinsic_counts["warn"],
                "intrinsic_fail": intrinsic_counts["fail"],
                "intrinsic_not_applicable": intrinsic_counts["not_applicable"],
                "intrinsic_informational": intrinsic_counts["informational"],
                "intrinsic_execution_issues": intrinsic_counts["execution_issues"],
                "intrinsic_inconsistent_metrics": inconsistent_intrinsic,
                "reference_assessments": len(reference_records),
                "reference_pass": reference_counts["pass"],
                "reference_warn": reference_counts["warn"],
                "reference_fail": reference_counts["fail"],
                "reference_not_applicable": reference_counts["not_applicable"],
                "reference_informational": reference_counts["informational"],
                "reference_execution_issues": reference_counts["execution_issues"],
            }
        )

        scoped_records: dict[tuple[str, str], list[tuple[str, dict]]] = {}
        for metric_id, record in intrinsic_map.items():
            dimension = _taxonomy_dimension(metric_id, taxonomy_paths)
            scoped_records.setdefault(("intrinsic", dimension), []).append((metric_id, record))
        for item in items:
            for metric_id, record in item["metric_records"].items():
                if not metric_id.endswith(REFERENCE_SUFFIX):
                    continue
                dimension = _taxonomy_dimension(metric_id, taxonomy_paths)
                scoped_records.setdefault(("reference", dimension), []).append((metric_id, record))

        for (scope, dimension), metric_records in sorted(scoped_records.items()):
            counts = _record_counts(record for _, record in metric_records)
            taxonomy_rows.append(
                {
                    "candidate": candidate,
                    "scope": scope,
                    "dimension": dimension,
                    "observations": len(metric_records),
                    "metric_ids": ";".join(sorted({metric_id for metric_id, _ in metric_records})),
                    "pass": counts["pass"],
                    "warn": counts["warn"],
                    "fail": counts["fail"],
                    "not_applicable": counts["not_applicable"],
                    "informational": counts["informational"],
                    "execution_issues": counts["execution_issues"],
                }
            )

    candidate_summary_path = output_dir / "candidate_summary.csv"
    _write_csv(
        candidate_summary_path,
        [
            "candidate", "candidate_path", "comparison_jobs", "successful_jobs", "jobs_needing_attention",
            "intrinsic_metrics", "intrinsic_pass", "intrinsic_warn", "intrinsic_fail",
            "intrinsic_not_applicable", "intrinsic_informational", "intrinsic_execution_issues",
            "intrinsic_inconsistent_metrics", "reference_assessments", "reference_pass", "reference_warn",
            "reference_fail", "reference_not_applicable", "reference_informational", "reference_execution_issues",
        ],
        candidate_rows,
    )

    taxonomy_summary_path = output_dir / "taxonomy_summary.csv"
    _write_csv(
        taxonomy_summary_path,
        [
            "candidate", "scope", "dimension", "observations", "metric_ids",
            "pass", "warn", "fail", "not_applicable", "informational", "execution_issues",
        ],
        taxonomy_rows,
    )

    attention_path = output_dir / "attention.csv"
    _write_csv(
        attention_path,
        [
            "candidate", "reference", "scope", "metric_id", "execution_status",
            "result_status", "reason_code", "summary", "outcome_path",
        ],
        attention_rows,
    )

    matrices_dir = output_dir / "matrices"
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
                    item["job"]
                    for item in loaded
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

    batch_name = batch_meta.get("name") or batch_meta.get("batch_id", "batch")
    markdown_path = output_dir / "report.md"
    markdown_lines = [
        f"# Comparison report — {batch_name}",
        "",
        f"- Run timestamp: `{timestamp}`",
        f"- Candidates: {len(candidate_paths)}",
        f"- References: {len(reference_paths)}",
        f"- Comparison jobs represented: {len(comparison_jobs)}",
        f"- Reference metrics: {len(sorted_metric_ids)}",
        f"- Attention records: {len(attention_rows)}",
        "",
        "The JSON outcomes remain authoritative. CSV and Markdown files are denormalised comparison views.",
        "No aggregate realism score is calculated: pass/warn/fail counts are descriptive verdict counts, not a weighted combined realism measure.",
        "Intrinsic metrics are counted once per candidate in candidate summaries even though all-v-all matrices re-run them for each reference pairing.",
        "For the current reference-distance/divergence metrics, lower values generally indicate a closer match; consult the metric documentation for exact interpretation.",
        "",
        "## Candidate summary",
        "",
        "Domain count columns are shown as `pass/warn/fail/not-applicable/informational`.",
        "",
        "| Candidate | Jobs OK | Intrinsic P/W/F/NA/I | Reference P/W/F/NA/I | Exec issues (I/R) | Intrinsic inconsistencies |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for row in candidate_rows:
        markdown_lines.append(
            "| "
            + str(row["candidate"])
            + " | "
            + f"{row['successful_jobs']}/{row['comparison_jobs']}"
            + " | "
            + _compact_domain_counts(row, "intrinsic")
            + " | "
            + _compact_domain_counts(row, "reference")
            + " | "
            + f"{row['intrinsic_execution_issues']}/{row['reference_execution_issues']}"
            + " | "
            + str(row["intrinsic_inconsistent_metrics"])
            + " |"
        )

    markdown_lines.extend(["", "## Items needing attention", ""])
    if not attention_rows:
        markdown_lines.append("No execution errors, domain warnings or domain failures were recorded.")
    else:
        markdown_lines.extend(
            [
                "| Candidate | Reference | Scope | Metric | Execution | Domain | Reason |",
                "|---|---|---|---|---|---|---|",
            ]
        )
        for row in attention_rows[:100]:
            markdown_lines.append(
                f"| {row['candidate']} | {row['reference'] or '—'} | {row['scope']} | "
                f"{row['metric_id'] or '—'} | {row['execution_status'] or '—'} | "
                f"{row['result_status'] or '—'} | {row['reason_code'] or row['summary'] or '—'} |"
            )
        if len(attention_rows) > 100:
            markdown_lines.append(f"\n{len(attention_rows) - 100} additional attention records are available in `attention.csv`.")

    markdown_lines.extend(["", "## Taxonomy/domain summary", ""])
    markdown_lines.extend(
        [
            "| Candidate | Scope | Dimension | Observations | Pass | Warn | Fail | N/A | Info | Exec issues |",
            "|---|---|---|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for row in taxonomy_rows:
        markdown_lines.append(
            f"| {row['candidate']} | {row['scope']} | {row['dimension']} | {row['observations']} | "
            f"{row['pass']} | {row['warn']} | {row['fail']} | {row['not_applicable']} | "
            f"{row['informational']} | {row['execution_issues']} |"
        )

    markdown_lines.extend(["", "## Overall run status", ""])
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

    readme_path = output_dir.parent / "README.md"
    readme_lines = [
        f"# Experiment matrix run — {batch_name}",
        "",
        f"Run timestamp: `{timestamp}`",
        "",
        "This directory contains one comparison-matrix run. The JSON files under `results/` are the authoritative outcomes; the files under `reports/` are derived views for review and analysis.",
        "",
        "No aggregate realism score is calculated by this report.",
        "",
        "## At a glance",
        "",
        f"- Candidates: {len(candidate_paths)}",
        f"- References: {len(reference_paths)}",
        f"- Comparison jobs: {len(comparison_jobs)}",
        f"- Jobs requiring attention: {sum(1 for row in job_rows if row['outcome_status'] != 'success')}",
        f"- Metric/domain attention records: {len(attention_rows)}",
        "",
        "## Candidate summary",
        "",
        "| Candidate | Jobs OK | Intrinsic fail/warn | Reference fail/warn | Execution issues |",
        "|---|---:|---:|---:|---:|",
    ]
    for row in candidate_rows:
        readme_lines.append(
            f"| {row['candidate']} | {row['successful_jobs']}/{row['comparison_jobs']} | "
            f"{row['intrinsic_fail']}/{row['intrinsic_warn']} | "
            f"{row['reference_fail']}/{row['reference_warn']} | "
            f"{row['intrinsic_execution_issues'] + row['reference_execution_issues']} |"
        )
    readme_lines.extend(
        [
            "",
            "## Reports",
            "",
            "- `reports/report.md` — detailed human-readable matrix report",
            "- `reports/candidate_summary.csv` — one row per candidate dataset",
            "- `reports/job_summary.csv` — one row per candidate/reference comparison job",
            "- `reports/taxonomy_summary.csv` — verdict counts grouped by taxonomy dimension",
            "- `reports/attention.csv` — execution errors plus domain warnings/failures",
            "- `reports/overview.csv` — wide reference-metric comparison table",
            "- `reports/long.csv` — long-form reference-metric data for analysis",
            "- `reports/matrices/` — candidate × reference matrices",
        ]
    )
    readme_path.write_text("\n".join(readme_lines) + "\n", encoding="utf-8")

    return {
        "comparison_overview_csv": str(overview_path),
        "comparison_long_csv": str(long_path),
        "comparison_markdown": str(markdown_path),
        "candidate_summary_csv": str(candidate_summary_path),
        "job_summary_csv": str(job_summary_path),
        "taxonomy_summary_csv": str(taxonomy_summary_path),
        "attention_csv": str(attention_path),
        "run_readme": str(readme_path),
        "comparison_matrices_directory": str(matrices_dir),
        "overall_status_matrix_csv": str(status_matrix_path),
        "metric_matrix_csvs": matrix_paths,
        "reference_metric_ids": sorted_metric_ids,
    }
