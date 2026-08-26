from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Any

DOMAIN_STATUSES = ("pass", "warn", "fail", "not_applicable")


def default_human_summary_path(output_path: Path) -> Path:
    """Return the default Markdown companion path for a JSON outcome."""
    output_path = Path(output_path)
    if output_path.suffix:
        return output_path.with_name(f"{output_path.stem}.summary.md")
    return output_path.with_name(f"{output_path.name}.summary.md")


def _clean_text(value: Any, *, limit: int = 240) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, float):
        text = f"{value:.6g}"
    else:
        text = str(value)
    text = " ".join(text.replace("\r", " ").replace("\n", " ").split())
    if len(text) > limit:
        return text[: max(0, limit - 1)].rstrip() + "…"
    return text


def _markdown(value: Any, *, limit: int = 240) -> str:
    return _clean_text(value, limit=limit).replace("|", "\\|")


def _scalar_pairs(value: Any, *, limit: int = 5, prefix: str = "") -> list[tuple[str, Any]]:
    """Extract a bounded set of human-readable scalar evidence values."""
    pairs: list[tuple[str, Any]] = []

    def visit(item: Any, key_prefix: str) -> None:
        if len(pairs) >= limit:
            return
        if isinstance(item, dict):
            for key, child in item.items():
                if len(pairs) >= limit:
                    break
                child_key = f"{key_prefix}.{key}" if key_prefix else str(key)
                visit(child, child_key)
            return
        if isinstance(item, (list, tuple)):
            if not item:
                return
            if all(not isinstance(child, (dict, list, tuple, set)) for child in item[:8]):
                preview = ", ".join(_clean_text(child, limit=50) for child in item[:5])
                if len(item) > 5:
                    preview += f", … (+{len(item) - 5})"
                pairs.append((key_prefix or "values", preview))
            return
        if item is not None:
            pairs.append((key_prefix or "value", item))

    visit(value, prefix)
    return pairs


def _test_result(outcome: dict, metric_id: str) -> Any:
    test_results = outcome.get("test_results")
    if not isinstance(test_results, dict):
        return None
    return test_results.get(metric_id)


def _result_summary(outcome: dict, record: dict) -> str:
    diagnostic = record.get("diagnostic")
    if isinstance(diagnostic, dict) and diagnostic.get("summary"):
        return _clean_text(diagnostic["summary"])
    if record.get("error"):
        return _clean_text(record["error"])
    if record.get("reason"):
        return _clean_text(record["reason"])

    metric_id = str(record.get("metric_id", "unknown_metric"))
    result = _test_result(outcome, metric_id)
    if isinstance(result, dict):
        summary = result.get("summary")
        if isinstance(summary, str):
            return _clean_text(summary)
        source = summary if isinstance(summary, dict) else {
            key: value
            for key, value in result.items()
            if key not in {"status", "diagnostic", "examples", "pairs", "matrix"}
        }
        pairs = _scalar_pairs(source, limit=3)
        if pairs:
            return "; ".join(f"{key}={_clean_text(value, limit=80)}" for key, value in pairs)
    elif result is not None:
        return _clean_text(result)

    status = record.get("result_status") or record.get("status") or "unknown"
    return f"Metric recorded {status}."


def _execution_counts(records: list[dict]) -> Counter:
    return Counter(str(record.get("status", "unknown")) for record in records)


def _domain_counts(records: list[dict]) -> Counter:
    return Counter(
        str(record.get("result_status"))
        for record in records
        if record.get("result_status") in DOMAIN_STATUSES
    )


def _interpretation(outcome: dict, execution: Counter, domain: Counter) -> list[str]:
    overall = str(outcome.get("status", "unknown"))
    lines: list[str] = []
    if overall == "success":
        if domain["fail"]:
            lines.append(
                f"The runner completed successfully, but {domain['fail']} metric(s) returned a domain-level FAIL result. "
                "These are dataset realism/quality findings, not software execution failures."
            )
        elif domain["warn"]:
            lines.append(
                f"The runner completed successfully with {domain['warn']} domain warning(s) requiring review."
            )
        else:
            lines.append("The runner completed successfully with no recorded domain FAIL or WARN results.")
    elif overall == "cancelled":
        lines.append(
            "The run was cancelled. Results from completed metrics may still be useful, but the outcome is incomplete."
        )
    else:
        lines.append(
            "The run did not complete cleanly. Resolve execution failures before treating the collected metric set as a complete evaluation."
        )

    successful = execution.get("success", 0)
    verdict_count = sum(domain.values())
    if successful > verdict_count:
        lines.append(
            f"{successful - verdict_count} successfully executed metric(s) did not expose a pass/warn/fail/not-applicable domain verdict; "
            "they are reported as informational rather than silently classified."
        )
    lines.append(
        "This summary does not invent an aggregate realism score or combined scientific pass/fail verdict; each metric retains its own interpretation."
    )
    return lines


def _attention_records(records: list[dict]) -> list[dict]:
    def severity(record: dict) -> tuple[int, str]:
        execution = str(record.get("status", "unknown"))
        domain = str(record.get("result_status", ""))
        if execution not in {"success", "skipped"}:
            rank = 0
        elif domain == "fail":
            rank = 1
        elif domain == "warn":
            rank = 2
        elif execution == "skipped":
            rank = 3
        else:
            rank = 9
        return rank, str(record.get("metric_id", ""))

    return [
        record
        for record in sorted(records, key=severity)
        if severity(record)[0] < 9
    ]


def _format_attention_record(outcome: dict, record: dict) -> list[str]:
    metric_id = str(record.get("metric_id", "unknown_metric"))
    execution = str(record.get("status", "unknown"))
    domain = record.get("result_status")
    heading_status = (
        str(domain).upper()
        if execution == "success" and domain in {"fail", "warn"}
        else execution.upper()
    )
    lines = [f"### {heading_status} — `{metric_id}`", ""]
    lines.append(f"- **Execution:** `{execution}`")
    lines.append(f"- **Domain result:** `{domain}`" if domain else "- **Domain result:** not recorded")

    diagnostic = record.get("diagnostic") if isinstance(record.get("diagnostic"), dict) else {}
    reason_code = diagnostic.get("reason_code") or record.get("reason_code") or record.get("reason")
    if reason_code:
        lines.append(f"- **Reason code:** `{_clean_text(reason_code, limit=120)}`")
    lines.append(f"- **Interpretation:** {_clean_text(diagnostic.get('summary') or _result_summary(outcome, record))}")

    evidence = diagnostic.get("evidence") or diagnostic.get("details")
    evidence_pairs = _scalar_pairs(evidence, limit=6) if evidence is not None else []
    if evidence_pairs:
        lines.append("- **Key evidence:**")
        for key, value in evidence_pairs:
            lines.append(f"  - `{_clean_text(key, limit=100)}`: {_clean_text(value, limit=180)}")

    missing_fields = record.get("missing_fields")
    if missing_fields:
        lines.append(f"- **Missing fields:** {', '.join(_clean_text(field, limit=80) for field in missing_fields)}")

    suggestion = diagnostic.get("suggestion")
    if suggestion:
        lines.append(f"- **Suggested action:** {_clean_text(suggestion)}")
    lines.append("")
    return lines


def format_human_summary(outcome: dict, *, outcome_path: Path | None = None) -> str:
    """Render an outcome as a concise, scientifically conservative Markdown report."""
    records = [record for record in outcome.get("metric_results", []) if isinstance(record, dict)]
    execution = _execution_counts(records)
    domain = _domain_counts(records)
    planned = len(outcome.get("metric_ids", []))
    successful = execution.get("success", 0)
    verdict_count = sum(domain.values())
    informational = max(0, successful - verdict_count)

    lines = [
        "# CBR-Tests Run Summary",
        "",
        "> This file is a human-readable companion derived from the JSON outcome. The JSON outcome remains the authoritative machine-readable research record.",
        "",
        "## At a glance",
        "",
        "| Item | Value |",
        "| --- | --- |",
        f"| Overall execution status | **{_markdown(str(outcome.get('status', 'unknown')).upper())}** |",
        f"| Case ID | `{_markdown(outcome.get('case_id', 'n/a'))}` |",
        f"| Plan ID | `{_markdown(outcome.get('plan_id', 'n/a'))}` |",
        f"| Run ID | `{_markdown(outcome.get('run_id', 'n/a'))}` |",
        f"| Dataset | `{_markdown(outcome.get('dataset_path', 'n/a'), limit=500)}` |",
        f"| Metrics planned | {planned} |",
        f"| Metrics executed successfully | {successful} |",
        f"| Metrics skipped | {execution.get('skipped', 0)} |",
        f"| Execution failures/cancellations | {sum(count for status, count in execution.items() if status not in {'success', 'skipped'})} |",
        f"| Domain PASS | {domain['pass']} |",
        f"| Domain WARN | {domain['warn']} |",
        f"| Domain FAIL | {domain['fail']} |",
        f"| Domain not applicable | {domain['not_applicable']} |",
        f"| Informational/no domain verdict | {informational} |",
        f"| Started | {_markdown(outcome.get('run_started_at', 'n/a'))} |",
        f"| Finished | {_markdown(outcome.get('run_finished_at', 'n/a'))} |",
        f"| Elapsed | {_markdown(outcome.get('run_elapsed_seconds', 'n/a'))} seconds |",
    ]
    if outcome_path is not None:
        lines.append(f"| Authoritative JSON | `{_markdown(Path(outcome_path), limit=500)}` |")

    lines.extend(["", "## Interpretation", ""])
    for paragraph in _interpretation(outcome, execution, domain):
        lines.extend([paragraph, ""])

    lines.extend(["## Findings requiring attention", ""])
    attention = _attention_records(records)
    if attention:
        for record in attention:
            lines.extend(_format_attention_record(outcome, record))
    else:
        lines.extend(["No execution failures, skipped metrics, domain FAIL results, or domain WARN results were recorded.", ""])

    lines.extend([
        "## All metric results",
        "",
        "| Metric | Execution | Domain result | Human-readable summary |",
        "| --- | --- | --- | --- |",
    ])
    for record in records:
        metric_id = _markdown(record.get("metric_id", "unknown_metric"), limit=160)
        execution_status = _markdown(record.get("status", "unknown"), limit=40)
        result_status = _markdown(record.get("result_status", "—"), limit=40)
        summary = _markdown(_result_summary(outcome, record), limit=260)
        lines.append(f"| `{metric_id}` | `{execution_status}` | `{result_status}` | {summary} |")

    provenance = outcome.get("provenance")
    if isinstance(provenance, dict):
        lines.extend(["", "## Reproducibility identifiers", ""])
        dataset_manifest = provenance.get("dataset") if isinstance(provenance.get("dataset"), dict) else {}
        plan_manifest = provenance.get("plan") if isinstance(provenance.get("plan"), dict) else {}
        software = provenance.get("software") if isinstance(provenance.get("software"), dict) else {}
        code = software.get("code") if isinstance(software.get("code"), dict) else {}
        lines.extend([
            f"- **Dataset SHA-256:** `{_clean_text(dataset_manifest.get('sha256', 'n/a'))}`",
            f"- **Plan SHA-256:** `{_clean_text(plan_manifest.get('sha256', 'n/a'))}`",
            f"- **Code revision:** `{_clean_text(code.get('revision', 'n/a'))}`",
        ])
        references = provenance.get("reference_datasets")
        if isinstance(references, list) and references:
            lines.append(f"- **Reference datasets:** {len(references)}")
            for reference in references:
                if isinstance(reference, dict):
                    lines.append(
                        f"  - `{_clean_text(reference.get('path', 'n/a'), limit=260)}` — SHA-256 `{_clean_text(reference.get('sha256', 'n/a'))}`"
                    )

    lines.extend([
        "",
        "---",
        "",
        "Generated automatically by CBR-Tests from the corresponding JSON outcome. Do not use this summary as a substitute for the full metric evidence when making research claims.",
        "",
    ])
    return "\n".join(lines)
