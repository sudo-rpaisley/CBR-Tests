import json
from pathlib import Path

from runner.human_summary import default_human_summary_path, format_human_summary
from runner.run_plan_helpers import write_outcome


def _example_outcome() -> dict:
    return {
        "schema_version": 2,
        "status": "success",
        "case_id": "example-case",
        "plan_id": "example-plan",
        "run_id": "run-123",
        "metric_ids": [
            "pass_metric",
            "warn_metric",
            "fail_metric",
            "info_metric",
            "skipped_metric",
        ],
        "dataset_path": "/data/example.pcap",
        "metric_results": [
            {
                "metric_id": "pass_metric",
                "status": "success",
                "result_status": "pass",
                "elapsed_seconds": 0.2,
            },
            {
                "metric_id": "warn_metric",
                "status": "success",
                "result_status": "warn",
                "elapsed_seconds": 0.3,
                "diagnostic": {
                    "reason_code": "non_standard_service_port",
                    "summary": "Observed service traffic outside the configured standard port.",
                    "evidence": {"expected_ports": [53], "unexpected_row_count": 4},
                    "suggestion": "Confirm whether the deployment intentionally uses a non-standard port.",
                },
            },
            {
                "metric_id": "fail_metric",
                "status": "success",
                "result_status": "fail",
                "elapsed_seconds": 0.4,
                "diagnostic": {
                    "reason_code": "invalid_port_values",
                    "summary": "Some transport ports are outside the valid 0-65535 range.",
                    "evidence": {"invalid_count": 2, "valid_ratio": 0.98},
                    "suggestion": "Inspect the invalid rows and confirm source exporter semantics.",
                },
            },
            {
                "metric_id": "info_metric",
                "status": "success",
                "elapsed_seconds": 0.1,
            },
            {
                "metric_id": "skipped_metric",
                "status": "skipped",
                "reason": "missing_field_mappings",
                "missing_fields": ["Slice ID"],
            },
        ],
        "test_results": {
            "pass_metric": {
                "status": "pass",
                "summary": {"valid_ratio": 1.0, "checked_rows": 100},
            },
            "warn_metric": {"status": "warn"},
            "fail_metric": {"status": "fail"},
            "info_metric": {"summary": {"rows": 100, "columns": 12}},
        },
        "run_started_at": "2026-08-26T10:00:00+00:00",
        "run_finished_at": "2026-08-26T10:00:05+00:00",
        "run_elapsed_seconds": 5.0,
        "provenance": {
            "dataset": {"sha256": "dataset-sha"},
            "plan": {"sha256": "plan-sha"},
            "software": {"code": {"revision": "abcdef123"}},
            "reference_datasets": [],
        },
    }


def test_default_human_summary_path_uses_companion_markdown_name():
    assert default_human_summary_path(Path("outcomes/run.json")) == Path("outcomes/run.summary.md")
    assert default_human_summary_path(Path("outcomes/run")) == Path("outcomes/run.summary.md")


def test_human_summary_separates_execution_from_domain_findings():
    report = format_human_summary(_example_outcome(), outcome_path=Path("outcomes/run.json"))

    assert "Overall execution status | **SUCCESS**" in report
    assert "Domain PASS | 1" in report
    assert "Domain WARN | 1" in report
    assert "Domain FAIL | 1" in report
    assert "Informational/no domain verdict | 1" in report
    assert "runner completed successfully, but 1 metric(s) returned a domain-level FAIL" in report
    assert "dataset realism/quality findings, not software execution failures" in report
    assert "does not invent an aggregate realism score" in report


def test_human_summary_surfaces_reasons_evidence_and_actions():
    report = format_human_summary(_example_outcome())

    assert "### FAIL — `fail_metric`" in report
    assert "`invalid_port_values`" in report
    assert "`invalid_count`: 2" in report
    assert "Inspect the invalid rows" in report
    assert "### WARN — `warn_metric`" in report
    assert "`non_standard_service_port`" in report
    assert "### SKIPPED — `skipped_metric`" in report
    assert "Slice ID" in report


def test_human_summary_lists_all_metrics_and_reproducibility_identifiers():
    report = format_human_summary(_example_outcome())

    for metric_id in _example_outcome()["metric_ids"]:
        assert f"`{metric_id}`" in report
    assert "Dataset SHA-256: **" not in report
    assert "**Dataset SHA-256:** `dataset-sha`" in report
    assert "**Plan SHA-256:** `plan-sha`" in report
    assert "**Code revision:** `abcdef123`" in report


def test_write_outcome_always_writes_json_and_human_summary(tmp_path):
    outcome_path = tmp_path / "experiment.json"
    outcome = _example_outcome()

    summary_path = write_outcome(outcome_path, outcome)

    assert summary_path == tmp_path / "experiment.summary.md"
    assert outcome_path.exists()
    assert summary_path.exists()
    assert json.loads(outcome_path.read_text(encoding="utf-8")) == outcome
    summary = summary_path.read_text(encoding="utf-8")
    assert "# CBR-Tests Run Summary" in summary
    assert str(outcome_path) in summary
    assert "The JSON outcome remains the authoritative machine-readable research record." in summary
