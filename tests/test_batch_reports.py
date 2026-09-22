import csv
import json
from pathlib import Path

from runner.batch_reports import extract_primary_metric_value, write_comparison_reports


KS_METRIC = "feature_wise_ks_statistic_from_reference"
PROTOCOL_METRIC = "protocol_mix_divergence_from_reference"
INTRINSIC_METRIC = "valid_port_range_profile"


def _write_outcome(
    path: Path,
    *,
    ks: float,
    protocol: float,
    result_status: str = "pass",
    intrinsic_status: str = "pass",
    intrinsic_execution_status: str = "success",
) -> None:
    intrinsic_record = {
        "metric_id": INTRINSIC_METRIC,
        "status": intrinsic_execution_status,
        "result_status": intrinsic_status,
    }
    if intrinsic_status in {"warn", "fail"}:
        intrinsic_record["diagnostic"] = {
            "reason_code": "port_realism_attention",
            "summary": "Port validity needs attention.",
        }

    payload = {
        "schema_version": 2,
        "status": "success",
        "metric_ids": [KS_METRIC, PROTOCOL_METRIC, INTRINSIC_METRIC],
        "metric_results": [
            {
                "metric_id": KS_METRIC,
                "status": "success",
                "result_status": result_status,
            },
            {
                "metric_id": PROTOCOL_METRIC,
                "status": "success",
                "result_status": result_status,
            },
            intrinsic_record,
        ],
        "test_results": {
            KS_METRIC: {
                "fields": [],
                "summary": {
                    f"mean_{KS_METRIC}": ks,
                    f"max_{KS_METRIC}": ks + 0.1,
                    "runnable_field_count": 2,
                },
            },
            PROTOCOL_METRIC: {
                "summary": {
                    PROTOCOL_METRIC: protocol,
                    "protocol_field": "Protocol",
                }
            },
            INTRINSIC_METRIC: {"summary": {"valid_ratio": 1.0}},
        },
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


def _read_csv(path: Path) -> list[dict[str, str]]:
    with open(path, "r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def test_extract_primary_metric_value_prefers_exact_then_mean_summary():
    exact = {"summary": {PROTOCOL_METRIC: 0.25}}
    value, max_value, summary = extract_primary_metric_value(PROTOCOL_METRIC, exact)
    assert value == 0.25
    assert max_value is None
    assert summary[PROTOCOL_METRIC] == 0.25

    feature = {
        "summary": {
            f"mean_{KS_METRIC}": 0.125,
            f"max_{KS_METRIC}": 0.5,
        }
    }
    value, max_value, _ = extract_primary_metric_value(KS_METRIC, feature)
    assert value == 0.125
    assert max_value == 0.5


def test_write_comparison_reports_creates_wide_long_and_metric_matrices(tmp_path):
    candidates = [tmp_path / "candidate_a.csv", tmp_path / "candidate_b.csv"]
    references = [tmp_path / "reference_1.csv", tmp_path / "reference_2.csv"]
    values = {
        (0, 0): (0.10, 0.20),
        (0, 1): (0.30, 0.40),
        (1, 0): (0.50, 0.60),
        (1, 1): (0.70, 0.80),
    }

    results = []
    for candidate_index, candidate in enumerate(candidates):
        for reference_index, reference in enumerate(references):
            ks, protocol = values[(candidate_index, reference_index)]
            outcome_path = tmp_path / f"outcome_{candidate_index}_{reference_index}.json"
            _write_outcome(outcome_path, ks=ks, protocol=protocol)
            results.append(
                {
                    "job_id": f"job-{candidate_index}-{reference_index}",
                    "dataset_path": str(candidate),
                    "reference_dataset_path": str(reference),
                    "output_path": str(outcome_path),
                    "outcome_status": "success",
                }
            )

    report = write_comparison_reports(
        output_dir=tmp_path / "reports",
        timestamp="2026-09-02_12-00-00",
        batch_meta={"batch_id": "comparison", "name": "Comparison"},
        results=results,
    )

    overview = _read_csv(Path(report["comparison_overview_csv"]))
    assert len(overview) == 4
    assert set(row["candidate"] for row in overview) == {"candidate_a.csv", "candidate_b.csv"}
    assert set(row["reference"] for row in overview) == {"reference_1.csv", "reference_2.csv"}
    assert KS_METRIC in overview[0]
    assert f"{KS_METRIC}__result_status" in overview[0]
    assert INTRINSIC_METRIC not in overview[0]

    long_rows = _read_csv(Path(report["comparison_long_csv"]))
    assert len(long_rows) == 8
    assert {row["metric_id"] for row in long_rows} == {KS_METRIC, PROTOCOL_METRIC}
    ks_row = next(
        row for row in long_rows
        if row["candidate"] == "candidate_a.csv"
        and row["reference"] == "reference_1.csv"
        and row["metric_id"] == KS_METRIC
    )
    assert float(ks_row["primary_value"]) == 0.10
    assert float(ks_row["max_value"]) == 0.20
    assert ks_row["result_status"] == "pass"

    ks_matrix = _read_csv(Path(report["metric_matrix_csvs"][KS_METRIC]))
    assert ks_matrix == [
        {
            "candidate": "candidate_a.csv",
            "reference_1.csv": "0.1",
            "reference_2.csv": "0.3",
        },
        {
            "candidate": "candidate_b.csv",
            "reference_1.csv": "0.5",
            "reference_2.csv": "0.7",
        },
    ]

    status_matrix = _read_csv(Path(report["overall_status_matrix_csv"]))
    assert status_matrix[0]["reference_1.csv"] == "success"
    assert status_matrix[1]["reference_2.csv"] == "success"

    markdown = Path(report["comparison_markdown"]).read_text(encoding="utf-8")
    assert "# Comparison report — Comparison" in markdown
    assert f"## `{KS_METRIC}`" in markdown
    assert "0.1 (pass)" in markdown
    assert "candidate_a.csv" in markdown
    assert "reference_2.csv" in markdown


def test_comparison_reports_add_research_ready_candidate_taxonomy_and_attention_views(tmp_path):
    candidate = tmp_path / "candidate_a.pcapng"
    references = [tmp_path / "reference_1.pcapng", tmp_path / "reference_2.pcapng"]
    results = []

    for index, reference in enumerate(references):
        outcome_path = tmp_path / f"attention_{index}.json"
        _write_outcome(
            outcome_path,
            ks=0.1 + index,
            protocol=0.2 + index,
            intrinsic_status="warn",
        )
        results.append(
            {
                "job_id": f"attention-{index}",
                "dataset_path": str(candidate),
                "reference_dataset_path": str(reference),
                "output_path": str(outcome_path),
                "outcome_status": "success",
            }
        )

    report = write_comparison_reports(
        output_dir=tmp_path / "run" / "reports",
        timestamp="2026-09-22_12-00-00",
        batch_meta={"batch_id": "research-ready", "name": "Research ready"},
        results=results,
    )

    candidate_rows = _read_csv(Path(report["candidate_summary_csv"]))
    assert len(candidate_rows) == 1
    candidate_row = candidate_rows[0]
    assert candidate_row["comparison_jobs"] == "2"
    assert candidate_row["intrinsic_metrics"] == "1"
    assert candidate_row["intrinsic_warn"] == "1"
    assert candidate_row["reference_assessments"] == "4"

    job_rows = _read_csv(Path(report["job_summary_csv"]))
    assert len(job_rows) == 2
    assert all(row["intrinsic_warn"] == "1" for row in job_rows)

    attention_rows = _read_csv(Path(report["attention_csv"]))
    intrinsic_attention = [row for row in attention_rows if row["metric_id"] == INTRINSIC_METRIC]
    assert len(intrinsic_attention) == 1
    assert intrinsic_attention[0]["reference"] == ""
    assert intrinsic_attention[0]["reason_code"] == "port_realism_attention"

    taxonomy_rows = _read_csv(Path(report["taxonomy_summary_csv"]))
    assert any(
        row["scope"] == "intrinsic" and row["dimension"] == "protocol_and_network_realism"
        for row in taxonomy_rows
    )
    assert any(
        row["scope"] == "reference" and row["dimension"] == "distributional_similarity"
        for row in taxonomy_rows
    )

    markdown = Path(report["comparison_markdown"]).read_text(encoding="utf-8")
    assert "No aggregate realism score is calculated" in markdown
    assert "## Candidate summary" in markdown
    assert "## Items needing attention" in markdown
    assert "## Taxonomy/domain summary" in markdown

    run_readme = Path(report["run_readme"])
    assert run_readme == tmp_path / "run" / "README.md"
    readme_text = run_readme.read_text(encoding="utf-8")
    assert "# Experiment matrix run — Research ready" in readme_text
    assert "reports/candidate_summary.csv" in readme_text
    assert "No aggregate realism score" in readme_text


def test_candidate_summary_flags_intrinsic_verdict_inconsistency_across_reference_jobs(tmp_path):
    candidate = tmp_path / "candidate.pcapng"
    references = [tmp_path / "reference_1.pcapng", tmp_path / "reference_2.pcapng"]
    results = []

    for index, reference in enumerate(references):
        outcome_path = tmp_path / f"inconsistent_{index}.json"
        _write_outcome(
            outcome_path,
            ks=0.1,
            protocol=0.2,
            intrinsic_status="pass" if index == 0 else "fail",
        )
        results.append(
            {
                "job_id": f"inconsistent-{index}",
                "dataset_path": str(candidate),
                "reference_dataset_path": str(reference),
                "output_path": str(outcome_path),
                "outcome_status": "success",
            }
        )

    report = write_comparison_reports(
        output_dir=tmp_path / "run" / "reports",
        timestamp="2026-09-22_12-00-00",
        batch_meta={"batch_id": "inconsistent"},
        results=results,
    )

    candidate_row = _read_csv(Path(report["candidate_summary_csv"]))[0]
    assert candidate_row["intrinsic_metrics"] == "1"
    assert candidate_row["intrinsic_inconsistent_metrics"] == "1"


def test_write_comparison_reports_returns_empty_for_non_reference_batch(tmp_path):
    report = write_comparison_reports(
        output_dir=tmp_path,
        timestamp="2026-09-02_12-00-00",
        batch_meta={"batch_id": "plain"},
        results=[
            {
                "job_id": "plain-1",
                "dataset_path": str(tmp_path / "dataset.csv"),
                "reference_dataset_path": None,
                "output_path": str(tmp_path / "outcome.json"),
                "outcome_status": "success",
            }
        ],
    )
    assert report == {}
