from cbr_tests.outcome_comparison import compare_outcomes, render_markdown


def test_comparison_ignores_volatile_run_metadata_by_default():
    before = {
        "status": "success",
        "run_id": "old-run",
        "started_at": "2026-09-09T10:00:00Z",
        "test_results": {"metric": {"score": 0.5}},
    }
    after = {
        "status": "success",
        "run_id": "new-run",
        "started_at": "2026-09-10T10:00:00Z",
        "test_results": {"metric": {"score": 0.5}},
    }

    report = compare_outcomes(before, after)

    assert report["summary"]["total_changes"] == 0
    assert report["summary"]["ignored_volatile_paths"] == 2


def test_comparison_can_include_volatile_metadata_when_requested():
    before = {"run_id": "old-run"}
    after = {"run_id": "new-run"}

    report = compare_outcomes(before, after, include_volatile=True)

    assert report["summary"]["total_changes"] == 1
    assert report["changes"][0]["path"] == "$.run_id"


def test_numeric_tolerance_suppresses_insignificant_float_noise():
    before = {"test_results": {"metric": {"distance": 0.123456789}}}
    after = {"test_results": {"metric": {"distance": 0.1234567891}}}

    report = compare_outcomes(before, after, abs_tol=1e-9, rel_tol=0.0)

    assert report["summary"]["total_changes"] == 0


def test_status_and_ratio_changes_are_high_impact_and_keep_numeric_delta():
    before = {
        "test_results": {
            "metric": {"status": "pass", "valid_ratio": 1.0}
        }
    }
    after = {
        "test_results": {
            "metric": {"status": "warn", "valid_ratio": 0.75}
        }
    }

    report = compare_outcomes(before, after)
    changes = {item["path"]: item for item in report["changes"]}

    assert report["summary"]["high_impact"] == 2
    assert changes["$.test_results.metric.status"]["impact"] == "high"
    assert changes["$.test_results.metric.valid_ratio"]["impact"] == "high"
    assert changes["$.test_results.metric.valid_ratio"]["delta"] == -0.25


def test_metric_result_reordering_does_not_create_false_changes():
    before = {
        "metric_results": [
            {"metric_id": "a", "status": "pass", "value": 1},
            {"metric_id": "b", "status": "warn", "value": 2},
        ]
    }
    after = {
        "metric_results": [
            {"metric_id": "b", "status": "warn", "value": 2},
            {"metric_id": "a", "status": "pass", "value": 1},
        ]
    }

    report = compare_outcomes(before, after)

    assert report["summary"]["total_changes"] == 0


def test_new_decision_policy_metadata_is_visible_as_high_impact():
    before = {
        "test_results": {
            "metric": {"status": "pass", "consistency_ratio": 1.0}
        }
    }
    after = {
        "test_results": {
            "metric": {
                "status": "pass",
                "consistency_ratio": 1.0,
                "decision_rule": {
                    "pass_threshold": 0.99,
                    "warn_threshold": 0.95,
                    "provenance": "framework-default",
                },
            }
        }
    }

    report = compare_outcomes(before, after)

    assert report["summary"]["added"] == 3
    assert report["summary"]["high_impact"] == 3
    assert all(
        change["impact"] == "high"
        for change in report["changes"]
        if "decision_rule" in change["path"]
    )


def test_render_markdown_includes_summary_and_difference_table():
    report = compare_outcomes(
        {"status": "success", "test_results": {"m": {"score": 1.0}}},
        {"status": "success", "test_results": {"m": {"score": 0.5}}},
    )

    markdown = render_markdown(report)

    assert "# CBR-Tests outcome comparison" in markdown
    assert "High-impact metric/status differences" in markdown
    assert "$.test_results.m.score" in markdown
    assert "-0.5" in markdown
