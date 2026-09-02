from runner.batch_progress import (
    batch_progress_lines_from_environment,
    build_batch_child_environment,
    build_batch_progress_lines,
    render_batch_progress,
)


def test_render_batch_progress_reports_completed_fraction():
    line = render_batch_progress(6, 24, width=20)

    assert "25%" in line
    assert "6/24 complete" in line
    assert line.startswith("[#####")


def test_build_batch_progress_lines_show_current_job_and_status_counts():
    lines = build_batch_progress_lines(
        batch_name="Reference experiment",
        batch_id="reference-experiment",
        current_job=7,
        total_jobs=24,
        completed_jobs=6,
        failed_jobs=1,
        candidate_name="synthetic-b.csv",
        reference_name="real-a.csv",
    )

    assert lines[0] == "Batch: Reference experiment (reference-experiment)"
    assert "25%" in lines[1]
    assert "running job 7/24" in lines[1]
    assert "5 successful" in lines[2]
    assert "1 needing attention" in lines[2]
    assert "17 waiting after current" in lines[2]
    assert lines[3] == "Candidate: synthetic-b.csv | Reference: real-a.csv"


def test_child_environment_round_trips_into_dashboard_lines():
    environment = build_batch_child_environment(
        {"EXISTING": "kept"},
        batch_name="Experiment A",
        batch_id="experiment-a",
        current_job=3,
        total_jobs=8,
        completed_jobs=2,
        failed_jobs=0,
        candidate_name="candidate.csv",
        reference_name="reference.csv",
    )

    assert environment["EXISTING"] == "kept"
    assert environment["CBR_BATCH_JOB_INDEX"] == "3"
    assert environment["CBR_BATCH_JOB_TOTAL"] == "8"

    lines = batch_progress_lines_from_environment(environment)
    assert "running job 3/8" in lines[1]
    assert "25%" in lines[1]
    assert lines[3] == "Candidate: candidate.csv | Reference: reference.csv"


def test_dashboard_batch_context_is_absent_for_normal_single_runs():
    assert batch_progress_lines_from_environment({}) == []
