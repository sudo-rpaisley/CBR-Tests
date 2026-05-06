from runner.progress import render_overall_progress_line, render_metric_activity_bar, colorize_status


def test_overall_progress_not_100_before_completion():
    line = render_overall_progress_line(current=8, total=9, run_elapsed=100, in_metric_elapsed=10)
    assert '100%' not in line


def test_overall_progress_100_at_completion():
    line = render_overall_progress_line(current=9, total=9, run_elapsed=120, in_metric_elapsed=20)
    assert '100%' in line


def test_activity_bar_has_expected_width():
    bar = render_metric_activity_bar(elapsed=5, expected_seconds=20, width=12)
    assert len(bar) == 12
    assert set(bar).issubset({'#', '-'})


def test_colorize_status_no_color_in_non_tty_context():
    assert colorize_status("success") == "success"
