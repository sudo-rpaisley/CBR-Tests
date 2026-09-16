from pathlib import Path

from run_batch import _outcome_path_for_attempt
from run_campaign import _matrix_output_dir


def test_batch_results_are_grouped_by_run_candidate_and_reference(tmp_path):
    path = _outcome_path_for_attempt(
        output_dir=tmp_path,
        index=1,
        dataset_path=Path("/datasets/Amazon5G.pcapng"),
        reference_path=Path("/datasets/Amazon5G2.pcapng"),
        timestamp="2026-09-16_16-30-00",
        attempt=1,
    )

    assert path == (
        tmp_path
        / "runs"
        / "2026-09-16_16-30-00"
        / "results"
        / "amazon5g"
        / "vs_amazon5g2"
        / "outcome.json"
    )
    assert path.parent.is_dir()


def test_retry_stays_with_original_comparison(tmp_path):
    path = _outcome_path_for_attempt(
        output_dir=tmp_path,
        index=1,
        dataset_path=Path("/datasets/Amazon5G.pcapng"),
        reference_path=Path("/datasets/Amazon5G2.pcapng"),
        timestamp="2026-09-16_16-30-00",
        attempt=3,
    )

    assert path.name == "retry03.json"
    assert path.parent.name == "vs_amazon5g2"


def test_non_reference_job_gets_standalone_folder(tmp_path):
    path = _outcome_path_for_attempt(
        output_dir=tmp_path,
        index=7,
        dataset_path=Path("/datasets/Bucket_1.pcapng"),
        reference_path=None,
        timestamp="2026-09-16_16-30-00",
        attempt=1,
    )

    assert path.parent.name == "standalone"
    assert path.parent.parent.name == "bucket_1"


def test_campaign_groups_matrices_under_matrices_directory(tmp_path):
    path = _matrix_output_dir(tmp_path, 2, {"batch_id": "Amazon comparison"})

    assert path == tmp_path / "matrices" / "002_amazon-comparison"
