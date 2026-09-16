from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    file = Path(path)
    text = file.read_text(encoding="utf-8")
    if old not in text:
        raise SystemExit(f"Expected text not found in {path}: {old[:120]!r}")
    file.write_text(text.replace(old, new, 1), encoding="utf-8")


replace_once(
    "run_batch.py",
    '''def _outcome_path_for_attempt(\n    *,\n    output_dir: Path,\n    index: int,\n    dataset_path: Path,\n    reference_path: Path | None,\n    timestamp: str,\n    attempt: int,\n) -> Path:\n    dataset_slug = _slug(dataset_path.stem)\n    reference_slug = f"_vs_{_slug(reference_path.stem)}" if reference_path is not None else ""\n    retry_suffix = "" if attempt <= 1 else f"_retry{attempt:02d}"\n    return output_dir / (\n        f"outcome_{index:02d}_{dataset_slug}{reference_slug}_{timestamp}{retry_suffix}.json"\n    )\n''',
    '''def _outcome_path_for_attempt(\n    *,\n    output_dir: Path,\n    index: int,\n    dataset_path: Path,\n    reference_path: Path | None,\n    timestamp: str,\n    attempt: int,\n) -> Path:\n    """Return the organised outcome path for one batch job attempt.\n\n    Outcomes are grouped by run, candidate dataset and reference dataset so\n    large all-v-all matrices do not create one flat directory containing\n    dozens or hundreds of JSON files.  ``index`` is retained in the public\n    helper signature for compatibility with older callers/checkpoints.\n    """\n\n    del index\n    run_dir = output_dir / "runs" / timestamp\n    candidate_dir = run_dir / "results" / _slug(dataset_path.stem)\n    if reference_path is None:\n        comparison_dir = candidate_dir / "standalone"\n    else:\n        comparison_dir = candidate_dir / f"vs_{_slug(reference_path.stem)}"\n    comparison_dir.mkdir(parents=True, exist_ok=True)\n    filename = "outcome.json" if attempt <= 1 else f"retry_{attempt:02d}.json"\n    return comparison_dir / filename\n''',
)

replace_once(
    "run_batch.py",
    '''    results: list[dict] = []\n    interrupted = False\n\n    print("=" * 88)\n''',
    '''    run_dir = output_dir / "runs" / timestamp\n    run_dir.mkdir(parents=True, exist_ok=True)\n    state["run_directory"] = str(run_dir.resolve())\n    write_batch_state(state_path, state)\n\n    results: list[dict] = []\n    interrupted = False\n\n    print("=" * 88)\n''',
)

replace_once(
    "run_batch.py",
    '''    print(f"Outputs: {output_dir}")\n    print(f"Checkpoint: {state_path}")\n''',
    '''    print(f"Batch root: {output_dir}")\n    print(f"Run outputs: {run_dir}")\n    print(f"Checkpoint: {state_path}")\n''',
)

replace_once(
    "run_batch.py",
    '''        comparison_reports = write_comparison_reports(\n            output_dir=output_dir,\n            timestamp=timestamp,\n''',
    '''        comparison_reports = write_comparison_reports(\n            output_dir=run_dir / "reports",\n            timestamp=timestamp,\n''',
)

replace_once(
    "run_batch.py",
    '''        "batch_state": str(state_path),\n        "started_at": batch_started_at.isoformat(),\n''',
    '''        "batch_state": str(state_path),\n        "run_directory": str(run_dir),\n        "started_at": batch_started_at.isoformat(),\n''',
)

replace_once(
    "run_batch.py",
    '''    summary_path = output_dir / f"batch_summary_{timestamp}.json"\n''',
    '''    summary_path = run_dir / "summary.json"\n''',
)

replace_once(
    "runner/batch_reports.py",
    '''    overview_path = output_dir / f"comparison_overview_{timestamp}.csv"\n    long_path = output_dir / f"comparison_long_{timestamp}.csv"\n''',
    '''    overview_path = output_dir / "overview.csv"\n    long_path = output_dir / "long.csv"\n''',
)
replace_once(
    "runner/batch_reports.py",
    '''    matrices_dir = output_dir / f"comparison_matrices_{timestamp}"\n''',
    '''    matrices_dir = output_dir / "matrices"\n''',
)
replace_once(
    "runner/batch_reports.py",
    '''    markdown_path = output_dir / f"comparison_report_{timestamp}.md"\n''',
    '''    markdown_path = output_dir / "report.md"\n''',
)

replace_once(
    "run_campaign.py",
    '''def _matrix_output_dir(campaign_output_dir: Path, index: int, matrix: dict[str, Any]) -> Path:\n    return campaign_output_dir / f"{index:03d}_{slug(str(matrix['batch_id']))}"\n''',
    '''def _matrix_output_dir(campaign_output_dir: Path, index: int, matrix: dict[str, Any]) -> Path:\n    """Return the organised directory for a matrix within a campaign."""\n\n    return campaign_output_dir / "matrices" / f"{index:03d}_{slug(str(matrix['batch_id']))}"\n''',
)

replace_once(
    "run_campaign.py",
    '''        batch_path = resolve_repo_path(repo_root, str(matrix["batch_path"]))\n        matrix_output = _matrix_output_dir(output_dir, index, matrix)\n        matrix_output.mkdir(parents=True, exist_ok=True)\n''',
    '''        batch_path = resolve_repo_path(repo_root, str(matrix["batch_path"]))\n        recorded_output = None\n        for checkpoint_record in (prior, current):\n            if (\n                isinstance(checkpoint_record, dict)\n                and str(checkpoint_record.get("queue_id") or "") == queue_id\n                and checkpoint_record.get("output_directory")\n            ):\n                recorded_output = Path(str(checkpoint_record["output_directory"])).expanduser().resolve()\n                break\n        matrix_output = recorded_output or _matrix_output_dir(output_dir, index, matrix)\n        matrix_output.mkdir(parents=True, exist_ok=True)\n''',
)

# Add regression coverage for the new layout.
Path("tests/test_result_layout.py").write_text(
    '''from pathlib import Path\n\nfrom run_batch import _outcome_path_for_attempt\nfrom run_campaign import _matrix_output_dir\n\n\ndef test_batch_results_are_grouped_by_run_candidate_and_reference(tmp_path):\n    path = _outcome_path_for_attempt(\n        output_dir=tmp_path,\n        index=1,\n        dataset_path=Path("/datasets/Amazon5G.pcapng"),\n        reference_path=Path("/datasets/Amazon5G2.pcapng"),\n        timestamp="2026-09-16_16-30-00",\n        attempt=1,\n    )\n\n    assert path == (\n        tmp_path\n        / "runs"\n        / "2026-09-16_16-30-00"\n        / "results"\n        / "amazon5g"\n        / "vs_amazon5g2"\n        / "outcome.json"\n    )\n    assert path.parent.is_dir()\n\n\ndef test_retry_stays_with_original_comparison(tmp_path):\n    path = _outcome_path_for_attempt(\n        output_dir=tmp_path,\n        index=1,\n        dataset_path=Path("/datasets/Amazon5G.pcapng"),\n        reference_path=Path("/datasets/Amazon5G2.pcapng"),\n        timestamp="2026-09-16_16-30-00",\n        attempt=3,\n    )\n\n    assert path.name == "retry_03.json"\n    assert path.parent.name == "vs_amazon5g2"\n\n\ndef test_non_reference_job_gets_standalone_folder(tmp_path):\n    path = _outcome_path_for_attempt(\n        output_dir=tmp_path,\n        index=7,\n        dataset_path=Path("/datasets/Bucket_1.pcapng"),\n        reference_path=None,\n        timestamp="2026-09-16_16-30-00",\n        attempt=1,\n    )\n\n    assert path.parent.name == "standalone"\n    assert path.parent.parent.name == "bucket_1"\n\n\ndef test_campaign_groups_matrices_under_matrices_directory(tmp_path):\n    path = _matrix_output_dir(tmp_path, 2, {"batch_id": "Amazon comparison"})\n\n    assert path == tmp_path / "matrices" / "002_amazon-comparison"\n''',
    encoding="utf-8",
)

batch_docs = Path("docs/batch_execution.md")
text = batch_docs.read_text(encoding="utf-8")
if "## Organised result layout" not in text:
    text += '''\n\n## Organised result layout\n\nNew batch runs keep checkpoint state at the batch root, but place run artefacts under a timestamped `runs/` directory. Within a run, authoritative JSON outcomes are grouped by candidate dataset and then by reference dataset. Supplementary CSV/Markdown reports live separately under `reports/`.\n\n```text\noutcomes/<batch-id>/\n├── batch_state.json\n└── runs/<timestamp>/\n    ├── summary.json\n    ├── reports/\n    │   ├── overview.csv\n    │   ├── long.csv\n    │   ├── report.md\n    │   └── matrices/\n    └── results/\n        ├── <candidate>/\n        │   ├── vs_<reference>/\n        │   │   ├── outcome.json\n        │   │   └── retry_02.json\n        │   └── ...\n        └── ...\n```\n\nExisting flat checkpoints are not moved. A resumed historical batch continues to honour the exact output paths already recorded in its checkpoint, while newly executed jobs use the organised layout.\n'''
    batch_docs.write_text(text, encoding="utf-8")

campaign_docs = Path("docs/comparison_campaigns.md")
text = campaign_docs.read_text(encoding="utf-8")
if "## Campaign result layout" not in text:
    text += '''\n\n## Campaign result layout\n\nCampaign outputs keep campaign-wide state and summary files at the root and place independent matrices under `matrices/`. Each matrix then uses the normal organised batch layout.\n\n```text\noutcomes/<campaign-id>/\n├── campaign_state.json\n├── campaign_summary.json\n└── matrices/\n    ├── 001_<batch-id>/\n    │   ├── batch_state.json\n    │   └── runs/<timestamp>/...\n    ├── 002_<batch-id>/\n    │   └── ...\n    └── ...\n```\n\nWhen resuming an older campaign checkpoint, any matrix output directory already recorded in the checkpoint remains authoritative. This preserves compatibility with campaigns started before the hierarchical layout was introduced.\n'''
    campaign_docs.write_text(text, encoding="utf-8")
